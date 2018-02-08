/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright 2010-2016, Pittsburgh Supercomputing Center
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or (at
 * your option) any later version.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License contained in the file
 * `COPYING-GPL' at the top of this distribution or at
 * https://www.gnu.org/licenses/gpl-2.0.html for more details.
 * ---------------------------------------------------------------------
 * %END_LICENSE%
 */

#include <fcntl.h>
#include <stddef.h>

#include "pfl/ctlsvr.h"
#include "pfl/log.h"
#include "pfl/str.h"

#include "fidc_iod.h"
#include "fidcache.h"
#include "mkfn.h"
#include "pathnames.h"
#include "slashrpc.h"
#include "slconfig.h"
#include "slconn.h"
#include "sltypes.h"
#include "slutil.h"
#include "slvr.h"

/*
 * Build the pathname in the FID object root that corresponds to a FID,
 * allowing easily lookup of file metadata via FIDs.
 */
void
sli_fg_makepath(const struct sl_fidgen *fg, char *fid_path)
{
	char *p, str[(FID_PATH_DEPTH * 2) + 1];
	uint64_t shift;
	int i;

	shift = BPHXC * (FID_PATH_START + FID_PATH_DEPTH - 1);
	for (p = str, i = 0; i < FID_PATH_DEPTH; i++, shift -= BPHXC) {
		*p = (fg->fg_fid & (0xf << shift)) >> shift;
		*p += *p < 10 ? '0' : 'a' - 10;
		p++;
		*p++ = '/';
	}
	*p = '\0';

	xmkfn(fid_path, "%s/%s/%"PRIx64"/%s/%s%016"PRIx64"_%"PRIx64,
	    slcfg_local->cfg_fsroot, SL_RPATH_META_DIR,
	    globalConfig.gconf_fsuuid, SL_RPATH_FIDNS_DIR,
	    str, fg->fg_fid, fg->fg_gen);

	psclog_debug("fid="SLPRI_FID" fidpath=%s", fg->fg_fid, fid_path);
}

static int
sli_open_backing_file(struct fidc_membh *f)
{
	int lvl = PLL_DIAG, rc = 0;
	char fidfn[PATH_MAX];

	sli_fg_makepath(&f->fcmh_fg, fidfn);
	fcmh_2_fd(f) = open(fidfn, O_CREAT|O_RDWR, 0600);
	if (fcmh_2_fd(f) == -1) {
		rc = errno;
		OPSTAT_INCR("open-fail");
		lvl = PLL_WARN;
	} else
		OPSTAT_INCR("open-succeed");
	psclog(lvl, "opened backing file path=%s fd=%d rc=%d",
	    strstr(fidfn, SL_RPATH_FIDNS_DIR), fcmh_2_fd(f), rc);
	return (rc);
}

int
sli_rmi_lookup_fid(struct slrpc_cservice *csvc,
    const struct sl_fidgen *pfg, const char *cpn,
    struct sl_fidgen *cfg, int *isdir)
{
	struct pscrpc_request *rq = NULL;
	struct srm_lookup_req *mq;
	struct srm_lookup_rep *mp;
	int rc = 0;

	rc = SL_RSX_NEWREQ(csvc, SRMT_LOOKUP, rq, mq, mp);
	if (rc)
		goto out;
	mq->pfg = *pfg;
	strlcpy(mq->name, cpn, sizeof(mq->name));
	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc == 0)
		rc = abs(mp->rc);
	if (rc)
		goto out;

	*cfg = mp->attr.sst_fg;
	*isdir = S_ISDIR(mp->attr.sst_mode);

 out:
	if (rq)
		pscrpc_req_finished(rq);
	return (rc);
}

/*
 * If the generation number changes, we assume a full truncation has
 * happened.  We need to open a new backing file and attach it to the
 * fcmh.
 */
int
sli_fcmh_reopen(struct fidc_membh *f, slfgen_t fgen)
{
	int rc = 0;

	FCMH_LOCK_ENSURE(f);

	OPSTAT_INCR("reopen");

	if (fgen == FGEN_ANY) {
		OPSTAT_INCR("generation-bogus");
		return (EBADF);
	}
	if (fgen < fcmh_2_gen(f)) {
		OPSTAT_INCR("generation-stale");
		return (ESTALE);
	}

	/*
	 * If our generation number is still unknown try to set it here.
	 */
	if (fcmh_2_gen(f) == FGEN_ANY && fgen != FGEN_ANY) {
		OPSTAT_INCR("generation-fix");
		fcmh_2_gen(f) = fgen;
	}

	if (fgen > fcmh_2_gen(f)) {
		struct sl_fidgen oldfg;
		char fidfn[PATH_MAX];

		DEBUG_FCMH(PLL_DIAG, f, "reopening new backing file");
		OPSTAT_INCR("slvr-remove-reopen");
		FCMH_ULOCK(f);
		slvr_remove_all(f);

		/*
		 * It's possible the pruning of all slivers and bmaps
		 * ended up fcmh_op_done() our fcmh so ensure it is
		 * locked upon finishing.
		 */
		FCMH_LOCK(f);

		/*
		 * Need to reopen the backing file and possibly remove
		 * the old one.
		 */
		if (f->fcmh_flags & FCMH_IOD_BACKFILE) {
			if (close(fcmh_2_fd(f)) == -1) {
				OPSTAT_INCR("close-fail");
				DEBUG_FCMH(PLL_ERROR, f,
				    "reopen/close errno=%d", errno);
			} else {
				OPSTAT_INCR("close-succeed");
			}
			fcmh_2_fd(f) = -1;
			f->fcmh_flags &= ~FCMH_IOD_BACKFILE;
		}

		oldfg.fg_fid = fcmh_2_fid(f);
		oldfg.fg_gen = fcmh_2_gen(f);

		fcmh_2_gen(f) = fgen;

		rc = sli_open_backing_file(f);
		/* Notify upper layers that open() has failed. */
		if (!rc)
			f->fcmh_flags |= FCMH_IOD_BACKFILE;

		/* Do some upfront garbage collection. */
		sli_fg_makepath(&oldfg, fidfn);

		errno = 0;
		unlink(fidfn);
		DEBUG_FCMH(PLL_INFO, f, "upfront unlink(), errno=%d",
		    errno);

	} else if (!(f->fcmh_flags & FCMH_IOD_BACKFILE)) {

		rc = sli_open_backing_file(f);
		if (!rc)
			f->fcmh_flags |= FCMH_IOD_BACKFILE;
		OPSTAT_INCR("generation-same");
	}
	return (rc);
}

int
sli_fcmh_ctor(struct fidc_membh *f, __unusedx int flags)
{
	int rc;
	struct stat stb;
	struct fcmh_iod_info *fii;

	fii = fcmh_2_fii(f);
	INIT_PSC_LISTENTRY(&fii->fii_lentry);
	INIT_PSC_LISTENTRY(&fii->fii_lentry2);

	psc_assert(f->fcmh_flags & FCMH_INITING);
	if (f->fcmh_fg.fg_gen == FGEN_ANY) {
		DEBUG_FCMH(PLL_NOTICE, f, "refusing to open backing file "
		    "with FGEN_ANY");

		/*
		 * This is not an error, we just don't have enough info
		 * to create the backing file.
		 */
		return (0);
	}

	/* try to get a file descriptor for this backing obj */
	rc = sli_open_backing_file(f);
	if (rc == 0) {
		if (fstat(fcmh_2_fd(f), &stb) == -1) {
			rc = -errno;
			DEBUG_FCMH(PLL_WARN, f, "error during "
			    "getattr backing file rc=%d", rc);
			close(fcmh_2_fd(f));
		} else {
			sl_externalize_stat(&stb, &f->fcmh_sstb);
			// XXX get ptruncgen and gen
			fii->fii_nblks = stb.st_blocks;
			f->fcmh_flags |= FCMH_HAVE_ATTRS;
		}
	}
	if (!rc)
		f->fcmh_flags |= FCMH_IOD_BACKFILE;
	return (rc);
}

void
sli_fcmh_dtor(__unusedx struct fidc_membh *f)
{
	struct fcmh_iod_info *fii;

	if (f->fcmh_flags & FCMH_IOD_BACKFILE) {
		if (close(fcmh_2_fd(f)) == -1) {
			OPSTAT_INCR("close-fail");
			DEBUG_FCMH(PLL_ERROR, f,
			    "dtor/close errno=%d", errno);
		} else
			OPSTAT_INCR("close-succeed");
		f->fcmh_flags &= ~FCMH_IOD_BACKFILE;
	}
	if (f->fcmh_flags & FCMH_IOD_DIRTYFILE) {
		fii = fcmh_2_fii(f);
		lc_remove(&sli_fcmh_dirty, fii);
		f->fcmh_flags &= ~FCMH_IOD_DIRTYFILE;
	}
}

struct sl_fcmh_ops sl_fcmh_ops = {
	sli_fcmh_ctor,		/* sfop_ctor */
	sli_fcmh_dtor,		/* sfop_dtor */
	NULL,			/* sfop_getattr */
	sli_fcmh_reopen		/* sfop_reopen */
};
