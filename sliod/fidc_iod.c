/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2010-2015, Pittsburgh Supercomputing Center (PSC).
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
 *
 * Pittsburgh Supercomputing Center	phone: 412.268.4960  fax: 412.268.5832
 * 300 S. Craig Street			e-mail: remarks@psc.edu
 * Pittsburgh, PA 15213			web: http://www.psc.edu/
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

#include <fcntl.h>
#include <stddef.h>

#include "pfl/ctlsvr.h"
#include "pfl/log.h"
#include "pfl/rlimit.h"
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

int
bcr_update_inodeinfo(struct bcrcupd *bcr)
{
	struct fidc_membh *f;
	struct stat stb;
	struct bmap *b;

	b = bcr_2_bmap(bcr);
	f = b->bcm_fcmh;

	psc_assert(bcr->bcr_crcup.fg.fg_fid == f->fcmh_fg.fg_fid);

	if ((f->fcmh_flags & FCMH_IOD_BACKFILE) == 0)
		return (EBADF);

	if (fstat(fcmh_2_fd(f), &stb) == -1)
		return (errno);
	bcr->bcr_crcup.fsize = stb.st_size;
	bcr->bcr_crcup.nblks = stb.st_blocks;
	bcr->bcr_crcup.utimgen = f->fcmh_sstb.sst_utimgen;

	return (0);
}

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
	int lvl = PLL_DIAG, incr, rc = 0;
	char fidfn[PATH_MAX];

	incr = psc_rlim_adj(RLIMIT_NOFILE, 1);
	sli_fg_makepath(&f->fcmh_fg, fidfn);
	fcmh_2_fd(f) = open(fidfn, O_CREAT|O_RDWR, 0600);
	if (fcmh_2_fd(f) == -1) {
		rc = errno;
		if (incr)
			psc_rlim_adj(RLIMIT_NOFILE, -1);
		OPSTAT_INCR("open-fail");
		lvl = PLL_WARN;
	} else
		OPSTAT_INCR("open-succeed");
	psclog(lvl, "opened backing file path=%s fd=%d rc=%d",
	    strstr(fidfn, SL_RPATH_FIDNS_DIR), fcmh_2_fd(f), rc);
	return (rc);
}

int
sli_fcmh_getattr(struct fidc_membh *f)
{
	struct stat stb;

	if (fstat(fcmh_2_fd(f), &stb) == -1)
		return (-errno);

	FCMH_LOCK(f);
	sl_externalize_stat(&stb, &f->fcmh_sstb);
	// XXX get ptruncgen and gen
	f->fcmh_flags |= FCMH_HAVE_ATTRS;
	FCMH_ULOCK(f);
	return (0);
}

int
sli_fcmh_lookup_fid(struct slashrpc_cservice *csvc,
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
sli_fcmh_reopen(struct fidc_membh *f, const struct sl_fidgen *fg)
{
	int rc = 0;

	FCMH_LOCK_ENSURE(f);
	psc_assert(fg->fg_fid == fcmh_2_fid(f));

	OPSTAT_INCR("reopen");

	if (fg->fg_gen == FGEN_ANY) {
		OPSTAT_INCR("generation-bogus");
		return (EBADF);
	}
	if (fg->fg_gen < fcmh_2_gen(f)) {
		OPSTAT_INCR("generation-stale");
		return (ESTALE);
	}

	/*
	 * If our generation number is still unknown try to set it here.
	 */
	if (fcmh_2_gen(f) == FGEN_ANY && fg->fg_gen != FGEN_ANY)
		fcmh_2_gen(f) = fg->fg_gen;

	if (fg->fg_gen > fcmh_2_gen(f)) {
		struct sl_fidgen oldfg;
		char fidfn[PATH_MAX];

		DEBUG_FCMH(PLL_DIAG, f, "reopening new backing file");
		OPSTAT_INCR("slvr-remove-reopen");
		slvr_remove_all(f);

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
			psc_rlim_adj(RLIMIT_NOFILE, -1);
			f->fcmh_flags &= ~FCMH_IOD_BACKFILE;
		}

		oldfg.fg_fid = fcmh_2_fid(f);
		oldfg.fg_gen = fcmh_2_gen(f);

		fcmh_2_gen(f) = fg->fg_gen;

		rc = sli_open_backing_file(f);
		/* Notify upper layers that open() has failed. */
		if (rc)
			f->fcmh_flags |= FCMH_CTOR_FAILED;
		else
			f->fcmh_flags |= FCMH_IOD_BACKFILE;

		/* Do some upfront garbage collection. */
		sli_fg_makepath(&oldfg, fidfn);

		errno = 0;
		unlink(fidfn);
		DEBUG_FCMH(PLL_INFO, f, "upfront unlink(), errno=%d",
		    errno);

	} else if (!(f->fcmh_flags & FCMH_IOD_BACKFILE)) {

		rc = sli_open_backing_file(f);
		if (!rc) {
			f->fcmh_flags &= ~FCMH_CTOR_FAILED;
			f->fcmh_flags |= FCMH_IOD_BACKFILE;
		}

	}
	return (rc);
}

int
sli_fcmh_ctor(struct fidc_membh *f, __unusedx int flags)
{
	int rc = 0;

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
		rc = sli_fcmh_getattr(f);
		if (rc)
			DEBUG_FCMH(PLL_WARN, f, "error during "
			    "getattr backing file rc=%d", rc);
	}
	if (!rc)
		f->fcmh_flags |= FCMH_IOD_BACKFILE;
	return (rc);
}

void
sli_fcmh_dtor(__unusedx struct fidc_membh *f)
{
	if (f->fcmh_flags & FCMH_IOD_BACKFILE) {
		if (close(fcmh_2_fd(f)) == -1) {
			OPSTAT_INCR("close-fail");
			DEBUG_FCMH(PLL_ERROR, f,
			    "dtor/close errno=%d", errno);
		} else
			OPSTAT_INCR("close-succeed");
		psc_rlim_adj(RLIMIT_NOFILE, -1);
		f->fcmh_flags &= ~FCMH_IOD_BACKFILE;
	}
}

struct sl_fcmh_ops sl_fcmh_ops = {
	sli_fcmh_ctor,		/* sfop_ctor */
	sli_fcmh_dtor,		/* sfop_dtor */
	NULL,			/* sfop_getattr */
	NULL,			/* sfop_postsetattr */
	sli_fcmh_reopen		/* sfop_modify */
};
