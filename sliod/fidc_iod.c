/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2014, Pittsburgh Supercomputing Center (PSC).
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

#include "pfl/rlimit.h"
#include "pfl/str.h"
#include "pfl/ctlsvr.h"
#include "pfl/log.h"

#include "fidc_iod.h"
#include "fidcache.h"
#include "mkfn.h"
#include "pathnames.h"
#include "slashrpc.h"
#include "slconfig.h"
#include "slconn.h"
#include "sltypes.h"
#include "slutil.h"

int
iod_inode_getinfo(struct slash_fidgen *fg, uint64_t *size,
    uint64_t *nblks, uint32_t *utimgen)
{
	struct fidc_membh *f;
	struct stat stb;
	int rc;

	rc = fidc_lookup_fg(fg, &f);
	if (rc)
		return (rc);

	FCMH_LOCK(f);
	if (fstat(fcmh_2_fd(f), &stb) == -1) {
		rc = -errno;
		fcmh_op_done(f);
		return (rc);
	}

	*size = stb.st_size;
	*nblks = stb.st_blocks;
	*utimgen = f->fcmh_sstb.sst_utimgen;

	fcmh_op_done(f);
	return (0);
}

struct fidc_membh *
iod_inode_lookup(const struct slash_fidgen *fg)
{
	struct fidc_membh *f;
	int rc;

	rc = fidc_lookup(fg, FIDC_LOOKUP_CREATE, NULL, 0, &f);
	psc_assert(rc == 0);
	return (f);
}

/**
 * sli_fid_makepath - Build the pathname in the FID object root that
 *	corresponds to a FID, allowing easily lookup of file metadata
 *	via FIDs.
 */
void
sli_fg_makepath(const struct slash_fidgen *fg, char *fid_path)
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
	    globalConfig.gconf_fsroot, SL_RPATH_META_DIR,
	    globalConfig.gconf_fsuuid, SL_RPATH_FIDNS_DIR,
	    str, fg->fg_fid, fg->fg_gen);

	psclog_debug("fid="SLPRI_FID" fidpath=%s", fg->fg_fid, fid_path);
}

static int
sli_open_backing_file(struct fidc_membh *f)
{
	int lvl = PLL_INFO, flags, incr, rc = 0;
	char fidfn[PATH_MAX];

	flags = O_CREAT | O_RDWR;
	if (f->fcmh_flags & FCMH_CAC_RLSBMAP)
		flags &= ~O_CREAT;
	incr = psc_rlim_adj(RLIMIT_NOFILE, 1);
	sli_fg_makepath(&f->fcmh_fg, fidfn);
	fcmh_2_fd(f) = open(fidfn, flags, 0600);
	if (fcmh_2_fd(f) == -1) {
		rc = errno;
		if (incr)
			psc_rlim_adj(RLIMIT_NOFILE, -1);
		OPSTAT_INCR(SLI_OPST_OPEN_FAIL);
		if (rc != ENOENT ||
		    (f->fcmh_flags & FCMH_CAC_RLSBMAP) == 0)
			lvl = PLL_WARN;
	} else
		OPSTAT_INCR(SLI_OPST_OPEN_SUCCEED);
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
    const struct slash_fidgen *pfg, const char *cpn,
    struct slash_fidgen *cfg, int *isdir)
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

/**
 * sli_fcmh_reopen - If the generation number changes, we assume a full
 *	truncation has happened.  We need to open a new backing file and
 *	attach it to the fcmh.
 */
int
sli_fcmh_reopen(struct fidc_membh *f, const struct slash_fidgen *fg)
{
	int rc = 0;

	FCMH_LOCK_ENSURE(f);
	psc_assert(fg->fg_fid == fcmh_2_fid(f));

	OPSTAT_INCR(SLI_OPST_REOPEN);

	/*
	 * If our generation number is still unknown try to set it here.
	 */
	if (fcmh_2_gen(f) == FGEN_ANY && fg->fg_gen != FGEN_ANY)
		fcmh_2_gen(f) = fg->fg_gen;

	if (fg->fg_gen == FGEN_ANY) {
		/*
		 * No-op.  The caller's operation is generation number
		 * agnostic (such as rlsbmap).
		 */
	} else if (fg->fg_gen > fcmh_2_gen(f)) {
		struct slash_fidgen oldfg;
		char fidfn[PATH_MAX];

		DEBUG_FCMH(PLL_INFO, f, "reopening new backing file");

		/*
		 * Need to reopen the backing file and possibly remove
		 * the old one.
		 */
		if (!(f->fcmh_flags & FCMH_NO_BACKFILE)) {
			if (close(fcmh_2_fd(f)) == -1) {
				OPSTAT_INCR(SLI_OPST_CLOSE_FAIL);
				DEBUG_FCMH(PLL_ERROR, f,
				    "reopen/close errno=%d", errno);
			} else {
				OPSTAT_INCR(SLI_OPST_CLOSE_SUCCEED);
				psc_rlim_adj(RLIMIT_NOFILE, -1);
			}
		}

		oldfg.fg_fid = fcmh_2_fid(f);
		oldfg.fg_gen = fcmh_2_gen(f);

		fcmh_2_gen(f) = fg->fg_gen;

		rc = sli_open_backing_file(f);
		/* Notify upper layers that open() has failed. */
		if (rc)
			f->fcmh_flags |= FCMH_CTOR_FAILED;

		/* Do some upfront garbage collection. */
		sli_fg_makepath(&oldfg, fidfn);

		errno = 0;
		unlink(fidfn);
		DEBUG_FCMH(PLL_INFO, f, "upfront unlink(), errno=%d",
		    errno);

	} else if (f->fcmh_flags & FCMH_NO_BACKFILE) {

		rc = sli_open_backing_file(f);
		if (!rc)
			f->fcmh_flags &=
			    ~(FCMH_CTOR_FAILED | FCMH_NO_BACKFILE);

	} else if (fg->fg_gen < fcmh_2_gen(f)) {
		/*
		 * For now, requests from old generations (i.e. old
		 * bdbufs) will be honored.  Clients which issue full
		 * truncates will release their bmaps, and associated
		 * cache pages, prior to issuing a truncate request to
		 * the MDS.
		 */
		DEBUG_FCMH(PLL_INFO, f, "request from old gen "
		    "(%"SLPRI_FGEN")", fg->fg_gen);
	}
	return (rc);
}

int
sli_fcmh_ctor(struct fidc_membh *f, __unusedx int flags)
{
	int rc = 0;

	if (f->fcmh_fg.fg_gen == FGEN_ANY) {
		f->fcmh_flags |= FCMH_NO_BACKFILE;
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
	if (rc == ENOENT && (f->fcmh_flags & FCMH_CAC_RLSBMAP)) {
		f->fcmh_flags |= FCMH_NO_BACKFILE;
		rc = 0;
	}
	return (rc);
}

void
sli_fcmh_dtor(__unusedx struct fidc_membh *f)
{
	if (!(f->fcmh_flags & FCMH_NO_BACKFILE)) {
		if (close(fcmh_2_fd(f)) == -1) {
			OPSTAT_INCR(SLI_OPST_CLOSE_FAIL);
			DEBUG_FCMH(PLL_ERROR, f,
			    "dtor/close errno=%d", errno);
		} else
			OPSTAT_INCR(SLI_OPST_CLOSE_SUCCEED);
		psc_rlim_adj(RLIMIT_NOFILE, -1);
	}
}

struct sl_fcmh_ops sl_fcmh_ops = {
/* sfop_ctor */		sli_fcmh_ctor,
/* sfop_dtor */		sli_fcmh_dtor,
/* sfop_getattr */	NULL,
/* sfop_postsetattr */	NULL,
/* sfop_modify */	sli_fcmh_reopen
};
