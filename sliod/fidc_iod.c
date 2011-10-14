/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2011, Pittsburgh Supercomputing Center (PSC).
 *
 * Permission to use, copy, and modify this software and its documentation
 * without fee for personal use or non-commercial use within your organization
 * is hereby granted, provided that the above copyright notice is preserved in
 * all copies and that the copyright and this permission notice appear in
 * supporting documentation.  Permission to redistribute this software to other
 * organizations or individuals is not permitted without the written permission
 * of the Pittsburgh Supercomputing Center.  PSC makes no representations about
 * the suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

#include <fcntl.h>
#include <stddef.h>

#include "psc_util/log.h"
#include "psc_util/rlimit.h"

#include "fidc_iod.h"
#include "fidcache.h"
#include "mkfn.h"
#include "pathnames.h"
#include "slconfig.h"
#include "sltypes.h"
#include "slutil.h"

/**
 * sli_fid_makepath - Build the pathname in the FID object root that
 *	corresponds to a FID, allowing easily lookup of file metadata
 *	via FIDs.
 */
void
sli_fg_makepath(const struct slash_fidgen *fg, char *fid_path)
{
	char str[(FID_PATH_DEPTH * 2) + 1];
	char *p;
	int i;

	for (p = str, i = 0; i < FID_PATH_DEPTH; i++) {
		*p = (fg->fg_fid & UINT64_C(0x0000000000f00000)) >>
		    (BPHXC * (5 - i));
		*p += *p < 10 ? '0' : 'a' - 10;
		p++;
		*p++ = '/';
	}
	*p = '\0';

	xmkfn(fid_path, "%s/%s/%"PRIx64"/%s/%s/%016"PRIx64"_%"PRIx64,
	    globalConfig.gconf_fsroot, SL_RPATH_META_DIR,
	    globalConfig.gconf_fsuuid, SL_RPATH_FIDNS_DIR,
	    str, fg->fg_fid, fg->fg_gen);

	psclog_dbg("fid="SLPRI_FID" fidpath=%s", fg->fg_fid, fid_path);
}

static int
sli_open_backing_file(struct fidc_membh *fcmh)
{
	char fidfn[PATH_MAX];
	int incr, rc = 0;

	incr = psc_rlim_adj(RLIMIT_NOFILE, 1);
	sli_fg_makepath(&fcmh->fcmh_fg, fidfn);
	DEBUG_FCMH(PLL_INFO, fcmh, "before opening new backing file");
	fcmh_2_fd(fcmh) = open(fidfn, O_CREAT | O_RDWR, 0600);
	if (fcmh_2_fd(fcmh) == -1) {
		rc = errno;
		if (incr)
			psc_rlim_adj(RLIMIT_NOFILE, -1);
	}
	psclog_info("path=%s fd=%d rc=%d", fidfn, fcmh_2_fd(fcmh), rc);
	return (rc);
}

int
sli_fcmh_getattr(struct fidc_membh *fcmh)
{
	struct stat stb;

	if (fstat(fcmh_2_fd(fcmh), &stb) == -1)
		return (-errno);

	FCMH_LOCK(fcmh);
	sl_externalize_stat(&stb, &fcmh->fcmh_sstb);
	// XXX get ptruncgen and gen
	fcmh->fcmh_flags |= FCMH_HAVE_ATTRS;
	FCMH_ULOCK(fcmh);
	return (0);
}

/**
 * sli_fcmh_reopen(): if the generation number changes, we assume a full
 *	truncation has happened.  We need to open a new backing file and
 *	attach it to the fcmh.
 */
int
sli_fcmh_reopen(struct fidc_membh *fcmh, const struct slash_fidgen *fg)
{
	int rc = 0;

	FCMH_LOCK_ENSURE(fcmh);
	psc_assert(fg->fg_fid == fcmh_2_fid(fcmh));

	/* If our generation number is still unknown try to set it here.
	 */
	if (fcmh_2_gen(fcmh) == FGEN_ANY && fg->fg_gen != FGEN_ANY)
		fcmh_2_gen(fcmh) = fg->fg_gen;

	if (fg->fg_gen == FGEN_ANY) {
		/* Noop.  The caller's operation is generation
		 *    number agnostic (such as rlsbmap).
		 */
	} else if (fg->fg_gen > fcmh_2_gen(fcmh)) {
		struct slash_fidgen oldfg;
		char fidfn[PATH_MAX];

		DEBUG_FCMH(PLL_INFO, fcmh, "reopening new backing file");
		/* Need to reopen the backing file and possibly
		 *   remove the old one.
		 */
		if (close(fcmh_2_fd(fcmh)) == -1)
			DEBUG_FCMH(PLL_ERROR, fcmh, "close() failed errno=%d",
			   errno);
		else
			psc_rlim_adj(RLIMIT_NOFILE, -1);

		oldfg.fg_fid = fcmh_2_fid(fcmh);
		oldfg.fg_gen = fcmh_2_gen(fcmh);

		fcmh_2_gen(fcmh) = fg->fg_gen;

		rc = sli_open_backing_file(fcmh);
		/* Notify upper layers that open() has failed
		 */
		if (rc)
			fcmh->fcmh_flags |= FCMH_CTOR_FAILED;

		/* Do some upfront garbage collection.
		 */
		sli_fg_makepath(&oldfg, fidfn);
		if (unlink(fidfn) == -1)
			DEBUG_FCMH(PLL_INFO, fcmh, "unlink() failed errno=%d",
				   errno);

	} else if (fg->fg_gen == fcmh_2_gen(fcmh) &&
		   (fcmh->fcmh_flags & FCMH_CTOR_DELAYED)) {

		rc = sli_open_backing_file(fcmh);
		if (!rc)
			fcmh->fcmh_flags &=
				~(FCMH_CTOR_FAILED | FCMH_CTOR_DELAYED);

		DEBUG_FCMH(PLL_NOTIFY, fcmh, "open FCMH_CTOR_DELAYED (rc=%d)",
		   rc);

	} else if (fg->fg_gen < fcmh_2_gen(fcmh)) {
		/*
		 * For now, requests from old generations (i.e. old
		 * bdbufs) will be honored.  Clients which issue full
		 * truncates will release their bmaps, and associated
		 * cache pages, prior to issuing a truncate request to
		 * the MDS.
		 */
		DEBUG_FCMH(PLL_WARN, fcmh, "request from old gen "
		    "(%"SLPRI_FGEN")", fg->fg_gen);
	}
	return (rc);
}

int
sli_fcmh_ctor(struct fidc_membh *fcmh)
{
	int rc = 0;

	if (fcmh->fcmh_fg.fg_gen == FGEN_ANY) {
		fcmh->fcmh_flags |= FCMH_CTOR_DELAYED;
		DEBUG_FCMH(PLL_NOTIFY, fcmh, "refusing to open backing file "
		   "with FGEN_ANY");
		/* This is not an error, we just don't have enough info
		 * to create the backing file.
		 */
		return (0);
	}

	/* try to get a file descriptor for this backing obj */
	rc = sli_open_backing_file(fcmh);
	if (rc == 0)
		rc = sli_fcmh_getattr(fcmh);
	DEBUG_FCMH(PLL_INFO, fcmh, "after opening new backing file rc=%d", rc);

	return (rc);
}

void
sli_fcmh_dtor(__unusedx struct fidc_membh *f)
{
	if (!(f->fcmh_flags & FCMH_CTOR_DELAYED)) {
		close(fcmh_2_fd(f));
		psc_rlim_adj(RLIMIT_NOFILE, -1);
	}
}

struct sl_fcmh_ops sl_fcmh_ops = {
/* ctor */		sli_fcmh_ctor,
/* dtor */		sli_fcmh_dtor,
/* getattr */		NULL,
/* postsetattr */	NULL,
/* modify */		sli_fcmh_reopen
};
