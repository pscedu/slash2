/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
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

#include "sltypes.h"
#include "slutil.h"
#include "fidc_iod.h"
#include "fidcache.h"

static int 
sli_open_backing_file(struct fidc_membh *fcmh) 
{
	int rc = 0;
	char fidfn[PATH_MAX];

	fg_makepath(&fcmh->fcmh_fg, fidfn);
	DEBUG_FCMH(PLL_INFO, fcmh, "before opening new backing file");
	fcmh_2_fd(fcmh) = open(fidfn, O_CREAT | O_RDWR, 0600);
	if (fcmh_2_fd(fcmh) == -1)
		rc = errno;

	return (rc);
}

int
sli_fcmh_getattr(struct fidc_membh *fcmh)
{
	struct stat stb;

	if (fstat(fcmh_2_fd(fcmh), &stb))
		return (-errno);

	FCMH_LOCK(fcmh);
	sl_externalize_stat(&stb, &fcmh->fcmh_sstb);
	// XXX get ptruncgen and gen
	fcmh->fcmh_flags |= FCMH_HAVE_ATTRS;
	FCMH_ULOCK(fcmh);

	return (0);
}

int
sli_fcmh_reopen(struct fidc_membh *fcmh, const struct slash_fidgen *fg)
{
	int rc = 0, incr;

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
		if (close(fcmh_2_fd(fcmh)))
			DEBUG_FCMH(PLL_ERROR, fcmh, "close() failed errno=%d",
			   errno);

		oldfg.fg_fid = fcmh_2_fid(fcmh);
		oldfg.fg_gen = fcmh_2_gen(fcmh);

		fcmh_2_gen(fcmh) = fg->fg_gen;

		incr = psc_rlim_adj(RLIMIT_NOFILE, 1);

		rc = sli_open_backing_file(fcmh);
		/* Notify upper layers that open() has failed 
		 */
		if (rc) {
			fcmh->fcmh_flags |= FCMH_CTOR_FAILED;
			if (incr)
				psc_rlim_adj(RLIMIT_NOFILE, -1);
		}

		/* Do some upfront garbage collection.
		 */
		fg_makepath(&oldfg, fidfn);
		if (unlink(fidfn))
			DEBUG_FCMH(PLL_ERROR, fcmh, "unlink() failed errno=%d",
				   errno);

	} else if (fg->fg_gen == fcmh_2_gen(fcmh) &&
		   (fcmh->fcmh_flags & FCMH_CTOR_DELAYED)) {

		incr = psc_rlim_adj(RLIMIT_NOFILE, 1);		
		rc = sli_open_backing_file(fcmh);
		if (!rc)
			fcmh->fcmh_flags &= 
				~(FCMH_CTOR_FAILED | FCMH_CTOR_DELAYED);
		else if (rc && incr)
			psc_rlim_adj(RLIMIT_NOFILE, -1);

		DEBUG_FCMH(PLL_NOTIFY, fcmh, "open FCMH_CTOR_DELAYED (rc=%d)", 
		   rc);

	} else if (fg->fg_gen < fcmh_2_gen(fcmh)) {
		/* For now, requests from old generations (i.e. old bdbufs)
		 *   will be honored.  Clients which issue full truncates will
		 *   release their bmaps, and associated cache pages, prior to
		 *   issuing a truncate request to the MDS.
		 */
		DEBUG_FCMH(PLL_WARN, fcmh, "request from old gen "
		    "(%"SLPRI_FGEN")", fg->fg_gen);
	}
	return (rc);
}

int
sli_fcmh_ctor(struct fidc_membh *fcmh)
{
	struct fcmh_iod_info *fii;
	int incr, rc = 0;

	if (fcmh->fcmh_fg.fg_gen == FGEN_ANY) {		
		fcmh->fcmh_flags |= FCMH_CTOR_DELAYED;
		DEBUG_FCMH(PLL_WARN, fcmh, "refusing to open backing file "
		   "with FGEN_ANY");
		/* This is not an error, we just don't have enough info
		 * to create the backing file.
		 */
		return (0);
	}

	incr = psc_rlim_adj(RLIMIT_NOFILE, 1);

	/* try to get an file descriptor for this backing obj */
	fii = fcmh_2_fii(fcmh);
	memset(fii, 0, sizeof(struct fcmh_iod_info));

	rc = sli_open_backing_file(fcmh);

	/* oops, an error; if we increased the rlim, decrease it */
	if (rc && incr)
		psc_rlim_adj(RLIMIT_NOFILE, -1);
	else
		rc = sli_fcmh_getattr(fcmh);
	DEBUG_FCMH(PLL_INFO, fcmh, "after opening new backing file, rc = %d", rc);

	return (rc);
}

void
sli_fcmh_dtor(__unusedx struct fidc_membh *f)
{
	if (!(f->fcmh_flags & FCMH_CTOR_DELAYED))
		close(fcmh_2_fd(f));
	psc_rlim_adj(RLIMIT_NOFILE, -1);
}

struct sl_fcmh_ops sl_fcmh_ops = {
/* ctor */		sli_fcmh_ctor,
/* dtor */		sli_fcmh_dtor,
/* getattr */		NULL,
/* postsetattr */	NULL,
/* modify */            sli_fcmh_reopen
};
