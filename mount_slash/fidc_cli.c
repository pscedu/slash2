/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2007-2018, Pittsburgh Supercomputing Center
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

#define PSC_SUBSYS SLSS_FCMH
#include "slsubsys.h"

#include <sys/types.h>
#include <sys/stat.h>

#include <stdio.h>
#include <string.h>

#include "pfl/alloc.h"
#include "pfl/atomic.h"
#include "pfl/cdefs.h"
#include "pfl/ctlsvr.h"
#include "pfl/hashtbl.h"
#include "pfl/list.h"
#include "pfl/listcache.h"
#include "pfl/rsx.h"
#include "pfl/str.h"
#include "pfl/time.h"
#include "pfl/treeutil.h"

#include "bmap_cli.h"
#include "cache_params.h"
#include "dircache.h"
#include "fid.h"
#include "fidc_cli.h"
#include "fidcache.h"
#include "mount_slash.h"
#include "rpc_cli.h"

extern struct psc_waitq		msl_bmap_waitq;

void
slc_fcmh_invalidate_bmap(struct fidc_membh *f, __unusedx int wait)
{
	int i;
	struct bmap *b;
	struct psc_dynarray a = DYNARRAY_INIT;

	/*
	 * Invalidate bmap lease so that we can renew it with 
	 * the correct lease.
	 */
 restart:

	pfl_rwlock_rdlock(&f->fcmh_rwlock);
	RB_FOREACH(b, bmaptree, &f->fcmh_bmaptree) {
		if (!BMAP_TRYLOCK(b)) {
			pfl_rwlock_unlock(&f->fcmh_rwlock);
			goto restart;
		}
		if (b->bcm_flags & BMAPF_TOFREE) {
			BMAP_ULOCK(b);
			continue;
		}
		if (b->bcm_flags & BMAPF_DISCARD) {
			BMAP_ULOCK(b);
			continue;
		}
		b->bcm_flags |= BMAPF_DISCARD;
		BMAP_ULOCK(b);
		psc_dynarray_add(&a, b);
	}
	pfl_rwlock_unlock(&f->fcmh_rwlock);

	DYNARRAY_FOREACH(b, i, &a) {

		/* 
		 * Hide the bmap from lookup to avoid waiting. This also hides
		 * the bmap from slctlrep_getbmap() as well. So be careful.
		 */
		OPSTAT_INCR("msl.stash-bmap");
		pfl_rwlock_wrlock(&f->fcmh_rwlock);
		PSC_RB_XREMOVE(bmaptree, &f->fcmh_bmaptree, b);
		pfl_rwlock_unlock(&f->fcmh_rwlock);
	}
	psc_dynarray_free(&a);
}

/*
 * Update the high-level app stat(2)-like attribute buffer for a FID
 * cache member.
 * @f: FID cache member to update.
 * @sstb: incoming stat attributes.
 * @flags: behavioral flags.
 * Notes:
 *     (1) if SAVELOCAL has been specified, save local field values:
 *		(o) file size
 *		(o) mtime
 *     (2) This function should only be used by a client.
 *
 * The current thinking is to store remote attributes in sstb.
 */ 
void
slc_fcmh_setattrf(struct fidc_membh *f, struct srt_stat *sstb,
    int flags)
{
	struct fcmh_cli_info *fci;

	if (flags & FCMH_SETATTRF_HAVELOCK)
		FCMH_LOCK_ENSURE(f);
	else
		FCMH_LOCK(f);

	if (fcmh_2_gen(f) == FGEN_ANY)
		fcmh_2_gen(f) = sstb->sst_gen;

	if ((FID_GET_INUM(fcmh_2_fid(f))) != SLFID_ROOT && fcmh_isreg(f)) {
		if (fcmh_2_gen(f) > sstb->sst_gen) {
			/*
			 * We bump it locally for a directory to avoid
			 * race with readdir operations.
			 */
			OPSTAT_INCR("msl.generation-backwards");
			DEBUG_FCMH(PLL_DIAG, f, "attempt to set attr with "
				"gen %"PRIu64" from old gen %"PRIu64,
				fcmh_2_gen(f), sstb->sst_gen);
			goto out;
		}
		if (fcmh_2_gen(f) < sstb->sst_gen) {
#if 0
			slc_fcmh_invalidate_bmap(f, 0);
#endif
			OPSTAT_INCR("msl.generation-forwards");
			DEBUG_FCMH(PLL_DIAG, f, "attempt to set attr with "
				"gen %"PRIu64" from old gen %"PRIu64,
				fcmh_2_gen(f), sstb->sst_gen);
		}
	}

	/*
 	 * Make sure that our generation number always goes up.
 	 * Currently, the MDS does not bump it at least for unlink.
 	 */
	if (fcmh_isdir(f) && sstb->sst_gen < fcmh_2_gen(f))
	    sstb->sst_gen = fcmh_2_gen(f);

	/*
	 * If we don't have stat attributes, how can we save our local
	 * updates?
	 */
	if ((f->fcmh_flags & FCMH_HAVE_ATTRS) == 0)
		flags |= FCMH_SETATTRF_CLOBBER;

	/*
	 * Always update for roots because we might have faked them
	 * with readdir at the super root.
	 */
	if ((FID_GET_INUM(fcmh_2_fid(f))) == SLFID_ROOT)
		flags |= FCMH_SETATTRF_CLOBBER;

	psc_assert(sstb->sst_gen != FGEN_ANY);
	psc_assert(f->fcmh_fg.fg_fid == sstb->sst_fid);

	/*
	 * The default behavior is to save st_size and st_mtim since we
	 * might have done I/O that the MDS does not know about.
	 */
	if ((flags & FCMH_SETATTRF_CLOBBER) == 0 &&
	    fcmh_isreg(f)) {
		/*
		 * If generation numbers match, take the highest of the
		 * values.  Otherwise, disregard local values and
		 * blindly accept whatever the MDS tells us.
		 */
		if (fcmh_2_ptruncgen(f) == sstb->sst_ptruncgen &&
		    fcmh_2_gen(f) == sstb->sst_gen &&
		    fcmh_2_fsz(f) > sstb->sst_size)
			sstb->sst_size = fcmh_2_fsz(f);
		if (fcmh_2_utimgen(f) == sstb->sst_utimgen)
			sstb->sst_mtim = f->fcmh_sstb.sst_mtim;
	}

	COPY_SSTB(sstb, &f->fcmh_sstb);
	f->fcmh_flags |= FCMH_HAVE_ATTRS;
	f->fcmh_flags &= ~FCMH_GETTING_ATTRS;

	if (fcmh_isdir(f))
		dircache_init(f);

	fci = fcmh_2_fci(f);
	PFL_GETTIMEVAL(&fci->fci_age);

	DEBUG_FCMH(PLL_DEBUG, f, "attr set");

 out:
	if (!(flags & FCMH_SETATTRF_HAVELOCK))
		FCMH_ULOCK(f);
}

void
msl_fcmh_stash_inode(struct fidc_membh *f, struct srt_inode *ino)
{
	struct fcmh_cli_info *fci;
	int i;

	fci = fcmh_2_fci(f);
	FCMH_LOCK_ENSURE(f);

	fci->fci_inode = *ino;
	/* XXX Do we really need this map just for stir? */
	for (i = 0; i < fci->fci_inode.nrepls; i++)
		fci->fcif_idxmap[i] = i;
	fci->fcif_mapstircnt = MAPSTIR_THRESH;
}

int
msl_fcmh_fetch_inode(struct fidc_membh *f)
{
	struct slrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_get_inode_req *mq;
	struct srm_get_inode_rep *mp;
	struct fcmh_cli_info *fci;
	int rc;

	fci = fcmh_2_fci(f);
	rc = slc_rmc_getcsvc(fci->fci_resm, &csvc, 0);
	if (rc)
		goto out;
	rc = SL_RSX_NEWREQ(csvc, SRMT_GET_INODE, rq, mq, mp);
	if (rc)
		goto out;

	mq->fg = f->fcmh_fg;
	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		goto out;

	FCMH_LOCK(f);
	msl_fcmh_stash_inode(f, &mp->ino);
	FCMH_ULOCK(f);

 out:
	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
	return (rc);
}

int
slc_fcmh_ctor(struct fidc_membh *f, __unusedx int flags)
{
	struct fcmh_cli_info *fci;
	struct sl_resource *res;
	struct sl_site *s;
	sl_siteid_t siteid;
	int i;

	fci = fcmh_get_pri(f);
	INIT_PSC_LISTENTRY(&fci->fci_lentry);
	siteid = FID_GET_SITEID(fcmh_2_fid(f));

	psc_assert(f->fcmh_flags & FCMH_INITING);

	/*
	 * ESTALE can happen with msctl working on a file in a different
	 * slash2 file system on the same machine. Make sure you supply
	 * the right socket to msctl.
	 *
	 * (gdb) p msl_rmc_resm->resm_res->res_site->site_id 
	 */
	if (fcmh_2_fid(f) != SLFID_ROOT &&
	    siteid != msl_rmc_resm->resm_siteid) {
		s = libsl_siteid2site(siteid);
		if (s == NULL) {
			psclog_errorx("fid "SLPRI_FID" has "
			    "invalid site ID %d",
			    fcmh_2_fid(f), siteid);
			OPSTAT_INCR("msl.stale1");
			return (ESTALE);
		}
		SITE_FOREACH_RES(s, res, i)
			if (res->res_type == SLREST_MDS) {
				fci->fci_resm = psc_dynarray_getpos(
				    &res->res_members, 0);
				return (0);
			}
		psclog_errorx("fid "SLPRI_FID" has invalid site ID %d",
		    fcmh_2_fid(f), siteid);
		OPSTAT_INCR("msl.stale2");
		return (ESTALE);
	}
	fci->fci_resm = msl_rmc_resm;

	return (0);
}

void
slc_fcmh_dtor(struct fidc_membh *f)
{
	if (f->fcmh_flags & FCMHF_INIT_DIRCACHE) {
		/*
		 * We must lock here or other threads with HOLDs on ents
		 * can race with us.
		 */
		DIRCACHE_WRLOCK(f);
		dircache_purge(f);
		DIRCACHE_ULOCK(f);
	}

	DEBUG_FCMH(PLL_DEBUG, f, "dtor");
}

void
msl_fcmh_stash_xattrsize(struct fidc_membh *f, uint32_t xattrsize)
{
	FCMH_LOCK_ENSURE(f);
	fcmh_2_fci(f)->fci_xattrsize = xattrsize;
	f->fcmh_flags |= FCMH_CLI_XATTR_INFO;
}

#if PFL_DEBUG > 0
void
dump_fcmh_flags(int flags)
{
	int seq = 0;

	_dump_fcmh_flags_common(&flags, &seq);
	PFL_PRFLAG(FCMHF_INIT_DIRCACHE, &flags, &seq);
	PFL_PRFLAG(FCMH_CLI_TRUNC, &flags, &seq);
	PFL_PRFLAG(FCMH_CLI_DIRTY_DSIZE, &flags, &seq);
	PFL_PRFLAG(FCMH_CLI_DIRTY_MTIME, &flags, &seq);
	PFL_PRFLAG(FCMH_CLI_DIRTY_QUEUE, &flags, &seq);
	PFL_PRFLAG(FCMH_CLI_XATTR_INFO, &flags, &seq);
	if (flags)
		printf(" unknown: %x", flags);
	printf("\n");
	fflush(stdout);
}
#endif

struct sl_fcmh_ops sl_fcmh_ops = {
	slc_fcmh_ctor,		/* sfop_ctor */
	slc_fcmh_dtor,		/* sfop_dtor */
	msl_stat,		/* sfop_getattr */
	NULL			/* sfop_reopen */
};
