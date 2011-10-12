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

#include "pfl/fs.h"
#include "psc_ds/lockedlist.h"
#include "psc_ds/tree.h"
#include "psc_ds/treeutil.h"
#include "psc_rpc/export.h"
#include "psc_rpc/rsx.h"
#include "psc_util/alloc.h"
#include "psc_util/atomic.h"
#include "psc_util/log.h"
#include "psc_util/odtable.h"

#include "bmap.h"
#include "bmap_mds.h"
#include "cache_params.h"
#include "fidc_mds.h"
#include "fidcache.h"
#include "inode.h"
#include "journal_mds.h"
#include "mdscoh.h"
#include "mdsio.h"
#include "mdslog.h"
#include "odtable_mds.h"
#include "repl_mds.h"
#include "rpc_mds.h"
#include "slashd.h"
#include "slashrpc.h"
#include "slerr.h"
#include "up_sched_res.h"

struct odtable	*mdsBmapAssignTable;
uint64_t	 mdsBmapSequenceNo;

int
mds_bmap_exists(struct fidc_membh *f, sl_bmapno_t n)
{
	sl_bmapno_t nb;
	int locked;
	//int rc;

	locked = FCMH_RLOCK(f);
#if 0
	if ((rc = mds_stat_refresh_locked(f)))
		return (rc);
#endif

	nb = fcmh_nvalidbmaps(f);

	/* XXX just read the bmap and check for valid state */

	psclog_debug("f+g="SLPRI_FG" nb=%u fsz=%"PSCPRIdOFFT,
	    SLPRI_FG_ARGS(&f->fcmh_fg), nb, fcmh_2_fsz(f));

	FCMH_URLOCK(f, locked);
	return (n < nb);
}

int64_t
slm_bmap_calc_repltraffic(struct bmapc_memb *b)
{
	struct fidc_membh *f;
	int64_t amt = 0;
	off_t bsiz, sz;
	int i;

	f = b->bcm_fcmh;
	FCMH_LOCK(f);
	BMAP_LOCK(b);
	for (i = 0; i < SLASH_SLVRS_PER_BMAP; i++) {
		if (b->bcm_crcstates[i] & BMAP_SLVR_DATA) {
			/*
			 * If this is the last bmap, tally only the
			 * portion of data that exists.  This is needed
			 * to fill big network pipes when dealing with
			 * lots of small files.
			 */
			bsiz = fcmh_2_fsz(f) % SLASH_BMAP_SIZE;
			if (bsiz == 0 && fcmh_2_fsz(f))
				bsiz = SLASH_BMAP_SIZE;
			if (b->bcm_bmapno == fcmh_nvalidbmaps(f) - 1 &&
			    i == bsiz / SLASH_SLVR_SIZE) {
				sz = bsiz % SLASH_SLVR_SIZE;
				if (sz == 0 && bsiz)
					sz = SLASH_SLVR_SIZE;
				amt += sz;
				break;
			}
			amt += SLASH_SLVR_SIZE;
		}
	}
	BMAP_ULOCK(b);
	FCMH_ULOCK(f);
	return (amt);
}

/**
 * mds_bmap_directio_locked - Called when a new read or write lease is
 *	added to the bmap.  Maintains the DIRECTIO status of the bmap
 *	based on the numbers of readers and writers present.
 * @b: the bmap
 * @rw: read / write op
 * @np: value-result target ION network + process ID.
 * Note: the new bml has yet to be added.
 */
__static int
mds_bmap_directio_locked(struct bmapc_memb *b, enum rw rw,
    lnet_process_id_t *np)
{
	struct bmap_mds_info *bmi = bmap_2_bmi(b);
	struct bmap_mds_lease *bml = NULL, *tmp;
	int rc = 0;

	BMAP_LOCK_ENSURE(b);
	psc_assert(rw == SL_WRITE || rw == SL_READ);

	if (b->bcm_flags & BMAP_DIO) {
		psc_assert(bmi->bmdsi_wr_ion);
		goto out;
	}
	if (b->bcm_flags & BMAP_DIORQ) {
		psc_assert(bmi->bmdsi_wr_ion);
		/* In the process of waiting for an async RPC to complete.
		 */
		rc = -SLERR_BMAP_DIOWAIT;
		goto out;
	}

	if (bmi->bmdsi_writers) {
		int wtrs;
		struct bmap_mds_lease *tmp1;
		/* A second writer or a reader wants access.  Ensure only
		 *    one lease is present.
		 */
		tmp = tmp1 = pll_peektail(&bmi->bmdsi_leases);
		do {
			if (tmp->bml_flags & BML_WRITE)
				wtrs++;

			if (!bml || bml->bml_seq < tmp->bml_seq)
				bml = tmp;
			
			tmp = tmp->bml_chain;
		} while (tmp != tmp1);

		psc_assert(bml && wtrs);

		psc_assert(!(b->bcm_flags & BMAP_DIORQ));
		if (bml->bml_flags & BML_CDIO) {
			b->bcm_flags |= BMAP_DIO;
			goto out;

		} else if (bml->bml_cli_nidpid.nid == np->nid &&
			   bml->bml_cli_nidpid.pid == np->pid) {
			/* Lease belongs to the client making the request.
			 */
			DEBUG_BMAP(PLL_NOTIFY, b, "dup lease");

		} else {
			b->bcm_flags |= BMAP_DIORQ;
			bml->bml_flags |= BML_COHDIO;
			DEBUG_BMAP(PLL_WARN, b, "set BMAP_DIORQ, issuing cb");
			/* Use a non-blocking revocation.  The client will
			 *   retry periodically until the bmap has either
			 *   reached DIO state or been timed out.
			 */
			mdscoh_req(bml, MDSCOH_NONBLOCK);
			rc = -SLERR_BMAP_DIOWAIT;
			goto out;
		}

	} else if (rw == SL_WRITE && bmi->bmdsi_readers) {
		int set_dio = 0;

		/* Writer being added amidst one or more readers.  Issue
		 *   courtesy callbacks to the readers.
		 */
		PLL_FOREACH(bml, &bmi->bmdsi_leases) {
			tmp = bml;
			do {
				psc_assert(!(bml->bml_flags & BML_WRITE));
				psc_assert(bml->bml_flags & BML_READ);

				if (bml->bml_cli_nidpid.nid != np->nid) {
					set_dio = 1;
					if (!(bml->bml_flags & BML_CDIO))
						mdscoh_req(bml,
						    MDSCOH_NONBLOCK);
				}
				tmp = tmp->bml_chain;
			} while (tmp != bml);
		}
		if (set_dio)
			b->bcm_flags |= BMAP_DIO;
	}
 out:
	return (rc);
}

__static int
mds_bmap_ios_restart(struct bmap_mds_lease *bml)
{
	struct sl_resm *resm = libsl_ios2resm(bml->bml_ios);
	struct resm_mds_info *rmmi;
	int rc = 0;

	rmmi = resm2rmmi(resm);
	atomic_inc(&rmmi->rmmi_refcnt);

	psc_assert(bml->bml_bmdsi->bmdsi_assign);
	bml->bml_bmdsi->bmdsi_wr_ion = rmmi;
	bmap_op_start_type(bml_2_bmap(bml), BMAP_OPCNT_IONASSIGN);

	if (mds_bmap_timeotbl_mdsi(bml, BTE_ADD | BTE_REATTACH) == BMAPSEQ_ANY)
		rc = 1;

	bml->bml_bmdsi->bmdsi_seq = bml->bml_seq;

	DEBUG_BMAP(PLL_INFO, bml_2_bmap(bml), "res(%s) seq=%"PRIx64, 
	   resm->resm_res->res_name, bml->bml_seq);

	return (rc);
}

/**
 * mds_resm_tryconnect - Try to connect to a member of the given I/O
 *   resource presumably in preparation for issuance of a write bmap.
 * @res: the I/O resource to connect
 * @nb: block (or not) when calling slm_geticsvc().
 * Notes:  mds_resm_tryconnect() assumes that SLREST_CLUSTER_NOSHARE_LFS I/O
 *   systems have a single resource member.
 */
__static struct sl_resm *
mds_resm_tryconnect(struct sl_resource *res, int nb)
{
	struct resprof_mds_info *rpmi = res2rpmi(res);
	struct slashrpc_cservice *csvc = NULL;
	struct sl_resm *resm;
	int i;

	for (i = 0; i < psc_dynarray_len(&res->res_members); i++) {
		spinlock(&rpmi->rpmi_lock);
		resm = psc_dynarray_getpos(&res->res_members,
					   slm_get_rpmi_idx(res));
		freelock(&rpmi->rpmi_lock);

		psclog_info("trying res(%s)", res->res_name);

		csvc = nb ? slm_geticsvc_nb(resm, NULL) : slm_geticsvc(resm);
		if (csvc)
			break;
	}

	if (!csvc)
		resm = NULL;
	else
		sl_csvc_decref(csvc);

	return (resm);
}

/**
 * mds_resm_select - Choose an I/O resource and one of its members.
 * @b:  The bmap which is being leased.
 * @pios:  The preferred I/O resource specified by the client.
 * Notes:  This call accounts for the existence of existing replicas.  When
 *    found, mds_resm_select() must choose a replica which is marked as
 *    BREPLST_VALID.
 */
__static struct sl_resm *
mds_resm_select(struct bmapc_memb *b, sl_ios_id_t pios)
{
	struct sl_resource *res;
	struct sl_resm *resm = NULL;
	struct slash_inode_od *ino = fcmh_2_ino(b->bcm_fcmh);
	struct slash_inode_extras_od *inox = NULL;
	struct psc_dynarray a = DYNARRAY_INIT;
	int i, off, val, nr, repls = 0, nb;
	sl_ios_id_t ios;

	FCMH_LOCK(b->bcm_fcmh);
	nr = fcmh_2_nrepls(b->bcm_fcmh);
	FCMH_ULOCK(b->bcm_fcmh);

	if (nr > SL_DEF_REPLICAS) {
		mds_inox_ensure_loaded(fcmh_2_inoh(b->bcm_fcmh));
		inox = fcmh_2_inox(b->bcm_fcmh);
	}

	for (i = 0, off = 0; i < nr; i++, off += SL_BITS_PER_REPLICA) {
		val = SL_REPL_GET_BMAP_IOS_STAT(b->bcm_repls, off);
		if (val != BREPLST_INVALID)
			/* Determine if there are any active replicas.
			 */
			repls++;

		if (val != BREPLST_VALID)
			continue;

		ios = (i < SL_DEF_REPLICAS) ? ino->ino_repls[i].bs_id :
			inox->inox_repls[i - SL_DEF_REPLICAS].bs_id;

		psc_dynarray_add(&a, libsl_id2res(ios));
	}

	if (repls) {
		struct sl_resource *tres;

		if (!psc_dynarray_len(&a)) {
			/* No replicas were marked BREPLST_VALID
			 */
			DEBUG_BMAP(PLL_ERROR, b,
			   "no replicas marked BREPLST_VALID");
			return (NULL);
		}
		/* XXX Here we should employ a function which sorts the
		 *   array to place the more appropriate IOS's towards
		 *   the front of array.
		 *
		 * For now just try to locate the PIOS and move it to the
		 *   front.
		 */
		DYNARRAY_FOREACH(res, i, &a) {
			if (res->res_id == pios && i) {
				tres = psc_dynarray_getpos(&a, 0);
				psc_dynarray_setpos(&a, 0, res);
				psc_dynarray_setpos(&a, i, tres);
				break;
			}
		}

	} else {
		/* There are no valid replicas.  Populate the resource
		 *   array with the pios followed by any other IOS which
		 *   are listed as replicas.
		 *
		 * XXX In the future it may be wise to rank all available
		 *   IOS's and place them into the array.
		 */
		psc_dynarray_add(&a, libsl_id2res(pios));
		for (i = 0; i < nr; i++) {
			ios = (i < SL_DEF_REPLICAS) ? ino->ino_repls[i].bs_id :
				inox->inox_repls[i - SL_DEF_REPLICAS].bs_id;

			psc_dynarray_add_ifdne(&a, libsl_id2res(ios));
		}
	}
	/* Iterate through the set of I/O resources until a connection
	 *   is established.
	 */
	for (nb = 1; nb > 0; nb--) {
		DYNARRAY_FOREACH(res, i, &a) {
			resm = mds_resm_tryconnect(res, nb);
			if (resm)
				break;
		}
	}

	psc_dynarray_free(&a);
	return (resm);
}

/**
 * mds_bmap_ios_assign - Bind a bmap to an ION for writing.  The process
 *	involves a round-robin'ing of an I/O system's nodes and
 *	attaching a resm_mds_info to the bmap, used for establishing
 *	connection to the ION.
 * @bml: the bmap lease
 * @pios: the preferred I/O system
 */
__static int
mds_bmap_ios_assign(struct bmap_mds_lease *bml, sl_ios_id_t pios)
{
	struct bmapc_memb *bmap = bml_2_bmap(bml);
	struct bmap_mds_info *bmi = bmap_2_bmi(bmap);
	struct slmds_jent_assign_rep *logentry;
	struct slmds_jent_bmap_assign *sjba;
	struct slash_inode_handle *ih;
	struct resm_mds_info *rmmi;
	struct bmap_ios_assign bia;
	struct sl_resm *resm;
	uint32_t nrepls;
	int iosidx, rc;

	psc_assert(!bmi->bmdsi_wr_ion);
	psc_assert(!bmi->bmdsi_assign);
	psc_assert(psc_atomic32_read(&bmap->bcm_opcnt) > 0);

	BMAP_LOCK(bmap);
	psc_assert(bmap->bcm_flags & BMAP_IONASSIGN);
	BMAP_ULOCK(bmap);

	resm = mds_resm_select(bmap, pios);
	if (!resm) {
		BMAP_LOCK(bmap);
		bmap->bcm_flags |= BMAP_MDS_NOION;
		BMAP_ULOCK(bmap);

		bml->bml_flags |= BML_ASSFAIL;

		psclog_warnx("unable to contact ION for lease");

		return (-SLERR_ION_OFFLINE);
	}

	bmi->bmdsi_wr_ion = rmmi = resm2rmmi(resm);
	atomic_inc(&rmmi->rmmi_refcnt);

	DEBUG_BMAP(PLL_INFO, bmap, "online res(%s)",
	    resm->resm_res->res_name);
	/*
	 * An ION has been assigned to the bmap, mark it in the odtable
	 *   so that the assignment may be restored on reboot.
	 */
	memset(&bia, 0, sizeof(bia));

	bia.bia_lastcli = bml->bml_cli_nidpid;
	bia.bia_ios = bml->bml_ios = rmmi->rmmi_resm->resm_res->res_id;
	bia.bia_fid = fcmh_2_fid(bmap->bcm_fcmh);
	bia.bia_bmapno = bmap->bcm_bmapno;
	bia.bia_start = time(NULL);
	bia.bia_flags = (bmap->bcm_flags & BMAP_DIO) ? BIAF_DIO : 0;
	bmi->bmdsi_seq = bia.bia_seq = mds_bmap_timeotbl_mdsi(bml, BTE_ADD);

	bmi->bmdsi_assign = mds_odtable_putitem(mdsBmapAssignTable, &bia,
	    sizeof(bia));

	if (!bmi->bmdsi_assign) {
		BMAP_SETATTR(bmap, BMAP_MDS_NOION);
		bml->bml_flags |= BML_ASSFAIL;

		DEBUG_BMAP(PLL_ERROR, bmap, "failed odtable_putitem()");
		// XXX fix me - dont leak the journal buf!
		return (-SLERR_XACT_FAIL);
	}
	BMAP_CLEARATTR(bmap, BMAP_MDS_NOION);
	/*
	 * Signify that a ION has been assigned to this bmap.  This
	 * opcnt ref will stay in place until the bmap has been released
	 * by the last client or has been timed out.
	 */
	bmap_op_start_type(bmap, BMAP_OPCNT_IONASSIGN);


	ih = fcmh_2_inoh(bmap->bcm_fcmh);
	nrepls = ih->inoh_ino.ino_nrepls;

	iosidx = mds_repl_ios_lookup_add(ih, bia.bia_ios, 0);
	if (iosidx < 0)
		psc_fatalx("ios_lookup_add %d: %s", bia.bia_ios,
		    slstrerror(iosidx));

	rc = mds_repl_inv_except(bmap, iosidx);
	if (rc) {
		DEBUG_BMAP(PLL_ERROR, bmap, "mds_repl_inv_except() failed");
		return (-1);
	}
	mds_reserve_slot(1);
	logentry = pjournal_get_buf(mdsJournal, sizeof(*logentry));

	logentry->sjar_flags = SLJ_ASSIGN_REP_NONE;
	if (nrepls != ih->inoh_ino.ino_nrepls) {
		mdslogfill_ino_repls(bmap->bcm_fcmh, &logentry->sjar_ino);
		logentry->sjar_flags |= SLJ_ASSIGN_REP_INO;
	}

	mdslogfill_bmap_repls(bmap, &logentry->sjar_rep);
	logentry->sjar_flags |= SLJ_ASSIGN_REP_REP;

	sjba = &logentry->sjar_bmap;
	sjba->sjba_lastcli.nid = bia.bia_lastcli.nid;
	sjba->sjba_lastcli.pid = bia.bia_lastcli.pid;
	sjba->sjba_ios = bia.bia_ios;
	sjba->sjba_fid = bia.bia_fid;
	sjba->sjba_seq = bia.bia_seq;
	sjba->sjba_bmapno = bia.bia_bmapno;
	sjba->sjba_start = bia.bia_start;
	sjba->sjba_flags = bia.bia_flags;
	logentry->sjar_flags |= SLJ_ASSIGN_REP_BMAP;
	logentry->sjar_elem = bmi->bmdsi_assign->odtr_elem;

	pjournal_add_entry(mdsJournal, 0, MDS_LOG_BMAP_ASSIGN, 0,
	    logentry, sizeof(*logentry));
	pjournal_put_buf(mdsJournal, logentry);
	mds_unreserve_slot(1);

	bml->bml_seq = bia.bia_seq;

	DEBUG_FCMH(PLL_INFO, bmap->bcm_fcmh, "bmap assign, elem=%zd",
	    bmi->bmdsi_assign->odtr_elem);
	DEBUG_BMAP(PLL_INFO, bmap, "using res(%s) "
	    "rmmi(%p) bia(%p)", resm->resm_res->res_name,
	    bmi->bmdsi_wr_ion, bmi->bmdsi_assign);

	return (0);
}

__static int
mds_bmap_ios_update(struct bmap_mds_lease *bml)
{
	struct bmapc_memb *b = bml_2_bmap(bml);
	struct bmap_mds_info *bmi = bmap_2_bmi(b);
	struct slmds_jent_assign_rep *logentry;
	struct slmds_jent_bmap_assign *sjba;
	struct slash_inode_handle *ih;
	struct bmap_ios_assign bia;
	int rc, iosidx, dio;
	uint32_t nrepls;

	BMAP_LOCK(b);
	psc_assert(b->bcm_flags & BMAP_IONASSIGN);
	dio = (b->bcm_flags & BMAP_DIO);
	BMAP_ULOCK(b);

	rc = mds_odtable_getitem(mdsBmapAssignTable,
	    bmi->bmdsi_assign, &bia, sizeof(bia));
	if (rc) {
		DEBUG_BMAP(PLL_ERROR, b, "odtable_getitem() failed");
		return (-1);
	}
	if (bia.bia_fid != fcmh_2_fid(b->bcm_fcmh)) {
		/* XXX release bia? */
		DEBUG_BMAP(PLL_ERROR, b, "different fid="SLPRI_FID,
		   bia.bia_fid);
		return (-1);
	}

	if (bml->bml_cli_nidpid.nid != bia.bia_lastcli.nid ||
	    bml->bml_cli_nidpid.pid != bia.bia_lastcli.pid)
		psc_assert(dio);

	psc_assert(bia.bia_seq == bmi->bmdsi_seq);
	bia.bia_start = time(NULL);
	bia.bia_seq = bmi->bmdsi_seq = mds_bmap_timeotbl_mdsi(bml, BTE_ADD);
	bia.bia_lastcli = bml->bml_cli_nidpid;
	bia.bia_flags = BIAF_DIO;

	bmi->bmdsi_assign = mds_odtable_replaceitem(mdsBmapAssignTable,
	    bmi->bmdsi_assign, &bia, sizeof(bia));

	psc_assert(bmi->bmdsi_assign);

	bml->bml_ios = bia.bia_ios;
	bml->bml_seq = bia.bia_seq;

	ih = fcmh_2_inoh(b->bcm_fcmh);
	nrepls = ih->inoh_ino.ino_nrepls;

	iosidx = mds_repl_ios_lookup_add(ih, bia.bia_ios, 0);
	if (iosidx < 0)
		psc_fatalx("ios_lookup_add %d: %s", bia.bia_ios,
		   slstrerror(iosidx));

	rc = mds_repl_inv_except(b, iosidx);
	if (rc) {
		DEBUG_BMAP(PLL_ERROR, b, "mds_repl_inv_except() failed");
		return (-1);
	}
	mds_reserve_slot(1);
	logentry = pjournal_get_buf(mdsJournal, sizeof(*logentry));

	logentry->sjar_flags = SLJ_ASSIGN_REP_NONE;
	if (nrepls != ih->inoh_ino.ino_nrepls) {
		mdslogfill_ino_repls(b->bcm_fcmh, &logentry->sjar_ino);
		logentry->sjar_flags |= SLJ_ASSIGN_REP_INO;
	}

	mdslogfill_bmap_repls(b, &logentry->sjar_rep);
	logentry->sjar_flags |= SLJ_ASSIGN_REP_REP;

	sjba = &logentry->sjar_bmap;
	sjba->sjba_lastcli.nid = bia.bia_lastcli.nid;
	sjba->sjba_lastcli.pid = bia.bia_lastcli.pid;
	sjba->sjba_ios = bia.bia_ios;
	sjba->sjba_fid = bia.bia_fid;
	sjba->sjba_seq = bia.bia_seq;
	sjba->sjba_bmapno = bia.bia_bmapno;
	sjba->sjba_start = bia.bia_start;
	sjba->sjba_flags = bia.bia_flags;
	logentry->sjar_flags |= SLJ_ASSIGN_REP_BMAP;
	logentry->sjar_elem = bmi->bmdsi_assign->odtr_elem;

	pjournal_add_entry(mdsJournal, 0, MDS_LOG_BMAP_ASSIGN, 0,
	    logentry, sizeof(*logentry));
	pjournal_put_buf(mdsJournal, logentry);
	mds_unreserve_slot(1);

	DEBUG_FCMH(PLL_INFO, b->bcm_fcmh, "bmap update, elem=%zd",
	    bmi->bmdsi_assign->odtr_elem);

	return (0);
}

/**
 * mds_bmap_dupls_find - Find the first lease of a given client based on
 *	its {nid, pid} pair.  Also walk the chain of duplicate leases to
 *	count the number of read and write leases.  Note that only the
 *	first lease of a client is linked on the bmi->bmdsi_leases
 *	list, the rest is linked on a private chain and tagged with
 *	BML_CHAIN flag.
 */
static __inline struct bmap_mds_lease *
mds_bmap_dupls_find(struct bmap_mds_info *bmi, lnet_process_id_t *cnp,
    int *wlease, int *rlease)
{
	struct bmap_mds_lease *tmp, *bml = NULL;

	*rlease = 0;
	*wlease = 0;

	PLL_FOREACH(tmp, &bmi->bmdsi_leases) {
		if (tmp->bml_cli_nidpid.nid != cnp->nid ||
		    tmp->bml_cli_nidpid.pid != cnp->pid)
			continue;
		/* Only one lease per client is allowed on the list.
		 */
		psc_assert(!bml);
		bml = tmp;
	}

	if (!bml)
		return (NULL);

	tmp = bml;
	do {
		/* All dup leases should be chained off the first bml.
		 */
		if (tmp->bml_flags & BML_READ)
			(*rlease)++;
		else
			(*wlease)++;

		DEBUG_BMAP(PLL_INFO, bmi_2_bmap(bmi), "bml=%p tmp=%p "
		    "(wlease=%d rlease=%d) (nwtrs=%d nrdrs=%d)",
		    bml, tmp, *wlease, *rlease,
		    bmi->bmdsi_writers, bmi->bmdsi_readers);

		tmp = tmp->bml_chain;
	} while (tmp != bml);

	return (bml);
}

/**
 * mds_bmap_bml_chwrmode - Attempt to upgrade a client-granted bmap
 *	lease from READ-only to READ+WRITE.
 * @bml: bmap lease.
 * @prefios: client's preferred I/O system ID.
 */
int
mds_bmap_bml_chwrmode(struct bmap_mds_lease *bml, sl_ios_id_t prefios)
{
	struct bmap_mds_info *bmi;
	struct bmapc_memb *b;
	int rc = 0, wlease, rlease;

	bmi = bml->bml_bmdsi;
	b = bmi_2_bmap(bmi);

	bcm_wait_locked(b, b->bcm_flags & BMAP_IONASSIGN);

	DEBUG_BMAP(PLL_INFO, b, "bml=%p bmdsi_writers=%d bmdsi_readers=%d",
	    bml, bmi->bmdsi_writers, bmi->bmdsi_readers);

	if (bml->bml_flags & BML_WRITE)
		return (EALREADY);

	rc = mds_bmap_directio_locked(b, SL_WRITE,
	    &bml->bml_cli_nidpid);
	if (rc)
		return (rc);

	mds_bmap_dupls_find(bmi, &bml->bml_cli_nidpid, &wlease,
	    &rlease);

	/* Account for the read lease which is to be converted.
	 */
	psc_assert(rlease);
	if (!wlease) {
		/* Only bump bmdsi_writers if no other write lease
		 *   is still leased to this client.
		 */
		bmi->bmdsi_writers++;
		wlease = -1;

		if (rlease) {
			bmi->bmdsi_readers--;
			rlease = -1;
		}
	}

	BMAP_SETATTR(b, BMAP_IONASSIGN);
	bml->bml_flags &= ~BML_READ;
	bml->bml_flags |= BML_UPGRADE | BML_WRITE;

	if (bmi->bmdsi_wr_ion) {
		BMAP_ULOCK(b);
		rc = mds_bmap_ios_update(bml);
	} else {
		BMAP_ULOCK(b);
		rc = mds_bmap_ios_assign(bml, prefios);
	}
	psc_assert(bmi->bmdsi_wr_ion);

	BMAP_LOCK(b);

	DEBUG_BMAP(PLL_INFO, b, "bml=%p rc=%d bmdsi_writers=%d bmdsi_readers=%d",
	    bml, rc, bmi->bmdsi_writers, bmi->bmdsi_readers);

	if (rc) {
		bml->bml_flags |= (BML_READ|BML_ASSFAIL);
		bml->bml_flags &= ~(BML_UPGRADE | BML_WRITE);

		if (wlease < 0) {
			bmi->bmdsi_writers--;
			psc_assert(bmi->bmdsi_writers >= 0);
		}

		if (rlease < 0)
			/* Restore the reader cnt.
			 */
			bmi->bmdsi_readers++;
	}
	BMAP_CLEARATTR(b, BMAP_IONASSIGN);
	bcm_wake_locked(b);
	return (rc);
}

/**
 * mds_bmap_getbml - Obtain the lease handle for a bmap denoted by the
 *	specified issued sequence number.
 * @b: locked bmap.
 * @cli_nid: client network ID.
 * @cli_pid: client network process ID.
 * @seq: lease sequence.
 */
struct bmap_mds_lease *
mds_bmap_getbml(struct bmapc_memb *b, struct srt_bmapdesc *sbd)
{
	struct bmap_mds_info *bmi;
	struct bmap_mds_lease *bml;

	BMAP_LOCK_ENSURE(b);

	bmi = bmap_2_bmi(b);
	PLL_FOREACH(bml, &bmi->bmdsi_leases) {
		if (bml->bml_cli_nidpid.nid == sbd->sbd_nid &&
		    bml->bml_cli_nidpid.pid == sbd->sbd_pid) {
			struct bmap_mds_lease *tmp = bml;

			do {
				if (tmp->bml_seq == sbd->sbd_seq)
					return (tmp);

				tmp = tmp->bml_chain;
			} while (tmp != bml);
		}
	}
	return (NULL);
}

/**
 * mds_bmap_bml_add - Add a read or write reference to the bmap's tree
 *	and refcnts.  This also calls into the directio_[check|set]
 *	calls depending on the number of read and/or write clients of
 *	this bmap.
 * @bml: bmap lease.
 * @rw: read/write access for bmap.
 * @prefios: client preferred I/O system.
 */
__static int
mds_bmap_bml_add(struct bmap_mds_lease *bml, enum rw rw,
    sl_ios_id_t prefios)
{
	struct bmap_mds_info *bmi = bml->bml_bmdsi;
	struct bmapc_memb *b = bmi_2_bmap(bmi);
	struct bmap_mds_lease *obml;
	int rlease, wlease, rc = 0;

	psc_assert(bml->bml_cli_nidpid.nid &&
		   bml->bml_cli_nidpid.pid &&
		   bml->bml_cli_nidpid.nid != LNET_NID_ANY &&
		   bml->bml_cli_nidpid.pid != LNET_PID_ANY);

	BMAP_LOCK(b);
	/* Wait for BMAP_IONASSIGN to be removed before proceeding.
	 */
	bcm_wait_locked(b, (b->bcm_flags & BMAP_IONASSIGN));

	if (!(bml->bml_flags & BML_RECOVER) &&
	    (rc = mds_bmap_directio_locked(b, rw, &bml->bml_cli_nidpid)))
		/* 'rc != 0' means that we're waiting on an async cb
		 *    completion.
		 */
		goto out;

	bmap_op_start_type(b, BMAP_OPCNT_LEASE);

	obml = mds_bmap_dupls_find(bmi, &bml->bml_cli_nidpid, &wlease,
	    &rlease);

	DEBUG_BMAP(PLL_INFO, b, "bml=%p obml=%p (wlease=%d rlease=%d) "
	    "(nwtrs=%d nrdrs=%d)",
	    bml, obml, wlease, rlease,
	    bmi->bmdsi_writers, bmi->bmdsi_readers);

	if (obml) {
		struct bmap_mds_lease *tmp = obml;

		bml->bml_flags |= BML_CHAIN;
		/* Add ourselves to the end.
		 */
		while (tmp->bml_chain != obml)
			tmp = tmp->bml_chain;

		tmp->bml_chain = bml;
		bml->bml_chain = obml;
		psc_assert(psclist_disjoint(&bml->bml_bmdsi_lentry));
	} else {
		/* First on the list.
		 */
		bml->bml_chain = bml;
		pll_addtail(&bmi->bmdsi_leases, bml);
	}
	bml->bml_flags |= BML_BMDSI;

	if (rw == SL_WRITE) {
		/* Drop the lock prior to doing disk and possibly network
		 *    I/O.
		 */
		b->bcm_flags |= BMAP_IONASSIGN;
		/* For any given chain of leases, the bmdsi_[readers|writers]
		 *    value may only be 1rd or 1wr.  In the case where 2
		 *    wtrs are present, the value is 1wr.  Mixed readers and
		 *    wtrs == 1wtr.  1-N rdrs, 1rd.
		 * Only increment writers if this is the first
		 *    write lease from the respective client.
		 */
		if (!wlease) {
			/* This is the first write from the client.
			 */
			bmi->bmdsi_writers++;

			if (bmi->bmdsi_writers > 1)
				psc_enter_debugger("bmi->bmdsi_writers");

			if (rlease)
				/* Remove the read cnt, it has been
				 *   superseded by the write.
				 */
				bmi->bmdsi_readers--;
		}

		if (bml->bml_flags & BML_RECOVER) {
			psc_assert(bmi->bmdsi_writers == 1);
			psc_assert(!bmi->bmdsi_readers);
			psc_assert(!bmi->bmdsi_wr_ion);
			psc_assert(bml->bml_ios &&
			   bml->bml_ios != IOS_ID_ANY);
			BMAP_ULOCK(b);
			rc = mds_bmap_ios_restart(bml);

		} else if (!wlease && bmi->bmdsi_writers == 1) {
			psc_assert(!bmi->bmdsi_wr_ion);
			BMAP_ULOCK(b);
			rc = mds_bmap_ios_assign(bml, prefios);

		} else {
			psc_assert(wlease && bmi->bmdsi_wr_ion);
			BMAP_ULOCK(b);
			rc = mds_bmap_ios_update(bml);
		}

		BMAP_LOCK(b);
		b->bcm_flags &= ~BMAP_IONASSIGN;

	} else { //rw == SL_READ
		bml->bml_seq = mds_bmap_timeotbl_getnextseq();

		if (!wlease && !rlease)
			bmi->bmdsi_readers++;
		bml->bml_flags |= BML_TIMEOQ;
		mds_bmap_timeotbl_mdsi(bml, BTE_ADD);
	}

 out:
	DEBUG_BMAP(rc ? PLL_WARN : PLL_INFO, b, "bml_add (mion=%p) bml=%p "
	   "(seq=%"PRId64") (rw=%d) (nwtrs=%d nrdrs=%d) (rc=%d)",
	   bmi->bmdsi_wr_ion, bml, bml->bml_seq, rw, bmi->bmdsi_writers,
	   bmi->bmdsi_readers, rc);

	bcm_wake_locked(b);
	BMAP_ULOCK(b);
	/* On error, the caller will issue mds_bmap_bml_release() which
	 *   deals with the gory details of freeing a fullly, or partially,
	 *   instantiated bml.  Therefore, BMAP_OPCNT_LEASE will not be
	 *   removed in the case of an error.
	 */
	return (rc);
}

static void
mds_bmap_bml_del_locked(struct bmap_mds_lease *bml)
{
	struct bmap_mds_info *bmi = bml->bml_bmdsi;
	struct bmap_mds_lease *obml, *tail;
	int rlease = 0, wlease = 0;

	BMAP_LOCK_ENSURE(bmi_2_bmap(bmi));
	BML_LOCK_ENSURE(bml);

	obml = mds_bmap_dupls_find(bmi, &bml->bml_cli_nidpid, &wlease,
	    &rlease);

	/*
	 * obml must be not NULL because at least the lease being freed
	 * must be present in the list.  Therefore lease cnt must be
	 * positive.   Also note that the find() function returns the
	 * head of the chain of duplicate leases.
	 */
	psc_assert(obml);
	psc_assert((wlease + rlease) > 0);
	psc_assert(!(obml->bml_flags & BML_CHAIN));
	psc_assert(psclist_conjoint(&obml->bml_bmdsi_lentry,
	    psc_lentry_hd(&obml->bml_bmdsi_lentry)));

	/* Find the bml's preceeding entry.
	 */
	tail = obml;
	while (tail->bml_chain != bml)
		tail = tail->bml_chain;
	psc_assert(tail->bml_chain == bml);

	/* Manage the bml list and bml_bmdsi_lentry.
	 */
	if (bml->bml_flags & BML_CHAIN) {
		psc_assert(psclist_disjoint(&bml->bml_bmdsi_lentry));
		psc_assert((wlease + rlease) > 1);
		tail->bml_chain = bml->bml_chain;

	} else {
		psc_assert(obml == bml);
		psc_assert(!(bml->bml_flags & BML_CHAIN));
		pll_remove(&bmi->bmdsi_leases, bml);

		if ((wlease + rlease) > 1) {
			psc_assert(bml->bml_chain->bml_flags & BML_CHAIN);
			psc_assert(psclist_disjoint(&bml->bml_chain->bml_bmdsi_lentry));

			bml->bml_chain->bml_flags &= ~BML_CHAIN;
			pll_addtail(&bmi->bmdsi_leases, bml->bml_chain);

			tail->bml_chain = bml->bml_chain;
		} else
			psc_assert(bml == bml->bml_chain);
	}

	if (bml->bml_flags & BML_WRITE) {
		if (wlease == 1) {
			psc_assert(bmi->bmdsi_writers > 0);
			bmi->bmdsi_writers--;

			DEBUG_BMAP(PLL_INFO, bmi_2_bmap(bmi),
			   "bml=%p bmdsi_writers=%d bmdsi_readers=%d",
			   bml, bmi->bmdsi_writers, bmi->bmdsi_readers);

			if (rlease)
				bmi->bmdsi_readers++;
		}
	} else {
		psc_assert(bml->bml_flags & BML_READ);
		if (!wlease && (rlease == 1)) {
			psc_assert(bmi->bmdsi_readers > 0);
			bmi->bmdsi_readers--;
		}
	}
}

/**
 * mds_bmap_bml_release - Remove a bmap lease from the MDS.  This can be
 *   called from the bmap_timeo thread, from a client bmap_release RPC,
 *   or from the nbreqset cb context.
 * @bml: bmap lease.
 * Notes:  the bml must be removed from the timeotbl in all cases.
 *    otherwise we determine list removals on a case by case basis.
 */
int
mds_bmap_bml_release(struct bmap_mds_lease *bml)
{
	struct bmapc_memb *b = bml_2_bmap(bml);
	struct bmap_mds_info *bmi = bml->bml_bmdsi;
	struct slmds_jent_assign_rep *logentry;
	struct odtable_receipt *odtr = NULL;
	int rc = 0, locked;
	uint64_t key;
	size_t elem;

	psc_assert(psc_atomic32_read(&b->bcm_opcnt) > 0);
	psc_assert(bml->bml_flags & BML_FREEING);

	DEBUG_BMAP(PLL_INFO, b, "bml=%p fl=%d seq=%"PRId64, bml,
		   bml->bml_flags, bml->bml_seq);

	locked = BMAP_RLOCK(b);
	/* BMAP_IONASSIGN acts as a barrier for operations which
	 *   may modify bmdsi_wr_ion.  Since ops associated with
	 *   BMAP_IONASSIGN do disk and net i/o, the spinlock is
	 *   dropped.
	 * XXX actually, the bcm_lock is not dropped until the very end.
	 *   if this becomes problematic we should investigate more.
	 *   ATM the BMAP_IONASSIGN is not relied upon
	 */
	bcm_wait_locked(b, (b->bcm_flags & BMAP_IONASSIGN));
	b->bcm_flags |= BMAP_IONASSIGN;

	BML_LOCK(bml);

	if (bml->bml_flags & BML_COHRLS) {
		/* Called from the mdscoh callback.  Nothing should be left
		 *   except for removing the bml from the bmi.
		 */
		psc_assert(!(bml->bml_flags & (BML_COH|BML_EXP|BML_TIMEOQ)));
		psc_assert(psclist_disjoint(&bml->bml_coh_lentry));
		psc_assert(psclist_disjoint(&bml->bml_exp_lentry));
		psc_assert(psclist_disjoint(&bml->bml_timeo_lentry));
		bml->bml_flags &= ~BML_COHRLS;
	}

	if (bml->bml_flags & BML_COH)
		/* Don't wait for any outstanding coherency callbacks
		 *   to complete.  Mark the bml so that the coh thread
		 *   will call this function upon rpc completion.
		 */
		bml->bml_flags |= BML_COHRLS;

	if (bml->bml_flags & BML_EXP) {
		/* Take the locks in the correct order. */
		BML_ULOCK(bml);
		EXPORT_LOCK(bml->bml_exp);
		sl_exp_getpri_cli(bml->bml_exp);
		BML_LOCK(bml);
		if (bml->bml_flags & BML_EXP) {
			psclist_del(&bml->bml_exp_lentry,
			    psc_lentry_hd(&bml->bml_exp_lentry));
			bml->bml_flags &= ~BML_EXP;
		} else
			psc_assert(psclist_disjoint(&bml->bml_exp_lentry));
		EXPORT_ULOCK(bml->bml_exp);
	}

	if (bml->bml_flags & BML_TIMEOQ) {
		BML_ULOCK(bml);
		mds_bmap_timeotbl_mdsi(bml, BTE_DEL);
		BML_LOCK(bml);
	}

	/* If BML_COHRLS was set above then the lease must remain on the
	 *    bmi so that directio can be managed properly.
	 */
	if (bml->bml_flags & BML_COHRLS) {
		bml->bml_flags &= ~BML_FREEING;
		BML_ULOCK(bml);
		b->bcm_flags &= ~BMAP_IONASSIGN;
		bcm_wake_locked(b);
		BMAP_URLOCK(b, locked);
		return (-EAGAIN);
	}

	if (!(bml->bml_flags & BML_BMDSI)) {
		BML_ULOCK(bml);
		goto out;
	}

	mds_bmap_bml_del_locked(bml);
	bml->bml_flags &= ~BML_BMDSI;

	BML_ULOCK(bml);

	if ((b->bcm_flags & BMAP_DIO) &&
	    (!bmi->bmdsi_writers ||
	     (bmi->bmdsi_writers == 1 && !bmi->bmdsi_readers)))
		/* Remove the directio flag if possible.
		 */
		b->bcm_flags &= ~BMAP_DIO;

	/* Only release the odtable entry if the key matches.  If a match
	 *   is found then verify the sequence number matches.
	 */
	if ((bml->bml_flags & BML_WRITE) && !bmi->bmdsi_writers) {
		if (b->bcm_flags & BMAP_MDS_NOION) {
			psc_assert(!bmi->bmdsi_assign);
			psc_assert(!bmi->bmdsi_wr_ion);

		} else {
			/* Bml's which have failed ion assignment shouldn't
			 *   be relevant to any odtable entry.
			 */
			if (!(bml->bml_flags & BML_ASSFAIL)) {
				/* odtable sanity checks:
				 */
				struct bmap_ios_assign bia;

				rc = mds_odtable_getitem(mdsBmapAssignTable,
				    bmi->bmdsi_assign, &bia, sizeof(bia));
				psc_assert(!rc &&
				   bia.bia_seq == bmi->bmdsi_seq);
				psc_assert(bia.bia_bmapno == b->bcm_bmapno);
				/* End sanity checks.
				 */
				atomic_dec(&bmi->bmdsi_wr_ion->rmmi_refcnt);
				odtr = bmi->bmdsi_assign;
				bmi->bmdsi_assign = NULL;
				bmi->bmdsi_wr_ion = NULL;
			}
		}
	}
	/* bmap_op_done_type(b, BMAP_OPCNT_IONASSIGN) may have released
	 *    the lock.
	 */
 out:
	BMAP_RLOCK(b);
	b->bcm_flags &= ~BMAP_IONASSIGN;
	bcm_wake_locked(b);
	bmap_op_done_type(b, BMAP_OPCNT_LEASE);

	if (odtr) {
		key = odtr->odtr_key;
		elem = odtr->odtr_elem;
		rc = mds_odtable_freeitem(mdsBmapAssignTable, odtr);
		DEBUG_BMAP(PLL_NOTIFY, b, "odtable remove seq=%"PRId64" "
		    "key=%#"PRIx64" rc=%d", bml->bml_seq, key, rc);
		bmap_op_done_type(b, BMAP_OPCNT_IONASSIGN);

		mds_reserve_slot(1);
		logentry = pjournal_get_buf(mdsJournal,
		    sizeof(*logentry));
		logentry->sjar_elem = elem;
		logentry->sjar_flags = SLJ_ASSIGN_REP_FREE;
		pjournal_add_entry(mdsJournal, 0, MDS_LOG_BMAP_ASSIGN,
		    0, logentry, sizeof(*logentry));
		pjournal_put_buf(mdsJournal, logentry);
		mds_unreserve_slot(1);
	}

	psc_pool_return(bmapMdsLeasePool, bml);

	return (rc);
}

/**
 * mds_handle_rls_bmap - Handle SRMT_RELEASEBMAP RPC from a client or an
 *	I/O server.
 */
int
mds_handle_rls_bmap(struct pscrpc_request *rq, int sliod)
{
	struct srm_bmap_release_req *mq;
	struct srm_bmap_release_rep *mp;
	struct bmap_mds_lease *bml;
	struct srt_bmapdesc *sbd;
	struct slash_fidgen fg;
	struct fidc_membh *f;
	struct bmapc_memb *b;
	uint32_t i;

	SL_RSX_ALLOCREP(rq, mq, mp);
	if (mq->nbmaps > MAX_BMAP_RELEASE) {
		mp->rc = EINVAL;
		return (0);
	}

	for (i = 0; i < mq->nbmaps; i++) {
		sbd = &mq->sbd[i];

		fg.fg_fid = sbd->sbd_fg.fg_fid;
		fg.fg_gen = 0;

		if (slm_fcmh_get(&fg, &f))
			continue;

		DEBUG_FCMH(PLL_INFO, f, "rls bmap=%u", sbd->sbd_bmapno);

		if (bmap_lookup(f, sbd->sbd_bmapno, &b))
			goto next;

		BMAP_LOCK(b);
		bml = mds_bmap_getbml(b, sbd);

		DEBUG_BMAP((bml ? PLL_INFO : PLL_WARN), b,
		   "release %"PRId64" nid=%"PRId64" pid=%u bml=%p",
		   sbd->sbd_seq, sbd->sbd_nid, sbd->sbd_pid, bml);
		if (bml) {
			psc_assert(sbd->sbd_seq == bml->bml_seq);
			BML_LOCK(bml);
			if (!(bml->bml_flags & BML_FREEING)) {
				bml->bml_flags |= BML_FREEING;
				BML_ULOCK(bml);
				(int)mds_bmap_bml_release(bml);
			} else
				BML_ULOCK(bml);
		}
		/* bmap_op_done_type will drop the lock.
		 */
		bmap_op_done_type(b, BMAP_OPCNT_LOOKUP);
 next:
		fcmh_op_done_type(f, FCMH_OPCNT_LOOKUP_FIDC);
	}
	return (0);
}

static struct bmap_mds_lease *
mds_bml_new(struct bmapc_memb *b, struct pscrpc_export *e, int flags,
	       lnet_process_id_t *cnp)
{
	struct bmap_mds_lease *bml;

	bml = psc_pool_get(bmapMdsLeasePool);
	memset(bml, 0, sizeof(*bml));

	INIT_PSC_LISTENTRY(&bml->bml_bmdsi_lentry);
	INIT_PSC_LISTENTRY(&bml->bml_timeo_lentry);
	INIT_PSC_LISTENTRY(&bml->bml_exp_lentry);
	INIT_PSC_LISTENTRY(&bml->bml_coh_lentry);
	INIT_SPINLOCK(&bml->bml_lock);

	bml->bml_exp = e;
	bml->bml_bmdsi = bmap_2_bmi(b);
	bml->bml_flags = flags;
	bml->bml_cli_nidpid = *cnp;
	bml->bml_start = time(NULL);
	bml->bml_expire = bml->bml_start + BMAP_TIMEO_MAX;

	return (bml);
}

void
mds_bia_odtable_startup_cb(void *data, struct odtable_receipt *odtr)
{
	struct fidc_membh *f = NULL;
	struct bmapc_memb *b = NULL;
	struct bmap_ios_assign *bia;
	struct bmap_mds_lease *bml;
	struct slash_fidgen fg;
	struct sl_resm *resm;
	int rc;

	bia = data;

	resm = libsl_ios2resm(bia->bia_ios);

	psclog_debug("fid="SLPRI_FID" seq=%"PRId64" res=(%s) bmapno=%u",
	    bia->bia_fid, bia->bia_seq, resm->resm_res->res_name,
	    bia->bia_bmapno);

	if (!bia->bia_fid) {
		psclog_warnx("found fid #0 in odtable");
		rc = -EINVAL;
		goto out;
	}

	fg.fg_fid = bia->bia_fid;
	fg.fg_gen = FGEN_ANY;

	/*
	 * Because we don't revoke leases when an unlink comes in, ENOENT
	 * is actually legitimate after a crash.
	 */
	rc = slm_fcmh_get(&fg, &f);
	if (rc) {
		psclog_errorx("failed to load: item=%zd, fid="SLPRI_FID,
		    odtr->odtr_elem, fg.fg_fid);
		goto out;
	}

	rc = mds_bmap_load(f, bia->bia_bmapno, &b);
	if (rc) {
		DEBUG_FCMH(PLL_ERROR, f, "failed to load bmap %u (rc=%d)",
		    bia->bia_bmapno, rc);
		goto out;
	}

	bml = mds_bml_new(b, NULL, (BML_WRITE | BML_RECOVER),
		  &bia->bia_lastcli);

	bml->bml_seq = bia->bia_seq;
	bml->bml_ios = bia->bia_ios;
	/* Taking the lease origination time in this manner leaves
	 *    us suscpetible to gross changes in the system time.
	 */
	bml->bml_start = bia->bia_start;
	/* Grant recovered leases some additional time.
	 */
	bml->bml_expire = time(NULL) + BMAP_RECOVERY_TIMEO_EXT;

	if (bia->bia_flags & BIAF_DIO) {
		bml->bml_flags |= BML_CDIO;
		b->bcm_flags |= BMAP_DIO;
	}

	bmap_2_bmi(b)->bmdsi_assign = odtr;

	rc = mds_bmap_bml_add(bml, SL_WRITE, IOS_ID_ANY);
	if (rc) {
		bmap_2_bmi(b)->bmdsi_assign = NULL;
		bml->bml_flags |= BML_FREEING;
		mds_bmap_bml_release(bml);
	}

 out:
	if (rc)
		mds_odtable_freeitem(mdsBmapAssignTable, odtr);
	if (f)
		fcmh_op_done_type(f, FCMH_OPCNT_LOOKUP_FIDC);
	if (b)
		bmap_op_done_type(b, BMAP_OPCNT_LOOKUP);
}

/**
 * mds_bmap_crc_write - Process a CRC update request from an ION.
 * @c: the RPC request containing the FID, bmapno, and chunk ID (cid).
 * @ios:  the IOS ID of the I/O node which sent the request.  It is
 *	compared against the ID stored in the bml
 */
int
mds_bmap_crc_write(struct srm_bmap_crcup *c, sl_ios_id_t ios,
    const struct srm_bmap_crcwrt_req *mq)
{
	struct bmapc_memb *bmap = NULL;
	struct bmap_mds_info *bmi;
	struct fidc_membh *fcmh;
	struct sl_resource *res = libsl_id2res(ios);
	int rc;

	rc = slm_fcmh_get(&c->fg, &fcmh);
	if (rc) {
		if (rc == ENOENT) {
			psclog_warnx("fid="SLPRI_FID" appears to have "
			    "been deleted", c->fg.fg_fid);
			return (0);
		}
		psclog_errorx("fid="SLPRI_FID" slm_fcmh_get() rc=%d",
		    c->fg.fg_fid, rc);
		return (-rc);
	}

	/* Ignore updates from old or invalid generation numbers.
	 * XXX XXX fcmh is not locked here
	 */
	FCMH_LOCK(fcmh);
	if (fcmh_2_gen(fcmh) != c->fg.fg_gen) {
		int x = (fcmh_2_gen(fcmh) > c->fg.fg_gen) ? 1 : 0;

		DEBUG_FCMH(x ? PLL_NOTICE : PLL_ERROR, fcmh,
		   "mds gen (%"PRIu64") %s than crcup gen (%"PRIu64")",
		   fcmh_2_gen(fcmh), x ? ">" : "<", c->fg.fg_gen);

		rc = -(x ? SLERR_GEN_OLD : SLERR_GEN_INVALID);
		FCMH_ULOCK(fcmh);
		goto out;
	}
	FCMH_ULOCK(fcmh);

	/* BMAP_OP #2
	 * XXX are we sure after restart bmap will be loaded?
	 */
	rc = bmap_lookup(fcmh, c->blkno, &bmap);
	if (rc) {
		DEBUG_FCMH(PLL_ERROR, fcmh, "failed lookup bmap(%u) rc=%d",
		    c->blkno, rc);
		rc = -EBADF;
		goto out;
	}
	BMAP_LOCK(bmap);

	DEBUG_BMAP(PLL_INFO, bmap, "blkno=%u sz=%"PRId64" ios(%s)",
	    c->blkno, c->fsize, res->res_name);

	psc_assert(psc_atomic32_read(&bmap->bcm_opcnt) > 1);

	bmi = bmap_2_bmi(bmap);
	/* These better check out.
	 */
	psc_assert(bmap->bcm_fcmh == fcmh);
	psc_assert(bmi);

	if (!bmi->bmdsi_wr_ion ||
	    ios != bmi->bmdsi_wr_ion->rmmi_resm->resm_res->res_id) {
		/* Whoops, we recv'd a request from an unexpected NID.
		 */
		rc = -EINVAL;
		BMAP_ULOCK(bmap);
		goto out;

	} else if (bmap->bcm_flags & BMAP_MDS_CRC_UP) {
		/* Ensure that this thread is the only thread updating the
		 *  bmap crc table.  XXX may have to replace this with a waitq
		 */
		rc = -EALREADY;
		BMAP_ULOCK(bmap);

		DEBUG_BMAP(PLL_ERROR, bmap,
		    "EALREADY blkno=%u sz=%"PRId64" ios(%s)",
		    c->blkno, c->fsize, res->res_name);

		DEBUG_FCMH(PLL_ERROR, fcmh,
		    "EALREADY blkno=%u sz=%"PRId64" ios(%s)",
		    c->blkno, c->fsize, res->res_name);
		goto out;

	} else {
		/* Mark that bmap is undergoing CRC updates - this is non-
		 *  reentrant so the ION must know better than to send
		 *  multiple requests for the same bmap.
		 */
		bmap->bcm_flags |= BMAP_MDS_CRC_UP;
	}
	BMAP_ULOCK(bmap);

	/* Call the journal and update the in-memory CRCs.
	 */
	mds_bmap_crc_update(bmap, c);

	if (mq->flags & SRM_BMAPCRCWRT_PTRUNC) {
		struct slash_inode_handle *ih;
		int iosidx, tract[NBREPLST];
		struct slm_workrq *wkrq;
		uint32_t bpol;

		ih = fcmh_2_inoh(fcmh);
		iosidx = mds_repl_ios_lookup(ih,
		    bmi->bmdsi_wr_ion->rmmi_resm->resm_res_id);
		if (iosidx < 0)
			psclog_errorx("ios not found");
		else {
			BMAPOD_MODIFY_START(bmap);

			brepls_init(tract, -1);
			tract[BREPLST_TRUNCPNDG] = BREPLST_VALID;
			tract[BREPLST_TRUNCPNDG_SCHED] = BREPLST_VALID;
			mds_repl_bmap_apply(bmap, tract, NULL,
			    SL_BITS_PER_REPLICA * iosidx);

			BHREPL_POLICY_GET(bmap, &bpol);

			brepls_init(tract, -1);
			tract[BREPLST_TRUNCPNDG] = bpol == BRPOL_PERSIST ?
			    BREPLST_REPL_QUEUED : BREPLST_GARBAGE;
			mds_repl_bmap_walk(bmap, tract, NULL,
			    REPL_WALKF_MODOTH, &iosidx, 1);

			BMAPOD_MODIFY_DONE(bmap);

			/*
			 * XXX modify all bmaps after this one and mark
			 * them INVALID since the sliod will have zeroed
			 * those regions.
			 */

			psc_multiwaitcond_wakeup(
			    &site2smi(bmi->bmdsi_wr_ion->rmmi_resm->
			    resm_site)->smi_mwcond);
		}

		FCMH_LOCK(fcmh);
		fcmh->fcmh_flags &= ~FCMH_IN_PTRUNC;
		fcmh_wake_locked(fcmh);
		FCMH_ULOCK(fcmh);

		wkrq = psc_pool_get(slm_workrq_pool);
		fcmh_op_start_type(fcmh, FCMH_OPCNT_WORKER);
		wkrq->wkrq_fcmh = fcmh;
		wkrq->wkrq_cbf = slm_ptrunc_wake_clients;
		slm_ptrunc_wake_clients(wkrq);
	}

	if (fcmh->fcmh_sstb.sst_mode & (S_ISGID | S_ISUID)) {
		struct srt_stat sstb;

		FCMH_LOCK(fcmh);
		fcmh_wait_locked(fcmh, fcmh->fcmh_flags &
		    FCMH_IN_SETATTR);
		sstb.sst_mode = fcmh->fcmh_sstb.sst_mode &
		    ~(S_ISGID | S_ISUID);
		mds_fcmh_setattr_nolog(fcmh, PSCFS_SETATTRF_MODE,
		    &sstb);
		FCMH_ULOCK(fcmh);
	}

 out:
	/*
	 * Mark that mds_bmap_crc_write() is done with this bmap
	 *  - it was incref'd in fcmh_bmap_lookup().
	 */
	if (bmap)
		/* BMAP_OP #2, drop lookup ref
		 */
		bmap_op_done_type(bmap, BMAP_OPCNT_LOOKUP);

	fcmh_op_done_type(fcmh, FCMH_OPCNT_LOOKUP_FIDC);
	return (rc);
}

/**
 * mds_bmap_loadvalid - Load a bmap if disk I/O is successful and the bmap
 *	has been initialized (i.e. is not all zeroes).
 * @f: fcmh.
 * @bmapno: bmap index number to load.
 * @bp: value-result bmap pointer.
 * NOTE: callers must issue bmap_op_done() if mds_bmap_loadvalid() is
 *     successful.
 */
int
mds_bmap_loadvalid(struct fidc_membh *f, sl_bmapno_t bmapno,
    struct bmapc_memb **bp)
{
	struct bmapc_memb *b;
	int n, rc;

	*bp = NULL;

	/* BMAP_OP #3 via lookup
	 */
	rc = mds_bmap_load(f, bmapno, &b);
	if (rc)
		return (rc);

	BMAP_LOCK(b);
	for (n = 0; n < SLASH_CRCS_PER_BMAP; n++)
		/*
		 * XXX need a bitmap to see which CRCs are
		 * actually uninitialized and not just happen
		 * to be zero.
		 */
		if (b->bcm_crcstates[n]) {
			BMAP_ULOCK(b);
			*bp = b;
			return (0);
		}
	/* BMAP_OP #3, unref if bmap is empty.
	 *    NOTE that our callers must drop this ref.
	 */
	bmap_op_done_type(b, BMAP_OPCNT_LOOKUP);
	return (SLERR_BMAP_ZERO);
}

int
mds_bmap_load_fg(const struct slash_fidgen *fg, sl_bmapno_t bmapno,
    struct bmapc_memb **bp)
{
	struct fidc_membh *f;
	struct bmapc_memb *b;
	int rc = 0;

	psc_assert(*bp == NULL);

	f = fidc_lookup_fg(fg);
	if (!f)
		return (-ENOENT);

	rc = mds_bmap_load(f, bmapno, &b);
	if (rc == 0)
		*bp = b;

	fcmh_op_done_type(f, FCMH_OPCNT_LOOKUP_FIDC);
	return (rc);
}

/**
 * mds_bmap_load_cli - Routine called to retrieve a bmap, presumably so that
 *	it may be sent to a client.  It first checks for existence in
 *	the cache otherwise the bmap is retrieved from disk.
 *
 *	mds_bmap_load_cli() also manages the bmap_lease reference
 *	which is used to track the bmaps a particular client knows
 *	about.  mds_bmap_read() is used to retrieve the bmap from disk
 *	or create a new 'blank-slate' bmap if one does not exist.
 *	Finally, a read or write reference is placed on the bmap
 *	depending on the client request.  This is factored in with
 *	existing references to determine whether or not the bmap should
 *	be in DIO mode.
 * @f: the FID cache handle for the inode.
 * @bmapno: bmap index number.
 * @flags: bmap lease flags (BML_*).
 * @rw: read/write access to the bmap.
 * @prefios: client preferred I/O system ID.
 * @sbd: value-result bmap descriptor to pass back to client.
 * @exp: RPC export to client.
 * @bmap: value-result bmap.
 * Note: the bmap is not locked during disk I/O; instead it is marked
 *	with a bit (i.e. INIT) and other threads block on its waitq.
 */
int
mds_bmap_load_cli(struct fidc_membh *f, sl_bmapno_t bmapno, int flags,
    enum rw rw, sl_ios_id_t prefios, struct srt_bmapdesc *sbd,
    struct pscrpc_export *exp, struct bmapc_memb **bmap)
{
	struct slashrpc_cservice *csvc;
	struct bmap_mds_lease *bml;
	struct slm_exp_cli *mexpc;
	struct bmapc_memb *b;
	int rc;

	psc_assert(!*bmap);

	FCMH_LOCK(f);
	rc = (f->fcmh_flags & FCMH_IN_PTRUNC) &&
	    bmapno >= fcmh_2_fsz(f) / SLASH_BMAP_SIZE;
	FCMH_ULOCK(f);
	if (rc) {
		csvc = slm_getclcsvc(exp);
		FCMH_LOCK(f);
		if (csvc && (f->fcmh_flags & FCMH_IN_PTRUNC)) {
			psc_dynarray_add(&fcmh_2_fmi(f)->fmi_ptrunc_clients,
			    csvc);
			FCMH_ULOCK(f);
			return (SLERR_BMAP_IN_PTRUNC);
		}
		FCMH_ULOCK(f);
	}

	rc = mds_bmap_load(f, bmapno, &b);
	if (rc)
		return (rc);

	bml = mds_bml_new(b, exp,
	    ((rw == SL_WRITE ? BML_WRITE : BML_READ) |
	     (flags & SRM_LEASEBMAPF_DIRECTIO ? BML_CDIO : 0)),
	    &exp->exp_connection->c_peer);

	EXPORT_LOCK(exp);
	mexpc = sl_exp_getpri_cli(exp);
	bml->bml_flags |= BML_EXP;
	psclist_add_tail(&bml->bml_exp_lentry, &mexpc->mexpc_bmlhd);
	EXPORT_ULOCK(exp);

	rc = mds_bmap_bml_add(bml, rw, prefios);
	if (rc) {
		if (rc == -SLERR_BMAP_DIOWAIT) {
			EXPORT_LOCK(exp);
			psclist_del(&bml->bml_exp_lentry,
			    &mexpc->mexpc_bmlhd);
			EXPORT_ULOCK(exp);
			mds_bml_free(bml);
		} else {
			if (rc == -SLERR_ION_OFFLINE)
				bml->bml_flags |= BML_ASSFAIL;
			bml->bml_flags |= BML_FREEING;
			mds_bmap_bml_release(bml);
		}
		goto out;
	}
	*bmap = b;

	/* SLASH2 monotonic coherency sequence number assigned to this lease.
	 */
	sbd->sbd_seq = bml->bml_seq;
	/* Stash the odtable key if this is a write lease.
	 */
	sbd->sbd_key = (rw == SL_WRITE) ?
	    bml->bml_bmdsi->bmdsi_assign->odtr_key : BMAPSEQ_ANY;
	/* Store the nid/pid of the client interface in the bmapdesc
	 *   to deal properly deal with IONs on other LNETs.
	 */
	sbd->sbd_nid = exp->exp_connection->c_peer.nid;
	sbd->sbd_pid = exp->exp_connection->c_peer.pid;
 out:
	bmap_op_done_type(b, BMAP_OPCNT_LOOKUP);
	return (rc);
}

int
mds_lease_renew(struct fidc_membh *f, struct srt_bmapdesc *sbd_in,
		struct srt_bmapdesc *sbd_out, struct pscrpc_export *exp)
{
	struct bmap_mds_lease *bml, *obml;
	struct bmapc_memb *b;
	struct slm_exp_cli *mexpc;
	int rc, rw;

	rc = mds_bmap_load(f, sbd_in->sbd_bmapno, &b);
	if (rc)
		return (rc);

	/* Lookup the original lease to ensure it actually exists.
	 */
	BMAP_LOCK(b);
	obml = mds_bmap_getbml(b, sbd_in);
	BMAP_ULOCK(b);

	if (!obml) {
		rc = ENOENT;
		goto out;
	}

	rw = (sbd_in->sbd_ios == IOS_ID_ANY) ? BML_READ : BML_WRITE;
	bml = mds_bml_new(b, exp, rw, &exp->exp_connection->c_peer);

	EXPORT_LOCK(exp);
	mexpc = sl_exp_getpri_cli(exp);
	bml->bml_flags |= BML_EXP;
	psclist_add_tail(&bml->bml_exp_lentry, &mexpc->mexpc_bmlhd);
	EXPORT_ULOCK(exp);

	rc = mds_bmap_bml_add(bml, (rw == BML_READ ? SL_READ : SL_WRITE),
	      sbd_in->sbd_ios);
	if (rc) {
		if (rc == -SLERR_BMAP_DIOWAIT) {
			EXPORT_LOCK(exp);
			psclist_del(&bml->bml_exp_lentry,
			    &mexpc->mexpc_bmlhd);
			EXPORT_ULOCK(exp);
			mds_bml_free(bml);
		} else {
			if (rc == -SLERR_ION_OFFLINE)
				bml->bml_flags |= BML_ASSFAIL;
			bml->bml_flags |= BML_FREEING;
			mds_bmap_bml_release(bml);
		}
		goto out;
	}

	/* Do some post setup on the new lease.
	 */
	sbd_out->sbd_seq = bml->bml_seq;
	sbd_out->sbd_bmapno = b->bcm_bmapno;
	sbd_out->sbd_fg = b->bcm_fcmh->fcmh_fg;

	if (rw == BML_WRITE) {
		struct bmap_mds_info *bmi = bmap_2_bmi(b);

		psc_assert(bmi->bmdsi_wr_ion);

		sbd_out->sbd_key = bml->bml_bmdsi->bmdsi_assign->odtr_key;
		sbd_out->sbd_ios =
			bmi->bmdsi_wr_ion->rmmi_resm->resm_res->res_id;
	} else {
		sbd_out->sbd_key = BMAPSEQ_ANY;
		sbd_out->sbd_ios = IOS_ID_ANY;
	}

	BMAP_LOCK(b);
	if (b->bcm_flags & BMAP_DIO)
		sbd_out->sbd_flags |= SRM_LEASEBMAPF_DIRECTIO;
	BMAP_ULOCK(b);

	/* By this point it should be safe to ignore the error from
	 *   mds_bmap_bml_release() since a new lease has already been
	 *   issued.
	 */
	BML_LOCK(obml);
	if (!(bml->bml_flags & BML_FREEING)) {
		obml->bml_flags |= BML_FREEING;
		BML_ULOCK(obml);
		(int)mds_bmap_bml_release(obml);
	} else
		BML_ULOCK(obml);

	DEBUG_BMAP(PLL_INFO, b,
		   "renew seq=%"PRId64" nid=%"PRId64" pid=%u",
		   bml->bml_seq, exp->exp_connection->c_peer.nid,
		   exp->exp_connection->c_peer.pid);
 out:
	bmap_op_done_type(b, BMAP_OPCNT_LOOKUP);
	return (rc);
}

void
slm_setattr_core(struct fidc_membh *fcmh, struct srt_stat *sstb,
    int to_set)
{
	int locked, deref = 0, rc = 0;
	struct slm_workrq *wkrq;
	struct fcmh_mds_info *fmi;

	if ((to_set & PSCFS_SETATTRF_DATASIZE) && sstb->sst_size) {
		if (fcmh == NULL) {
			rc = slm_fcmh_get(&sstb->sst_fg, &fcmh);
			if (rc) {
				psclog_errorx("unable to retrieve FID "
				    SLPRI_FID": %s",
				    sstb->sst_fid, slstrerror(rc));
				return;
			}

			deref = 1;
		}

		locked = FCMH_RLOCK(fcmh);
		fcmh->fcmh_flags |= FCMH_IN_PTRUNC;
		fmi = fcmh_2_fmi(fcmh);
		fmi->fmi_ptrunc_size = sstb->sst_size;
		FCMH_URLOCK(fcmh, locked);

		wkrq = psc_pool_get(slm_workrq_pool);
		fcmh_op_start_type(fcmh, FCMH_OPCNT_WORKER);
		wkrq->wkrq_fcmh = fcmh;
		wkrq->wkrq_cbf = slm_ptrunc_prepare;
		lc_add(&slm_workq, wkrq);

		if (deref)
			fcmh_op_done_type(fcmh, FCMH_OPCNT_LOOKUP_FIDC);
	}
}

struct ios_list {
	sl_replica_t	iosv[SL_MAX_REPLICAS];
	int		nios;
};

__static void
ptrunc_tally_ios(struct bmapc_memb *bcm, int iosidx, int val, void *arg)
{
	struct ios_list *ios_list = arg;
	sl_ios_id_t ios_id;
	int i;

	switch (val) {
	case BREPLST_VALID:
	case BREPLST_REPL_SCHED:
	case BREPLST_REPL_QUEUED:
		break;
	default:
		return;
	}

	ios_id = bmap_2_repl(bcm, iosidx);

	for (i = 0; i < ios_list->nios; i++)
		if (ios_list->iosv[i].bs_id == ios_id)
			return;

	ios_list->iosv[ios_list->nios++].bs_id = ios_id;
}

int
slm_ptrunc_prepare(struct slm_workrq *wkrq)
{
	int to_set, rc, wait = 0;
	struct srm_bmap_release_req *mq;
	struct srm_bmap_release_rep *mp;
	struct slashrpc_cservice *csvc;
	struct bmap_mds_lease *bml;
	struct pscrpc_request *rq;
	struct fcmh_mds_info *fmi;
	struct psc_dynarray *da;
	struct fidc_membh *fcmh;
	struct bmapc_memb *b;
	sl_bmapno_t i;

	fcmh = wkrq->wkrq_fcmh;
	fmi = fcmh_2_fmi(fcmh);
	da = &fcmh_2_fmi(fcmh)->fmi_ptrunc_clients;

	/*
	 * Wait until any leases for this or any bmap after have been
	 * relinquished.
	 */
	FCMH_LOCK(fcmh);

	/*
	 * XXX bmaps issued in the wild not accounted for in fcmh_fsz
	 * are skipped here.
	 */
	if (fmi->fmi_ptrunc_size >= fcmh_2_fsz(fcmh)) {
		fcmh_wait_locked(fcmh, fcmh->fcmh_flags & FCMH_IN_SETATTR);
		if (fmi->fmi_ptrunc_size > fcmh_2_fsz(fcmh)) {
			struct srt_stat sstb;

			sstb.sst_size = fmi->fmi_ptrunc_size;
			mds_fcmh_setattr_nolog(fcmh,
			    PSCFS_SETATTRF_DATASIZE, &sstb);
		}
		fcmh->fcmh_flags &= ~FCMH_IN_PTRUNC;
		fcmh_op_done_type(fcmh, FCMH_OPCNT_WORKER);
		slm_ptrunc_wake_clients(wkrq);
		return (0);
	}

	i = fmi->fmi_ptrunc_size / SLASH_BMAP_SIZE;
	FCMH_ULOCK(fcmh);
	for (;; i++) {
		if (bmap_getf(fcmh, i, SL_WRITE, BMAPGETF_LOAD |
		    BMAPGETF_NOAUTOINST, &b))
			break;

		BMAP_LOCK(b);
		BMAP_FOREACH_LEASE(b, bml) {
			BMAP_ULOCK(b);

			csvc = slm_getclcsvc(bml->bml_exp);
			if (csvc == NULL)
				continue;
			if (!psc_dynarray_exists(da, csvc) &&
			    SL_RSX_NEWREQ(csvc, SRMT_RELEASEBMAP, rq,
			    mq, mp) == 0) {
				mq->sbd[0].sbd_fg.fg_fid = fcmh_2_fid(fcmh);
				mq->sbd[0].sbd_bmapno = i;
				mq->nbmaps = 1;
				SL_RSX_WAITREP(csvc, rq, mp);
				pscrpc_req_finished(rq);

				FCMH_LOCK(fcmh);
				psc_dynarray_add(da, csvc);
				FCMH_ULOCK(fcmh);
			} else
				sl_csvc_decref(csvc);

			BMAP_LOCK(b);
		}
		if (pll_nitems(&bmap_2_bmi(b)->bmdsi_leases))
			wait = 1;
		bmap_op_done_type(b, BMAP_OPCNT_LOOKUP);
	}
	if (wait)
		return (1);

	/* all client leases have been relinquished */
	/* XXX: wait for any CRC updates coming from sliods */

	FCMH_LOCK(fcmh);
	to_set = PSCFS_SETATTRF_DATASIZE | SL_SETATTRF_PTRUNCGEN;
	fcmh_2_ptruncgen(fcmh)++;
	fcmh->fcmh_sstb.sst_size = fmi->fmi_ptrunc_size;

	mds_reserve_slot(1);
	rc = mdsio_setattr(fcmh_2_mdsio_fid(fcmh), &fcmh->fcmh_sstb,
	    to_set, &rootcreds, &fcmh->fcmh_sstb,
	    fcmh_2_mdsio_data(fcmh), mdslog_namespace);
	mds_unreserve_slot(1);

	FCMH_ULOCK(fcmh);
	slm_ptrunc_apply(wkrq);
	return (0);
}

void
slm_ptrunc_apply(struct slm_workrq *wkrq)
{
	int rc, done = 0, tract[NBREPLST];
	struct up_sched_work_item *uswi;
	struct ios_list ios_list;
	struct fidc_membh *fcmh;
	struct bmapc_memb *b;
	sl_bmapno_t i;

	fcmh = wkrq->wkrq_fcmh;

	brepls_init(tract, -1);
	tract[BREPLST_VALID] = BREPLST_TRUNCPNDG;

	ios_list.nios = 0;

	i = fcmh_2_fsz(fcmh) / SLASH_BMAP_SIZE;
	if (fcmh_2_fsz(fcmh) % SLASH_BMAP_SIZE) {
		/*
		 * If a bmap sliver was sliced, we must await for a
		 * sliod to reply with the new CRC.
		 */
		if (mds_bmap_load(fcmh, i, &b) == 0) {
			mds_repl_bmap_walkcb(b, tract, NULL, 0,
			    ptrunc_tally_ios, &ios_list);
			mds_bmap_write_repls_rel(b);
		}
		i++;
	} else {
		done = 1;
	}

	brepls_init(tract, -1);
	tract[BREPLST_REPL_SCHED] = BREPLST_GARBAGE;
	tract[BREPLST_VALID] = BREPLST_GARBAGE;

	for (;; i++) {
		if (bmap_getf(fcmh, i, SL_WRITE, BMAPGETF_LOAD |
		    BMAPGETF_NOAUTOINST, &b))
			break;

		BHGEN_INCREMENT(b);
		mds_repl_bmap_walkcb(b, tract, NULL, 0,
		    ptrunc_tally_ios, &ios_list);
		mds_bmap_write_repls_rel(b);
	}

	rc = uswi_findoradd(&fcmh->fcmh_fg, &uswi);
	if (rc)
		psc_fatalx("uswi_findoradd: %s",
		    slstrerror(rc));
	uswi_enqueue_sites(uswi, ios_list.iosv, ios_list.nios);
	uswi_unref(uswi);

	if (done) {
		FCMH_LOCK(fcmh);
		fcmh->fcmh_flags &= ~FCMH_IN_PTRUNC;
		fcmh_wake_locked(fcmh);
		FCMH_ULOCK(fcmh);
		slm_ptrunc_wake_clients(wkrq);
	}

	fcmh_op_done_type(fcmh, FCMH_OPCNT_WORKER);
}

int
slm_ptrunc_wake_clients(struct slm_workrq *wkrq)
{
	struct slashrpc_cservice *csvc;
	struct srm_bmap_wake_req *mq;
	struct srm_bmap_wake_rep *mp;
	struct pscrpc_request *rq;
	struct fcmh_mds_info *fmi;
	struct fidc_membh *fcmh;
	struct psc_dynarray *da;
	int rc;

	fcmh = wkrq->wkrq_fcmh;
	fmi = fcmh_2_fmi(fcmh);
	da = &fmi->fmi_ptrunc_clients;
	FCMH_LOCK(fcmh);
	while (psc_dynarray_len(da)) {
		csvc = psc_dynarray_getpos(da, 0);
		psc_dynarray_remove(da, csvc);
		FCMH_ULOCK(fcmh);

		rc = SL_RSX_NEWREQ(csvc, SRMT_BMAP_WAKE, rq, mq, mp);
		if (rc == 0) {
			mq->fg = fcmh->fcmh_fg;
			mq->bmapno = fcmh_2_fsz(fcmh) / SLASH_BMAP_SIZE;
			SL_RSX_WAITREP(csvc, rq, mp);
			pscrpc_req_finished(rq);
		}
		sl_csvc_decref(csvc);

		FCMH_LOCK(fcmh);
	}
	fcmh_op_done_type(fcmh, FCMH_OPCNT_WORKER);
	return (0);
}
