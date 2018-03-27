/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2006-2018, Pittsburgh Supercomputing Center
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

#include "pfl/alloc.h"
#include "pfl/atomic.h"
#include "pfl/ctlsvr.h"
#include "pfl/export.h"
#include "pfl/fs.h"
#include "pfl/hashtbl.h"
#include "pfl/lockedlist.h"
#include "pfl/log.h"
#include "pfl/random.h"
#include "pfl/rpclog.h"
#include "pfl/rsx.h"
#include "pfl/tree.h"
#include "pfl/treeutil.h"
#include "pfl/workthr.h"

#include "bmap.h"
#include "bmap_mds.h"
#include "cache_params.h"
#include "fidc_mds.h"
#include "fidcache.h"
#include "inode.h"
#include "journal_mds.h"
#include "mdsio.h"
#include "mkfn.h"
#include "pathnames.h"
#include "repl_mds.h"
#include "rpc_mds.h"
#include "slashd.h"
#include "slashrpc.h"
#include "slerr.h"
#include "up_sched_res.h"

#include "zfs-fuse/zfs_slashlib.h"

#define	SLM_CBARG_SLOT_CSVC	0

struct pfl_odt		*slm_bia_odt;

int			slm_max_ios = SL_MAX_REPLICAS;

/*
 * Knobs that allow us to turn off some features easily if
 * they turn out to be unstable.
 */
int			slm_ptrunc_enabled = 1;
int			slm_preclaim_enabled = 1;

__static int slm_ptrunc_prepare(struct fidc_membh *, struct srt_stat *, int);

int
mds_bmap_exists(struct fidc_membh *f, sl_bmapno_t n)
{
	sl_bmapno_t nb;
	int locked;

	if (n == 0)
		return (1);

	locked = FCMH_RLOCK(f);
#if 0
	rc = mds_stat_refresh_locked(f);
	if (rc)
		return (rc);
#endif

	nb = fcmh_nvalidbmaps(f);

	/* XXX just read the bmap and check for valid state */

	psclog_debug("f+g="SLPRI_FG" nb=%u fsz=%"PSCPRIdOFFT,
	    SLPRI_FG_ARGS(&f->fcmh_fg), nb, fcmh_2_fsz(f));

	FCMH_URLOCK(f, locked);
	return (n < nb);
}

/*
 * Calculate the number of valid bytes in the bmap.
 */
int64_t
slm_bmap_calc_repltraffic(struct bmap *b)
{
	struct fidc_membh *f;
	sl_bmapno_t lastbno;
	int64_t amt;

	f = b->bcm_fcmh;
	lastbno = fcmh_nvalidbmaps(f);
	if (lastbno)
		lastbno--;

	/*
 	 * Relying on file size could be a problem if the file size
 	 * has not been updated yet.
 	 */
	if (b->bcm_bmapno == lastbno) {
		amt = fcmh_2_fsz(f) % SLASH_BMAP_SIZE;
		if (amt == 0)
			amt = SLASH_BMAP_SIZE;
	} else
		amt = SLASH_BMAP_SIZE;

	return (amt);
}

/*
 * Called when a new read or write lease is added to the bmap.
 * Maintains the DIO status of the bmap based on the numbers of readers
 * and writers present.
 * @b: the locked bmap
 * @rw: read / write op
 * @np: value-result target ION network + process ID.
 * Note: the new bml has yet to be added.
 */
__static int
mds_bmap_directio(struct bmap *b, enum rw rw, int want_dio,
    lnet_process_id_t *np)
{
	struct bmap_mds_info *bmi = bmap_2_bmi(b);
	struct bmap_mds_lease *bml, *tmp;
	int rc = 0, force_dio = 0;

	BMAP_LOCK_ENSURE(b);

	if (b->bcm_flags & BMAPF_DIO)
		return (0);

	/*
	 * We enter into DIO mode in three cases:
	 *
	 *  (1) Requester specifically wants a DIO lease (not currently
	 *	possible).
	 *  (2) There is already a write lease issued.
	 *  (3) Requester wants a write lease when there are existing
	 *	read leases.
	 *
	 * In addition, even if the current lease request does not
	 * trigger a DIO by itself, it has to wait if there is a DIO
	 * downgrade already in progress.
	 */
	if (bmi->bmi_diocb)
		want_dio = 1;

	if (want_dio || bmi->bmi_writers ||
	    (rw == SL_WRITE && bmi->bmi_readers)) {
		PLL_FOREACH(bml, &bmi->bmi_leases) {
			tmp = bml;
			do {

				/*
				 * A client can have more than one lease
				 * in flight even though it really uses
				 * one at any time.
				 */
				if (bml->bml_cli_nidpid.nid == np->nid &&
				    bml->bml_cli_nidpid.pid == np->pid)
					goto next;

				force_dio = 1;

				if (bml->bml_flags & BML_DIO)
					goto next;

				rc = -SLERR_BMAP_DIOWAIT;
				if (bml->bml_flags & BML_DIOCB)
					goto next;

				mdscoh_req(bml);

 next:
				bml = bml->bml_chain;
			} while (tmp != bml);
		}
	}
	if (!rc && (want_dio || force_dio)) {
		OPSTAT_INCR("bmap-dio-set");
		b->bcm_flags |= BMAPF_DIO;
	}
	return (rc);
}

__static void 
mds_bmap_ios_restart(struct bmap_mds_lease *bml)
{
	struct sl_resm *resm = libsl_ios2resm(bml->bml_ios);
	struct resm_mds_info *rmmi;

	rmmi = resm2rmmi(resm);

	psc_assert(bml->bml_bmi->bmi_assign);

	if (bml->bml_bmi->bmi_wr_ion) {
		/*
		 * Looks like we somehow end up with two write leases on
		 * the same bmap, but likely with different IOSes, hence
		 * the following assert triggers.  The bml_start value
		 * between the two lease exceeds 60000.
		 */
		psc_assert(bml->bml_bmi->bmi_wr_ion == rmmi);
	} else {
		psc_atomic32_inc(&rmmi->rmmi_refcnt);
		bml->bml_bmi->bmi_wr_ion = rmmi;
	}

	mds_bmap_timeotbl_mdsi(bml, BTE_REATTACH);

	if (bml->bml_seq > bml->bml_bmi->bmi_seq)
		bml->bml_bmi->bmi_seq = bml->bml_seq;

	DEBUG_BMAP(PLL_DIAG, bml_2_bmap(bml), "res(%s) seq=%"PRIx64,
	    resm->resm_res->res_name, bml->bml_seq);

}

int
mds_sliod_alive(void *arg)
{
	struct sl_mds_iosinfo *si = arg;
	int ok = 0;

	if (si->si_lastcomm.tv_sec) {
		struct timespec a, b;

		clock_gettime(CLOCK_MONOTONIC, &a);
		b = si->si_lastcomm;
		b.tv_sec += CSVC_PING_INTV * 2;

		if (timespeccmp(&a, &b, <))
			ok = 1;
	}

	return (ok);
}

/*
 * Given an I/O resource, iterate through its members looking for one
 * which is suitable for assignment.
 * @res: the resource
 * @prev_ios:  list of previously assigned resources (used for
 *	reassignment requests by the client).
 * @nprev_ios: size of the list.
 * Notes:  Because of logical I/O systems like CNOS, we use
 *	'resm->resm_res_id' instead of 'res->res_id' since the former
 *	points at the real resm's identifier not the logical identifier.
 */
__static int
slm_try_sliodresm(struct sl_resm *resm, int repls)
{
	struct slrpc_cservice *csvc = NULL;
	struct sl_mds_iosinfo *si;
	int rc;

	psclog_info("trying res(%s)", resm->resm_res->res_name);

	/*
	 * Access the resm's res pointer to get around resources which
	 * are marked RES_ISCLUSTER().  resm_res always points back to
	 * the member's native resource and not to a logical resource
	 * like a CNOS.
	 */
	si = res2iosinfo(resm->resm_res);
	if (si->si_flags & SIF_DISABLE_LEASE) {
		OPSTAT_INCR("sliod-disable-lease");
		psclog_diag("res=%s skipped due to DISABLE_LEASE",
		    resm->resm_name);
		return (-SLERR_ION_READONLY);
	}
	/*
	 * If the IOS already has the block mapped, giving out a lease 
	 * might be okay because it does not necessarily increase disk
	 * usage.  In other words, SIF_DISABLE_ADVLEASE only affecting
	 * a write into a new block.
	 */
	if (!repls && (si->si_flags & SIF_DISABLE_ADVLEASE)) {
		OPSTAT_INCR("sliod-disable-advlease");
		psclog_diag("res=%s skipped due to DISABLE_ADVLEASE",
		    resm->resm_name);
		return (-SLERR_ION_READONLY);
	}

	csvc = slm_geticsvc(resm, NULL, CSVCF_NONBLOCK | CSVCF_NORECON,
	    NULL, 0);
	if (!csvc) {
		OPSTAT_INCR("sliod-offline");
		/* This sliod hasn't established a connection to us. */
		psclog_diag("res=%s skipped due to NULL csvc",
		    resm->resm_name);
		return (-SLERR_ION_OFFLINE);
	}

	rc = mds_sliod_alive(si);
	if (!rc) {
		OPSTAT_INCR("sliod-ping-awol");
		psclog_notice("res=%s skipped due to lastcomm",
		    resm->resm_name);
		return (-SLERR_ION_OFFLINE);
	}

	sl_csvc_decref(csvc);

	return (0);
}

/*
 * Do proper shuffling to avoid statistical bias when some IOS are
 * offline, which would give unfair advantage to the first IOS that 
 * was online if we simply filled this list in sequentially.
 */
void
slm_res_shuffle(struct psc_dynarray *a, int begin)
{
	int i;

	/* We never touch the element before begin */
	for (i = 1; i < psc_dynarray_len(a) - begin; i++)
		psc_dynarray_swap(a, begin + i, begin +
		    psc_random32u(i + 1));
}

__static void
slm_res_fillmembers(struct sl_resource *r, struct psc_dynarray *a,
    int shuffle)
{
	struct sl_resm *m;
	int begin, i;

	begin = psc_dynarray_len(a);
	DYNARRAY_FOREACH(m, i, &r->res_members)
		psc_dynarray_add_ifdne(a, m);

	if (shuffle)
		slm_res_shuffle(a, begin);
}

/*
 * Gather a list of I/O servers.  This is used by the IOS selector and
 * should the client's preferred choices as well as factor in other
 * considerations.
 *
 * XXX: Make sure that we don't give out dead IOS and possibly avoid
 * shuffling when there is only one IOS in the table.
 */
void
slm_get_ioslist(struct fidc_membh *f, sl_ios_id_t piosid,
    struct psc_dynarray *a)
{
	int i, nr, begin;
	struct sl_resource *pios, *r;

	pios = libsl_id2res(piosid);
	if (!pios || (!RES_ISFS(pios) && !RES_ISCLUSTER(pios)))
		return;

	if (slm_max_ios < 1)
		slm_max_ios = 1;
	if (slm_max_ios > SL_MAX_REPLICAS)
		slm_max_ios = SL_MAX_REPLICAS;

	nr = fcmh_2_nrepls(f);
	if (nr < slm_max_ios) {

		/* If affinity, prefer the first resm from the reptbl. */
		if (fcmh_2_inoh(f)->inoh_flags & INOF_IOS_AFFINITY) {
			r = libsl_id2res(fcmh_getrepl(f, 0).bs_id);
			if (r)
				slm_res_fillmembers(r, a, 1);
		}
		/*
		 * Add the preferred IOS member(s) next.  Note that PIOS
		 * may be a CNOS, parallel IOS, or stand-alone.
		 */
		slm_res_fillmembers(pios, a, 1);
		begin = psc_dynarray_len(a);

		/*
		 * Add everything else.  Archival are not considered and 
		 * must be specifically set via PREF_IOS to obtain write 
		 * leases.
		 */
		DYNARRAY_FOREACH(r, i, &nodeSite->site_resources) {
			if (!RES_ISFS(r) || r == pios ||
			    r->res_type == SLREST_ARCHIVAL_FS)
				continue;

			slm_res_fillmembers(r, a, 0);
		}

	} else {

		OPSTAT_INCR("reuse-iolist");

		begin = psc_dynarray_len(a);
		for (i = 0; i < slm_max_ios; i++) {
			r = libsl_id2res(fcmh_getrepl(f, i).bs_id);
			if (r)
				slm_res_fillmembers(r, a, 0);
		}
	}
	slm_res_shuffle(a, begin);
}

/*
 * Choose an I/O resource member for write bmap lease assignment.
 * @b: The bmap which is being leased.
 * @pios: The preferred I/O resource specified by the client.
 * @to_skip: IONs to skip
 * @nskip: # IONS in @to_skip.
 * Notes:  This call accounts for the existence of existing replicas.
 *	When found, slm_resm_select() must choose a replica which is
 *	marked as BREPLST_VALID.
 */
__static int
slm_resm_select(struct bmap *b, sl_ios_id_t pios, sl_ios_id_t *to_skip,
    int nskip, struct sl_resm **resmp)
{
	int rc = -SLERR_ION_OFFLINE;
	int i, j, skip, off, val, nr, repls = 0;
	struct bmap_mds_info *bmi = bmap_2_bmi(b);
	struct psc_dynarray a = DYNARRAY_INIT;
	struct fidc_membh *f = b->bcm_fcmh;
	struct sl_resm *resm = NULL;
	sl_ios_id_t ios;

	nr = fcmh_2_nrepls(f);

	/* XXX if CRC check fails, we could end up with NULL inoh_extras */
	if (nr > SL_DEF_REPLICAS) {
		rc = mds_inox_ensure_loaded(fcmh_2_inoh(f));
		if (rc)
			goto out;
	}

	j = 0;
	for (i = 0, off = 0; i < nr; i++, off += SL_BITS_PER_REPLICA) {
		val = SL_REPL_GET_BMAP_IOS_STAT(bmi->bmi_repls, off);

		if (val != BREPLST_VALID)
			continue;

		repls++;
		ios = fcmh_getrepl(f, i).bs_id;
		resm = libsl_try_ios2resm(ios);
		if (resm) {
			j++;
			psc_dynarray_add(&a, resm);
			/*
 			 * When we have multiple copies, put preferred
 			 * I/O server up front to be selected first.
 			 */
			if (pios == ios && j > 2) {
				OPSTAT_INCR("bmap-pref-ios");
				psc_dynarray_swap(&a, 0, j-1);
			}
		}
	}

	if (nskip) {
		/*
 		 * This is because we already invalidate other bmaps 
 		 * when handing out a write lease.
 		 */
		if (repls != 1) {
			DEBUG_FCMH(PLL_WARN, f,
			    "invalid reassign req");
			DEBUG_BMAP(PLL_WARN, b,
			    "invalid reassign req (repls=%d)", repls);
			goto out;
		}

		/*
		 * Make sure the client had the resource ID which
		 * corresponds to that in the fcmh + bmap.
		 */
		resm = psc_dynarray_getpos(&a, 0);
		for (i = 0, ios = IOS_ID_ANY; i < nskip; i++)
			if (resm->resm_res_id == to_skip[i]) {
				ios = resm->resm_res_id;
				break;
			}

		if (ios == IOS_ID_ANY) {
			DEBUG_FCMH(PLL_WARN, f,
			    "invalid reassign req (res=%x)",
			    resm->resm_res_id);
			DEBUG_BMAP(PLL_WARN, b,
			    "invalid reassign req (res=%x)",
			    resm->resm_res_id);
			goto out;
		}
	}

	/*
 	 * If we have valid replicas, we must select one of them.
 	 * Otherwise, we can lose data.
 	 */
	if (!repls)
		slm_get_ioslist(f, pios, &a);

	DYNARRAY_FOREACH(resm, i, &a) {
		for (j = 0, skip = 0; j < nskip; j++)
			/* (gdb) p resm->resm_res->res_id */
			if (resm->resm_res_id == to_skip[j]) {
				skip = 1;
				psclog_notice("res=%s skipped due being a "
				    "prev_ios", resm->resm_name);
				break;
			}

		if (skip)
			continue;
		rc = slm_try_sliodresm(resm, repls);
		if (!rc) {
			*resmp = resm;
			break;
		}
	}

 out:
	psc_dynarray_free(&a);
	return (rc);
}

__static int
mds_bmap_add_repl(struct bmap *b, struct bmap_ios_assign *bia)
{
	struct slmds_jent_assign_rep *sjar;
	struct slmds_jent_bmap_assign *sjba;
	struct slash_inode_handle *ih;
	struct fidc_membh *f;
	uint32_t nrepls;
	int rc, iosidx;

	f = b->bcm_fcmh;
	ih = fcmh_2_inoh(f);
	nrepls = ih->inoh_ino.ino_nrepls;

	psc_assert(b->bcm_flags & BMAPF_IOSASSIGNED);

	FCMH_LOCK(f);
	iosidx = mds_repl_ios_lookup_add(current_vfsid, ih, bia->bia_ios);
	FCMH_ULOCK(f);

	if (iosidx < 0) {
		psclog_warnx("ios_lookup_add: ios=%d, iosidx=%d", 
		    bia->bia_ios, iosidx);
		return (iosidx);
	}

	BMAP_LOCK(b);
	/*
 	 * Here we assign a bmap as VALID even before a single byte
 	 * has been written to it. This might be a problem.
 	 *
 	 * If another client wants to read and write to the same 
 	 * bmap, the writing client should flush its dirty data first.
 	 * So we should be safe in theory.
 	 */
	rc = mds_repl_inv_except(b, iosidx);
	if (rc) {
		DEBUG_BMAP(PLL_ERROR, b, "mds_repl_inv_except() failed");
		BMAP_ULOCK(b);
		return (rc);
	}
	mds_reserve_slot(1);
	sjar = pjournal_get_buf(slm_journal, sizeof(*sjar));

	sjar->sjar_flags = SLJ_ASSIGN_REP_NONE;
	if (nrepls != ih->inoh_ino.ino_nrepls) {
		mdslogfill_ino_repls(f, &sjar->sjar_ino);
		sjar->sjar_flags |= SLJ_ASSIGN_REP_INO;
	}

	mdslogfill_bmap_repls(b, &sjar->sjar_rep);

	BMAP_ULOCK(b);

	sjar->sjar_flags |= SLJ_ASSIGN_REP_REP;

	sjba = &sjar->sjar_bmap;
	sjba->sjba_lastcli.nid = bia->bia_lastcli.nid;
	sjba->sjba_lastcli.pid = bia->bia_lastcli.pid;
	sjba->sjba_ios = bia->bia_ios;
	sjba->sjba_fid = bia->bia_fid;
	sjba->sjba_seq = bia->bia_seq;
	sjba->sjba_bmapno = bia->bia_bmapno;
	sjba->sjba_start = bia->bia_start;
	sjba->sjba_flags = bia->bia_flags;
	sjar->sjar_flags |= SLJ_ASSIGN_REP_BMAP;
	sjar->sjar_item = bmap_2_bmi(b)->bmi_assign;

	pjournal_add_entry(slm_journal, 0, MDS_LOG_BMAP_ASSIGN, 0, sjar,
	    sizeof(*sjar));
	pjournal_put_buf(slm_journal, sjar);
	mds_unreserve_slot(1);

	return (0);
}

/*
 * Bind a bmap to an ION for writing.  The process involves a
 * round-robin'ing of an I/O system's nodes and attaching a
 * resm_mds_info to the bmap, used for establishing connection to the
 * ION.
 * @bml: the bmap lease
 * @iosid: the preferred I/O system
 */
__static int
mds_bmap_ios_assign(struct bmap_mds_lease *bml, sl_ios_id_t iosid)
{
	int rc;
	size_t item;
	struct bmap *b = bml_2_bmap(bml);
	struct bmap_mds_info *bmi = bmap_2_bmi(b);
	struct resm_mds_info *rmmi;
	struct bmap_ios_assign *bia;
	struct sl_resm *resm;
	struct sl_resource *r;
	struct fidc_membh *f = b->bcm_fcmh;

	psc_assert(!bmi->bmi_wr_ion);
	psc_assert(!bmi->bmi_assign);
	psc_assert(psc_atomic32_read(&b->bcm_opcnt) > 0);

	rc = slm_resm_select(b, iosid, NULL, 0, &resm);
	if (rc) {
		r = libsl_id2res(iosid);
		psclog_warnx("Fail to contact IOS %#x (pref_ios=%s) "
		    "for lease, fid = "SLPRI_FID, iosid, 
		    r ? r->res_name : NULL, fcmh_2_fid(f)); 
		return (rc);
	}

	bmi->bmi_wr_ion = rmmi = resm2rmmi(resm);
	psc_atomic32_inc(&rmmi->rmmi_refcnt);

	DEBUG_BMAP(PLL_DIAG, b, "online res(%s)",
	    resm->resm_res->res_name);

	/*
	 * An ION has been assigned to the bmap, mark it in the odtable
	 * so that the assignment may be restored on reboot.
	 */
	item = pfl_odt_allocslot(slm_bia_odt);

	if (item == ODTBL_SLOT_INV) {
		DEBUG_BMAP(PLL_ERROR, b, "failed pfl_odt_allocslot()");
		return (-ENOMEM);
	}

	pfl_odt_allocitem(slm_bia_odt, (void **)&bia);

	bia->bia_ios = bml->bml_ios = rmmi2resm(rmmi)->resm_res_id;
	bia->bia_lastcli = bml->bml_cli_nidpid;
	bia->bia_fid = fcmh_2_fid(b->bcm_fcmh);
	bia->bia_seq = bmi->bmi_seq = mds_bmap_timeotbl_mdsi(bml, BTE_ADD);
	bia->bia_bmapno = b->bcm_bmapno;
	bia->bia_start = time(NULL);
	bia->bia_flags = (b->bcm_flags & BMAPF_DIO) ? BIAF_DIO : 0;

	bmi->bmi_assign = item;
	pfl_odt_putitem(slm_bia_odt, item, bia, 1);

	/* journalling happens in the following function */
	rc = mds_bmap_add_repl(b, bia);
	if (rc) {
		PSCFREE(bia);
		pfl_odt_freeitem(slm_bia_odt, item);
		return (rc);
	}

	bml->bml_seq = bia->bia_seq;

	DEBUG_FCMH(PLL_DIAG, b->bcm_fcmh, "bmap assign, item=%zd",
	    bmi->bmi_assign);
	DEBUG_BMAP(PLL_DIAG, b, "using res(%s) "
	    "rmmi(%p)", resm->resm_res->res_name, bmi->bmi_wr_ion);

	PSCFREE(bia);

	return (0);
}

__static int
mds_bmap_ios_update(struct bmap_mds_lease *bml)
{
	struct bmap *b = bml_2_bmap(bml);
	struct bmap_mds_info *bmi = bmap_2_bmi(b);
	struct bmap_ios_assign *bia;
	int rc, dio;

	OPSTAT_INCR("bmap-update");

	BMAP_LOCK(b);
	psc_assert(b->bcm_flags & BMAPF_IOSASSIGNED);
	dio = b->bcm_flags & BMAPF_DIO;
	BMAP_ULOCK(b);

	pfl_odt_getitem(slm_bia_odt, bmi->bmi_assign, &bia);
	if (bia->bia_fid != fcmh_2_fid(b->bcm_fcmh)) {
		DEBUG_BMAP(PLL_ERROR, b, "different fid="SLPRI_FID,
		   bia->bia_fid);
		PSCFREE(bia);
		return (-1); // errno
	}
	bia->bia_start = time(NULL);
	/*
 	 * 07/11/2017: Crash due to bmi->bmi_seq = -1.
 	 */
	psc_assert(bia->bia_seq == bmi->bmi_seq);
	bia->bia_seq = bmi->bmi_seq = mds_bmap_timeotbl_mdsi(bml, BTE_ADD);
	bia->bia_lastcli = bml->bml_cli_nidpid;
	bia->bia_flags = dio ? BIAF_DIO : 0;

	pfl_odt_replaceitem(slm_bia_odt, bmi->bmi_assign, bia);

	bml->bml_ios = bia->bia_ios;

	rc = mds_bmap_add_repl(b, bia);
	PSCFREE(bia);

	if (rc)
		return (rc);

	DEBUG_FCMH(PLL_DIAG, b->bcm_fcmh, "bmap update, item=%zd",
	    bmi->bmi_assign);

	return (0);
}

/*
 * Find the first lease of a given client based on its {nid, pid} pair.
 * Also walk the chain of duplicate leases to count the number of read
 * and write leases.  Note that only the first lease of a client is
 * linked on the bmi->bmi_leases list, the rest is linked on a private
 * chain and tagged with BML_CHAIN flag.
 */
static __inline struct bmap_mds_lease *
mds_bmap_dupls_find(struct bmap_mds_info *bmi, lnet_process_id_t *cnp,
    int *wlease, int *rlease)
{
	struct bmap_mds_lease *tmp, *bml = NULL;

	*rlease = 0;
	*wlease = 0;

	PLL_FOREACH(tmp, &bmi->bmi_leases) {
		if (tmp->bml_cli_nidpid.nid != cnp->nid ||
		    tmp->bml_cli_nidpid.pid != cnp->pid)
			continue;
		/* Only one lease per client is allowed on the list. */
		psc_assert(!bml);
		bml = tmp;
	}

	if (!bml)
		return (NULL);

	tmp = bml;
	do {
		/* All dup leases should be chained off the first bml. */
		if (tmp->bml_flags & BML_READ)
			(*rlease)++;
		else
			(*wlease)++;

		DEBUG_BMAP(PLL_DIAG, bmi_2_bmap(bmi), "bml=%p tmp=%p "
		    "(wlease=%d rlease=%d) (nwtrs=%d nrdrs=%d)",
		    bml, tmp, *wlease, *rlease,
		    bmi->bmi_writers, bmi->bmi_readers);

		tmp = tmp->bml_chain;
	} while (tmp != bml);

	return (bml);
}

/*
 * Attempt to upgrade a client-granted bmap lease from READ-only to
 * READ+WRITE.
 * @bml: bmap lease.
 * @prefios: client's preferred I/O system ID.
 */
int
mds_bmap_bml_chwrmode(struct bmap_mds_lease *bml, sl_ios_id_t prefios)
{
	int rc, wlease, rlease;
	struct bmap_mds_info *bmi;
	struct bmap *b;

	bmi = bml->bml_bmi;
	b = bmi_2_bmap(bmi);

	bmap_wait_locked(b, b->bcm_flags & BMAPF_IOSASSIGNED);
	b->bcm_flags |= BMAPF_IOSASSIGNED;

	DEBUG_BMAP(PLL_DIAG, b, "bml=%p bmi_writers=%d bmi_readers=%d",
	    bml, bmi->bmi_writers, bmi->bmi_readers);

	if (bml->bml_flags & BML_WRITE) {
		rc = -PFLERR_ALREADY;
		goto out;
	}

	rc = mds_bmap_directio(b, SL_WRITE, 0, &bml->bml_cli_nidpid);
	if (rc)
		goto out;

	BMAP_ULOCK(b);

	if (bmi->bmi_wr_ion)
		rc = mds_bmap_ios_update(bml);
	else
		rc = mds_bmap_ios_assign(bml, prefios);

	BMAP_LOCK(b);

	DEBUG_BMAP(PLL_DIAG, b, "bml=%p rc=%d "
	    "bmi_writers=%d bmi_readers=%d",
	    bml, rc, bmi->bmi_writers, bmi->bmi_readers);

	if (rc) {
		bml->bml_flags |= BML_FREEING;
		goto out;
	}
	psc_assert(bmi->bmi_wr_ion);

	mds_bmap_dupls_find(bmi, &bml->bml_cli_nidpid, &wlease, &rlease);

	/* Account for the read lease which is to be converted. */
	psc_assert(rlease);
	if (!wlease) {
		/*
		 * Only bump bmi_writers if no other write lease is
		 * still leased to this client.
		 */
		bmi->bmi_writers++;
		bmi->bmi_readers--;
	}
	bml->bml_flags &= ~BML_READ;
	bml->bml_flags |= BML_WRITE;

  out:
	if (rc)
		OPSTAT_INCR("bmap-chwrmode-ok");
	else
		OPSTAT_INCR("bmap-chwrmode-err");
	b->bcm_flags &= ~BMAPF_IOSASSIGNED;
	bmap_wake_locked(b);
	return (rc);
}

/*
 * Obtain the lease handle for a bmap denoted by the specified issued
 * sequence number.
 * @b: locked bmap.
 * @cli_nid: client network ID.
 * @cli_pid: client network process ID.
 * @seq: lease sequence.
 */
struct bmap_mds_lease *
mds_bmap_getbml(struct bmap *b, uint64_t seq, uint64_t nid, uint32_t pid)
{
	struct bmap_mds_lease *bml, *bml1, *bml2;
	struct bmap_mds_info *bmi;

	BMAP_LOCK_ENSURE(b);

	bml1 = NULL;
	bmi = bmap_2_bmi(b);
	PLL_FOREACH(bml, &bmi->bmi_leases) {
		if (bml->bml_cli_nidpid.nid != nid ||
		    bml->bml_cli_nidpid.pid != pid)
			continue;

		bml2 = bml;
		do {
			if (bml2->bml_seq == seq) {
				/*
				 * A lease won't go away with bmap lock
				 * taken.
				 */
				if (!(bml2->bml_flags & BML_FREEING)) {
					bml1 = bml2;
					bml1->bml_refcnt++;
				}
				goto out;
			}

			bml2 = bml2->bml_chain;
		} while (bml != bml2);
	}
 out:
	return (bml1);
}

/*
 * Add a read or write reference to the bmap's tree refcnts.  This also
 * calls into the directio_[check|set] calls depending on the number of
 * read and/or write clients of this bmap.
 * @bml: bmap lease.
 * @rw: read/write access for bmap.
 * @prefios: client preferred I/O system.
 */
__static int
mds_bmap_bml_add(struct bmap_mds_lease *bml, enum rw rw,
    sl_ios_id_t prefios)
{
	struct bmap_mds_info *bmi = bml->bml_bmi;
	struct bmap *b = bmi_2_bmap(bmi);
	struct bmap_mds_lease *obml;
	int rlease, wlease, rc = 0;

	psc_assert(bml->bml_cli_nidpid.nid &&
		   bml->bml_cli_nidpid.pid &&
		   bml->bml_cli_nidpid.nid != LNET_NID_ANY &&
		   bml->bml_cli_nidpid.pid != LNET_PID_ANY);

	bmap_wait_locked(b, b->bcm_flags & BMAPF_IOSASSIGNED);
	bmap_op_start_type(b, BMAP_OPCNT_LEASE);

	rc = mds_bmap_directio(b, rw, bml->bml_flags & BML_DIO,
	    &bml->bml_cli_nidpid);
	if (rc && (bml->bml_flags & BML_RECOVER))
		rc = 0;

	if (rc)
		/*
		 * 'rc' means that we're waiting on an async cb
		 * completion.
		 */
		goto out;

	obml = mds_bmap_dupls_find(bmi, &bml->bml_cli_nidpid, &wlease,
	    &rlease);

	DEBUG_BMAP(PLL_DIAG, b, "bml=%p obml=%p (wlease=%d rlease=%d) "
	    "(nwtrs=%d nrdrs=%d)", bml, obml, wlease, rlease,
	    bmi->bmi_writers, bmi->bmi_readers);

	if (obml) {
		struct bmap_mds_lease *tmp = obml;

		bml->bml_flags |= BML_CHAIN;
		/* Add ourselves to the end. */
		while (tmp->bml_chain != obml)
			tmp = tmp->bml_chain;

		tmp->bml_chain = bml;
		bml->bml_chain = obml;
		psc_assert(psclist_disjoint(&bml->bml_bmi_lentry));
	} else {
		/* First on the list. */
		bml->bml_chain = bml;
		pll_addtail(&bmi->bmi_leases, bml);
		PFLOG_BML(PLL_DIAG, bml, "added");
	}

	bml->bml_flags |= BML_BMI;

	if (rw == SL_READ) {
		if (!wlease && !rlease)
			bmi->bmi_readers++;
		mds_bmap_timeotbl_mdsi(bml, BTE_ADD);
	} else {
		/*
		 * Drop the lock prior to doing disk and possibly
		 * network I/O.
		 */
		b->bcm_flags |= BMAPF_IOSASSIGNED;

		/*
		 * For any given chain of leases, the
		 * bmi_[readers|writers] value may only be 1rd or 1wr.
		 * In the case where 2 wtrs are present, the value is
		 * 1wr.  Mixed readers and wtrs == 1wtr.  1-N rdrs, 1rd.
		 *
		 * Only increment writers if this is the first write
		 * lease from the respective client.
		 */
		if (!wlease) {
			/* This is the first write from the client. */
			bmi->bmi_writers++;

			if (rlease)
				/*
				 * Remove the read cnt; it has been
				 * superseded by the write.
				 */
				bmi->bmi_readers--;
		}

		if (bml->bml_flags & BML_RECOVER) {
			psc_assert(bmi->bmi_writers == 1);
			psc_assert(!bmi->bmi_readers);
			psc_assert(bml->bml_ios &&
			    bml->bml_ios != IOS_ID_ANY);
			BMAP_ULOCK(b);
			mds_bmap_ios_restart(bml);

		} else if (!wlease && bmi->bmi_writers == 1) {
			/*
			 * No duplicate lease detected and this client
			 * is the first writer.
			 *
			 * Need to confirm that we can reach here in 
			 * the case of extending a bmap lease. If so,
			 * we can probably get rid of reassign RPC.
			 */
			psc_assert(!bmi->bmi_wr_ion);
			BMAP_ULOCK(b);
			rc = mds_bmap_ios_assign(bml, prefios);

		} else {
			/* Possible duplicate and/or multiple writer. */
			psc_assert(bmi->bmi_wr_ion);
			BMAP_ULOCK(b);
			rc = mds_bmap_ios_update(bml);
		}

		BMAP_LOCK(b);
		b->bcm_flags &= ~BMAPF_IOSASSIGNED;
	}

 out:
	/*
	 * This used to print 8192 bytes of message and cause fprintf()
	 * to return -1 and we abort. So it was simplified.
	 */
	psclogs(rc && rc != -SLERR_BMAP_DIOWAIT ? PLL_WARN : PLL_DIAG,
	     SLSS_BMAP, "bno = %d, fid = "SLPRI_FID,
     	     b->bcm_bmapno, fcmh_2_fid(b->bcm_fcmh));

	/*
	 * On error, the caller will issue mds_bmap_bml_release() which
	 * deals with the gory details of freeing a fully, or
	 * partially, instantiated bml.  Therefore, BMAP_OPCNT_LEASE will
	 * not be removed in the case of an error.
	 */
	return (rc);
}

static void
mds_bmap_bml_del_locked(struct bmap_mds_lease *bml)
{
	struct bmap_mds_info *bmi = bml->bml_bmi;
	struct bmap_mds_lease *obml, *tail;
	int rlease = 0, wlease = 0;

	BMAP_LOCK_ENSURE(bmi_2_bmap(bmi));

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
	psc_assert(psclist_conjoint(&obml->bml_bmi_lentry,
	    psc_lentry_hd(&obml->bml_bmi_lentry)));

	/* Find the bml's preceding entry. */
	tail = obml;
	while (tail->bml_chain != bml)
		tail = tail->bml_chain;
	psc_assert(tail->bml_chain == bml);

	/* Manage the bml list and bml_bmi_lentry. */
	if (bml->bml_flags & BML_CHAIN) {
		psc_assert(psclist_disjoint(&bml->bml_bmi_lentry));
		psc_assert((wlease + rlease) > 1);
		tail->bml_chain = bml->bml_chain;

	} else {
		psc_assert(obml == bml);
		psc_assert(!(bml->bml_flags & BML_CHAIN));
		pll_remove(&bmi->bmi_leases, bml);
		PFLOG_BML(PLL_DIAG, bml, "removed");

		if ((wlease + rlease) > 1) {
			psc_assert(bml->bml_chain->bml_flags & BML_CHAIN);
			psc_assert(psclist_disjoint(
			    &bml->bml_chain->bml_bmi_lentry));

			bml->bml_chain->bml_flags &= ~BML_CHAIN;
			pll_addtail(&bmi->bmi_leases, bml->bml_chain);
			PFLOG_BML(PLL_DIAG, bml, "added");

			tail->bml_chain = bml->bml_chain;
		} else
			psc_assert(bml == bml->bml_chain);
	}

	if (bml->bml_flags & BML_WRITE) {
		if (wlease == 1) {
			psc_assert(bmi->bmi_writers > 0);
			bmi->bmi_writers--;

			DEBUG_BMAP(PLL_DIAG, bmi_2_bmap(bmi),
			    "bml=%p bmi_writers=%d bmi_readers=%d",
			    bml, bmi->bmi_writers, bmi->bmi_readers);

			if (rlease)
				bmi->bmi_readers++;
		}
	} else {
		psc_assert(bml->bml_flags & BML_READ);
		if (!wlease && (rlease == 1)) {
			psc_assert(bmi->bmi_readers > 0);
			bmi->bmi_readers--;
		}
	}
}

/*
 * Remove a bmap lease from the MDS.  This can be called from the
 * bmap_timeo thread, from a client bmap_release RPC, or from the
 * nbreqset cb context.
 * @bml: bmap lease.
 * Notes:  the bml must be removed from the timeotbl in all cases.
 *    otherwise we determine list removals on a case by case basis.
 */
int
mds_bmap_bml_release(struct bmap_mds_lease *bml)
{
	struct bmap *b = bml_2_bmap(bml);
	struct bmap_mds_info *bmi = bml->bml_bmi;
	struct fidc_membh *f = b->bcm_fcmh;
	int64_t item = 0;
	int rc = 0;

	/* On the last release, BML_FREEING must be set. */
	psc_assert(psc_atomic32_read(&b->bcm_opcnt) > 0);

	DEBUG_BMAP(PLL_DIAG, b, "bml=%p fl=%d seq=%"PRId64, bml,
	    bml->bml_flags, bml->bml_seq);

	/*
	 * BMAPF_IOSASSIGNED acts as a barrier for operations which may
	 * modify bmi_wr_ion.  Since ops associated with
	 * BMAPF_IOSASSIGNED do disk and net I/O, the spinlock is
	 * dropped.
	 *
	 * XXX actually, the bcm_lock is not dropped until the very end.
	 * If this becomes problematic we should investigate more.
	 * ATM BMAPF_IOSASSIGNED is not relied upon.
	 */
	BMAP_LOCK_ENSURE(b);
	bmap_wait_locked(b, b->bcm_flags & BMAPF_IOSASSIGNED);
	b->bcm_flags |= BMAPF_IOSASSIGNED;

	bml->bml_refcnt--;
	psc_assert(bml->bml_refcnt >= 0);
	if (bml->bml_refcnt > 0 || !(bml->bml_flags & BML_FREEING)) {
		b->bcm_flags &= ~BMAPF_IOSASSIGNED;
		bmap_wake_locked(b);
		BMAP_ULOCK(b);
		return (0);
	}

	if (bml->bml_flags & BML_TIMEOQ)
		mds_bmap_timeotbl_mdsi(bml, BTE_DEL);

	if (!(bml->bml_flags & BML_BMI))
		goto out;

	mds_bmap_bml_del_locked(bml);
	bml->bml_flags &= ~BML_BMI;

	/* Remove the direct I/O flag if possible. */
	if (b->bcm_flags & BMAPF_DIO &&
	    (!bmi->bmi_writers ||
	     (bmi->bmi_writers == 1 && !bmi->bmi_readers))) {
		b->bcm_flags &= ~BMAPF_DIO;
		OPSTAT_INCR("bmap-dio-clr");
	}

	/*
	 * Only release the odtable entry if the key matches.  If a
	 * match is found then verify the sequence number matches.
	 *
	 * bmi->bmi_writers is decremented in mds_bmap_bml_del_locked()
	 * above. Anyway, I don't understand why we allow more than one
	 * write lease on a bmap form a single client. In reality, I
	 * suspect bmi->bmi_writers will be zero all the time at this
	 * point. Not really, if obml is not NULL in renew path, we could
	 * have its value as 1.
	 */
	if ((bml->bml_flags & BML_WRITE) && !bmi->bmi_writers) {
		if (bmi->bmi_assign) {
			struct bmap_ios_assign *bia;

			pfl_odt_getitem(slm_bia_odt, bmi->bmi_assign, &bia);

			psc_assert(bia->bia_bmapno == b->bcm_bmapno);
			/*
 			 * Hit crash with bmapno of 13577, bia_bmapno = 336169404,
 			 * bmi_seq = -1, and bml_flags = 101000010.
 			 */
			if (bia->bia_seq !=  bmi->bmi_seq) {
				psclog_warnx("Mismatch seqno: %ld vs. %ld, "
				     "bno = %d, fid = "SLPRI_FID,
				     bia->bia_seq, bmi->bmi_seq, 
				     b->bcm_bmapno, fcmh_2_fid(f)); 
			}

			PSCFREE(bia);

			/* End sanity checks. */
			item = bmi->bmi_assign;
			bmi->bmi_assign = 0;
		}
		if (bmi->bmi_wr_ion) {
			psc_atomic32_dec(&bmi->bmi_wr_ion->rmmi_refcnt);
			bmi->bmi_wr_ion = NULL;
		}
	}

 out:
	b->bcm_flags &= ~BMAPF_IOSASSIGNED;
	bmap_wake_locked(b);
	bmap_op_done_type(b, BMAP_OPCNT_LEASE);

	if (bml->bml_flags & BML_WRITE)
		OPSTAT_DECR("write-lease");
	else
		OPSTAT_DECR("read-lease");

	psc_pool_return(slm_bml_pool, bml);

	if (item) {
		struct slmds_jent_assign_rep *sjar;

		mds_reserve_slot(1);
		sjar = pjournal_get_buf(slm_journal, sizeof(*sjar));
		sjar->sjar_item = item;
		sjar->sjar_flags = SLJ_ASSIGN_REP_FREE;
		pjournal_add_entry(slm_journal, 0, MDS_LOG_BMAP_ASSIGN,
		    0, sjar, sizeof(*sjar));
		pjournal_put_buf(slm_journal, sjar);
		mds_unreserve_slot(1);

		pfl_odt_freeitem(slm_bia_odt, item);
	}

	return (rc);
}

/*
 * Handle an SRMT_RELEASEBMAP RPC from a client or an I/O server.
 */
int
mds_handle_rls_bmap(struct pscrpc_request *rq, __unusedx int sliod)
{
	struct srm_bmap_release_req *mq;
	struct srm_bmap_release_rep *mp;
	struct bmap_mds_lease *bml;
	struct srt_bmapdesc *sbd;
	struct sl_fidgen fg;
	struct fidc_membh *f;
	struct bmap *b;
	uint32_t i;

	SL_RSX_ALLOCREP(rq, mq, mp);
	if (mq->nbmaps > MAX_BMAP_RELEASE) {
		mp->rc = -EINVAL;
		return (0);
	}

	for (i = 0; i < mq->nbmaps; i++) {
		sbd = &mq->sbd[i];

		fg.fg_fid = sbd->sbd_fg.fg_fid;
		fg.fg_gen = 0; // XXX FGEN_ANY

		if (slm_fcmh_get(&fg, &f))
			continue;

		DEBUG_FCMH(PLL_DIAG, f, "rls bmap=%u", sbd->sbd_bmapno);

		if (bmap_lookup(f, sbd->sbd_bmapno, &b))
			goto next;

		/*
 		 * Leases are timed out by slmbmaptimeothr_begin().
 		 */
		bml = mds_bmap_getbml(b, sbd->sbd_seq,
		    sbd->sbd_nid, sbd->sbd_pid);

		DEBUG_BMAP(bml ? PLL_DIAG : PLL_WARN, b,
		    "release %"PRId64" nid=%"PRId64" pid=%u bml=%p",
		    sbd->sbd_seq, sbd->sbd_nid, sbd->sbd_pid, bml);
		if (bml) {
			bml->bml_flags |= BML_FREEING;
			mds_bmap_bml_release(bml);
		}
		/*
 		 * 04/25/2017: Hit crash on bmi->bmi_assign == NULL
 		 * I suspect it has something to do with the ENOSPC
 		 * bug in selecting a sliod.
 		 */
		bmap_op_done(b);
 next:
		fcmh_op_done(f);
	}
	return (0);
}

static struct bmap_mds_lease *
mds_bml_new(struct bmap *b, struct pscrpc_export *e, int flags,
    lnet_process_id_t *cnp)
{
	struct bmap_mds_lease *bml;

	bml = psc_pool_get(slm_bml_pool);

	/*
 	 * The order of initialization matches the definition
 	 * for easy check.
 	 */
	bml->bml_seq = 0;
	bml->bml_refcnt = 1;
	bml->bml_ios = 0;
	bml->bml_cli_nidpid = *cnp;

	bml->bml_flags = flags;
	bml->bml_start = time(NULL);
	bml->bml_expire = bml->bml_start + BMAP_TIMEO_MAX;
	bml->bml_bmi = bmap_2_bmi(b);

	bml->bml_exp = e;
	INIT_PSC_LISTENTRY(&bml->bml_bmi_lentry);
	INIT_PSC_LISTENTRY(&bml->bml_timeo_lentry);
	bml->bml_chain = NULL;

	if (flags & BML_WRITE)
		OPSTAT_INCR("write-lease");
	else
		OPSTAT_INCR("read-lease");

	return (bml);
}

void
mds_bia_odtable_startup_cb(void *data, int64_t item,
    __unusedx void *arg)
{
	struct bmap_ios_assign *bia = data;
	struct fidc_membh *f = NULL;
	struct bmap_mds_lease *bml;
	struct sl_fidgen fg;
	struct bmap *b = NULL;
	int rc;

	OPSTAT_INCR("bmap-restart-check");

	if (item == 0)
		return;

	psclog_debug("fid="SLPRI_FID" seq=%"PRId64" res=(%s) bmapno=%u",
	    bia->bia_fid, bia->bia_seq,
	    libsl_ios2resm(bia->bia_ios)->resm_name,
	    bia->bia_bmapno);

	if (!bia->bia_fid) {
		psclog_warnx("found fid #0 in odtable");
		PFL_GOTOERR(out, rc = -EINVAL);
	}

	fg.fg_fid = bia->bia_fid;
	fg.fg_gen = FGEN_ANY;

	/*
	 * Because we don't revoke leases when an unlink comes in,
	 * ENOENT is actually legitimate after a crash.
	 */
	rc = slm_fcmh_get(&fg, &f);
	if (rc) {
		psclog_errorx("failed to load: item=%zd, fid="SLPRI_FID,
		    item, fg.fg_fid);
		PFL_GOTOERR(out, rc);
	}

	rc = bmap_get(f, bia->bia_bmapno, SL_WRITE, &b);
	if (rc) {
		DEBUG_FCMH(PLL_ERROR, f, "failed to load bmap %u (rc=%d)",
		    bia->bia_bmapno, rc);
		PFL_GOTOERR(out, rc);
	}

	/*
 	 * So we only put a write lease into the odtable.
 	 */
	BMAP_ULOCK(b);
	bml = mds_bml_new(b, NULL, BML_WRITE|BML_RECOVER, &bia->bia_lastcli);
	BMAP_LOCK(b);

	bml->bml_seq = bia->bia_seq;
	bml->bml_ios = bia->bia_ios;

	/*
	 * Taking the lease origination time in this manner leaves us
	 * susceptible to gross changes in the system time.
	 */
	bml->bml_start = bia->bia_start;
	bml->bml_expire = bml->bml_start + BMAP_TIMEO_MAX;
	if (bml->bml_expire <= time(NULL))
		OPSTAT_INCR("bmap-restart-expired");

	if (bia->bia_flags & BIAF_DIO)
		// XXX BMAP_LOCK(b)
		b->bcm_flags |= BMAPF_DIO;

	bmap_2_bmi(b)->bmi_assign = item;

	rc = mds_bmap_bml_add(bml, SL_WRITE, IOS_ID_ANY);
	psc_assert(!rc);
	/*
 	 * Leave it to the slmbmaptimeothr_begin() thread to free me.
 	 */
	mds_bmap_bml_release(bml);

 out:
	if (!rc)
		OPSTAT_INCR("bmap-restart-ok");
	else
		/* XXX odtable leaks */
		OPSTAT_INCR("bmap-restart-err");

	if (rc && slm_opstate == SLM_OPSTATE_NORMAL)
		/*
		 * XXX On startup, this will stuck at dmu_tx_try_assign()
		 * which calls cv_wait().
		 *	
		 * However, it should be able to work because our cursor
		 * thread (i.e. slmjcursorthr_main() has already started.
		 */
		pfl_odt_freeitem(slm_bia_odt, item);
	if (b)
		bmap_op_done(b);
	if (f)
		fcmh_op_done(f);
}

void
slm_fill_bmapdesc(struct srt_bmapdesc *sbd, struct bmap *b)
{
	struct bmap_mds_info *bmi;
	int i, locked;

	bmi = bmap_2_bmi(b);
	locked = BMAP_RLOCK(b);
	sbd->sbd_fg = b->bcm_fcmh->fcmh_fg;
	sbd->sbd_bmapno = b->bcm_bmapno;
	if (b->bcm_flags & BMAPF_DIO || slm_force_dio)
		sbd->sbd_flags |= SRM_LEASEBMAPF_DIO;
	for (i = 0; i < SLASH_SLVRS_PER_BMAP; i++)
		if (bmi->bmi_crcstates[i] & BMAP_SLVR_DATA) {
			sbd->sbd_flags |= SRM_LEASEBMAPF_DATA;
			break;
		}
	BMAP_URLOCK(b, locked);
}

/*
 * Routine called to lease access to a bmap for a client.  It first
 * checks for existence in the cache otherwise the bmap is retrieved
 * from disk.
 *
 * This routine also manages the bmap_lease reference which is used to
 * track the bmaps a particular client knows about.  mds_bmap_read() is
 * used to retrieve the bmap from disk or create a new 'blank-slate'
 * bmap if one does not exist.  Finally, a read or write reference is
 * placed on the bmap depending on the client request.  This is factored
 * in with existing references to determine whether or not the bmap
 * should be in DIO mode.
 *
 * @f: the FID cache handle for the inode.
 * @bmapno: bmap index number.
 * @lflags: bmap lease flags (SRM_LEASEBMAPF_*).
 * @rw: read/write access to the bmap.
 * @prefios: client preferred I/O system ID.
 * @sbd: value-result bmap descriptor to pass back to client.
 * @exp: RPC export to client.
 * @bmap: value-result bmap.
 *
 * Note: the bmap is not locked during disk I/O; instead it is marked
 *	with a bit (i.e. INIT) and other threads block on its waitq.
 */
int
mds_bmap_load_cli(struct fidc_membh *f, sl_bmapno_t bmapno, int lflags,
    enum rw rw, sl_ios_id_t prefios, struct srt_bmapdesc *sbd,
    struct pscrpc_export *exp, uint8_t *repls, int new)
{
	struct bmap_mds_lease *bml;
	struct bmap_mds_info *bmi;
	struct bmap *b;
	int rc, bflags;

	/*
	 * Reject any bmap request at or beyond the truncation point.
	 * It is up to the client to either retry or bail out.  The MDS
	 * does NOT provide any notification upon completion, which may
	 * never happen in the worst case.
	 */
	FCMH_LOCK(f);
	if ((f->fcmh_flags & FCMH_MDS_IN_PTRUNC) &&
	    (bmapno >= fcmh_2_fsz(f) / SLASH_BMAP_SIZE)) {
		FCMH_ULOCK(f);
		return (-SLERR_BMAP_IN_PTRUNC);
	}
	FCMH_ULOCK(f);

	bflags = BMAPGETF_CREATE;
	if (new)
		bflags |= BMAPGETF_NODISKREAD;
	rc = bmap_getf(f, bmapno, SL_WRITE, bflags, &b);
	if (rc)
		return (rc);

	bml = mds_bml_new(b, exp,
	    (rw == SL_WRITE ? BML_WRITE : BML_READ) |
	     (lflags & SRM_LEASEBMAPF_DIO ? BML_DIO : 0),
	    &exp->exp_connection->c_peer);

	rc = mds_bmap_bml_add(bml, rw, prefios);
	if (rc) {
		bml->bml_flags |= BML_FREEING;
		goto out;
	}

	slm_fill_bmapdesc(sbd, b);

	/*
	 * SLASH2 monotonic coherency sequence number assigned to this
	 * lease.
	 */
	sbd->sbd_seq = bml->bml_seq;
	sbd->sbd_key = BMAPSEQ_ANY;

	/*
	 * Store the nid/pid of the client interface in the bmapdesc to
	 * deal properly deal with IONs on other LNETs.
	 */
	sbd->sbd_nid = exp->exp_connection->c_peer.nid;
	sbd->sbd_pid = exp->exp_connection->c_peer.pid;

	bmi = bmap_2_bmi(b);

	if (rw == SL_WRITE) {
		psc_assert(bmi->bmi_wr_ion);
		sbd->sbd_ios = rmmi2resm(bmi->bmi_wr_ion)->resm_res_id;
	} else
		sbd->sbd_ios = IOS_ID_ANY;

	if (repls)
		memcpy(repls, bmi->bmi_repls, sizeof(bmi->bmi_repls));

 out:
	mds_bmap_bml_release(bml);
	bmap_op_done(b);
	return (rc);
}

int
mds_lease_reassign(struct fidc_membh *f, struct srt_bmapdesc *sbd_in,
    sl_ios_id_t pios, sl_ios_id_t *prev_ios, int nprev_ios,
    struct srt_bmapdesc *sbd_out, struct pscrpc_export *exp)
{
	struct bmap_ios_assign *bia = NULL;
	struct bmap_mds_lease *obml;
	struct bmap_mds_info *bmi;
	struct sl_resm *resm;
	struct bmap *b;
	int rc;

	rc = bmap_get(f, sbd_in->sbd_bmapno, SL_WRITE, &b);
	if (rc)
		return (rc);

	obml = mds_bmap_getbml(b, sbd_in->sbd_seq,
	    sbd_in->sbd_nid, sbd_in->sbd_pid);

	if (!obml)
		PFL_GOTOERR(out2, rc = -ENOENT);

	if (!(obml->bml_flags & BML_WRITE))  {
		BMAP_LOCK(b);
		PFL_GOTOERR(out2, rc = -EINVAL);
	}

	bmap_wait_locked(b, b->bcm_flags & BMAPF_IOSASSIGNED);

	/*
	 * Set BMAPF_IOSASSIGNED before checking the lease counts since
	 * BMAPF_IOSASSIGNED will block further lease additions and
	 * removals
	 *   - including the removal this lease's odtable entry.
	 */
	b->bcm_flags |= BMAPF_IOSASSIGNED;

	bmi = bmap_2_bmi(b);
	if (bmi->bmi_writers > 1 || bmi->bmi_readers) {
		BMAP_ULOCK(b);
		/*
		 * Other clients have been assigned this sliod.
		 * Therefore the sliod may not be reassigned.
		 */
		PFL_GOTOERR(out1, rc = -EAGAIN);
	}
	psc_assert(bmi->bmi_wr_ion);
	psc_assert(!(b->bcm_flags & BMAPF_DIO));
	BMAP_ULOCK(b);

	pfl_odt_getitem(slm_bia_odt, bmi->bmi_assign, &bia);
	psc_assert(bia->bia_seq == bmi->bmi_seq);

	rc = slm_resm_select(b, pios, prev_ios, nprev_ios, &resm);
	if (rc)
		PFL_GOTOERR(out1, rc);

	/*
	 * Deal with the lease renewal and repl_add before modifying the
	 * IOS part of the lease or bmi so that mds_bmap_add_repl()
	 * failure doesn't compromise the existing lease.
	 */
	bia->bia_seq = mds_bmap_timeotbl_mdsi(obml, BTE_ADD);
	bia->bia_lastcli = obml->bml_cli_nidpid;
	bia->bia_ios = resm->resm_res_id;
	bia->bia_start = time(NULL);

	rc = mds_bmap_add_repl(b, bia);
	if (rc)
		PFL_GOTOERR(out1, rc);

	bmi->bmi_seq = obml->bml_seq = bia->bia_seq;
	obml->bml_ios = resm->resm_res_id;

	pfl_odt_replaceitem(slm_bia_odt, bmi->bmi_assign, bia);

	/* Do some post setup on the modified lease. */
	slm_fill_bmapdesc(sbd_out, b);
	sbd_out->sbd_seq = obml->bml_seq;
	sbd_out->sbd_nid = exp->exp_connection->c_peer.nid;
	sbd_out->sbd_pid = exp->exp_connection->c_peer.pid;
	sbd_out->sbd_key = BMAPSEQ_ANY;
	sbd_out->sbd_ios = obml->bml_ios;

 out1:
	if (bia)
		PSCFREE(bia);

	BMAP_LOCK(b);
	psc_assert(b->bcm_flags & BMAPF_IOSASSIGNED);
	b->bcm_flags &= ~BMAPF_IOSASSIGNED;
	bmap_wake_locked(b);

 out2:
	if (obml)
		mds_bmap_bml_release(obml);
	DEBUG_BMAP(rc ? PLL_WARN : PLL_DIAG, b,
	    "reassign oseq=%"PRIu64" nseq=%"PRIu64" "
	    "nid=%"PRIu64" pid=%u rc=%d",
	    sbd_in->sbd_seq, (obml ? obml->bml_seq : 0),
	    exp->exp_connection->c_peer.nid,
	    exp->exp_connection->c_peer.pid, rc);
	bmap_op_done(b);
	return (rc);
}

int
mds_lease_renew(struct fidc_membh *f, struct srt_bmapdesc *sbd_in,
    struct srt_bmapdesc *sbd_out, struct pscrpc_export *exp)
{
	struct bmap_mds_lease *bml = NULL, *obml;
	struct bmap *b = NULL;
	int rc, rw;

	OPSTAT_INCR("lease-renew");
	rc = bmap_get(f, sbd_in->sbd_bmapno, SL_WRITE, &b);
	if (rc)
		goto out;

	/* Lookup the original lease to ensure it actually exists. */
	obml = mds_bmap_getbml(b, sbd_in->sbd_seq,
	    sbd_in->sbd_nid, sbd_in->sbd_pid);
	if (!obml)
		OPSTAT_INCR("lease-renew-enoent");

	rw = (sbd_in->sbd_ios == IOS_ID_ANY) ? BML_READ : BML_WRITE;
	bml = mds_bml_new(b, exp, rw, &exp->exp_connection->c_peer);
	rc = mds_bmap_bml_add(bml, rw == BML_READ ? SL_READ : SL_WRITE,
	    sbd_in->sbd_ios);
	if (rc) {
		bml->bml_flags |= BML_FREEING;
		goto out;
	}

	/* 
	 * Do some post setup on the new lease. This is probably a 
	 * good idea because the bmap replication table can change
	 * at anytime.
	 */
	slm_fill_bmapdesc(sbd_out, b);
	sbd_out->sbd_seq = bml->bml_seq;
	sbd_out->sbd_nid = exp->exp_connection->c_peer.nid;
	sbd_out->sbd_pid = exp->exp_connection->c_peer.pid;

	if (rw == BML_WRITE) {
		struct bmap_mds_info *bmi = bmap_2_bmi(b);

		psc_assert(bmi->bmi_wr_ion);

		sbd_out->sbd_key = BMAPSEQ_ANY;
		sbd_out->sbd_ios = rmmi2resm(bmi->bmi_wr_ion)->resm_res_id;
	} else {
		sbd_out->sbd_key = BMAPSEQ_ANY;
		sbd_out->sbd_ios = IOS_ID_ANY;
	}

	/*
	 * By this point it should be safe to ignore the error from
	 * mds_bmap_bml_release() since a new lease has already been
	 * issued.
	 */
	if (obml)
		obml->bml_flags |= BML_FREEING;

 out:
	if (bml) {
		mds_bmap_bml_release(bml);
		BMAP_LOCK(b);
	}
	if (obml)
		mds_bmap_bml_release(obml);

	psclogs(rc ? PLL_WARN : PLL_DIAG, SLSS_BMAP,
	    "renew oseq=%"PRIu64" nseq=%"PRIu64" nid=%"PRIu64" pid=%u",
	    sbd_in->sbd_seq, bml ? bml->bml_seq : 0,
	    exp->exp_connection->c_peer.nid,
	    exp->exp_connection->c_peer.pid);

	if (b)
		bmap_op_done(b);
	return (rc);
}

struct ios_list {
	sl_replica_t	iosv[SL_MAX_REPLICAS];
	int		nios;
};

__static void
ptrunc_tally_ios(struct bmap *b, int iosidx, int val, void *arg)
{
	struct ios_list *ios_list = arg;
	sl_ios_id_t ios_id;
	int i;

	if (val == BREPLST_VALID) {
		ios_id = bmap_2_repl(b, iosidx);
		for (i = 0; i < ios_list->nios; i++)
			if (ios_list->iosv[i].bs_id == ios_id)
				return;

		ios_list->iosv[ios_list->nios++].bs_id = ios_id;
	}
}

__static int 
slm_ptrunc_apply(struct fidc_membh *f)
{
	int rc = 0, ret;
	int queued = 0, tract[NBREPLST], retifset[NBREPLST];
	struct ios_list ios_list;
	struct bmap *b;
	sl_bmapno_t i;
	struct fcmh_mds_info *fmi;
	struct slm_update_data *upd;

	fmi = fcmh_2_fmi(f);

	/* get the number of replies we expect */
	ios_list.nios = 0;
	for (i = 0; i < SL_MAX_REPLICAS; i++)
		ios_list.iosv[i].bs_id = 0;
	fmi->fmi_ptrunc_nios = 0;

	i = fcmh_2_fsz(f) / SLASH_BMAP_SIZE;
	if ((fcmh_2_fsz(f) % SLASH_BMAP_SIZE) == 0)
		goto out1;

	/*
 	 * If we truncate in the middle of a bmap, we have to do
 	 * partial truncation of the bmap.
 	 */
	rc = bmap_get(f, i, SL_WRITE, &b);
	if (rc)
		goto out2;
	/*
	 * Arrange upd_proc_bmap() to call slm_upsch_tryptrunc().
	 */
	brepls_init(tract, -1);
	tract[BREPLST_VALID] = BREPLST_TRUNC_QUEUED;	/* 't' */

	mds_repl_bmap_walkcb(b, tract, NULL, 0, ptrunc_tally_ios, &ios_list);
	fmi->fmi_ptrunc_nios = ios_list.nios;
	if (fmi->fmi_ptrunc_nios) {
		/*
		 * fcmh will be unbusied in slm_wkcb_wr_brepl().
		 *
		 * bmaps are replayed by mds_replay_bmap_repls().
		 */
		rc = mds_bmap_write_logrepls(b);
		if (rc) {
			bmap_op_done(b);
			goto out2;
		}
		queued++;
		/*
		 * Queue work immediately instead
		 * of waiting for it to be causally
		 * paged to reduce latency to the
		 * client.
		 */
		OPSTAT_INCR("ptrunc-enqueue");
		upd = bmap_2_upd(b);
		DEBUG_FCMH(PLL_MAX, f, "ptrunc queued, upd = %p", upd);
		/*
                 * If queued, upsch will take a reference with UPD_INCREF().
		 */
		upsch_enqueue(upd);
	}
	bmap_op_done(b);

	i++;

 out1:
	brepls_init(tract, -1);
	tract[BREPLST_VALID] = BREPLST_INVALID;

	brepls_init(retifset, 0);
	retifset[BREPLST_VALID] = 1;

	for (;; i++) {
		/*
 		 * We could use file size to terminate the loop. Howerver,
 		 * the file size in fcmh has already been updated.
 		 */
		if (bmap_getf(f, i, SL_WRITE, BMAPGETF_CREATE |
		    BMAPGETF_NOAUTOINST, &b))
			break;

		BHGEN_INCREMENT(b);
		ret = mds_repl_bmap_walkcb(b, tract, NULL, 0,
		    NULL, NULL);
		if (ret)
			mds_bmap_write_logrepls(b);
		bmap_op_done(b);
	}

 out2:
	if (!queued && !rc) {
		FCMH_LOCK(f);
		f->fcmh_flags &= ~FCMH_MDS_IN_PTRUNC;
		fcmh_wake_locked(f);
		DEBUG_FCMH(PLL_MAX, f, "ptrunc completed.");
		FCMH_ULOCK(f);
	}
	OPSTAT_INCR("ptrunc-apply");
	return (rc);
}

int
slm_bmap_release_cb(__unusedx struct pscrpc_request *rq,
    struct pscrpc_async_args *av)
{
	struct slrpc_cservice *csvc = av->pointer_arg[SLM_CBARG_SLOT_CSVC];

	sl_csvc_decref(csvc);
	return (0);
}

int	slm_lease_skip = 1;

int
slm_ptrunc_prepare(struct fidc_membh *f, struct srt_stat *sstb, int to_set)
{
	int rc;
	struct srm_bmap_release_req *mq;
	struct srm_bmap_release_rep *mp;
	struct slrpc_cservice *csvc;
	struct bmap_mds_lease *bml;
	struct pscrpc_request *rq;
	struct fcmh_mds_info *fmi;
	struct bmap *b;
	sl_bmapno_t i;
	uint64_t size;

	fmi = fcmh_2_fmi(f);
	psc_assert(sstb->sst_size);

	FCMH_LOCK_ENSURE(f);
	f->fcmh_flags |= FCMH_MDS_IN_PTRUNC;
	OPSTAT_INCR("msl.ptrunc-start");
	fmi = fcmh_2_fmi(f);
	fmi->fmi_ptrunc_size = sstb->sst_size;

	DEBUG_FCMH(PLL_MAX, f, "prepare ptrunc");
	/*
	 * Inform lease holders to give up their leases.  This is only
	 * best-effort.
	 */
	i = fmi->fmi_ptrunc_size / SLASH_BMAP_SIZE;

	FCMH_ULOCK(f);

	if (slm_lease_skip)
		goto skip;

	for (;; i++) {
		if (bmap_getf(f, i, SL_WRITE, BMAPGETF_CREATE |
		    BMAPGETF_NOAUTOINST, &b))
			break;

		BMAP_FOREACH_LEASE(b, bml) {
			BMAP_ULOCK(b);

			/* we are recovering after restart */
			if (bml->bml_exp == NULL) {
				BMAP_LOCK(b);
				continue;
			}
			/*
			 * 10/18/2017:
			 *
			 * This leads to psclist_add() crash, we should 
			 * probably have a reference to protect bml_exp.
			 *
			 * 11/05/2017:
			 *
			 * Segment fault (11) from here.  We just can't
			 * trust bml_exp.
			 */
			csvc = slm_getclcsvc(bml->bml_exp, 0);
			if (csvc == NULL) {
				psclog_warnx("Unable to get csvc: %p",
				    bml->bml_exp);
				BMAP_LOCK(b);
				continue;
			}
			rc = SL_RSX_NEWREQ(csvc, SRMT_RELEASEBMAP, rq,
			    mq, mp);
			if (rc) {
				sl_csvc_decref(csvc);
				BMAP_LOCK(b);
				continue;
			}
			mq->sbd[0].sbd_fg.fg_fid = fcmh_2_fid(f);
			mq->sbd[0].sbd_fg.fg_gen = fcmh_2_gen(f);
			mq->sbd[0].sbd_bmapno = b->bcm_bmapno;
			mq->nbmaps = 1;
			rq->rq_async_args.pointer_arg[SLM_CBARG_SLOT_CSVC] = csvc;
			rq->rq_interpret_reply = slm_bmap_release_cb;
			rc = SL_NBRQSET_ADD(csvc, rq);
			if (rc) {
				pscrpc_req_finished(rq);
				sl_csvc_decref(csvc);
			}

			BMAP_LOCK(b);
			OPSTAT_INCR("msl.truncate-revoke-lease");
		}
		bmap_op_done(b);
	}

 skip:

	FCMH_LOCK(f);
	to_set |= SL_SETATTRF_PTRUNCGEN;
	fcmh_2_ptruncgen(f)++;
	size = f->fcmh_sstb.sst_size;
	f->fcmh_sstb.sst_size = fmi->fmi_ptrunc_size;
	FCMH_ULOCK(f);

	mds_reserve_slot(1);
	rc = mdsio_setattr(current_vfsid, fcmh_2_mfid(f),
	    &f->fcmh_sstb, to_set, &rootcreds, &f->fcmh_sstb,
	    fcmh_2_mfh(f), mdslog_namespace);
	mds_unreserve_slot(1);

	if (!rc) 
		rc = slm_ptrunc_apply(f);

	if (rc) {
		FCMH_LOCK(f);
		f->fcmh_flags &= ~FCMH_MDS_IN_PTRUNC;
		fcmh_2_ptruncgen(f)--;
		f->fcmh_sstb.sst_size = size;
		DEBUG_FCMH(PLL_MAX, f, "ptrunc aborted, rc = %d", rc);
		FCMH_ULOCK(f);
	}

	return (rc);
}

/*
 * Execute an SQL query on the SQLite database.
 *
 * @cb: optional; callback to retrieve fields for a SELECT.
 * @cbarg: optional; argument to provide to callback.
 * @fmt: printf(3)-like format string to properly escape any
 * interpolated values in the SQL query.
 */
int
_dbdo(const struct pfl_callerinfo *pci,
    int (*cb)(sqlite3_stmt *, void *), void *cbarg,
    const char *fmt, ...)
{
	int type, log = 0, dbuf_off = 0, rc, n, i;
	char *p, dbuf[LINE_MAX] = "";
	struct timeval tv, tv0, tvd;
	sqlite3_stmt *sth;
	va_list ap;

	spinlock(&slm_upsch_lock);
	do {
		rc = sqlite3_prepare_v2(db_handle, fmt, -1, &sth, NULL);
		if (rc == SQLITE_BUSY)
			pscthr_yield();
	} while (rc == SQLITE_BUSY);
	/* saw SQLITE_MISUSE  = 21  */
	psc_assert(rc == SQLITE_OK);

	n = sqlite3_bind_parameter_count(sth);
	va_start(ap, fmt);
	log = psc_log_shouldlog(pci, PLL_DEBUG);
	if (log) {
		strlcpy(dbuf, fmt, sizeof(dbuf));
		dbuf_off = strlen(fmt);
	}
	PFL_GETTIMEVAL(&tv0);
	for (i = 0; i < n; i++) {
		type = va_arg(ap, int);
		switch (type) {
		case SQLITE_INTEGER64: {
			int64_t arg;

			arg = va_arg(ap, int64_t);
			rc = sqlite3_bind_int64(sth, i + 1,
			    arg);
			if (log)
				dbuf_off += snprintf(dbuf + dbuf_off,
				    sizeof(dbuf) - dbuf_off,
				    "; arg %d: %"PRId64, i + 1, arg);
			break;
		    }
		case SQLITE_INTEGER: {
			int32_t arg;

			arg = va_arg(ap, int32_t);
			rc = sqlite3_bind_int(sth, i + 1, arg);
			if (log)
				dbuf_off += snprintf(dbuf + dbuf_off,
				    sizeof(dbuf) - dbuf_off,
				    "; arg %d: %d", i + 1, arg);
			break;
		    }
		case SQLITE_TEXT:
			p = va_arg(ap, char *);
			rc = sqlite3_bind_text(sth, i + 1, p,
			    strlen(p), SQLITE_STATIC);
			if (log)
				dbuf_off += snprintf(dbuf + dbuf_off,
				    sizeof(dbuf) - dbuf_off,
				    "; arg %d: %s", i + 1, p);
			break;
		case SQLITE_NULL:
			(void)va_arg(ap, int);
			rc = sqlite3_bind_null(sth, i + 1);
			if (log)
				dbuf_off += snprintf(dbuf + dbuf_off,
				    sizeof(dbuf) - dbuf_off,
				    "; arg %d: NULL", i + 1);
			break;
		default:
			psc_fatalx("type");
		}
		psc_assert(rc == SQLITE_OK);
	}
	va_end(ap);

	do {
		rc = sqlite3_step(sth);
		if (rc == SQLITE_ROW && cb)
			cb(sth, cbarg);
		if (rc != SQLITE_DONE)
			pscthr_yield();
		if (rc == SQLITE_LOCKED)
			sqlite3_reset(sth);
	} while (rc == SQLITE_ROW || rc == SQLITE_BUSY || rc == SQLITE_LOCKED);

	PFL_GETTIMEVAL(&tv);
	timersub(&tv, &tv0, &tvd);
	OPSTAT_INCR("sql-query-wait");
	OPSTAT_ADD("sql-query-wait-usecs",
	    tvd.tv_sec * 1000000 + tvd.tv_usec);
	if (log)
		psclog_debug("ran SQL in %.2fs: %s", tvd.tv_sec +
		    tvd.tv_usec / 1000000.0, dbuf);

	if (rc != SQLITE_DONE)
		psclog_errorx("SQL error: rc=%d query=%s; msg=%s", rc,
		    fmt, sqlite3_errmsg(db_handle));

	sqlite3_finalize(sth);
	freelock(&slm_upsch_lock);
	return (rc == SQLITE_DONE ? 0 : rc);
}
