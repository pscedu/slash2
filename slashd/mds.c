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

#include "psc_ds/lockedlist.h"
#include "psc_ds/tree.h"
#include "psc_ds/treeutil.h"
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
#include "mdscoh.h"
#include "mdsio.h"
#include "mdslog.h"
#include "repl_mds.h"
#include "rpc_mds.h"
#include "slashd.h"
#include "slerr.h"

struct odtable				*mdsBmapAssignTable;
uint64_t				 mdsBmapSequenceNo;
const struct slash_bmap_od		 null_bmap_od;
const struct slash_inode_od		 null_inode_od;
const struct slash_inode_extras_od	 null_inox_od;

__static void
mds_inode_od_initnew(struct slash_inode_handle *i)
{
	i->inoh_flags = INOH_INO_NEW | INOH_INO_DIRTY;
	COPYFG(&i->inoh_ino.ino_fg, &i->inoh_fcmh->fcmh_fg);

	/* For now this is a fixed size. */
	i->inoh_ino.ino_bsz = SLASH_BMAP_SIZE;
	i->inoh_ino.ino_version = INO_VERSION;
	i->inoh_ino.ino_flags = 0;
	i->inoh_ino.ino_nrepls = 0;
	mds_inode_sync(i);
}

int
mds_inode_read(struct slash_inode_handle *i)
{
	psc_crc64_t crc;
	int rc=0, locked;

	locked = reqlock(&i->inoh_lock);
	psc_assert(i->inoh_flags & INOH_INO_NOTLOADED);

	rc = mdsio_inode_read(i);

	if (rc == SLERR_SHORTIO && i->inoh_ino.ino_crc == 0 &&
	    memcmp(&i->inoh_ino, &null_inode_od, INO_OD_CRCSZ) == 0) {
		DEBUG_INOH(PLL_INFO, i, "detected a new inode");
		mds_inode_od_initnew(i);
		rc = 0;

	} else if (rc) {
		DEBUG_INOH(PLL_WARN, i, "mdsio_inode_read: %d", rc);

	} else {
		psc_crc64_calc(&crc, &i->inoh_ino, INO_OD_CRCSZ);
		if (crc == i->inoh_ino.ino_crc) {
			i->inoh_flags &= ~INOH_INO_NOTLOADED;
			DEBUG_INOH(PLL_INFO, i, "successfully loaded inode od");
		} else {
			DEBUG_INOH(PLL_WARN, i,
				   "CRC failed want=%"PRIx64", got=%"PRIx64,
				   i->inoh_ino.ino_crc, crc);
			rc = EIO;
		}
	}
	ureqlock(&i->inoh_lock, locked);
	return (rc);
}

int
mds_inox_load_locked(struct slash_inode_handle *ih)
{
	psc_crc64_t crc;
	int rc;

	INOH_LOCK_ENSURE(ih);

	psc_assert(!(ih->inoh_flags & INOH_HAVE_EXTRAS));

	psc_assert(ih->inoh_extras == NULL);
	ih->inoh_extras = PSCALLOC(sizeof(*ih->inoh_extras));

	rc = mdsio_inode_extras_read(ih);
	if (rc == SLERR_SHORTIO && ih->inoh_extras->inox_crc == 0 &&
	    memcmp(&ih->inoh_extras, &null_inox_od, INOX_OD_CRCSZ) == 0) {
		rc = 0;
	} else if (rc) {
		DEBUG_INOH(PLL_WARN, ih, "mdsio_inode_extras_read: %d", rc);
	} else {
		psc_crc64_calc(&crc, ih->inoh_extras, INOX_OD_CRCSZ);
		if (crc == ih->inoh_extras->inox_crc)
			ih->inoh_flags |= INOH_HAVE_EXTRAS;
		else {
			psc_errorx("inox CRC fail; disk %"PSCPRIxCRC64
			    " mem %"PSCPRIxCRC64,
			    ih->inoh_extras->inox_crc, crc);
			rc = EIO;
		}
	}
	if (rc) {
		PSCFREE(ih->inoh_extras);
		ih->inoh_extras = NULL;
		ih->inoh_flags &= ~INOH_HAVE_EXTRAS;
	}
	return (rc);
}

int
mds_inox_ensure_loaded(struct slash_inode_handle *ih)
{
	int locked, rc = 0;

	locked = INOH_RLOCK(ih);
	if (ATTR_NOTSET(ih->inoh_flags, INOH_HAVE_EXTRAS))
		rc = mds_inox_load_locked(ih);
	INOH_URLOCK(ih, locked);
	return (rc);
}

int
mds_bmap_exists(struct fidc_membh *f, sl_blkno_t n)
{
	sl_blkno_t lblk;
	int locked;
	//int rc;

	locked = FCMH_RLOCK(f);
#if 0
	if ((rc = mds_stat_refresh_locked(f)))
		return (rc);
#endif

	lblk = fcmh_2_nbmaps(f);

	psc_trace("fid="FIDFMT" lblk=%u fsz=%"PSCPRIdOFF,
	    FIDFMTARGS(&f->fcmh_fg), lblk, fcmh_2_fsz(f));

	FCMH_URLOCK(f, locked);
	return (n < lblk);
}

static int
mds_bmap_directio(struct bmapc_memb *b, enum rw rw)
{
	struct bmap_mds_info *bmdsi=b->bcm_pri;
	struct bmap_mds_lease *bml;
	int dio=0;

	psc_assert(b->bcm_mode & BMAP_IONASSIGN);
	psc_assert(bmdsi->bmdsi_wr_ion);

	BMAP_LOCK(b);
	if (!bmdsi->bmdsi_writers ||
	    ((bmdsi->bmdsi_writers == 1) &&
	     (pll_nitems(&bmdsi->bmdsi_leases) == 1))) {
		if (b->bcm_mode & BMAP_DIO)
			b->bcm_mode &= ~BMAP_DIO;
		/* Unset.
		 */
		goto out;

	} else {
		if ((pll_nitems(&bmdsi->bmdsi_leases) > 1) &&
		    !(b->bcm_mode & BMAP_DIO)) {
			/* Set.
			 */
			b->bcm_mode |= BMAP_DIO;
			dio = 1;
		}
	}

	if (!dio)
		goto out;

	if (rw == SL_READ) {
		/* A read request has forced the bmap into directio mode.
		 *   Inform the write-client to drop his cache.  This call
		 *   is blocking.
		 */
		bml = pll_gethdpeek(&bmdsi->bmdsi_leases);
		psc_assert(bml->bml_flags & BML_WRITE);
		BMAP_ULOCK(b);
		/* A failure here could be handled through the bmap timeout
		 *   mechanism where the client will block on this I/O until
		 *   the client has either closed the bmap or the mds has
		 *   phased out the seq number.
		 */
		if (bml->bml_flags & BML_CDIO)
			return (0);
		else
			return (mdscoh_req(bml, MDSCOH_BLOCK));
	} else {
		int wtrs=0;

		/* Inform our readers to use directio mode.
		 */
		PLL_FOREACH(bml, &bmdsi->bmdsi_leases) {
			if (bml->bml_flags & BML_WRITE)
				wtrs++;
			else {
				if (bml->bml_flags & BML_CDIO)
					continue;
				(int)mdscoh_req(bml, MDSCOH_NONBLOCK);
			}
		}
		psc_assert(wtrs == 1);
	}
 out:
	BMAP_ULOCK(b);
	return (0);
}

void
mds_bmi_odtable_startup_cb(void *data, struct odtable_receipt *odtr)
{
	struct bmi_assign *bmi;
	struct sl_resm *resm;

	bmi = data;

	resm = libsl_nid2resm(bmi->bmi_ion_nid);

	psc_warnx("fid=%"PRId64" res=(%s) ion=(%s) bmapno=%u",
		  bmi->bmi_fid,
		  resm->resm_res->res_name,
		  libcfs_nid2str(bmi->bmi_ion_nid),
		  bmi->bmi_bmapno);

	odtable_freeitem(mdsBmapAssignTable, odtr);

	//XXX pull the current min and max mdsbmap sequence number from
	//  the odtable.
}

/**
 * mds_bmap_ion_assign - bind a bmap to a ion node for writing.  The process
 *    involves a round-robin'ing of an I/O system's nodes and attaching a
 *    a resm_mds_info to the bmap, used for establishing connection to the ION.
 * @bref: the bmap reference
 * @pios: the preferred I/O system
 */
__static int
mds_bmap_ion_assign(struct bmap_mds_lease *bml, sl_ios_id_t pios)
{
	struct bmapc_memb *bmap=bml_2_bmap(bml);
	struct bmap_mds_info *bmdsi=bmap->bcm_pri;
	struct bmi_assign bmi;
	struct sl_resource *res=libsl_id2res(pios);
	struct sl_resm *resm;
	struct resm_mds_info *rmmi;
	struct resprof_mds_info *rpmi;
	int j, n, len;

	psc_assert(bmap->bcm_mode & BMAP_IONASSIGN);
	psc_assert(!bmdsi->bmdsi_wr_ion);
	psc_assert(!bmdsi->bmdsi_assign);
	psc_assert(psc_atomic32_read(&bmap->bcm_opcnt) > 0);

	if (!res) {
		psc_warnx("Failed to find pios %d", pios);
		return (-SLERR_ION_UNKNOWN);
	}
	rpmi = res->res_pri;
	psc_assert(rpmi);
	len = psc_dynarray_len(&res->res_members);

	for (j = 0; j < len; j++) {
		spinlock(&rpmi->rpmi_lock);
		if (rpmi->rpmi_cnt >= len)
			rpmi->rpmi_cnt = 0;
		n = rpmi->rpmi_cnt++;
		resm = psc_dynarray_getpos(&res->res_members, n);

		psc_trace("trying res(%s) ion(%s)",
		    res->res_name, resm->resm_addrbuf);

		freelock(&rpmi->rpmi_lock);

		rmmi = resm->resm_pri;
		spinlock(&rmmi->rmmi_lock);

		/*
		 * If we fail to establish a connection, try next node.
		 * The loop guarantees that we always bail out.
		 */
		if (slm_geticsvc(resm) == NULL) {
			freelock(&rmmi->rmmi_lock);
			continue;
		}

		DEBUG_BMAP(PLL_TRACE, bmap, "res(%s) ion(%s)",
		    res->res_name, resm->resm_addrbuf);

		atomic_inc(&rmmi->rmmi_refcnt);
		bmdsi->bmdsi_wr_ion = rmmi;
		freelock(&rmmi->rmmi_lock);
		break;
	}

	if (!bmdsi->bmdsi_wr_ion)
		return (-SLERR_ION_OFFLINE);

	/* An ION has been assigned to the bmap, mark it in the odtable
	 *   so that the assignment may be restored on reboot.
	 */
	bmi.bmi_ion_nid = rmmi->rmmi_resm->resm_nid;
	bmi.bmi_ios = rmmi->rmmi_resm->resm_res->res_id;
	bmi.bmi_fid = fcmh_2_fid(bmap->bcm_fcmh);
	bmi.bmi_bmapno = bmap->bcm_blkno;
	bmi.bmi_start = time(NULL);
	bmdsi->bmdsi_seq = bmi.bmi_seq = mds_bmap_timeotbl_mdsi(bml, BTE_ADD);

	bmdsi->bmdsi_assign = odtable_putitem(mdsBmapAssignTable, &bmi);
	if (!bmdsi->bmdsi_assign) {
		DEBUG_BMAP(PLL_ERROR, bmap, "failed odtable_putitem()");
		return (-SLERR_XACT_FAIL);
	}

	/* Signify that a ION has been assigned to this bmap.  This
	 *   opcnt ref will stay in place until the bmap has been released
	 *   by the last client or has been timed out.
	 */
	bmap_op_start_type(bmap, BMAP_OPCNT_IONASSIGN);

	mds_repl_inv_except(bmap, bmi.bmi_ios);

	bml->bml_seq = bmi.bmi_seq;
	bml->bml_key = bmdsi->bmdsi_assign->odtr_key;

	DEBUG_FCMH(PLL_INFO, bmap->bcm_fcmh, "bmap assignment");
	DEBUG_BMAP(PLL_INFO, bmap, "using res(%s) ion(%s) "
	    "rmmi(%p)", res->res_name, resm->resm_addrbuf,
	    bmdsi->bmdsi_wr_ion);

	return (0);
}

__static int
mds_bmap_ion_update(struct bmap_mds_lease *bml)
{
	struct bmapc_memb *b=bml_2_bmap(bml);
	struct bmap_mds_info *bmdsi=b->bcm_pri;
	struct bmi_assign *bmi;

	psc_assert(b->bcm_mode & BMAP_IONASSIGN);

	bmi = odtable_getitem(mdsBmapAssignTable, bmdsi->bmdsi_assign);
	if (!bmi) {
		DEBUG_BMAP(PLL_WARN, b, "odtable_getitem() failed");
		return (-1);
	}

	psc_assert(bmi->bmi_seq == bmdsi->bmdsi_seq);
	bmi->bmi_start = time(NULL);
	bmi->bmi_seq = bmdsi->bmdsi_seq = mds_bmap_timeotbl_mdsi(bml, BTE_ADD);
	bmdsi->bmdsi_assign = odtable_replaceitem(mdsBmapAssignTable,
					  bmdsi->bmdsi_assign, bmi);

	bml->bml_seq = bmi->bmi_seq;
	bml->bml_key = bmdsi->bmdsi_assign->odtr_key;

	psc_assert(bmdsi->bmdsi_assign);
	return (0);
}

/**
 * mds_bmap_ref_add - add a read or write reference to the bmap's tree
 *	and refcnts.  This also calls into the directio_[check|set]
 *	calls depending on the number of read and/or write clients of
 *	this bmap.
 * @bref: the bref to be added, it must have a bmapc_memb already attached.
 * @mq: the RPC request for examining the bmap access mode (read/write).
 */
__static int
mds_bmap_bml_add(struct bmap_mds_lease *bml, enum rw rw,
    sl_ios_id_t prefios)
{
	struct bmap_mds_info *bmdsi=bml->bml_bmdsi;
	struct bmapc_memb *b=bmdsi->bmdsi_bmap;
	int rc=0;

	BMAP_LOCK(b);
	bmap_op_start_type(b, BMAP_OPCNT_LEASE);
	/* Wait for BMAP_IONASSIGN to be removed before proceeding.
	 */
	bcm_wait_locked(b, (b->bcm_mode & BMAP_IONASSIGN));

	if (rw == SL_WRITE) {
		/* Drop the lock prior to doing disk and possibly network
		 *    I/O.
		 */
		bmdsi->bmdsi_writers++;
		b->bcm_mode |= BMAP_IONASSIGN;
		bml->bml_flags |= BML_TIMEOQ;

		if (bmdsi->bmdsi_writers == 1) {
			psc_assert(!bmdsi->bmdsi_wr_ion);
			BMAP_ULOCK(b);
			rc = mds_bmap_ion_assign(bml, prefios);
		} else {
			psc_assert(bmdsi->bmdsi_wr_ion);
			BMAP_ULOCK(b);
			rc = mds_bmap_ion_update(bml);
		}

		if (rc) {
			BMAP_LOCK(b);
			bmdsi->bmdsi_writers--;
			b->bcm_mode &= ~BMAP_IONASSIGN;
			b->bcm_mode |= BMAP_MDS_NOION;
			goto out;
		}
		rc = mds_bmap_directio(b, SL_WRITE);
		BMAP_LOCK(b);
		b->bcm_mode &= ~BMAP_IONASSIGN;

	} else {
		/* Read leases aren't required to be present in the
		 *   timeout table though their sequence number must
		 *   be accounted for.
		 */
		bml->bml_seq = mds_bmap_timeotbl_getnextseq();
		bml->bml_key = BMAPSEQ_ANY;

		if (bmdsi->bmdsi_writers) {
			b->bcm_mode |= BMAP_IONASSIGN;
			BMAP_ULOCK(b);
			/* Don't hold the lock when calling
			 *   mds_bmap_directio_handle_locked(), it will likely
			 *   send an RPC.
			 */
			rc = mds_bmap_directio(b, SL_READ);
			BMAP_LOCK(b);
			b->bcm_mode &= ~BMAP_IONASSIGN;
		}
	}
	pll_addtail(&bmdsi->bmdsi_leases, bml);

 out:
	DEBUG_BMAP(rc ? PLL_ERROR : PLL_INFO, b, "bml_add (mion=%p) (rc=%d)",
	   bmdsi->bmdsi_wr_ion, rc);

	bcm_wake_locked(b);
	BMAP_ULOCK(b);
	if (rc)
		bmap_op_done_type(b, BMAP_OPCNT_LEASE);

	return (rc);
}

/**
 * mds_bmap_bml_release - remove a bmap lease from the mds.  This can be
 *   called from the bmap_timeo thread, from a client bmap_release rpc,
 *   or from the nbreqset cb context.
 * Notes:  the bml must be removed from the timeotbl in all cases.
 *    otherwise we determine list removals on a case by case basis.
 */
int
mds_bmap_bml_release(struct bmapc_memb *b, uint64_t seq, uint64_t key)
{
	struct bmap_mds_info *bmdsi;
	struct bmap_mds_lease *bml;
	struct odtable_receipt *odtr=NULL;
	int found=0, rc = 0;

	bmdsi = b->bcm_pri;
	psc_assert(psc_atomic32_read(&b->bcm_opcnt) > 0);

	DEBUG_BMAP(PLL_INFO, b, "seq=%"PRId64" key=%"PRId64, seq, key);
	BMAP_LOCK(b);
	/* BMAP_IONASSIGN acts as a barrier for operations which
	 *   may modify bmdsi_wr_ion.  Since ops associated with
	 *   BMAP_IONASSIGN do disk and net i/o, the spinlock is
	 *   dropped.
	 * XXX actually, the bcm_lock is not dropped until the very end.
	 *   if this becomes problematic we should investigate more.
	 *   ATM the BMAP_IONASSIGN is not relied upon
	 */
	bcm_wait_locked(b, (b->bcm_mode & BMAP_IONASSIGN));
	b->bcm_mode |= BMAP_IONASSIGN;

	PLL_FOREACH(bml, &bmdsi->bmdsi_leases) {
		if (bml->bml_seq == seq) {
			found = 1;
			break;
		}
	}
	if (!found) {
		b->bcm_mode &= ~BMAP_IONASSIGN;
		bcm_wake_locked(b);
		BMAP_ULOCK(b);
		return (-ENOENT);
	}

	BML_LOCK(bml);

	if (bml->bml_flags & BML_COHRLS) {
		/* Called from the mdscoh callback.  Nothing should be left
		 *   except for removing the bml from the bmdsi.
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
		struct slashrpc_export *slexp;

		/* Take the locks in the correct order.
		 */
		BML_ULOCK(bml);
		slexp = slexp_get(bml->bml_exp, SLCONNT_CLI);
		spinlock(&slexp->slexp_export->exp_lock);
		BML_LOCK(bml);
		if (bml->bml_flags & BML_EXP) {
			psc_assert(psclist_conjoint(&bml->bml_exp_lentry));
			psclist_del(&bml->bml_exp_lentry);
			bml->bml_flags &= ~BML_EXP;
		} else
			psc_assert(psclist_disjoint(&bml->bml_exp_lentry));

		freelock(&slexp->slexp_export->exp_lock);
		slexp_put(bml->bml_exp);

		bml->bml_flags &= ~BML_EXP;
	}
	/* Note: reads are not attached to the timeout table.
	 */
	if (bml->bml_flags & BML_TIMEOQ)
		(uint64_t)mds_bmap_timeotbl_mdsi(bml, BTE_DEL);

	/* If BML_COHRLS was set above then the lease must remain on the
	 *    bmdsi so that directio can be managed properly.
	 */
	if (bml->bml_flags & BML_COHRLS) {
		BML_ULOCK(bml);
		return (-EAGAIN);
	}

	pll_remove(&bmdsi->bmdsi_leases, bml);
	if (bmdsi->bmdsi_writers)
		psc_assert(bmdsi->bmdsi_wr_ion);

	if (bml->bml_flags & BML_WRITE)
		bmdsi->bmdsi_writers--;

	BML_ULOCK(bml);

	if ((((!bmdsi->bmdsi_writers) ||
	      ((bmdsi->bmdsi_writers == 1))) &&
	     (pll_nitems(&bmdsi->bmdsi_leases) == 1)) &&
	    (b->bcm_mode & BMAP_DIO))
		/* Remove the directio flag if possible.
		 */
		b->bcm_mode &= ~BMAP_DIO;

	/* Only release the odtable entry if the key matches.  If a match
	 *   is found then verify the sequence number matches.
	 */
	if ((bml->bml_flags & BML_WRITE) &&
	    !bmdsi->bmdsi_writers        &&
	    (key == bmdsi->bmdsi_assign->odtr_key)) {
		/* odtable sanity checks:
		 */
		struct bmi_assign *bmi;

		bmi = odtable_getitem(mdsBmapAssignTable, bmdsi->bmdsi_assign);
		psc_assert(bmi->bmi_seq == seq);
		psc_assert(bmi->bmi_bmapno == b->bcm_bmapno);
		/* End Sanity Checks.
		 */
		psc_assert(seq == bmdsi->bmdsi_seq);
		atomic_dec(&bmdsi->bmdsi_wr_ion->rmmi_refcnt);
		odtr = bmdsi->bmdsi_assign;
		bmdsi->bmdsi_assign = NULL;
		bmdsi->bmdsi_wr_ion = NULL;
		bmap_op_done_type(b, BMAP_OPCNT_IONASSIGN);
	}
	/* bmap_op_done_type(b, BMAP_OPCNT_IONASSIGN) may have released the lock.
	 */
	(int)BMAP_RLOCK(b);
	b->bcm_mode &= ~BMAP_IONASSIGN;
	bcm_wake_locked(b);
	bmap_op_done_type(b, BMAP_OPCNT_LEASE);

	if (odtr) {
		rc = odtable_freeitem(mdsBmapAssignTable, odtr);
		DEBUG_BMAP(PLL_NOTIFY, b, "odtable remove seq=%"PRId64" key=%"
		   PRId64, seq, key);
	}

	psc_pool_return(bmapMdsLeasePool, bml);

	return (rc);
}

/**
 * mds_bmap_crc_write - process a CRC update request from an ION.
 * @c: the RPC request containing the FID, bmapno, and chunk ID (cid).
 * @ion_nid:  the id of the io node which sent the request.  It is
 *	compared against the ID stored in the bmdsi.
 */
int
mds_bmap_crc_write(struct srm_bmap_crcup *c, lnet_nid_t ion_nid)
{
	struct fidc_membh *fcmh;
	struct bmapc_memb *bmap;
	struct bmap_mds_info *bmdsi;
	struct slash_bmap_od *bmapod;
	int rc=0;

	fcmh = fidc_lookup_fg(&c->fg);
	if (!fcmh)
		return (-EBADF);

	/* BMAP_OP #2
	 */
	rc = bmap_lookup(fcmh, c->blkno, &bmap);
	if (rc) {
		rc = -EBADF;
		goto out;
	}
	BMAP_LOCK(bmap);

	DEBUG_BMAP(PLL_TRACE, bmap, "blkno=%u sz=%"PRId64" ion=%s",
		   c->blkno, c->fsize, libcfs_nid2str(ion_nid));

	psc_assert(psc_atomic32_read(&bmap->bcm_opcnt) > 1);

	bmdsi = bmap->bcm_pri;
	bmapod = bmap->bcm_od;
	/* These better check out.
	 */
	psc_assert(bmap->bcm_fcmh == fcmh);
	psc_assert(bmdsi);
	psc_assert(bmapod);
	psc_assert(bmdsi->bmdsi_wr_ion);

	if (ion_nid != bmdsi->bmdsi_wr_ion->rmmi_resm->resm_nid) {
		/* Whoops, we recv'd a request from an unexpected nid.
		 */
		rc = -EINVAL;
		BMAP_ULOCK(bmap);
		goto out;

	} else if (bmap->bcm_mode & BMAP_MDS_CRC_UP) {
		/* Ensure that this thread is the only thread updating the
		 *  bmap crc table.  XXX may have to replace this with a waitq
		 */
		rc = -EALREADY;
		BMAP_ULOCK(bmap);

		DEBUG_BMAP(PLL_ERROR, bmap, "EALREADY blkno=%u sz=%"PRId64
			   "ion=%s", c->blkno, c->fsize,
			   libcfs_nid2str(ion_nid));

		DEBUG_FCMH(PLL_ERROR, fcmh, "EALREADY blkno=%u sz=%"PRId64
			   " ion=%s", c->blkno, c->fsize,
			   libcfs_nid2str(ion_nid));
		goto out;

	} else {
		/* Mark that bmap is undergoing crc updates - this is non-
		 *  reentrant so the ION must know better than to send
		 *  multiple requests for the same bmap.
		 */
		bmap->bcm_mode |= BMAP_MDS_CRC_UP;
	}
	/* XXX Note the lock ordering here BMAP -> INODEH
	 * mds_repl_inv_except() takes the lock.
	 *  This shouldn't be racy because
	 *   . only one export may be here (ion_nid)
	 *   . the bmap is locked.
	 */
	if ((rc = mds_repl_inv_except(bmap,
	    resm_2_resid(bmdsi->bmdsi_wr_ion->rmmi_resm)))) {
		BMAP_ULOCK(bmap);
		goto out;
	}

	/* XXX ok if replicas exist, the gen has to be bumped and the
	 *  replication bmap modified.
	 *  Schedule the bmap for writing.
	 */
	BMAP_ULOCK(bmap);
	/* Call the journal and update the in-memory crc's.
	 */
	mds_bmap_crc_log(bmap, c);

	if (c->rls)
		/* Sliod may be finished with this bmap.
		 */
		(int)mds_bmap_bml_release(bmap, c->seq, c->key);
 out:
	/* Mark that mds_bmap_crc_write() is done with this bmap
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
 * mds_bmapod_initnew - called when a read request offset exceeds the
 *	bounds of the file causing a new bmap to be created.
 * Notes:  Bmap creation race conditions are prevented because the bmap
 *	handle already exists at this time with
 *	bmapi_mode == BMAP_INIT.
 *
 *	This causes other threads to block on the waitq until
 *	read/creation has completed.
 * More Notes:  this bmap is not written to disk until a client actually
 *	writes something to it.
 */
__static void
mds_bmapod_initnew(struct slash_bmap_od *b)
{
	int i;

	for (i=0; i < SL_CRCS_PER_BMAP; i++)
		b->bh_crcs[i].gc_crc = SL_NULL_CRC;

	psc_crc64_calc(&b->bh_bhcrc, b, BMAP_OD_CRCSZ);
}

/**
 * mds_bmap_read - retrieve a bmap from the ondisk inode file.
 * @fcmh: inode structure containing the fid and the fd.
 * @blkno: the bmap block number.
 * @bmapod: on disk structure containing crc's and replication bitmap.
 * @skip_zero: only return initialized bmaps.
 * Returns zero on success, negative errno code on failure.
 */
int
mds_bmap_read(struct bmapc_memb *bcm,  __unusedx enum rw rw)
{
	struct fidc_membh *f = bcm->bcm_fcmh;
	struct slash_bmap_od *bod;
	psc_crc64_t crc;
	int rc;

	psc_assert(bcm->bcm_od == NULL);
	bod = bcm->bcm_od = PSCALLOC(BMAP_OD_SZ);

	/* Try to pread() the bmap from the mds file.
	 */
	rc = mdsio_bmap_read(bcm);

	/*
	 * Check for a NULL CRC if we had a good read.  NULL CRC can happen when
	 * bmaps are gaps that have not been written yet.   Note that a short
	 * read is tolerated as long as the bmap is zeroed.
	 */
	if (!rc || rc == SLERR_SHORTIO) {
		if (bod->bh_bhcrc == 0 &&
		    memcmp(bod, &null_bmap_od,
		    sizeof(null_bmap_od)) == 0) {
			DEBUG_BMAPOD(PLL_INFO, bcm, "");
			mds_bmapod_initnew(bod);
			DEBUG_BMAPOD(PLL_INFO, bcm, "");
			return (0);
		}
	}

	/* At this point, the short I/O is an error since the bmap isn't zeros. */
	if (rc) {
		DEBUG_FCMH(PLL_ERROR, f, "mdsio_bmap_read: "
		    "bmapno=%u, rc=%d", bcm->bcm_bmapno, rc);
		rc = -EIO;
		goto out;
	}

	DEBUG_BMAPOD(PLL_INFO, bcm, "");

	/* Calculate and check the CRC now */
	psc_crc64_calc(&crc, bod, BMAP_OD_CRCSZ);
	if (crc == bod->bh_bhcrc)
		return (0);

	DEBUG_FCMH(PLL_ERROR, f, "CRC failed; bmapno=%u, want=%"PRIx64", got=%"PRIx64,
	    bcm->bcm_bmapno, bod->bh_bhcrc, crc);
	rc = -EIO;
 out:
	PSCFREE(bcm->bcm_od);
	return (rc);
}

void
mds_bmap_init(struct bmapc_memb *bcm)
{
	struct bmap_mds_info *bmdsi;

	bmdsi = bcm->bcm_pri;
	bmdsi->bmdsi_bmap = bcm;
	pll_init(&bmdsi->bmdsi_leases, struct bmap_mds_lease,
		 bml_bmdsi_lentry, NULL);
	jfi_init(&bmdsi->bmdsi_jfi, mds_bmap_sync, mds_bmap_jfiprep, bcm);
	bmdsi->bmdsi_xid = 0;
}

void
mds_bmap_sync_if_changed(struct bmapc_memb *bcm)
{
	struct bmap_mds_info *bmdsi;

	BMAP_LOCK_ENSURE(bcm);

	bmdsi = bcm->bcm_pri;
	if (bmdsi->bmdsi_flags & BMIM_LOGCHG) {
		bmdsi->bmdsi_flags &= ~BMIM_LOGCHG;
		mds_bmap_repl_log(bcm);
	}
}

/**
 * mds_bmap_loadvalid - Load a bmap if disk I/O is successful and the bmap
 *	has been initialized (i.e. is not all zeroes).
 * @f: fcmh.
 * @bmapno: bmap index number to load.
 * @bp: value-result bmap.
 * NOTE: callers must issue bmap_op_done() if mds_bmap_loadvalid() is
 *     successful.
 */
int
mds_bmap_loadvalid(struct fidc_membh *f, sl_blkno_t bmapno,
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
	for (n = 0; n < SL_CRCS_PER_BMAP; n++)
		/*
		 * XXX need a bitmap to see which CRCs are
		 * actually uninitialized and not just happen
		 * to be zero.
		 */
		if (b->bcm_od->bh_crcstates[n]) {
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
mds_bmap_load_ion(const struct slash_fidgen *fg, sl_blkno_t bmapno,
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
	return (rc);
}

/**
 * mds_bmap_load_cli - routine called to retrieve a bmap, presumably so that
 *	it may be sent to a client.  It first checks for existence in
 *	the cache, if needed, the bmap is retrieved from disk.
 *
 *	mds_bmap_load_cli() also manages the bmap_lease reference
 *	which is used to track the bmaps a particular client knows
 *	about.  mds_bmap_read() is used to retrieve the bmap from disk
 *	or create a new 'blank-slate' bmap if one does not exist.
 *	Finally, a read or write reference is placed on the bmap
 *	depending on the client request.  This is factored in with
 *	existing references to determine whether or not the bmap should
 *	be in DIO mode.
 * @fcmh: the FID cache handle for the inode.
 * @mq: the client RPC request.
 * @bmap: structure to be allocated and returned to the client.
 * Note: the bmap is not locked during disk I/O, instead it is marked
 *	with a bit (ie INIT) and other threads block on the waitq.
 */
int
mds_bmap_load_cli(struct fidc_membh *f, sl_bmapno_t bmapno, int flags,
    enum rw rw, sl_ios_id_t prefios, struct srt_bmapdesc *sbd,
    struct pscrpc_export *exp, struct bmapc_memb **bmap)
{
	struct slashrpc_export *slexp;
	struct bmap_mds_lease *bml;
	struct bmapc_memb *b;
	int rc;

	psc_assert(!*bmap);

	rc = mds_bmap_load(f, bmapno, &b);
	if (rc)
		return (rc);

	bml = psc_pool_get(bmapMdsLeasePool);
	memset(bml, 0, bmapMdsLeasePool->ppm_master->pms_entsize);
	LOCK_INIT(&bml->bml_lock);
	bml->bml_exp = exp;
	bml->bml_bmdsi = b->bcm_pri;
	bml->bml_flags = (rw == SL_WRITE ? BML_WRITE : BML_READ);

	if (flags & SRM_GETBMAPF_DIRECTIO)
		bml->bml_flags |= BML_CDIO;

	rc = mds_bmap_bml_add(bml, rw, prefios);
	if (rc) {
		psc_pool_return(bmapMdsLeasePool, bml);
		goto out;
	} else
		*bmap = b;

	/* Note the lock ordering here.
	 */
	slexp = slexp_get(exp, SLCONNT_CLI);
	spinlock(&exp->exp_lock);
	psclist_xadd_tail(&bml->bml_exp_lentry, &slexp->slexp_list);
	BML_LOCK(bml);
	bml->bml_flags |= BML_EXP;
	BML_ULOCK(bml);
	freelock(&exp->exp_lock);
	slexp_put(exp);

	sbd->sbd_seq = bml->bml_seq;
	sbd->sbd_key = (rw == SL_WRITE) ?
		   bml->bml_bmdsi->bmdsi_assign->odtr_key : BMAPSEQ_ANY;
 out:
	bmap_op_done_type(b, BMAP_OPCNT_LOOKUP);
	return (rc);
}

struct bmap_ops bmap_ops = {
	mds_bmap_init,
	mds_bmap_read,
	NULL
};
