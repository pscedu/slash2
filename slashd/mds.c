/* $Id$ */

#include "psc_ds/tree.h"
#include "psc_util/alloc.h"
#include "psc_util/assert.h"
#include "psc_util/atomic.h"

#include "cache_params.h"
#include "cfd.h"
#include "fidc_common.h"
#include "fidcache.h"
#include "inode.h"
#include "mds.h"
#include "mdscoh.h"
#include "mdsexpc.h"
#include "mdsrpc.h"
#include "slashexport.h"

struct cfdops mdsCfdOps;
struct sl_fsops mdsFsops;
extern list_cache_t pndgBmapCbs;

__static SPLAY_GENERATE(fcm_exports, mexpfcm,
			mexpfcm_fcm_tentry, mexpfcm_cache_cmp);

__static SPLAY_GENERATE(exp_bmaptree, mexpbcm,
			mexpbcm_exp_tentry, mexpbmapc_cmp);

__static SPLAY_GENERATE(bmap_exports, mexpbcm,
			mexpbcm_bmap_tentry, mexpbmapc_exp_cmp);

/**
 * mexpfcm_cfd_init - callback issued from cfdnew() which adds the
 *	provided cfd to the export tree and attaches to the fid's
 *	respective fcmh.
 * @c: the cfd, pre-initialized with fid and private data.
 * @e: the export to which the cfd belongs.
 */
int
mexpfcm_cfd_init(struct cfdent *c, struct pscrpc_export *e)
{
	struct slashrpc_export *sexp;
	struct mexpfcm *m = PSCALLOC(sizeof(*m));
	struct fidc_membh *f;
	struct fidc_mds_info *i;
	int rc=0;

	/* c->pri holds the zfs file info for this inode, it must be present.
	 */
	psc_assert(c->pri);

	sexp = e->exp_private;
	psc_assert(sexp);
	/* Serialize access to our bmap cache tree.
	 */
	LOCK_INIT(&m->mexpfcm_lock);
	/* Back pointer to our export.
	 */
	m->mexpfcm_export = e;
	/* Locate our fcmh in the global cache bumps the refcnt.
	 *  We do a simple lookup here because the inode should already exist
	 *  in the cache.
	 */
	m->mexpfcm_fcmh = f = fidc_lookup_simple(c->fdb.sfdb_secret.sfs_fg.fg_fid);
	psc_assert(f);
	/* Ensure our ref has been added.
	 */
	psc_assert(atomic_read(&f->fcmh_refcnt) > 0);
	/* Verify that the file type is consistent.
	 */
	if (fcmh_2_isdir(f))
		m->mexpfcm_flags |= MEXPFCM_DIRECTORY;
	else {
		m->mexpfcm_flags |= MEXPFCM_REGFILE;
		SPLAY_INIT(&m->mexpfcm_bmaps);
	}
	FCMH_LOCK(f);
	if (!(f->fcmh_fcoo || (f->fcmh_state & FCMH_FCOO_CLOSING))) {
		/* Allocate 'open file' data structures if they
		 *  don't already exist.
		 */
		fidc_fcoo_start_locked(f);
 fcoo_start:
		f->fcmh_fcoo->fcoo_pri = i = PSCALLOC(sizeof(*i));
		/* Set up a bogus rw ref count here.
		 */
		f->fcmh_fcoo->fcoo_oref_rw[0] = 1;
		/* This is a tricky move, copy the zfs private file data
		 *  stored in  c->pri to the mdsi. c->pri will be overwritten
		 *  at the bottom.
		 */
		fmdsi_init(i, c->pri);
		FCMH_ULOCK(f);
		// XXX do we need to read anything from the disk?
		fidc_fcoo_startdone(f);

	} else {
		int rc=fidc_fcoo_wait_locked(f, FCOO_START);

		if (rc < 0) {
			DEBUG_FCMH(PLL_ERROR, f,
				   "fidc_fcoo_wait_locked() failed");
			FCMH_ULOCK(f);
			free(m);
			return (-1);

		} else if (rc == 1)
			goto fcoo_start;

		else {
			psc_assert(f->fcmh_fcoo);
			psc_assert(f->fcmh_fcoo->fcoo_pri);
			i = f->fcmh_fcoo->fcoo_pri;
			f->fcmh_fcoo->fcoo_oref_rw[0]++;
			psc_assert(i->fmdsi_data);
			FCMH_ULOCK(f);
		}
	}
	/* Add ourselves to the fidc_mds_info structure's splay tree.
	 *  fmdsi_ref is the real open refcnt (one ref per export or client).
	 *  Note that muliple client opens are handled on the client and
	 *  should no be passed to the mds.  However the open mode can change.
	 */
	atomic_inc(&i->fmdsi_ref);
	FCMH_LOCK(f);
	if (SPLAY_INSERT(fcm_exports, &i->fmdsi_exports, m)) {
		psc_warnx("Tried to reinsert m(%p) "FIDFMT,
			   m, FIDFMTARGS(mexpfcm2fidgen(m)));
		rc = EINVAL;
	} else
		psc_info("Added m=%p e=%p to tree %p",
			 m, e,  &i->fmdsi_exports);

	FCMH_ULOCK(f);
	/* Add the fidcache reference to the cfd's private slot.
	 */
	c->pri = m;
	fidc_membh_dropref(f);
	return (rc);
}

void *
mexpfcm_cfd_get_zfsdata(struct cfdent *c, __unusedx struct pscrpc_export *e)
{
	struct mexpfcm *m=c->pri;
	struct fidc_membh *f=m->mexpfcm_fcmh;
	struct fidc_mds_info *i=f->fcmh_fcoo->fcoo_pri;

	psc_info("zfs opendir data (%p)", i->fmdsi_data);

	return(i->fmdsi_data);
}

__static void
mds_bmap_ref_del(struct mexpbcm *bref);

int
mexpfcm_cfd_free(struct cfdent *c, __unusedx struct pscrpc_export *e)
{
	int l;
	struct mexpfcm *m=c->pri;
	struct fidc_membh *f=m->mexpfcm_fcmh;
	struct fidc_mds_info *i=f->fcmh_fcoo->fcoo_pri;

	spinlock(&m->mexpfcm_lock);
	psc_assert(!(m->mexpfcm_flags & MEXPFCM_CLOSING));
	m->mexpfcm_flags |= MEXPFCM_CLOSING;
	/* Verify that all of our bmap references have already been freed.
	 */
	if (m->mexpfcm_flags & MEXPFCM_REGFILE) {
		psc_assert(c->type & CFD_CLOSING);

		if (c->type & CFD_FORCE_CLOSE) {
			struct mexpbcm *bref;
			SPLAY_FOREACH(bref, exp_bmaptree,
				      &m->mexpfcm_bmaps)
				mds_bmap_ref_del(bref);
		} else
			psc_assert(SPLAY_EMPTY(&m->mexpfcm_bmaps));
	}
	freelock(&m->mexpfcm_lock);
	/* Grab the fcmh lock (it is responsible for fidc_mds_info
	 *  serialization) and remove ourselves from the tree.
	 */
	l = reqlock(&f->fcmh_lock);
	if (SPLAY_REMOVE(fcm_exports, &i->fmdsi_exports, m) == NULL)
		psc_fatalx("Failed to remove %p export(%p) from %p",
		    m, m->mexpfcm_export, &i->fmdsi_exports);
	ureqlock(&f->fcmh_lock, l);

	c->pri = NULL;
	PSCFREE(m);
	return (0);
}

__static int
mds_bmap_fsz_check_locked(struct fidc_membh *f, sl_blkno_t n)
{
	struct fidc_mds_info *mdsi=f->fcmh_fcoo->fcoo_pri;
	sl_inodeh_t *i=&mdsi->fmdsi_inodeh;
	//int rc;

	FCMH_LOCK_ENSURE(f);
#if 0
	if ((rc = mds_stat_refresh_locked(f)))
		return (rc);
#endif

	psc_trace("fid="FIDFMT" lblk=%zu fsz=%zu",
		  FIDFMTARGS(fcmh_2_fgp(f)), i->inoh_ino.ino_lblk,
		  fcmh_2_fsz(f));

	/* Verify that the inode agrees with file contents.
	 *  XXX this assert is a bit too aggressive - perhaps a more
	 *  interesting method is to read the bmap anyway and compare its
	 *  checksum against the bmapod NULL chksum.  This method would cope
	 *  with holes in the inode file.
	 */
	psc_assert((((i->inoh_ino.ino_lblk+1) * BMAP_OD_SZ) - 1)
		   <= fcmh_2_fsz(f));
	return ((n > i->inoh_ino.ino_lblk) ? n : 0);
}

/**
 * mds_bmap_directio - queue mexpbcm's so that their clients may be
 *	notified of the cache policy change (to directio) for this bmap.
 *	The mds_coherency thread is responsible for actually issuing and
 *	checking the status of the rpc's.  Communication between this
 *	thread and the mds_coherency thread occurs by placing the
 *	mexpbcm's onto the pndgBmapCbs listcache.
 * @bmap: the bmap which is going dio.
 * Notes:  XXX this needs help but it's making my brain explode right now.
 */
#define mds_bmap_directio_check(b) mds_bmap_directio(b, 1, 1)
#define mds_bmap_directio_set(b)   mds_bmap_directio(b, 1, 0)
#define mds_bmap_directio_unset(b) mds_bmap_directio(b, 0, 0)

static void
mds_bmap_directio(struct mexpbcm *bref, int enable_dio, int check)
{
	struct bmapc_memb *bmap=bref->mexpbcm_bmap;
	struct bmap_mds_info *mdsi=bmap->bcm_pri;
	int mode=bref->mexpbcm_mode;

	psc_assert(mdsi && mdsi->bmdsi_wr_ion);
	BMAP_LOCK_ENSURE(bmap);

	DEBUG_BMAP(PLL_TRACE, bmap, "enable=%d check=%d",
		   enable_dio, check);

	/* Iterate over the tree and pick up any clients which still cache
	 *   this bmap.
	 */
	SPLAY_FOREACH(bref, bmap_exports, &mdsi->bmdsi_exports) {
		struct psclist_head *e=&bref->mexpbcm_lentry;
		/* Lock while the attributes of the this bref are
		 *  tested.
		 */
		MEXPBCM_LOCK(bref);
		psc_assert(bref->mexpbcm_export);
		/* Don't send rpc if the client is already using DIO or
		 *  has an rpc in flight (_REQD).
		 */
		if (enable_dio &&                    /* turn dio on */
		    !(mode & MEXPBCM_CDIO) &&        /* client allows dio */
		    (!((mode & MEXPBCM_DIO) ||       /* dio not already on */
		       (mode & MEXPBCM_DIO_REQD)) || /* dio not coming on */
		     (mode & MEXPBCM_CIO_REQD))) {   /* dio being disabled */
			if (check) {
				char clinidstr[PSC_NIDSTR_SIZE];
				psc_nid2str(mexpbcm2nid(bref), clinidstr);

				DEBUG_BMAP(PLL_WARN, bref->mexpbcm_bmap,
					   "cli(%s) has not acknowledged dio "
					   "rpc for bmap(%p) bref(%p) "
					   "sent:(%d 0==true)",
					   clinidstr, bmap, bref,
					   atomic_read(&bref->mexpbcm_msgcnt));
					   continue;
			}
			bref->mexpbcm_mode |= MEXPBCM_DIO_REQD;

			if (mode & MEXPBCM_CIO_REQD) {
				/* This bref is already enqueued and may
				 *   have completed.
				 *  Verify the current inflight mode.
				 */
				mdscoh_infmode_chk(bref, MEXPBCM_CIO_REQD);
				psc_assert(psclist_conjoint(e));
				if (!bref->mexpbcm_net_inf) {
					/* Unschedule this rpc, the coh thread will
					 *  remove it from the listcache.
					 */
					bref->mexpbcm_mode &= ~MEXPBCM_CIO_REQD;
					bref->mexpbcm_net_cmd = MEXPBCM_RPC_CANCEL;
				} else {
					/* Inform the coh thread to requeue.
					 */
					bref->mexpbcm_net_cmd = MEXPBCM_DIO_REQD;
					bref->mexpbcm_mode |= MEXPBCM_DIO_REQD;
				}
			} else {
				/* Queue this one.
				 */
				bref->mexpbcm_mode |= MEXPBCM_DIO_REQD;
				bref->mexpbcm_net_cmd = MEXPBCM_DIO_REQD;
				psc_assert(psclist_disjoint(e));
				lc_queue(&pndgBmapCbs, e);
			}

		} else if (!enable_dio &&                  /* goto cache io */
			   ((mode & MEXPBCM_DIO) ||        /* we're in dio mode OR */
			    (mode & MEXPBCM_DIO_REQD))) {  /* we're going to dio mode */

			psc_assert(!mode & MEXPBCM_CDIO);
			if (mode & MEXPBCM_DIO_REQD) {
				/* We'd like to disable DIO mode but a re-enable request
				 *  has been queued recently.  Determine if it's inflight
				 *  of if it still queued.
				 */
				psc_assert(psclist_conjoint(e));
				mdscoh_infmode_chk(bref, MEXPBCM_DIO_REQD);
				if (!bref->mexpbcm_net_inf) {
					/* Unschedule this rpc, the coh thread will
					 *  remove it from the listcache.
					 */
					bref->mexpbcm_mode &= ~MEXPBCM_DIO_REQD;
					bref->mexpbcm_net_cmd = MEXPBCM_RPC_CANCEL;
				} else {
					bref->mexpbcm_net_cmd = MEXPBCM_CIO_REQD;
					bref->mexpbcm_mode |= MEXPBCM_CIO_REQD;
				}
			} else {
				bref->mexpbcm_mode |= MEXPBCM_CIO_REQD;
				bref->mexpbcm_net_cmd = MEXPBCM_CIO_REQD;
				psc_assert(psclist_disjoint(e));
				lc_queue(&pndgBmapCbs, e);
			}
		}
		MEXPBCM_ULOCK(bref);
	}
}

__static void
mds_mion_init(struct mexp_ion *mion, sl_resm_t *resm)
{
	dynarray_init(&mion->mi_bmaps);
	dynarray_init(&mion->mi_bmaps_deref);
	INIT_PSCLIST_ENTRY(&mion->mi_lentry);
	atomic_set(&mion->mi_refcnt, 0);
	mion->mi_resm = resm;
	mion->mi_csvc = rpc_csvc_create(SRIM_REQ_PORTAL, SRIM_REP_PORTAL);
}

__static int
mds_bmap_ion_assign(struct mexpbcm *bref, sl_ios_id_t pios)
{
	struct bmapc_memb *bmap=bref->mexpbcm_bmap;
	struct bmap_mds_info *mdsi=bmap->bcm_pri;
	//struct fidc_membh *f=bmap->bcm_fcmh;
	struct mexp_ion *mion;
	sl_resource_t *res=libsl_id2res(pios);
	sl_resm_t *resm;
	struct resprof_mds_info *rmi;
	int n, x, rc=0;

	psc_assert(!mdsi->bmdsi_wr_ion);
	n = atomic_read(&bmap->bcm_wr_ref);
	psc_assert(n == 0 || n == 1);
	n = atomic_read(&mdsi->bmdsi_wr_ref);
	psc_assert(n == 0 || n == 1);

	if (!res) {
		psc_warnx("Failed to find pios %d", pios);
		return (-1);
	}
	rmi = res->res_pri;
	psc_assert(rmi);
	x = res->res_nnids;

	do {
		spinlock(&rmi->rmi_lock);
		n = rmi->rmi_cnt++;
		if (rmi->rmi_cnt > (int)res->res_nnids)
			rmi->rmi_cnt = 0;

		psc_trace("trying res(%s) ion(%s)",
			  res->res_name, libcfs_nid2str(res->res_nids[n]));

		resm = libsl_nid2resm(res->res_nids[n]);
		if (!resm)
			psc_fatalx("Failed to lookup %s, verify that slash "
				   "configs are uniform across all servers",
				   libcfs_nid2str(res->res_nids[n]));

		if (!resm->resm_pri) {
			/* First time this resm has been used.
			 */
			resm->resm_pri = PSCALLOC(sizeof(*mion));
			mds_mion_init(resm->resm_pri, resm);
		}
		mion = resm->resm_pri;
		freelock(&rmi->rmi_lock);

		DEBUG_BMAP(PLL_TRACE, bref->mexpbcm_bmap,
		    "res(%s) ion(%s) init=%d, failed=%d",
		    res->res_name, libcfs_nid2str(res->res_nids[n]),
		    mion->mi_csvc->csvc_initialized,
		    mion->mi_csvc->csvc_failed);

		if (mion->mi_csvc->csvc_failed)
			continue;

		if (!mion->mi_csvc->csvc_initialized) {
			//struct pscrpc_connection *c=
			//      mion->mi_csvc->csvc_import->imp_connection;

			rc = rpc_issue_connect(res->res_nids[n],
			    mion->mi_csvc->csvc_import,
			    SRIM_MAGIC, SRIM_VERSION);
			if (rc) {
				mion->mi_csvc->csvc_failed = 1;
				continue;
			} else
				mion->mi_csvc->csvc_initialized = 1;
		}
		mdsi->bmdsi_wr_ion = mion;
	} while (--x);

	if (!mdsi->bmdsi_wr_ion)
		return (-1);

	DEBUG_BMAP(PLL_INFO, bref->mexpbcm_bmap, "using res(%s) ion(%s)",
		   res->res_name, libcfs_nid2str(res->res_nids[n]));
	return (0);
}

/**
 * mds_bmap_ref_add - add a read or write reference to the bmap's tree
 *	and refcnts.  This also calls into the directio_[check|set]
 *	calls depending on the number of read and/or write clients of
 *	this bmap.
 * @bref: the bref to be added, it must have a bmapc_memb already attached.
 * @rw: the explicit read/write flag from the rpc.  It is probably
 *	unwise to use bref's flag.
 */
__static void
mds_bmap_ref_add(struct mexpbcm *bref, struct srm_bmap_req *mq)
{
	struct bmapc_memb *bmap=bref->mexpbcm_bmap;
	struct bmap_mds_info *bmdsi=bmap->bcm_pri;
	int wr[2], rw=mq->rw,
		mode=(rw == SRIC_BMAP_READ ? BMAP_MDS_RD : BMAP_MDS_WR);
	atomic_t *a=(rw == SRIC_BMAP_READ ? &bmdsi->bmdsi_rd_ref :
		     &bmdsi->bmdsi_wr_ref);

	if (rw == SRIC_BMAP_READ)
		psc_assert(bref->mexpbcm_mode & MEXPBCM_RD);
	else if (rw == SRIC_BMAP_WRITE)
		psc_assert(bref->mexpbcm_mode & MEXPBCM_WR);
	else
		psc_fatalx("mode value (%d) is invalid", rw);

	BMAP_LOCK(bmap);
	if (!atomic_read(a)) {
		/* There are no refs for this mode, therefore the
		 *   bcm_bmapih.bmapi_mode should not be set.
		 */
		psc_assert(!(bmap->bcm_mode & mode));
		bmap->bcm_mode = mode;
	}
	/* Set and check ref cnts now.
	 */
	atomic_inc(a);
	bmdsi_sanity_locked(bmap, 0, wr);

	if (wr[0] == 1 && mode == BMAP_MDS_WR) {
		psc_assert(!bmdsi->bmdsi_wr_ion);
		/* XXX Should not send connect rpc's here while
		 *  the bmap is locked.  This may have to be
		 *  replaced by a waitq and init flag.
		 */
		mds_bmap_ion_assign(bref, mq->pios);
	}
	/* Do directio checks here.
	 */
	if (wr[0] > 2)
		/* It should have already been set.
		 */
		mds_bmap_directio_check(bref);

	else if (wr[0] == 2 || (wr[0] == 1 && wr[1]))
		/* These represent the two possible 'add' related transitional
		 *  states, more than 1 writer or the first writer amidst
		 *  existing readers.
		 */
		mds_bmap_directio_set(bref);
	/* Pop it on the tree.
	 */
	if (SPLAY_INSERT(bmap_exports, &bmdsi->bmdsi_exports, bref))
		psc_fatalx("found duplicate bref on bmap_exports");

	DEBUG_BMAP(PLL_TRACE, bmap, "done with ref_add");
	BMAP_ULOCK(bmap);
}

/**
 * mds_bmap_ref_add - add a read or write reference to the bmap's tree
 *	and refcnts.  This also calls into the directio_[check|set]
 *	calls depending on the number of read and/or write clients of
 *	this bmap.
 * @bref: the bref to be added, it must have a bmapc_memb already attached.
 */
__static void
mds_bmap_ref_del(struct mexpbcm *bref)
{
	struct bmapc_memb *bmap=bref->mexpbcm_bmap;
	struct bmap_mds_info *mdsi=bmap->bcm_pri;
	int wr[2];

	BMAP_LOCK(bmap);

	if (bref->mexpbcm_mode & MEXPBCM_WR) {
		psc_assert(atomic_read(&mdsi->bmdsi_wr_ref));
		if (atomic_dec_and_test(&mdsi->bmdsi_wr_ref)) {
			psc_assert(bmap->bcm_mode & BMAP_MDS_WR);
			bmap->bcm_mode &= ~BMAP_MDS_WR;
		}

	} else if (bref->mexpbcm_mode & MEXPBCM_RD) {
		psc_assert(atomic_read(&mdsi->bmdsi_rd_ref));
		if (atomic_dec_and_test(&mdsi->bmdsi_rd_ref)) {
			bmap->bcm_mode &= ~BMAP_MDS_RD;
		}
	}

	bmdsi_sanity_locked(bmap, 0, wr);

	if (!wr[0] || (wr[0] == 1 && !wr[1]))
		mds_bmap_directio_unset(bref);

	if (!SPLAY_REMOVE(bmap_exports, &mdsi->bmdsi_exports, bref))
		psc_fatalx("found duplicate bref on bmap_exports");

	DEBUG_BMAP(PLL_TRACE, bmap, "done with ref_del");
	BMAP_ULOCK(bmap);
}

/**
 * mds_bmap_crc_write - process an crc update request from an ION.
 * @mq: the rpc request containing the fid, the bmap blkno, and the bmap
 *	chunk id (cid).
 * @ion_nid:  the id of the io node which sent the request.  It is
 *	compared against the id stored in bmdsi.
 */
int
mds_bmap_crc_write(struct srm_bmap_crcup *c, lnet_nid_t ion_nid)
{
	struct fidc_membh *fcmh;
	struct bmapc_memb *bmap;
	struct bmap_mds_info *bmdsi;
	sl_blkh_t *bmapod;
	int rc=0, wr[2];

	fcmh = fidc_lookup_inode(c->fid);
	if (!fcmh)
		return (-EBADF);

	bmap = bmap_lookup(fcmh, c->blkno);
	if (!bmap)
		return (-EBADF);

	BMAP_LOCK(bmap);

	DEBUG_BMAP(PLL_TRACE, bmap, "blkno=%u ion=%s",
		   c->blkno, libcfs_nid2str(ion_nid));

	bmdsi = bmap->bcm_pri;
	bmapod = bmdsi->bmdsi_od;
	/* These better check out.
	 */
	psc_assert(bmap->bcm_fcmh == fcmh);
	psc_assert(bmdsi);
	psc_assert(bmapod);
	psc_assert(bmap->bcm_mode & BMAP_MDS_WR);
	bmdsi_sanity_locked(bmap, 1, wr);

	if (ion_nid != bmdsi->bmdsi_wr_ion->mi_resm->resm_nid) {
		/* Whoops, we recv'd a request from an unexpected nid.
		 */
		rc = -EINVAL;
		BMAP_ULOCK(bmap);
		goto out;

	} else if (bmap->bcm_mode & BMAP_MDS_CRC_UP) {
		/* Ensure that this thread is the only thread updating the
		 *  bmap crc table.
		 */
		rc = -EALREADY;
		BMAP_ULOCK(bmap);
		goto out;

	} else {
		/* Mark that bmap is undergoing crc updates - this is non-
		 *  reentrant so the ION must know better than to send
		 *  multiple requests for the same bmap.
		 */
		bmap->bcm_mode |= BMAP_MDS_CRC_UP;
		/* If the bmap had no contents, denote that it now does.
		 */
		if (bmap->bcm_mode & BMAP_MDS_EMPTY)
			bmap->bcm_mode &= ~BMAP_MDS_EMPTY;
	}
	/* XXX Note the lock ordering here BMAP -> INODEH
	 * mds_repl_inv_except_locked() takes the lock.
	 *  This shouldn't be racy because
	 *   . only one export may be here (ion_nid)
	 *   . the bmap is locked.
	 */
	if ((rc = mds_repl_inv_except_locked(bmap, (sl_ios_id_t)ion_nid))) {
		BMAP_ULOCK(bmap);
		goto out;
	}
	if (bmap->bcm_mode & BMAP_MDS_EMPTY)
		bmap->bcm_mode &= ~BMAP_MDS_EMPTY;

	/* XXX ok if replicas exist, the gen has to be bumped and the
	 *  replication bmap modified.
	 *  Schedule the bmap for writing.
	 */
	BMAP_ULOCK(bmap);
	/* Call the journal and update the in-memory crc's.
	 */
	mds_bmap_crc_log(bmap, c);
 out:
	/* Mark that mds_bmap_crc_write() is done with this bmap
	 *  - it was incref'd in fcmh_bmap_lookup().
	 */
	bmap_op_done(bmap);
	return (rc);
}

/**
 * mds_bmapod_initnew - called when a read request offset exceeds the
 *	bounds of the file causing a new bmap to be created.
 * Notes:  Bmap creation race conditions are prevented because the bmap
 *	handle already exists at this time with
 *	bmapi_mode == BMAP_MDS_INIT.
 *
 *	This causes other threads to block on the waitq until
 *	read/creation has completed.
 * More Notes:  this bmap is not written to disk until a client actually
 *	writes something to it.
 */
__static void
mds_bmapod_initnew(sl_blkh_t *b)
{
	int i;

	for (i=0; i < SL_CRCS_PER_BMAP; i++)
		b->bh_crcs[i].gc_crc = SL_NULL_CRC;

	PSC_CRC_CALC(b->bh_bhcrc, b, BMAP_OD_CRCSZ);
}

/**
 * mds_bmap_read - retrieve a bmap from the ondisk inode file.
 * @fcmh: inode structure containing the fid and the fd.
 * @blkno: the bmap block number.
 * @bmapod: on disk structure containing crc's and replication bitmap.
 */
__static int
mds_bmap_read(struct fidc_membh *f, sl_blkno_t blkno,
	      sl_blkh_t **bmapod)
{
	int rc=0;
	psc_crc_t crc;

	*bmapod = PSCALLOC(BMAP_OD_SZ);
#if 0
	ssize_t szrc;

	/* Try to pread() the bmap from the mds file.
	 *  XXX replace with mds read ops.
	 */
	szrc = pread(f->fcmh_fd, *bmapod, BMAP_OD_SZ,
	     (blkno * BMAP_OD_SZ));

	if (szrc != BMAP_OD_SZ) {
		DEBUG_FCMH(PLL_WARN, f, "bmap (%u) pread (rc=%zd, e=%d)",
			   blkno, szrc, errno);
		rc = -errno;
		goto out;
	}
#endif
	PSC_CRC_CALC(crc, *bmapod, BMAP_OD_CRCSZ);
	if (crc == SL_NULL_BMAPOD_CRC) {
		/* sl_blkh_t may be a bit large for the stack.
		 */
		sl_blkh_t *t = PSCALLOC(sizeof(sl_blkh_t));
		int rc;
		/* Hit the NULL crc, verify that this is not a collision
		 *  by comparing with null bmapod.
		 */
		rc = memcmp(t, *bmapod, sizeof(*t));
		PSCFREE(t);
		if (rc)
			goto crc_fail;
		else {
			/* It really is a null bmapod, create a new, blank
			 *  bmapod for the cache.
			 */
			mds_bmapod_initnew(*bmapod);
		}
	} else if (crc != (*bmapod)->bh_bhcrc)
		goto crc_fail;
 out:
	if (rc) {
		PSCFREE(*bmapod);
		*bmapod = NULL;
	}
	return (rc);

 crc_fail:
	DEBUG_FCMH(PLL_WARN, f, "bmap (%u) crc failed want(%zu) got(%zu)",
		   blkno, (*bmapod)->bh_bhcrc, crc);
	rc = -EIO;
	goto out;
}

/**
 * mds_bmap_load - routine called to retrieve a bmap, presumably so that
 *	it may be sent to a client.  It first checks for existence in
 *	the cache, if needed, the bmap is retrieved from disk.
 *
 *	mds_bmap_load() also manages the mexpfcm's mexpbcm reference
 *	which is used to track the bmaps a particular client knows
 *	about.  mds_bmap_read() is used to retrieve the bmap from disk
 *	or create a new 'blank-slate' bmap if one does not exist.
 *	Finally, a read or write reference is placed on the bmap
 *	depending on the client request.  This is factored in with
 *	existing references to determine whether or not the bmap should
 *	be in DIO mode.
 * @fref: the fidcache reference for the inode (stored in the private
 *	pointer of the cfd).
 * @mq: the client rpc request.
 * @bmap: structure to be allocated and returned to the client.
 * Note: the bmap is not locked during disk I/O, instead it is marked
 *	with a bit (ie INIT) and other threads block on the waitq.
 */
int
mds_bmap_load(struct mexpfcm *fref, struct srm_bmap_req *mq,
	      struct bmapc_memb **bmap)
{
	struct bmapc_memb tbmap;
	struct fidc_membh *f=fref->mexpfcm_fcmh;
	struct fidc_mds_info *fmdsi=f->fcmh_fcoo->fcoo_pri;
	sl_inodeh_t *inoh=&fmdsi->fmdsi_inodeh;
	struct mexpbcm *bref, tbref;
	int rc=0;

	psc_assert(inoh);
	psc_assert(!*bmap);

	if ((mq->rw != SRIC_BMAP_READ) && (mq->rw != SRIC_BMAP_WRITE))
		return -EINVAL;

	tbmap.bcm_blkno = mq->blkno;
	tbref.mexpbcm_bmap = &tbmap;
	/* This bmap load *should* be for a bmap which the client has not
	 *   already referenced and therefore no mexpbcm should exist.  The
	 *   mexpbcm exists for each bmap that the client has cached.
	 */
	MEXPFCM_LOCK(fref);
	bref = SPLAY_FIND(exp_bmaptree, &fref->mexpfcm_bmaps, &tbref);
	if (bref) {
		/* If we're here then the same client tried to cache this
		 *  bmap more than once which is an invalid behavior.
		 */
		MEXPFCM_ULOCK(fref);
		return (-EBADF);
	} else {
		/* Establish a reference here, note that mexpbcm_bmap will
		 *   be null until either the bmap is loaded or pulled from
		 *   the cache.
		 */
		bref = PSCALLOC(sizeof(*bref));
		bref->mexpbcm_mode = MEXPBCM_INIT;
		bref->mexpbcm_blkno = mq->blkno;
		bref->mexpbcm_export = fref->mexpfcm_export;
		SPLAY_INSERT(exp_bmaptree, &fref->mexpfcm_bmaps, bref);
	}
	MEXPFCM_ULOCK(fref);
	/* Ok, the bref has been initialized and loaded into the tree.  We
	 *  still need to set the bmap pointer mexpbcm_bmap though.  Lock the
	 *  fcmh during the bmap lookup.
	 */
	FCMH_LOCK(f);
	*bmap = SPLAY_FIND(bmap_cache, &f->fcmh_fcoo->fcoo_bmapc, &tbmap);
	if (*bmap) {
		/* Found it, still don't know if we're in directio mode..
		 */
		FCMH_ULOCK(f);
 retry:
		BMAP_LOCK(*bmap);
		if ((*bmap)->bcm_mode & BMAP_MDS_INIT) {
			/* Only the init bit is allowed to be set.
			 */
			psc_assert((*bmap)->bcm_mode ==
				   BMAP_MDS_INIT);
			/* Sanity checks for BMAP_MDS_INIT
			 */
			psc_assert(!(*bmap)->bcm_pri);
			psc_assert(!(*bmap)->bcm_fcmh);
			/* Block until the other thread has completed the io.
			 */
			psc_waitq_wait(&(*bmap)->bcm_waitq,
				       &(*bmap)->bcm_lock);
			goto retry;
		} else {
			/* Sanity check relevant pointers.
			 */
			psc_assert((*bmap)->bcm_pri);
			psc_assert((*bmap)->bcm_fcmh);
		}

	} else {
		struct bmap_mds_info *bmdsi;
		/* Create and initialize the new bmap while holding the
		 *  fcmh lock which is needed for atomic tree insertion.
		 */
		*bmap = PSCALLOC(sizeof(struct bmapc_memb)); /* XXX not freed */
		(*bmap)->bcm_blkno = mq->blkno;
		(*bmap)->bcm_mode = BMAP_MDS_INIT;
		bmdsi = (*bmap)->bcm_pri = PSCALLOC(sizeof(struct bmap_mds_info)); /* XXX not freed */
		LOCK_INIT(&(*bmap)->bcm_lock);
		psc_waitq_init(&(*bmap)->bcm_waitq);
		(*bmap)->bcm_fcmh = f;
		/* It's ready to go, place it in the tree.
		 */
		SPLAY_INSERT(bmap_cache, &f->fcmh_fcoo->fcoo_bmapc, *bmap);
		/* Finally, the fcmh may be unlocked.  Other threads
		 *   wishing to access the bmap will block on bcm_waitq
		 *   until we have finished reading it from disk.
		 */
		FCMH_ULOCK(f);

		rc = mds_bmap_read(f, mq->blkno,
				   &bmdsi->bmdsi_od);
		if (rc)
			goto fail;

		(*bmap)->bcm_mode = 0;
		/* Notify other threads that this bmap has been loaded, they're
		 *  blocked on BMAP_MDS_INIT.
		 */
		psc_waitq_wakeall(&(*bmap)->bcm_waitq);
	}

	bmap_set_accesstime(*bmap);
	/* Not sure if these are really needed on the mds.
	 */
	if (mq->rw == SRIC_BMAP_WRITE)
		atomic_inc(&(*bmap)->bcm_wr_ref);
	else
		atomic_inc(&(*bmap)->bcm_rd_ref);
	/* Sanity checks, make sure that we didn't let the client in
	 *  before this bmap was ready.
	 */
	MEXPBCM_LOCK(bref);

	psc_assert(bref->mexpbcm_mode == MEXPBCM_INIT);

	bref->mexpbcm_bmap = *bmap;
	bref->mexpbcm_mode = ((mq->rw == SRIC_BMAP_WRITE) ? MEXPBCM_WR : 0);
	/* Check if the client requested directio, if so tag it in the
	 *  bref.
	 */
	if (mq->dio)
		bref->mexpbcm_mode |= MEXPBCM_CDIO;

	MEXPBCM_ULOCK(bref);
	/* Place our bref on the tree, manage any mode changes that result
	 *  from this new reference.  Also, on write choose an ION if needed.
	 */
	mds_bmap_ref_add(bref, mq);

	return (0);
 fail:
	(*bmap)->bcm_mode = BMAP_MDS_FAILED;
	/* XXX think about policy updates in fail mode.
	 */
	return (rc);
}

/**
 * mds_fidfs_lookup - "lookup file id via filesystem".  This call does a
 *	getattr on the provided pathname, loads (and verifies) the info
 *	from the getattr and then does a lookup into the fidcache.
 * XXX clean me up.. extract crc stuff..
 * XXX until dcache is done, this must be done for every lookup.
 */
#if 0
int
mds_fidfs_lookup(const char *path, struct slash_creds *creds,
		 struct fidc_membh **fcmh)
{
	int rc;
	sl_inodeh_t  inoh;
	size_t       sz=sizeof(sl_inode_t);
	psc_crc_t    crc;

	psc_assert(fcmh && !(*fcmh));

	rc = access_fsop(ACSOP_GETATTR, creds->uid, creds->gid, path,
			 SFX_INODE, &inoh.inoh_ino, sz);

	if (rc < 0)
		psc_warn("Attr lookup on (%s) failed", path);
	else if (rc != sz)
		psc_warn("Attr lookup on (%s) gave invalid sz (%d)", path, rc);
	else
		rc=0;

	PSC_CRC_CALC(&crc, &inoh.inoh_ino, sz);
	if (crc != inoh.inoh_ino.ino_crc) {
		psc_warnx("Crc failure on inode");
		errno = EIO;
		return -1;
	}
	if (inoh.inoh_ino.ino_nrepls) {
		sz = sizeof(sl_replica_t) * inoh.inoh_ino.ino_nrepls;
		inoh->inoh_replicas = PSCALLOC(sz);
		rc = access_fsop(ACSOP_GETATTR, creds->uid, creds->gid, path,
				 SFX_REPLICAS, inoh.inoh_replicas, sz);
		if (rc < 0)
			psc_warn("Attr lookup on (%s) failed", path);
		else if (rc != sz)
			psc_warn("Attr lookup on (%s) gave invalid sz (%d)",
				 path, rc);
		else
			rc=0;

		PSC_CRC_CALC(&crc, inoh.inoh_replicas, sz);
		if (crc != inoh.inoh_ino.ino_rs_crc) {
			psc_warnx("Crc failure on replicas");
			errno = EIO;
			*fcmh = NULL;
			return -1;
		}
	}
	*fcmh = fidc_lookup_ino(&inoh.inoh_ino);
	psc_assert(*fcmh);

	return(rc);
}
#endif

__static void
mds_cfdops_init(void)
{
	mdsCfdOps.cfd_init = mexpfcm_cfd_init;
	mdsCfdOps.cfd_free = mexpfcm_cfd_free;
	mdsCfdOps.cfd_insert = NULL;
	mdsCfdOps.cfd_get_pri = mexpfcm_cfd_get_zfsdata;
}

void
mds_init(void)
{
	mds_cfdops_init();
	//	initFcooCb = mds_fcoo_init_cb;
	//mdsFsops.slfsop_getattr  = mds_fidfs_lookup;
	//mdsFsops.slfsop_fgetattr = mds_fid_lookup;
	//slFsops = &mdsFsops;

	//mdscoh_init();
	//mdsfssync_init();
}
