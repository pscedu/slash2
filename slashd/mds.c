/* $Id$ */

#include "psc_ds/tree.h"
#include "psc_util/alloc.h"
#include "psc_util/atomic.h"
#include "psc_util/log.h"

#include "cache_params.h"
#include "cfd.h"
#include "fidc_mds.h"
#include "fidcache.h"
#include "inode.h"
#include "mds_repl.h"
#include "mdscoh.h"
#include "mdsexpc.h"
#include "mdsio_zfs.h"
#include "mdslog.h"
#include "mdsrpc.h"
#include "slashdthr.h"
#include "slashexport.h"
#include "slashd.h"

struct odtable *mdsBmapAssignTable;
struct slash_bmap_od null_bmap_od;
struct slash_inode_od null_inode_od;
struct cfdops mdsCfdOps;
struct sl_fsops mdsFsops;

__static SPLAY_GENERATE(fcm_exports, mexpfcm,
			mexpfcm_fcm_tentry, mexpfcm_cache_cmp);

__static SPLAY_GENERATE(exp_bmaptree, mexpbcm,
			mexpbcm_exp_tentry, mexpbmapc_cmp);

__static SPLAY_GENERATE(bmap_exports, mexpbcm,
			mexpbcm_bmap_tentry, mexpbmapc_exp_cmp);


__static void
mds_inode_od_initnew(struct slash_inode_handle *i)
{
	i->inoh_flags = (INOH_INO_NEW | INOH_INO_DIRTY);
	COPYFID(&i->inoh_ino.ino_fg, fcmh_2_fgp(i->inoh_fcmh));
	/* For now this is a fixed size.
	 */
	i->inoh_ino.ino_bsz = SLASH_BMAP_SIZE;
	i->inoh_ino.ino_version = INO_VERSION;
	i->inoh_ino.ino_flags = 0;
	i->inoh_ino.ino_nrepls = 0;
	mds_inode_sync(i);
}

int
mds_inode_release(struct fidc_membh *f)
{
	struct fidc_mds_info *i;
	int rc=0;

	spinlock(&f->fcmh_lock);

	psc_assert(f->fcmh_fcoo && f->fcmh_fcoo->fcoo_pri);
	/* It should be safe to look at the fmdsi without calling
	 *   fidc_fcmh2fmdsi() because this thread should absolutely have
	 *   a ref to the fcmh which should be open.
	 */
	i = fcmh_2_fmdsi(f);

	DEBUG_FCMH(PLL_DEBUG, f, "i->fmdsi_ref (%d) (oref=%d)",
		   atomic_read(&i->fmdsi_ref), f->fcmh_fcoo->fcoo_oref_rw[0]);

	if (atomic_dec_and_test(&i->fmdsi_ref)) {
		psc_assert(SPLAY_EMPTY(&i->fmdsi_exports));
		/* We held the final reference to this fcoo, it must
		 *   return attached.
		 */
		psc_assert(!fidc_fcoo_wait_locked(f, 1));

		f->fcmh_state |= FCMH_FCOO_CLOSING;
		DEBUG_FCMH(PLL_DEBUG, f, "calling zfsslash2_release");
		rc = mdsio_zfs_release(&i->fmdsi_inodeh);
		PSCFREE(i);
		f->fcmh_fcoo->fcoo_pri = NULL;
		f->fcmh_fcoo->fcoo_oref_rw[0] = 0;
		freelock(&f->fcmh_lock);
		fidc_fcoo_remove(f);
	} else
		freelock(&f->fcmh_lock);
	return (rc);
}

__static int
mds_inode_read(struct slash_inode_handle *i)
{
	psc_crc_t crc;
	int rc=0, locked;

	locked = reqlock(&i->inoh_lock);
	psc_assert(i->inoh_flags & INOH_INO_NOTLOADED);

	rc = mdsio_zfs_inode_read(i);
	if (rc) {
		DEBUG_INOH(PLL_WARN, i, "mdsio_zfs_inode_read: %d", errno);
		rc = -EIO;
		goto out;
	}

	if ((!i->inoh_ino.ino_crc) &&
	    (!memcmp(&i->inoh_ino, &null_inode_od, sizeof(null_inode_od)))) {
		DEBUG_INOH(PLL_INFO, i, "detected a new inode");
		mds_inode_od_initnew(i);
		goto out;
	}

	psc_crc_calc(&crc, &i->inoh_ino, INO_OD_CRCSZ);
	if (crc == i->inoh_ino.ino_crc) {
		i->inoh_flags &= ~INOH_INO_NOTLOADED;
		DEBUG_INOH(PLL_INFO, i, "successfully loaded inode od");
		goto out;
	}

	DEBUG_INOH(PLL_WARN, i, "CRC failed want=%"PRIx64", got=%"PRIx64,
		   i->inoh_ino.ino_crc, crc);
//	rc = -EIO;

 out:
	ureqlock(&i->inoh_lock, locked);
	return (rc);
}

int
mds_fcmh_tryref_fmdsi(struct fidc_membh *f)
{
	int rc;

	rc = 0;
	FCMH_LOCK(f);
	if (f->fcmh_fcoo && fcmh_2_fmdsi(f) &&
	    (f->fcmh_state & FCMH_FCOO_CLOSING) == 0)
		atomic_inc(&fcmh_2_fmdsi(f)->fmdsi_ref);
	else
		rc = ENOENT;
	FCMH_ULOCK(f);
	return (rc);
}

int
mds_fcmh_load_fmdsi(struct fidc_membh *f, void *data, int isfile)
{
	struct fidc_mds_info *i;
	int rc;

	FCMH_LOCK(f);
	if (f->fcmh_fcoo || (f->fcmh_state & FCMH_FCOO_CLOSING)) {
		rc = fidc_fcoo_wait_locked(f, FCOO_START);

		if (rc < 0) {
			DEBUG_FCMH(PLL_ERROR, f,
				   "fidc_fcoo_wait_locked() failed");
			FCMH_ULOCK(f);
			return (rc);
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
	} else {
		fidc_fcoo_start_locked(f);
 fcoo_start:
		f->fcmh_fcoo->fcoo_pri = i = PSCALLOC(sizeof(*i));
		f->fcmh_fcoo->fcoo_oref_rw[0] = 1;
		fmdsi_init(i, f, data);
		if (isfile) {
			/* XXX For now assert here */
			psc_assert(i->fmdsi_inodeh.inoh_fcmh);
			psc_assert(!mds_inode_read(&i->fmdsi_inodeh));
		}

		FCMH_ULOCK(f);
		fidc_fcoo_startdone(f);
	}
	atomic_inc(&i->fmdsi_ref);
	return (0);
}

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
	struct slashrpc_export *slexp;
	struct mexpfcm *m = PSCALLOC(sizeof(*m));
	struct fidc_membh *f;
	struct fidc_mds_info *i;
	int rc=0;

	/* c->pri holds the zfs file info for this inode, it must be present.
	 */
	psc_assert(c->pri);
	psc_assert(c->type == CFD_DIR || c->type == CFD_FILE);

	slexp = e->exp_private;
	psc_assert(slexp);
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
	m->mexpfcm_fcmh = f =
		fidc_lookup_simple(c->fdb.sfdb_secret.sfs_fg.fg_fid);
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

	rc = mds_fcmh_load_fmdsi(f, c->pri, c->type & CFD_FILE);
	if (rc) {
		PSCFREE(m);
		return (-1);
	}

	/* Add ourselves to the fidc_mds_info structure's splay tree.
	 *  fmdsi_ref is the real open refcnt (one ref per export or client).
	 *  Note that muliple client opens are handled on the client and
	 *  should no be passed to the mds.  However the open mode can change.
	 */
	FCMH_LOCK(f);
	i = fcmh_2_fmdsi(f);
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

	if (rc)
		PSCFREE(m);
	return (rc);
}

void *
mexpfcm_cfd_get_zfsdata(struct cfdent *c, __unusedx struct pscrpc_export *e)
{
	return (cfd_2_zfsdata(c));
}

__static void mexpfcm_release_bref(struct mexpbcm *);

void
mexpfcm_release_brefs(struct mexpfcm *m)
{
	struct mexpbcm *bref, *bn;

	MEXPFCM_LOCK_ENSURE(m);
	psc_assert(m->mexpfcm_flags & MEXPFCM_CLOSING);
	psc_assert(m->mexpfcm_flags & MEXPFCM_REGFILE);
	psc_assert(!(m->mexpfcm_flags & MEXPFCM_DIRECTORY));

	for (bref = SPLAY_MIN(exp_bmaptree, &m->mexpfcm_bmaps);
	    bref; bref = bn) {
		bn = SPLAY_NEXT(exp_bmaptree, &m->mexpfcm_bmaps, bref);
		SPLAY_XREMOVE(exp_bmaptree, &m->mexpfcm_bmaps, bref);
		mexpfcm_release_bref(bref);
	}
}

int
mexpfcm_cfd_free(struct cfdent *c, __unusedx struct pscrpc_export *e)
{
	int locked;
	struct mexpfcm *m=c->pri;
	struct fidc_membh *f=m->mexpfcm_fcmh;
	struct fidc_mds_info *i=f->fcmh_fcoo->fcoo_pri;

	spinlock(&m->mexpfcm_lock);
	/* Ensure the mexpfcm has the correct pointers before
	 *   dereferencing them.
	 */
	if (!(f = m->mexpfcm_fcmh)) {
		psc_errorx("mexpfcm %p has no fcmh", m);
		goto out;
	}

	if (!(i = fidc_fcmh2fmdsi(f))) {
		DEBUG_FCMH(PLL_WARN, f, "fid has no fcoo");
		goto out;
	}

	if (c->type & CFD_FORCE_CLOSE)
		/* A force close comes from a network drop, don't make
		 *  the export code have to know about our private
		 *  structures.
		 */
		m->mexpfcm_flags |= MEXPFCM_CLOSING;
	else
		psc_assert(m->mexpfcm_flags & MEXPFCM_CLOSING);

	/* Verify that all of our bmap references have already been freed.
	 */
	if (m->mexpfcm_flags & MEXPFCM_REGFILE) {
		/* Iterate across our brefs dropping this cfd's reference.
		 */
		mexpfcm_release_brefs(m);

		psc_assert(c->type & CFD_CLOSING);
		psc_assert(SPLAY_EMPTY(&m->mexpfcm_bmaps));
	}
	freelock(&m->mexpfcm_lock);
	/* Grab the fcmh lock (it is responsible for fidc_mds_info
	 *  serialization) and remove ourselves from the tree.
	 */
	locked = reqlock(&f->fcmh_lock);
	SPLAY_XREMOVE(fcm_exports, &i->fmdsi_exports, m);
	ureqlock(&f->fcmh_lock, locked);
 out:
	c->pri = NULL;
	PSCFREE(m);
	return (0);
}

int
mds_bmap_valid(struct fidc_membh *f, sl_blkno_t n)
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

	psc_trace("fid="FIDFMT" lblk=%u fsz=%zu",
		  FIDFMTARGS(fcmh_2_fgp(f)), lblk, fcmh_2_fsz(f));

	FCMH_URLOCK(f, locked);
	return (n < lblk);
}

/**
 * mexpbcm_directio - queue mexpbcm's so that their clients may be
 *	notified of the cache policy change (to directio) for this bmap.
 *	The mds_coherency thread is responsible for actually issuing and
 *	checking the status of the rpc's.  Communication between this
 *	thread and the mds_coherency thread occurs by placing the
 *	mexpbcm's onto the pndgBmapCbs listcache.
 * @bmap: the bmap which is going dio.
 * Notes:  XXX this needs help but it's making my brain explode right now.
 */
#define mexpbcm_directio_check(b) mexpbcm_directio((b), 1, 1)
#define mexpbcm_directio_set(b)   mexpbcm_directio((b), 1, 0)
#define mexpbcm_directio_unset(b) mexpbcm_directio((b), 0, 0)

static void
mexpbcm_directio(struct mexpbcm *bref, int enable_dio, int check)
{
	struct bmapc_memb *bmap=bref->mexpbcm_bmap;
	struct bmap_mds_info *mdsi=bmap->bcm_pri;
	int mode=bref->mexpbcm_mode, locked;

	psc_assert(mdsi);

	BMAP_LOCK_ENSURE(bmap);

	if (atomic_read(&mdsi->bmdsi_wr_ref))
		psc_assert(mdsi->bmdsi_wr_ion);

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
		locked = MEXPBCM_REQLOCK(bref);
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
		MEXPBCM_UREQLOCK(bref, locked);
	}
}

__static void
mds_mion_init(struct mexp_ion *mion, struct sl_resm *resm)
{
	dynarray_init(&mion->mi_bmaps);
	dynarray_init(&mion->mi_bmaps_deref);
	INIT_PSCLIST_ENTRY(&mion->mi_lentry);
	atomic_set(&mion->mi_refcnt, 0);
	mion->mi_resm = resm;
	mion->mi_csvc = rpc_csvc_create(SRIM_REQ_PORTAL, SRIM_REP_PORTAL);
}

/**
 * mds_bmap_ion_assign - bind a bmap to a ion node for writing.  The process
 *    involves a round-robin'ing of an i/o system's nodes and attaching a
 *    a mexp_ion to the bmap.  The mexp_ion is stored in the i/o node's
 *    resouce_member struct (resm_pri).  It is here that an initial connection
 *    to the i/o node is created.
 * @bref: the bmap reference
 * @pios: the preferred i/o system
 */
__static int
mds_bmap_ion_assign(struct bmapc_memb *bmap, sl_ios_id_t pios)
{
	struct bmap_mds_info *mdsi=bmap->bcm_pri;
	struct mexp_ion *mion;
	struct bmi_assign bmi;
	struct sl_resource *res=libsl_id2res(pios);
	struct sl_resm *resm;
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
		if (rmi->rmi_cnt >= (int)res->res_nnids)
			n = rmi->rmi_cnt = 0;

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

		DEBUG_BMAP(PLL_TRACE, bmap,
		    "res(%s) ion(%s) init=%d, failed=%d",
		    res->res_name, libcfs_nid2str(res->res_nids[n]),
		    mion->mi_csvc->csvc_initialized,
		    mion->mi_csvc->csvc_failed);

		if (mion->mi_csvc->csvc_failed)
			continue;

		if (!mion->mi_csvc->csvc_initialized) {
			rc = rpc_issue_connect(res->res_nids[n],
			    mion->mi_csvc->csvc_import,
			    SRIM_MAGIC, SRIM_VERSION);
			if (rc) {
				mion->mi_csvc->csvc_failed = 1;
				continue;
			} else
				mion->mi_csvc->csvc_initialized = 1;
		}
		atomic_inc(&mion->mi_refcnt);
		mdsi->bmdsi_wr_ion = mion;
	} while (--x);

	if (!mdsi->bmdsi_wr_ion)
		return (-1);

	/* A mion has been assigned to the bmap, mark it in the odtable
	 *   so that the assignment may be restored on reboot.
	 */
	bmi.bmi_ion_nid = mion->mi_resm->resm_nid;
	bmi.bmi_ios = mion->mi_resm->resm_res->res_id;
	bmi.bmi_fid = fcmh_2_fid(bmap->bcm_fcmh);
	bmi.bmi_bmapno = bmap->bcm_blkno;
	bmi.bmi_start = time(NULL);

	mdsi->bmdsi_assign = odtable_putitem(mdsBmapAssignTable, &bmi);
	if (!mdsi->bmdsi_assign) {
		DEBUG_BMAP(PLL_ERROR, bmap, "failed odtable_putitem()");
		return (-1);
	}

	atomic_inc(&(fidc_fcmh2fmdsi(bmap->bcm_fcmh))->fmdsi_ref);

	DEBUG_FCMH(PLL_INFO, bmap->bcm_fcmh,
		   "inc fmdsi_ref (%d) for bmap assignment",
		   atomic_read(&(fidc_fcmh2fmdsi(bmap->bcm_fcmh))->fmdsi_ref));

	DEBUG_BMAP(PLL_INFO, bmap, "using res(%s) ion(%s) "
		   "mion(%p)", res->res_name,
		   libcfs_nid2str(res->res_nids[n]), mdsi->bmdsi_wr_ion);
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
mds_bmap_ref_add(struct mexpbcm *bref, struct srm_bmap_req *mq)
{
	struct bmapc_memb *bmap=bref->mexpbcm_bmap;
	struct bmap_mds_info *bmdsi=bmap->bcm_pri;
	int wr[2], locked, rc=0, rw=mq->rw;
	int mode=(rw == SRIC_BMAP_READ ? BMAP_RD : BMAP_WR);
	atomic_t *a=(rw == SRIC_BMAP_READ ? &bmdsi->bmdsi_rd_ref :
		     &bmdsi->bmdsi_wr_ref);

	if (rw == SRIC_BMAP_READ)
		psc_assert(bref->mexpbcm_mode & MEXPBCM_RD);
	else if (rw == SRIC_BMAP_WRITE)
		psc_assert(bref->mexpbcm_mode & MEXPBCM_WR);
	else
		psc_fatalx("mode value (%d) is invalid", rw);

	locked = BMAP_RLOCK(bmap);
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

	if (wr[0] == 1 && mode == BMAP_WR && !bmdsi->bmdsi_wr_ion) {
		/* XXX Should not send connect rpc's here while
		 *  the bmap is locked.  This may have to be
		 *  replaced by a waitq and init flag.
		 */
		rc = mds_bmap_ion_assign(bmap, mq->pios);
		if (rc)
			goto out;
	}
	/* Do directio checks here.
	 */
	if (wr[0] > 2)
		/* It should have already been set.
		 */
		mexpbcm_directio_check(bref);

	else if (wr[0] == 2 || (wr[0] == 1 && wr[1]))
		/* These represent the two possible 'add' related transitional
		 *  states, more than 1 writer or the first writer amidst
		 *  existing readers.
		 */
		mexpbcm_directio_set(bref);
	/* Pop it on the tree.
	 */
	if (SPLAY_INSERT(bmap_exports, &bmdsi->bmdsi_exports, bref))
		psc_fatalx("found duplicate bref on bmap_exports");

 out:
	DEBUG_BMAP(rc ? PLL_ERROR : PLL_INFO, bmap,
		   "ref_add (mion=%p) (rc=%d)",
		   bmdsi->bmdsi_wr_ion, rc);
	BMAP_URLOCK(bmap, locked);

	return (rc);
}

void
mds_bmap_ref_drop(struct bmapc_memb *bcm, int mode)
{
	struct bmap_mds_info *mdsi;

	mdsi = bcm->bcm_pri;
	if (mode & BMAP_WR) {
		psc_assert(atomic_read(&mdsi->bmdsi_wr_ref));
		atomic_dec(&bcm->bcm_wr_ref);
		if (atomic_dec_and_test(&mdsi->bmdsi_wr_ref)) {
			psc_assert(bcm->bcm_mode & BMAP_WR);
			bcm->bcm_mode &= ~BMAP_WR;
			if (mdsi->bmdsi_wr_ion &&
			    atomic_dec_and_test(&mdsi->bmdsi_wr_ion->mi_refcnt)) {
				//XXX cleanup mion here?
			}
			//mdsi->bmdsi_wr_ion = NULL;
		}
	} else {
		atomic_dec(&bcm->bcm_rd_ref);
		psc_assert(atomic_read(&mdsi->bmdsi_rd_ref));
		if (atomic_dec_and_test(&mdsi->bmdsi_rd_ref))
			bcm->bcm_mode &= ~BMAP_RD;
	}
	if (atomic_read(&bcm->bcm_rd_ref) == 0 &&
	    atomic_read(&bcm->bcm_wr_ref) == 0 &&
	    atomic_read(&mdsi->bmdsi_rd_ref) == 0 &&
	    atomic_read(&mdsi->bmdsi_wr_ref) == 0 &&
	    atomic_read(&bcm->bcm_opcnt) == 0)
		psc_pool_return(bmap_pool, bcm);
	else
		BMAP_ULOCK(bcm);
}

/**
 * mexpfcm_release_bref - Drop a read or write reference to the bmap's tree.
 * @bref: the bmap link to unref
 */
void
mexpfcm_release_bref(struct mexpbcm *bref)
{
	struct bmapc_memb *bmap=bref->mexpbcm_bmap;
	struct bmap_mds_info *mdsi=bmap->bcm_pri;
	int refs[2];

	BMAP_LOCK(bmap);

	if (!SPLAY_REMOVE(bmap_exports, &mdsi->bmdsi_exports, bref) &&
	    !(bmap->bcm_mode & BMAP_MDS_NOION))
		psc_fatalx("bref not found on bmap_exports");

	bmdsi_sanity_locked(bmap, 0, refs);
	psc_assert(refs[0] > 1);
	psc_assert(refs[1] > 1);
	refs[0]--;
	refs[1]--;

	if (!refs[0] || (refs[0] == 1 && !refs[1]))
		mexpbcm_directio_unset(bref);

	DEBUG_BMAP(PLL_INFO, bmap, "done with ref_del bref=%p", bref);
	mds_bmap_ref_drop(bmap, bref->mexpbcm_mode & MEXPBCM_WR ?
	    BMAP_WR : BMAP_RD);
	PSCFREE(bref);
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
	struct slash_bmap_od *bmapod;
	int rc=0, wr[2];

	fcmh = fidc_lookup_inode(c->fid);
	if (!fcmh)
		return (-EBADF);

	bmap = bmap_lookup(fcmh, c->blkno);
	if (!bmap) {
		rc = -EBADF;
		goto out;
	}
	BMAP_LOCK(bmap);

	DEBUG_BMAP(PLL_TRACE, bmap, "blkno=%u sz=%"PRId64" ion=%s",
		   c->blkno, c->fsize, libcfs_nid2str(ion_nid));

	bmdsi = bmap->bcm_pri;
	bmapod = bmdsi->bmdsi_od;
	/* These better check out.
	 */
	psc_assert(bmap->bcm_fcmh == fcmh);
	psc_assert(bmdsi);
	psc_assert(bmapod);
	psc_assert(bmdsi->bmdsi_wr_ion);
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
	if ((rc = mds_repl_inv_except_locked(bmap,
			     slresm_2_resid(bmdsi->bmdsi_wr_ion->mi_resm)))) {
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
	fidc_membh_dropref(fcmh);
	return (rc);
}

void
mds_bmapod_dump(const struct bmapc_memb *bmap)
{
	uint8_t mask, *b=bmap_2_bmdsiod(bmap)->bh_repls;
	char buf[SL_MAX_REPLICAS+1], *ob=buf;
	uint32_t pos, k;
	int ch[4];

	ch[SL_REPL_INACTIVE] = '-';
	ch[SL_REPL_TOO_OLD] = 'o';
	ch[SL_REPL_OLD] = 'O';
	ch[SL_REPL_ACTIVE] = 'A';

	for (k=0; k < SL_REPLICA_NBYTES; k++, mask=0)
		for (pos=0, mask=0; pos < NBBY; b++, pos+=SL_BITS_PER_REPLICA) {
			mask = (uint8_t)(SL_REPLICA_MASK << pos);
			*ob = ch[(b[k] & mask) >> pos];
		}

	*ob = '\0';

	DEBUG_BMAP(PLL_NOTIFY, bmap, "replicas(%s) SL_REPLICA_NBYTES=%u",
		   buf, SL_REPLICA_NBYTES);
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
mds_bmapod_initnew(struct slash_bmap_od *b)
{
	int i;

	for (i=0; i < SL_CRCS_PER_BMAP; i++)
		b->bh_crcs[i].gc_crc = SL_NULL_CRC;

	psc_crc_calc(&b->bh_bhcrc, b, BMAP_OD_CRCSZ);
}

/**
 * mds_bmap_read - retrieve a bmap from the ondisk inode file.
 * @fcmh: inode structure containing the fid and the fd.
 * @blkno: the bmap block number.
 * @bmapod: on disk structure containing crc's and replication bitmap.
 */
__static int
mds_bmap_read(struct fidc_membh *f, sl_blkno_t blkno, struct bmapc_memb *bcm)
{
	struct bmap_mds_info *bmdsi;
	psc_crc_t crc;
	int rc=0;

	bmdsi = bcm->bcm_pri;
	psc_assert(bmdsi->bmdsi_od == NULL);
	bmdsi->bmdsi_od = PSCALLOC(BMAP_OD_SZ);

	/* Try to pread() the bmap from the mds file.
	 */
	rc = mdsio_zfs_bmap_read(bcm);
	if (rc) {
		DEBUG_FCMH(PLL_ERROR, f, "mdsio_zfs_bmap_read: "
		    "blkno=%u, rc=%d", blkno, rc);
		rc = -EIO;
		goto out;
	}

	/* Check for a NULL CRC, which can happen when
	 * bmaps are gaps that have not been written yet.
	 */
	if (bmdsi->bmdsi_od->bh_bhcrc == 0 && memcmp(bmdsi->bmdsi_od,
	    &null_bmap_od, sizeof(null_bmap_od)) == 0) {
		mds_bmapod_dump(bcm);

		mds_bmapod_initnew(bmdsi->bmdsi_od);

		mds_bmapod_dump(bcm);
		return (0);
	}
	/* Calculate and check the CRC now
	 */
	mds_bmapod_dump(bcm);

	psc_crc_calc(&crc, bmdsi->bmdsi_od, BMAP_OD_CRCSZ);
	if (crc == bmdsi->bmdsi_od->bh_bhcrc)
		return (0);

	DEBUG_FCMH(PLL_ERROR, f, "CRC failed; blkno=%u, want=%"PRIx64", got=%"PRIx64,
	    blkno, bmdsi->bmdsi_od->bh_bhcrc, crc);
	rc = -EIO;
 out:
	PSCFREE(bmdsi->bmdsi_od);
	bmdsi->bmdsi_od = NULL;
	return (rc);
}

void
mds_bmap_init(struct bmapc_memb *bcm)
{
	struct bmap_mds_info *bmdsi;

	bmdsi = bcm->bcm_pri;
	jfi_init(&bmdsi->bmdsi_jfi, mds_bmap_sync, bcm);
	bmdsi->bmdsi_xid = 0;
	atomic_set(&bmdsi->bmdsi_rd_ref, 0);
	atomic_set(&bmdsi->bmdsi_wr_ref, 0);

	/* must be locked before it is placed on the tree */
	BMAP_LOCK(bcm);
}

struct bmapc_memb *
mds_bmap_load(struct fidc_membh *f, sl_blkno_t bmapno)
{
	struct bmap_mds_info *bmdsi;
	struct bmapc_memb *b;
	int rc, initializing = 1;

	b = bmap_lookup_add(f, bmapno, mds_bmap_init);
	bmdsi = b->bcm_pri;
	if (bmdsi->bmdsi_od) {
		/* Add check for directio mode. */
		while (initializing) {
			BMAP_LOCK(b);
			if (b->bcm_mode & BMAP_MDS_INIT) {
				/* Only the init bit is allowed to be set.
				 */
				psc_assert(b->bcm_mode == BMAP_MDS_INIT);
				/* Sanity checks for BMAP_MDS_INIT
				 */
				psc_assert(!b->bcm_pri);
				psc_assert(!b->bcm_fcmh);
				/* Block until the other thread has completed the io.
				 */
				psc_waitq_wait(&b->bcm_waitq, &b->bcm_lock);
			} else {
				/* Sanity check relevant pointers.
				 */
				psc_assert(b->bcm_pri);
				psc_assert(b->bcm_fcmh);
				initializing = 0;
			}
		}
	} else {
		rc = mds_bmap_read(f, bmapno, b);
		if (rc) {
			DEBUG_FCMH(PLL_WARN, f, "mds_bmap_read() rc=%d blkno=%u",
			    rc, bmapno);
			b->bcm_mode |= BMAP_MDS_FAILED;
			psc_waitq_wakeall(&b->bcm_waitq);
			return (NULL);
		} else {
			b->bcm_mode = 0;
			/* Notify other threads that this bmap has been loaded,
			 *  they're blocked on BMAP_MDS_INIT.
			 */
			psc_waitq_wakeall(&b->bcm_waitq);
		}
	}

	bmap_set_accesstime(b);
	BMAP_ULOCK(b);

	return (b);
}

int
mds_bmap_load_ion(const struct slash_fidgen *fg, sl_blkno_t bmapno,
		  struct bmapc_memb **bmap)
{
	struct fidc_membh *f;
	struct bmapc_memb *b;

	psc_assert(!*bmap);

	f = fidc_lookup_fg(fg);
	if (!f)
		return (-ENOENT);

	b = mds_bmap_load(f, bmapno);
	if (!b)
		return (-EIO);

	*bmap = b;
	return (0);
}

/**
 * mds_bmap_load_cli - routine called to retrieve a bmap, presumably so that
 *	it may be sent to a client.  It first checks for existence in
 *	the cache, if needed, the bmap is retrieved from disk.
 *
 *	mds_bmap_load_cli() also manages the mexpfcm's mexpbcm reference
 *	which is used to track the bmaps a particular client knows
 *	about.  mds_bmap_read() is used to retrieve the bmap from disk
 *	or create a new 'blank-slate' bmap if one does not exist.
 *	Finally, a read or write reference is placed on the bmap
 *	depending on the client request.  This is factored in with
 *	existing references to determine whether or not the bmap should
 *	be in DIO mode.
 * @fref: the fidcache reference for the inode (stored in the private
 *	pointer of the cfd).
 * @mq: the client RPC request.
 * @bmap: structure to be allocated and returned to the client.
 * Note: the bmap is not locked during disk I/O, instead it is marked
 *	with a bit (ie INIT) and other threads block on the waitq.
 */
int
mds_bmap_load_cli(struct mexpfcm *fref, struct srm_bmap_req *mq,
		  struct bmapc_memb **bmap)
{
	struct bmapc_memb *b, tbmap;
	struct fidc_membh *f=fref->mexpfcm_fcmh;
	struct fidc_mds_info *fmdsi=f->fcmh_fcoo->fcoo_pri;
	struct slash_inode_handle *inoh=&fmdsi->fmdsi_inodeh;
	struct mexpbcm *bref, *newref, tbref;
	int rc=0;

	newref = NULL;
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
		newref = bref = PSCALLOC(sizeof(*bref));
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
	b = mds_bmap_load(f, mq->blkno);
	if (!b) {
		rc = -1;
		goto out;
	}
	/* Not sure if these are really needed on the mds.
	 */
	if (mq->rw == SRIC_BMAP_WRITE)
		atomic_inc(&b->bcm_wr_ref);
	else
		atomic_inc(&b->bcm_rd_ref);
	/* Sanity checks, make sure that we didn't let the client in
	 *  before this bmap was ready.
	 */
	MEXPBCM_LOCK(bref);

	psc_assert(bref->mexpbcm_mode == MEXPBCM_INIT);

	bref->mexpbcm_bmap = b;
	bref->mexpbcm_mode = ((mq->rw == SRIC_BMAP_WRITE) ?
			      MEXPBCM_WR : MEXPBCM_RD);
	/* Check if the client requested directio, if so tag it in the
	 *  bref.
	 */
	if (mq->dio)
		bref->mexpbcm_mode |= MEXPBCM_CDIO;

	MEXPBCM_ULOCK(bref);
	/* Place our bref on the tree, manage any mode changes that result
	 *  from this new reference.  Also, on write choose an ION if needed.
	 */
	if ((rc = mds_bmap_ref_add(bref, mq)))
		b->bcm_mode |= BMAP_MDS_NOION;
 out:
	if (rc)
		PSCFREE(newref);
	else
		*bmap = b;
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
	struct slash_inode_handle inoh;
	size_t       sz = sizeof(struct slash_inode_od);
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

	psc_crc_calc(&crc, &inoh.inoh_ino, sz);
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

		psc_crc_calc(&crc, inoh.inoh_replicas, sz);
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
mds_bmi_cb(void *data, struct odtable_receipt *odtr)
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
}

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
	mds_journal_init();
	psc_assert(!odtable_load(_PATH_SLODTABLE, &mdsBmapAssignTable));
	odtable_scan(mdsBmapAssignTable, mds_bmi_cb);

	mdsfssync_init();
	mdscoh_init();
}
