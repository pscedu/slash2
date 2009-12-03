/* $Id$ */

#include "psc_ds/tree.h"
#include "psc_util/alloc.h"
#include "psc_util/atomic.h"
#include "psc_util/log.h"

#include "bmap.h"
#include "cache_params.h"
#include "cfd.h"
#include "fidc_mds.h"
#include "fidcache.h"
#include "inode.h"
#include "mdscoh.h"
#include "mdsexpc.h"
#include "mdsio_zfs.h"
#include "mdslog.h"
#include "repl_mds.h"
#include "rpc_mds.h"
#include "slashd.h"
#include "slashexport.h"
#include "slerr.h"

struct odtable				*mdsBmapAssignTable;
const struct slash_bmap_od		 null_bmap_od;
const struct slash_inode_od		 null_inode_od;
const struct slash_inode_extras_od	 null_inox_od;
struct cfdops				 mdsCfdOps;
struct sl_fsops				 mdsFsops;

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
		   atomic_read(&i->fmdsi_ref), f->fcmh_fcoo->fcoo_oref_rd);

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
		f->fcmh_fcoo->fcoo_oref_rd = 0;
		freelock(&f->fcmh_lock);
		fidc_fcoo_remove(f);
	} else
		freelock(&f->fcmh_lock);
	return (rc);
}

__static int
mds_inode_read(struct slash_inode_handle *i)
{
	psc_crc64_t crc;
	int rc=0, locked;

	locked = reqlock(&i->inoh_lock);
	psc_assert(i->inoh_flags & INOH_INO_NOTLOADED);

	rc = mdsio_zfs_inode_read(i);
	if (rc == SLERR_SHORTIO && i->inoh_ino.ino_crc == 0 &&
	    memcmp(&i->inoh_ino, &null_inode_od, INO_OD_CRCSZ) == 0) {
		DEBUG_INOH(PLL_INFO, i, "detected a new inode");
		mds_inode_od_initnew(i);
		rc = 0;
	} else if (rc) {
		DEBUG_INOH(PLL_WARN, i, "mdsio_zfs_inode_read: %d", rc);
	} else {
		psc_crc64_calc(&crc, &i->inoh_ino, INO_OD_CRCSZ);
		if (crc == i->inoh_ino.ino_crc) {
			i->inoh_flags &= ~INOH_INO_NOTLOADED;
			DEBUG_INOH(PLL_INFO, i, "successfully loaded inode od");
		} else {
			DEBUG_INOH(PLL_WARN, i, "CRC failed want=%"PRIx64", got=%"PRIx64,
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

	rc = mdsio_zfs_inode_extras_read(ih);
	if (rc == SLERR_SHORTIO && ih->inoh_extras->inox_crc == 0 &&
	    memcmp(&ih->inoh_extras, &null_inox_od, INOX_OD_CRCSZ) == 0) {
		rc = 0;
	} else if (rc) {
		DEBUG_INOH(PLL_WARN, ih, "mdsio_zfs_inode_extras_read: %d", rc);
	} else {
		psc_crc64_calc(&crc, ih->inoh_extras, INOX_OD_CRCSZ);
		if (crc == ih->inoh_extras->inox_crc)
			ih->inoh_flags |= INOH_HAVE_EXTRAS;
		else {
			psc_errorx("inox CRC fail; disk %lx mem %lx",
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
			f->fcmh_fcoo->fcoo_oref_rd++;
			psc_assert(i->fmdsi_data);
			FCMH_ULOCK(f);
		}
	} else {
		fidc_fcoo_start_locked(f);
 fcoo_start:
		psc_assert(f->fcmh_fcoo->fcoo_pri == NULL);
		f->fcmh_fcoo->fcoo_pri = i = PSCALLOC(sizeof(*i));
		f->fcmh_fcoo->fcoo_oref_rd = 1;
		fmdsi_init(i, f, data);
		if (isfile) {
			/* XXX For now assert here */
			psc_assert(i->fmdsi_inodeh.inoh_fcmh);
			rc = mds_inode_read(&i->fmdsi_inodeh);
			if (rc)
				psc_fatalx("could not load inode; rc=%d", rc);
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
	psc_assert(c->cfd_pri);
	psc_assert(c->cfd_type == CFD_DIR || c->cfd_type == CFD_FILE);

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
	m->mexpfcm_fcmh = f = fidc_lookup_simple(c->cfd_fdb.sfdb_secret.sfs_fg.fg_fid);
	//m->mexpfcm_fcmh = f = fidc_lookup_fg(&c->cfd_fdb.sfdb_secret.sfs_fg);
	//fidc_lookup_simple(c->cfd_fdb.sfdb_secret.sfs_fg.fg_fid);
	//psc_assert(f);
	if (!f)
		return (-1);

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

	rc = mds_fcmh_load_fmdsi(f, c->cfd_pri, c->cfd_type & CFD_FILE);
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
	c->cfd_pri = m;
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
	struct mexpfcm *m=c->cfd_pri;
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

	if (c->cfd_type & CFD_FORCE_CLOSE)
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

		psc_assert(c->cfd_type & CFD_CLOSING);
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
	c->cfd_pri = NULL;
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
#define mds_bmap_directio_check(b) mds_bmap_directio((b), 1, 1)
#define mds_bmap_directio_set(b)   mds_bmap_directio((b), 1, 0)
#define mds_bmap_directio_unset(b) mds_bmap_directio((b), 0, 0)

static void
mds_bmap_directio(struct bmapc_memb *bmap, int enable_dio, int check)
{
	struct bmap_mds_info *mdsi=bmap->bcm_pri;
	struct mexpbcm *bref;
	int mode, locked;

	psc_assert(mdsi);

	BMAP_LOCK_ENSURE(bmap);

	if (atomic_read(&bmap->bcm_wr_ref))
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

		mode = bref->mexpbcm_mode;
		/* Don't send rpc if the client is already using DIO or
		 *  has an rpc in flight (_REQD).
		 */
		if (enable_dio &&                 /* turn dio on */
		    !(mode & MEXPBCM_CDIO) &&     /* client already uses dio */
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
				 *     have completed.
				 * Verify the current inflight mode.
				 */
				mdscoh_infmode_chk(bref, MEXPBCM_CIO_REQD);
				psc_assert(psclist_conjoint(e));
				if (!bref->mexpbcm_net_inf) {
					/* Unschedule this rpc, the coh
					 *    thread will remove it from
					 *    the listcache.
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
	atomic_set(&mion->mi_refcnt, 0);
	mion->mi_resm = resm;
}

/**
 * mds_bmap_ion_assign - bind a bmap to a ion node for writing.  The process
 *    involves a round-robin'ing of an i/o system's nodes and attaching a
 *    a mexp_ion to the bmap.  The mexp_ion is stored in the i/o node's
 *    resouce_member struct (resm_pri->rmpi_data).  It is here that an initial
 *	connection to the ION may be created.
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
	struct mds_resm_info *mrmi;
	struct mds_resprof_info *mrpi;
	int n, x;

	psc_assert(!mdsi->bmdsi_wr_ion);
	n = atomic_read(&bmap->bcm_wr_ref);
	psc_assert(n == 0 || n == 1);

	if (!res) {
		psc_warnx("Failed to find pios %d", pios);
		return (-SLERR_ION_UNKNOWN);
	}
	mrpi = res->res_pri;
	psc_assert(mrpi);
	x = res->res_nnids;

	do {
		spinlock(&mrpi->mrpi_lock);
		n = mrpi->mrpi_cnt++;
		if (mrpi->mrpi_cnt >= (int)res->res_nnids)
			n = mrpi->mrpi_cnt = 0;

		psc_trace("trying res(%s) ion(%s)",
			  res->res_name, libcfs_nid2str(res->res_nids[n]));

		resm = libsl_nid2resm(res->res_nids[n]);
		if (!resm)
			psc_fatalx("Failed to lookup %s, verify that slash "
				   "configs are uniform across all servers",
				   libcfs_nid2str(res->res_nids[n]));
		freelock(&mrpi->mrpi_lock);

		mrmi = resm->resm_pri;
		spinlock(&mrmi->mrmi_lock);

		if (!mrmi->mrmi_data) {
			mrmi->mrmi_data = PSCALLOC(sizeof(*mion));
			mds_mion_init(mrmi->mrmi_data, resm);
		}
		mion = mrmi->mrmi_data;

		/*
		 * If we fail to establish a connection, try next node.
		 * The while loop guarantees that we always bail out.
		 */
		if (slm_geticonn(resm) == NULL) {
			freelock(&mrmi->mrmi_lock);
			continue;
		}

		DEBUG_BMAP(PLL_TRACE, bmap, "res(%s) ion(%s)",
			   res->res_name, libcfs_nid2str(res->res_nids[n]));

		atomic_inc(&mion->mi_refcnt);
		mdsi->bmdsi_wr_ion = mion;
		freelock(&mrmi->mrmi_lock);
		break;

	} while (--x);

	if (!mdsi->bmdsi_wr_ion)
		return (-SLERR_ION_OFFLINE);

	/* A mion has been assigned to the bmap, mark it in the odtable
	 *   so that the assignment may be restored on reboot.
	 */
	bmi.bmi_ion_nid = mrmi->mrmi_resm->resm_nid;
	bmi.bmi_ios = mrmi->mrmi_resm->resm_res->res_id;
	bmi.bmi_fid = fcmh_2_fid(bmap->bcm_fcmh);
	bmi.bmi_bmapno = bmap->bcm_blkno;
	bmi.bmi_start = time(NULL);

	mdsi->bmdsi_assign = odtable_putitem(mdsBmapAssignTable, &bmi);
	if (!mdsi->bmdsi_assign) {
		DEBUG_BMAP(PLL_ERROR, bmap, "failed odtable_putitem()");
		return (-SLERR_XACT_FAIL);
	}
	/* Signify that a ION has been assigned to this bmap.  This
	 *   opcnt ref will stay in place until the ION informs us that
	 *   he's finished with it.
	 */
	atomic_inc(&bmap->bcm_opcnt);
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
mds_bmap_ref_add(struct mexpbcm *bref, const struct srm_bmap_req *mq)
{
	struct bmapc_memb *bmap=bref->mexpbcm_bmap;
	struct bmap_mds_info *bmdsi=bmap->bcm_pri;
	int locked, rc=0, rw=mq->rw;
	int mode=(rw == SRIC_BMAP_READ ? BMAP_RD : BMAP_WR);
	atomic_t *a=(rw == SRIC_BMAP_READ ?
		     &bmap->bcm_rd_ref : &bmap->bcm_wr_ref);

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
	bmap_dio_sanity_locked(bmap, 0);

	if ((atomic_read(&bmap->bcm_wr_ref) == 1) &&
	    (mode == BMAP_WR) &&
	    !bmdsi->bmdsi_wr_ion) {
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
	if (atomic_read(&bmap->bcm_wr_ref) > 2)
		/* It should have already been set.
		 */
		mds_bmap_directio_check(bmap);

	else if (atomic_read(&bmap->bcm_wr_ref) == 2 ||
		 (atomic_read(&bmap->bcm_wr_ref) == 1 &&
		  atomic_read(&bmap->bcm_rd_ref)))
		/* These represent the two possible 'add' related transitional
		 *  states, more than 1 writer or the first writer amidst
		 *  existing readers.
		 */
		mds_bmap_directio_set(bmap);
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


/**
 *
 * Notes:  I unlock the bmap or free it.
 */
void
mds_bmap_ref_drop_locked(struct bmapc_memb *bmap, int mode)
{
	struct bmap_mds_info *mdsi;

	BMAP_LOCK_ENSURE(bmap);

	DEBUG_BMAP(PLL_WARN, bmap, "try close 1");

	mdsi = bmap->bcm_pri;
	if (mode & BMAP_WR) {
		psc_assert(atomic_read(&bmap->bcm_wr_ref) > 0);
		if (atomic_dec_and_test(&bmap->bcm_wr_ref)) {
			psc_assert(bmap->bcm_mode & BMAP_WR);
			bmap->bcm_mode &= ~BMAP_WR;
			if (mdsi->bmdsi_wr_ion &&
			    atomic_dec_and_test(&mdsi->bmdsi_wr_ion->mi_refcnt)) {
				//XXX cleanup mion here?
			}
			//mdsi->bmdsi_wr_ion = NULL;
		}

	} else {
		psc_assert(atomic_read(&bmap->bcm_rd_ref) > 0);
		if (atomic_dec_and_test(&bmap->bcm_rd_ref))
			bmap->bcm_mode &= ~BMAP_RD;
	}

	bmap_dio_sanity_locked(bmap, 1);
	/* Disable directio if the last writer has left OR
	 *   no readers exist amongst a single writer.
	 */
	if (!atomic_read(&bmap->bcm_wr_ref) ||
	    ((atomic_read(&bmap->bcm_wr_ref) == 1) &&
	     (!atomic_read(&bmap->bcm_rd_ref))))
		mds_bmap_directio_unset(bmap);

	bmap_try_release(bmap);
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

	BMAP_LOCK(bmap);

	if (!SPLAY_REMOVE(bmap_exports, &mdsi->bmdsi_exports, bref) &&
	    !(bmap->bcm_mode & BMAP_MDS_NOION))
		psc_fatalx("bref not found on bmap_exports");

	DEBUG_BMAP(PLL_INFO, bmap, "done with ref_del bref=%p", bref);

	/* mds_bmap_ref_drop_locked() may free the bmap therefore
	 *   we don't try to unlock it here, mds_bmap_ref_drop_locked()
	 *   will unlock it for us.
	 */
	mds_bmap_ref_drop_locked(bmap, bref->mexpbcm_mode & MEXPBCM_WR ?
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
	int rc=0;

	fcmh = fidc_lookup_fg(&c->fg);
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
	bmap_dio_sanity_locked(bmap, 1);

	if (ion_nid != bmdsi->bmdsi_wr_ion->mi_resm->resm_nid) {
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
	if (bmap)
		bmap_op_done(bmap);
	fidc_membh_dropref(fcmh);
	return (rc);
}

void
mds_bmapod_dump(const struct bmapc_memb *bmap)
{
	uint8_t mask, *b = bmap_2_bmdsiod(bmap)->bh_repls;
	char buf[SL_MAX_REPLICAS + 1], *ob=buf;
	uint32_t pos, k;
	int ch[4];

	ch[SL_REPL_INACTIVE] = '-';
	ch[SL_REPL_SCHED] = 's';
	ch[SL_REPL_OLD] = 'o';
	ch[SL_REPL_ACTIVE] = '+';

	for (k = 0; k < SL_REPLICA_NBYTES; k++, mask = 0)
		for (pos = mask = 0; pos < NBBY; pos += SL_BITS_PER_REPLICA) {
			mask = (uint8_t)(SL_REPLICA_MASK << pos);
			*ob++ = ch[(b[k] & mask) >> pos];
		}

	*ob = '\0';

	DEBUG_BMAP(PLL_NOTICE, bmap, "replicas(%s) SL_REPLICA_NBYTES=%u",
		   buf, SL_REPLICA_NBYTES);
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
 */
__static int
mds_bmap_read(struct fidc_membh *f, sl_blkno_t blkno, struct bmapc_memb *bcm)
{
	struct bmap_mds_info *bmdsi;
	psc_crc64_t crc;
	int rc;

	bmdsi = bcm->bcm_pri;
	psc_assert(bmdsi->bmdsi_od == NULL);
	bmdsi->bmdsi_od = PSCALLOC(BMAP_OD_SZ);

	/* Try to pread() the bmap from the mds file.
	 */
	rc = mdsio_zfs_bmap_read(bcm);

	/* Check for a NULL CRC if we had a good read.  NULL CRC can happen when
	 * bmaps are gaps that have not been written yet.   Note that a short
	 * read is tolerated as long as the bmap is zeroed.
	 */
	if (!rc || rc == SLERR_SHORTIO) {
		if (bmdsi->bmdsi_od->bh_bhcrc == 0 && 
		    memcmp(bmdsi->bmdsi_od, &null_bmap_od, sizeof(null_bmap_od)) == 0) {

			mds_bmapod_dump(bcm);
			mds_bmapod_initnew(bmdsi->bmdsi_od);
			mds_bmapod_dump(bcm);
			return (0);
		}
	}

	/* At this point, the short I/O is an error since the bmap isn't zeros. */
	if (rc) {
		DEBUG_FCMH(PLL_ERROR, f, "mdsio_zfs_bmap_read: "
		    "blkno=%u, rc=%d", blkno, rc);
		rc = -EIO;
		goto out;
	}

	/* Calculate and check the CRC now */
	mds_bmapod_dump(bcm);

	psc_crc64_calc(&crc, bmdsi->bmdsi_od, BMAP_OD_CRCSZ);
	if (crc == bmdsi->bmdsi_od->bh_bhcrc)
		return (0);

	DEBUG_FCMH(PLL_ERROR, f, "CRC failed; blkno=%u, want=%"PRIx64", got=%"PRIx64,
	    blkno, bmdsi->bmdsi_od->bh_bhcrc, crc);
	rc = -EIO;
 out:
	PSCFREE(bmdsi->bmdsi_od);
	return (rc);
}

void
mds_bmap_init(struct bmapc_memb *bcm)
{
	struct bmap_mds_info *bmdsi;

	bmdsi = bcm->bcm_pri;
	jfi_init(&bmdsi->bmdsi_jfi, mds_bmap_sync, bcm);
	bmdsi->bmdsi_xid = 0;
}

int
mds_bmap_load(struct fidc_membh *f, sl_blkno_t bmapno,
    struct bmapc_memb **bp)
{
	struct bmapc_memb *b;
	int rc = 0;

	b = bmap_lookup_add(f, bmapno, mds_bmap_init);

	BMAP_LOCK(b);
	while (b->bcm_mode & BMAP_INFLIGHT) {
		psc_waitq_wait(&b->bcm_waitq, &b->bcm_lock);
		BMAP_LOCK(b);
	}
	if (b->bcm_mode & BMAP_INIT) {
		b->bcm_mode |= BMAP_INFLIGHT;
		BMAP_ULOCK(b);
		rc = mds_bmap_read(f, bmapno, b);
		BMAP_LOCK(b);
		if (rc) {
			DEBUG_FCMH(PLL_WARN, f,
				   "mds_bmap_read() rc=%d blkno=%u",
				   rc, bmapno);
			b->bcm_mode |= BMAP_MDS_FAILED;
		} else
			b->bcm_mode &= ~BMAP_INIT;
		/* Notify other threads that this bmap has been loaded;
		 *  they're blocked on BMAP_INFLIGHT.  */
		b->bcm_mode &= ~BMAP_INFLIGHT;
		psc_waitq_wakeall(&b->bcm_waitq);
	}
	BMAP_ULOCK(b);

	*bp = b;
	return (rc);
}

int
mds_bmap_load_ion(const struct slash_fidgen *fg, sl_blkno_t bmapno,
		  struct bmapc_memb **bmap)
{
	struct fidc_membh *f;
	int rc;

	psc_assert(!*bmap);

	f = fidc_lookup_fg(fg);
	if (!f)
		return (-ENOENT);

	rc = mds_bmap_load(f, bmapno, bmap);
	if (rc)
		return (rc);
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
mds_bmap_load_cli(struct mexpfcm *fref, const struct srm_bmap_req *mq,
    struct bmapc_memb **bmap)
{
	struct bmapc_memb *b, tbmap;
	struct fidc_membh *f=fref->mexpfcm_fcmh;
	struct fidc_mds_info *fmdsi=f->fcmh_fcoo->fcoo_pri;
	struct slash_inode_handle *inoh=&fmdsi->fmdsi_inodeh;
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
	}
	/* Establish a reference here, note that mexpbcm_bmap will
	 *   be null until either the bmap is loaded or pulled from
	 *   the cache.
	 */
	bref = PSCALLOC(sizeof(*bref));
	MEXPBCM_LOCK_INIT(bref);
	bref->mexpbcm_mode = MEXPBCM_INIT;
	bref->mexpbcm_blkno = mq->blkno;
	bref->mexpbcm_export = fref->mexpfcm_export;
	SPLAY_INSERT(exp_bmaptree, &fref->mexpfcm_bmaps, bref);

	MEXPFCM_ULOCK(fref);
	/* Ok, the bref has been initialized and loaded into the tree.  We
	 *  still need to set the bmap pointer mexpbcm_bmap though.  Lock the
	 *  fcmh during the bmap lookup.
	 */
	rc = mds_bmap_load(f, mq->blkno, &b);
	if (rc)
		goto out;
	/* Sanity checks, make sure that we didn't let the client in
	 *  before this bmap was ready.
	 */
	MEXPBCM_LOCK(bref);

	psc_assert(bref->mexpbcm_mode == MEXPBCM_INIT);

	bref->mexpbcm_bmap = b;
	bref->mexpbcm_mode = ((mq->rw == SRIC_BMAP_WRITE) ?  MEXPBCM_WR : MEXPBCM_RD);

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

	/* Release the reference taken by mds_bmap_load().  This call
	 *   established a read or write ref to pin the bmap.
	 */
	bmap_op_done(b);
 out:
	if (rc)
		PSCFREE(bref);
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
	psc_crc64_t    crc;

	psc_assert(fcmh && !(*fcmh));

	rc = access_fsop(ACSOP_GETATTR, creds->uid, creds->gid, path,
			 SFX_INODE, &inoh.inoh_ino, sz);

	if (rc < 0)
		psc_warn("Attr lookup on (%s) failed", path);
	else if (rc != sz)
		psc_warn("Attr lookup on (%s) gave invalid sz (%d)", path, rc);
	else
		rc=0;

	psc_crc64_calc(&crc, &inoh.inoh_ino, sz);
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

		psc_crc64_calc(&crc, inoh.inoh_replicas, sz);
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

	slmfssyncthr_init();
	mdscoh_init();
}
