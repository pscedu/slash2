/* $Id$ */

#include <sys/param.h>

#include <linux/fuse.h>

#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>

#include "psc_ds/pool.h"
#include "psc_ds/tree.h"
#include "psc_ds/vbitmap.h"
#include "psc_util/alloc.h"
#include "psc_util/crc.h"
#include "psc_util/lock.h"
#include "psc_util/log.h"
#include "psc_util/waitq.h"

#include "fid.h"
#include "fidc_mds.h"
#include "fidcache.h"
#include "inode.h"
#include "inodeh.h"
#include "mdsexpc.h"
#include "mdsio_zfs.h"
#include "mdslog.h"
#include "repl_mds.h"
#include "slashd.h"
#include "slerr.h"

#include "zfs-fuse/zfs_slashlib.h"

struct replrqtree	 replrq_tree = SPLAY_INITIALIZER(&replrq_tree);
struct psc_poolmaster	 replrq_poolmaster;
struct psc_poolmgr	*replrq_pool;
psc_spinlock_t		 replrq_tree_lock = LOCK_INITIALIZER;

struct vbitmap		*repl_busytable;
psc_spinlock_t		 repl_busytable_lock = LOCK_INITIALIZER;
int			 repl_busytable_nents;

__static int
iosidx_cmp(const void *a, const void *b)
{
	const int *x = a, *y = b;

	if (*x < *y)
		return (-1);
	else if (*x > *y)
		return (1);
	return (0);
}

__static int
iosidx_in(int idx, int iosidx[], int nios)
{
	if (bsearch(&idx, iosidx, nios,
	    sizeof(iosidx[0]), iosidx_cmp))
		return (1);
	return (0);
}

int
replrq_cmp(const void *a, const void *b)
{
	const struct sl_replrq *x = a, *y = b;

	if (REPLRQ_FID(x) < REPLRQ_FID(y))
		return (-1);
	else if (REPLRQ_FID(x) > REPLRQ_FID(y))
		return (1);
	return (0);
}

SPLAY_GENERATE(replrqtree, sl_replrq, rrq_tentry, replrq_cmp);

void
mds_repl_dequeue_sites(struct sl_replrq *rrq, sl_replica_t *iosv, int nios)
{
	struct mds_site_info *msi;
	struct sl_site *site;
	int locked, n;

	locked = reqlock(&rrq->rrq_lock);
	for (n = 0; n < nios; n++) {
		site = libsl_resid2site(iosv[n].bs_id);
		msi = site->site_pri;

		spinlock(&msi->msi_lock);
		if (psc_dynarray_exists(&msi->msi_replq, rrq)) {
			psc_dynarray_remove(&msi->msi_replq, rrq);
			psc_atomic32_dec(&rrq->rrq_refcnt);
			msi->msi_flags |= MSIF_DIRTYQ;
			psc_multilock_cond_wakeup(&msi->msi_mlcond);
		}
		freelock(&msi->msi_lock);
	}
	ureqlock(&rrq->rrq_lock, locked);
}

void
mds_repl_enqueue_sites(struct sl_replrq *rrq, sl_replica_t *iosv, int nios)
{
	struct mds_site_info *msi;
	struct sl_site *site;
	int locked, n;

	locked = reqlock(&rrq->rrq_lock);
	rrq->rrq_flags |= REPLRQF_REQUEUE;
	for (n = 0; n < nios; n++) {
		site = libsl_resid2site(iosv[n].bs_id);
		msi = site->site_pri;

		spinlock(&msi->msi_lock);
		if (!psc_dynarray_exists(&msi->msi_replq, rrq)) {
			psc_dynarray_add(&msi->msi_replq, rrq);
			msi->msi_flags |= MSIF_DIRTYQ;
			psc_atomic32_inc(&rrq->rrq_refcnt);
		}
		psc_multilock_cond_wakeup(&msi->msi_mlcond);
		freelock(&msi->msi_lock);
	}
	ureqlock(&rrq->rrq_lock, locked);
}

int
_mds_repl_ios_lookup(struct slash_inode_handle *i, sl_ios_id_t ios, int add)
{
	uint32_t j=0, k;
	int rc = -ENOENT;
	sl_replica_t *repl;

	INOH_LOCK(i);
	if (!i->inoh_ino.ino_nrepls) {
		if (!add)
			goto out;
		else
			goto add_repl;
	}

	for (j=0, k=0, repl=i->inoh_ino.ino_repls; j < i->inoh_ino.ino_nrepls;
	     j++, k++) {
		if (j >= INO_DEF_NREPLS) {
			/* The first few replicas are in the inode itself,
			 *   the rest are in the extras block.
			 */
			if (!(i->inoh_flags & INOH_HAVE_EXTRAS))
				if (!(rc = mds_inox_load_locked(i)))
					goto out;

			repl = i->inoh_extras->inox_repls;
			k = 0;
		}

		DEBUG_INOH(PLL_INFO, i, "rep%u[%u] == %u",
			   k, repl[k].bs_id, ios);
		if (repl[k].bs_id == ios) {
			rc = j;
			goto out;
		}
	}
	/* It does not exist, add the replica to the inode if 'add' was
	 *   specified, else return.
	 */
	if (rc == -ENOENT && add) {
 add_repl:
		psc_assert(i->inoh_ino.ino_nrepls == j);

		if (i->inoh_ino.ino_nrepls >= SL_MAX_REPLICAS) {
			DEBUG_INOH(PLL_WARN, i, "too many replicas");
			rc = -ENOSPC;
			goto out;
		}

		if (j > INO_DEF_NREPLS) {
			/* Note that both the inode structure and replication
			 *  table must be synced.
			 */
			psc_assert(i->inoh_extras);
			i->inoh_flags |= (INOH_EXTRAS_DIRTY | INOH_INO_DIRTY);
			repl = i->inoh_extras->inox_repls;
			k = j - INO_DEF_NREPLS;
		} else {
			i->inoh_flags |= INOH_INO_DIRTY;
			repl = i->inoh_ino.ino_repls;
			k = j;
		}

		repl[k].bs_id = ios;
		i->inoh_ino.ino_nrepls++;

		DEBUG_INOH(PLL_INFO, i, "add IOS(%u) to repls, replica %d",
			   ios, i->inoh_ino.ino_nrepls-1);

		mds_inode_addrepl_log(i, ios, j);

		rc = j;
	}
 out:
	INOH_ULOCK(i);
	return (rc);
}

#define mds_repl_iosv_lookup(ih, ios, iosidx, nios)	_mds_repl_iosv_lookup((ih), (ios), (iosidx), (nios), 0)
#define mds_repl_iosv_lookup_add(ih, ios, iosidx, nios)	_mds_repl_iosv_lookup((ih), (ios), (iosidx), (nios), 1)

__static int
_mds_repl_iosv_lookup(struct slash_inode_handle *ih,
    sl_replica_t iosv[], int iosidx[], int nios, int add)
{
	int k, last;

	for (k = 0; k < nios; k++)
		if ((iosidx[k] = _mds_repl_ios_lookup(ih, iosv[k].bs_id, add)) < 0)
			return (-iosidx[k]);

	qsort(iosidx, nios, sizeof(iosidx[0]), iosidx_cmp);
	/* check for dups */
	last = -1;
	for (k = 0; k < nios; k++, last = iosidx[k])
		if (iosidx[k] == last)
			return (EINVAL);
	return (0);
}

#if 0
__static int
mds_repl_xattr_load_locked(struct slash_inode_handle *i)
{
	char fidfn[FID_MAX_PATH];
	size_t sz;
	int rc;

	DEBUG_INOH(PLL_INFO, i, "trying to load replica table");

	INOH_LOCK_ENSURE(i);
	psc_assert(i->inoh_ino.ino_nrepls);
	psc_assert(!i->inoh_replicas);
	psc_assert(!(i->inoh_flags & INOH_HAVE_REPS));

	fid_makepath(i->inoh_ino.ino_fg.fg_fid, fidfn);
	sz = (sizeof(sl_replica_t) * i->inoh_ino.ino_nrepls);

	if (fid_getxattr(fidfn, SFX_REPLICAS,  i->inoh_replicas, sz)) {
		psc_warnx("fid_getxattr failed to get %s", SFX_REPLICAS);
		rc = -errno;
		goto fail;

	} else if (mds_repl_crc_check(i)) {
		rc = -EIO;
		goto fail;

	} else {
		i->inoh_flags |= INOH_HAVE_REPS;
		DEBUG_INOH(PLL_INFO, i, "replica table loaded");
	}
	return (0);

 fail:
	DEBUG_INOH(PLL_INFO, i, "replica table load failed");
	return (rc);
}
#endif

/* replication state walking flags */
#define REPL_WALKF_SCIRCUIT	(1 << 0)	/* short circuit on return value set */
#define REPL_WALKF_MODOTH	(1 << 1)	/* modify everyone except specified ios */

int
mds_repl_bmap_apply(struct bmapc_memb *bcm, const int tract[4],
    const int retifset[4], int flags, int off, int *scircuit)
{
	struct slash_bmap_od *bmapod;
	struct bmap_mds_info *bmdsi;
	int val, rc = 0;

	*scircuit = 0;

	bmdsi = bmap_2_bmdsi(bcm);
	bmapod = bmdsi->bmdsi_od;
	val = SL_REPL_GET_BMAP_IOS_STAT(bmapod->bh_repls, off);

	/* Check for return values */
	if (retifset && retifset[val]) {
		/*
		 * Assign here instead of above to prevent
		 * overwriting a zero return value.
		 */
		rc = retifset[val];
		if (flags & REPL_WALKF_SCIRCUIT) {
			*scircuit = 1;
			return (rc);
		}
	}

	/* Apply any translations */
	if (tract && tract[val] != -1) {
		SL_REPL_SET_BMAP_IOS_STAT(bmapod->bh_repls,
		    off, tract[val]);
		bmdsi->bmdsi_flags |= BMIM_LOGCHG;
		if (val == SL_REPL_ACTIVE)
			bmdsi->bmdsi_flags |= BMIM_BUMPGEN;
	}
	return (rc);
}

/*
 * mds_repl_bmap_walk - walk the bmap replication bits, performing any
 *	specified translations and returning any queried states.
 * @b: bmap.
 * @tract: action translation array.
 * @retifset: return given value, last one wins.
 * @flags: operational flags.
 * @iosidx: indexes of I/O systems to exclude or query, or NULL for everyone.
 * @nios: # I/O system indexes specified.
 */
int
mds_repl_bmap_walk(struct bmapc_memb *bcm, const int tract[4],
    const int retifset[4], int flags, int iosidx[], int nios)
{
	int scircuit, nr, off, k, rc, trc;
	struct slash_bmap_od *bmapod;
	struct bmap_mds_info *bmdsi;

	BMAP_LOCK_ENSURE(bcm);

	scircuit = rc = 0;
	nr = fcmh_2_inoh(bcm->bcm_fcmh)->inoh_ino.ino_nrepls;
	bmdsi = bmap_2_bmdsi(bcm);
	bmapod = bmdsi->bmdsi_od;

	if (nios == 0)
		for (k = 0, off = 0; k < nr;
		    k++, off += SL_BITS_PER_REPLICA) {
			trc = mds_repl_bmap_apply(bcm, tract,
			    retifset, flags, off, &scircuit);
			if (trc)
				rc = trc;
			if (scircuit)
				break;
		}
	else if (flags & REPL_WALKF_MODOTH) {
		for (k = 0, off = 0; k < nr; k++,
		    off += SL_BITS_PER_REPLICA)
			if (!iosidx_in(k, iosidx, nios)) {
				trc = mds_repl_bmap_apply(bcm, tract,
				    retifset, flags, off, &scircuit);
				if (trc)
					rc = trc;
				if (scircuit)
					break;
			}
	} else
		for (k = 0; k < nios; k++) {
			trc = mds_repl_bmap_apply(bcm, tract,
			    retifset, flags, iosidx[k] *
			    SL_BITS_PER_REPLICA, &scircuit);
			if (trc)
				rc = trc;
			if (scircuit)
				break;
		}
	return (rc);
}

int
mds_repl_inv_except_locked(struct bmapc_memb *bcm, sl_ios_id_t ios)
{
	int dummy, rc, iosidx, tract[4], retifset[4];
	struct slash_bmap_od *bmapod;
	struct bmap_mds_info *bmdsi;
	struct sl_replrq *rrq;
	sl_replica_t repl;

	BMAP_LOCK_ENSURE(bcm);

	/* Find/add our replica's IOS ID */
	iosidx = mds_repl_ios_lookup_add(fcmh_2_inoh(bcm->bcm_fcmh), ios);
	if (iosidx < 0)
		psc_fatalx("lookup ios %d: %s", ios, slstrerror(iosidx));

	bmdsi = bmap_2_bmdsi(bcm);
	bmapod = bmdsi->bmdsi_od;

	/*
	 * If this bmap is marked for persistent replication,
	 * do not release the replication request for this file.
	 */
	if (bmdsi->bmdsi_repl_policy == BRP_PERSIST) {
		rrq = mds_repl_findrq(fcmh_2_fgp(bcm->bcm_fcmh), &dummy);
		repl.bs_id = ios;
		mds_repl_enqueue_sites(rrq, &repl, 1);
		mds_repl_unrefrq(rrq);
	}

	/* ensure this replica is marked active */
	tract[SL_REPL_INACTIVE] = SL_REPL_ACTIVE;
	tract[SL_REPL_OLD] = -1;
	tract[SL_REPL_SCHED] = -1;
	tract[SL_REPL_ACTIVE] = -1;

	retifset[SL_REPL_INACTIVE] = 0;
	retifset[SL_REPL_OLD] = EINVAL;
	retifset[SL_REPL_SCHED] = EINVAL;
	retifset[SL_REPL_ACTIVE] = 0;

	rc = mds_repl_bmap_walk(bcm, tract, retifset, 0, &iosidx, 1);
	if (rc)
		psc_error("bh_repls is marked OLD or SCHED for fid %lx "
		    "bmap %d iosidx %d", fcmh_2_fid(bcm->bcm_fcmh),
		    bcm->bcm_blkno, iosidx);

	/* invalidate all other replicas */
	tract[SL_REPL_INACTIVE] = -1;
	tract[SL_REPL_OLD] = -1;
	tract[SL_REPL_SCHED] = SL_REPL_OLD;
	tract[SL_REPL_ACTIVE] = SL_REPL_OLD;

	mds_repl_bmap_walk(bcm, tract, NULL, REPL_WALKF_MODOTH, &iosidx, 1);

	/* write changes back to disk */
	if (bmdsi->bmdsi_flags & BMIM_LOGCHG) {
		bmdsi->bmdsi_flags &= ~BMIM_LOGCHG;
		if (bmdsi->bmdsi_flags & BMIM_BUMPGEN) {
			bmdsi->bmdsi_flags &= ~BMIM_BUMPGEN;
			bmapod->bh_gen.bl_gen++;
		}
		mds_bmap_repl_log(bcm);
	}
	return (0);
}

__static void
mds_repl_bmap_rel(struct bmapc_memb *bcm)
{
	struct bmap_mds_info *bmdsi;
	struct slash_bmap_od *bmapod;

	BMAP_LOCK_ENSURE(bcm);

	bmdsi = bmap_2_bmdsi(bcm);
	bmapod = bmdsi->bmdsi_od;

	if (bmdsi->bmdsi_flags & BMIM_LOGCHG) {
		bmdsi->bmdsi_flags &= ~BMIM_LOGCHG;
		if (bmdsi->bmdsi_flags & BMIM_BUMPGEN) {
			bmdsi->bmdsi_flags &= ~BMIM_BUMPGEN;
			bmapod->bh_gen.bl_gen++;
		}
		mds_bmap_repl_log(bcm);
	}
	bmap_op_done(bcm);
}

/**
 * mds_repl_accessrq - Obtain processing access to a replication request.
 *	This routine assumes the refcnt has already been bumped.
 * @rrq: replication request to access, locked on return.
 * Returns Boolean true on success or false if the request is going away.
 */
int
mds_repl_accessrq(struct sl_replrq *rrq)
{
	int rc = 1;

	reqlock(&rrq->rrq_lock);

	/* Wait for someone else to finish processing. */
	while (rrq->rrq_flags & REPLRQF_BUSY) {
		psc_waitq_wait(&rrq->rrq_waitq, &rrq->rrq_lock);
		spinlock(&rrq->rrq_lock);
	}

	/* Release if going away. */
	if (rrq->rrq_flags & REPLRQF_DIE) {
		psc_atomic32_dec(&rrq->rrq_refcnt);
		psc_waitq_wakeall(&rrq->rrq_waitq);
		rc = 0;
	} else
		rrq->rrq_flags |= REPLRQF_BUSY;
	return (rc);
}

void
mds_repl_unrefrq(struct sl_replrq *rrq)
{
	reqlock(&rrq->rrq_lock);
	psc_atomic32_dec(&rrq->rrq_refcnt);
	rrq->rrq_flags &= ~REPLRQF_BUSY;
	psc_waitq_wakeall(&rrq->rrq_waitq);
	freelock(&rrq->rrq_lock);
}

struct sl_replrq *
mds_repl_findrq(struct slash_fidgen *fgp, int *locked)
{
	struct slash_inode_handle inoh;
	struct sl_replrq q, *rrq;

	inoh.inoh_ino.ino_fg = *fgp;
	q.rrq_inoh = &inoh;

	*locked = reqlock(&replrq_tree_lock);
	rrq = SPLAY_FIND(replrqtree, &replrq_tree, &q);
	if (rrq) {
		spinlock(&rrq->rrq_lock);
		psc_atomic32_inc(&rrq->rrq_refcnt);
	}

	if (rrq == NULL) {
		ureqlock(&replrq_tree_lock, *locked);
		return (NULL);
	}
	freelock(&replrq_tree_lock);
	*locked = 0;

	mds_repl_accessrq(rrq);
	freelock(&rrq->rrq_lock);
	return (rrq);
}

/* XXX this should be refactored into a generic inode loader in mds.c */
int
mds_repl_loadino(struct slash_fidgen *fgp, struct fidc_membh **fp)
{
	struct slash_inode_handle *ih;
	struct fidc_membh *fcmh;
	struct slash_fidgen fg;
	struct stat stb;
	void *data;
	int rc;

	*fp = NULL;

	rc = fidc_lookup_load_fg(fgp, &rootcreds, &fcmh);
	if (rc)
		return (rc);

	rc = mds_fcmh_tryref_fmdsi(fcmh);
	if (rc) {
		rc = zfsslash2_opencreate(zfsVfs, fgp->fg_fid,
		    &rootcreds, SL_FREAD, 0, NULL, &fg, &stb, &data);
		if (rc)
			return (rc);
		rc = mds_fcmh_load_fmdsi(fcmh, data, 1);
		/* don't release the ZFS handle on success */
		if (rc || fcmh_2_zfsdata(fcmh) != data)
			zfsslash2_release(zfsVfs, fg.fg_fid,
			    &rootcreds, data);
		if (rc)
			return (EINVAL); /* XXX need better errno */
	}

	ih = fcmh_2_inoh(fcmh);
	rc = mds_inox_ensure_loaded(ih);
	if (rc)
		psc_fatalx("mds_inox_ensure_loaded: %s", slstrerror(rc));
	*fp = fcmh;
	return (0);
}

int
mds_repl_addrq(struct slash_fidgen *fgp, sl_blkno_t bmapno,
    sl_replica_t *iosv, int nios)
{
	int iosidx[SL_MAX_REPLICAS], rc, locked, tract[4], retifset[4], retifset2[4];
	struct sl_replrq *newrq, *rrq;
	struct fidc_membh *fcmh;
	struct slash_fidgen fg;
	struct bmapc_memb *bcm;
	struct stat stb;
	char fn[FID_MAX_PATH];
	uint64_t inum;
	sl_blkno_t n;

	if (nios < 1 || nios > SL_MAX_REPLICAS)
		return (EINVAL);

	newrq = psc_pool_get(replrq_pool);

	rc = 0;
 restart:
	spinlock(&replrq_tree_lock);
	rrq = mds_repl_findrq(fgp, &locked);
	if (rrq == NULL) {
		if (!locked)
			goto restart;

		/* Not found, add it and its persistent link. */
		inum = sl_get_repls_inum();
		rc = snprintf(fn, sizeof(fn), "%016"PRIx64, fgp->fg_fid);
		if (rc == -1)
			rc = errno;
		else if (rc >= (int)sizeof(fn))
			rc = ENAMETOOLONG;
		else if ((rc = mds_repl_loadino(fgp, &fcmh)) != ENOENT) {
			if (rc)
				psc_fatalx("fidc_lookup_load_fg: %s",
				    slstrerror(rc));

			/* Find/add our replica's IOS ID */
			rc = mds_repl_iosv_lookup_add(fcmh_2_inoh(fcmh),
			    iosv, iosidx, nios);
			if (rc)
				goto bail;

			/* Create persistent file system link */
			rc = zfsslash2_link(zfsVfs, fgp->fg_fid,
			    inum, fn, &fg, &rootcreds, &stb);
			if (rc == 0) {
				rrq = newrq;
				newrq = NULL;

				memset(rrq, 0, sizeof(*rrq));
				LOCK_INIT(&rrq->rrq_lock);
				psc_waitq_init(&rrq->rrq_waitq);
				psc_atomic32_set(&rrq->rrq_refcnt, 1);
				rrq->rrq_inoh = fcmh_2_inoh(fcmh);
				rrq->rrq_flags |= REPLRQF_BUSY;
				SPLAY_INSERT(replrqtree, &replrq_tree, rrq);
			}
		}
	} else {
		/* Find/add our replica's IOS ID */
		rc = mds_repl_iosv_lookup_add(rrq->rrq_inoh,
		    iosv, iosidx, nios);
	}
 bail:
	if (locked)
		freelock(&replrq_tree_lock);

	if (newrq)
		psc_pool_return(replrq_pool, newrq);

	if (rc) {
		if (rrq)
			mds_repl_unrefrq(rrq);
		return (rc);
	}

	/*
	 * Check inode's bmap state.  INACTIVE and ACTIVE states
	 * become OLD, signifying that replication needs to happen.
	 */
	tract[SL_REPL_INACTIVE] = SL_REPL_OLD;
	tract[SL_REPL_SCHED] = SL_REPL_OLD;
	tract[SL_REPL_OLD] = -1;
	tract[SL_REPL_ACTIVE] = -1;

	if (bmapno == (sl_blkno_t)-1) {
		int repl_some_act = 0, repl_all_act = 1;

		/* check if all bmaps are already old/queued */
		retifset[SL_REPL_INACTIVE] = 1;
		retifset[SL_REPL_SCHED] = 0;
		retifset[SL_REPL_OLD] = 0;
		retifset[SL_REPL_ACTIVE] = 1;

		/* check if all bmaps are already active */
		retifset2[SL_REPL_INACTIVE] = 1;
		retifset2[SL_REPL_SCHED] = 1;
		retifset2[SL_REPL_OLD] = 1;
		retifset2[SL_REPL_ACTIVE] = 0;

		for (n = 0; n < REPLRQ_NBMAPS(rrq); n++) {
			if (mds_bmap_load(REPLRQ_FCMH(rrq), n, &bcm))
				continue;
			BMAP_LOCK(bcm);
			repl_some_act |= mds_repl_bmap_walk(bcm,
			    tract, retifset, 0, iosidx, nios);
			if (repl_all_act && mds_repl_bmap_walk(bcm,
			    NULL, retifset2, REPL_WALKF_SCIRCUIT, NULL, 0))
				repl_all_act = 0;
			mds_repl_bmap_rel(bcm);
		}
		if (repl_some_act == 0)
			rc = EALREADY;
		else if (repl_all_act)
			rc = SLERR_REPL_ALREADY_ACT;
	} else if (mds_bmap_valid(REPLRQ_FCMH(rrq), bmapno)) {
		/*
		 * If this bmap is already being
		 * replicated, return EALREADY.
		 */
		retifset[SL_REPL_INACTIVE] = 0;
		retifset[SL_REPL_SCHED] = EALREADY;
		retifset[SL_REPL_OLD] = EALREADY;
		retifset[SL_REPL_ACTIVE] = 0;
		rc = mds_bmap_load(REPLRQ_FCMH(rrq), bmapno, &bcm);
		if (rc == 0) {
			BMAP_LOCK(bcm);
			rc = mds_repl_bmap_walk(bcm,
			    tract, retifset, 0, iosidx, nios);
			mds_repl_bmap_rel(bcm);
		}
	} else
		rc = SLERR_INVALID_BMAP;

	if (rc == 0)
		mds_repl_enqueue_sites(rrq, iosv, nios);

	mds_repl_unrefrq(rrq);
	return (rc);
}

/* XXX this should also remove any ios that are empty in all bmaps from the inode */
void
mds_repl_tryrmqfile(struct sl_replrq *rrq)
{
	struct bmapc_memb *bcm;
	char fn[FID_MAX_PATH];
	int rc, retifset[4];
	uint64_t inum;
	sl_blkno_t n;

	/* Scan for any OLD states. */
	retifset[SL_REPL_INACTIVE] = 0;
	retifset[SL_REPL_ACTIVE] = 0;
	retifset[SL_REPL_OLD] = 1;
	retifset[SL_REPL_SCHED] = 1;

	/*
	 * If this request is currently being requeued, wait.
	 * After such a time, if it was requeued again, there must have
	 * been work to do, and some one else should tryrmqfile().
	 */
	reqlock(&rrq->rrq_lock);
	while (rrq->rrq_flags & REPLRQF_REQUEUE) {
		psc_waitq_wait(&rrq->rrq_waitq, &rrq->rrq_lock);
		spinlock(&rrq->rrq_lock);
	}
	freelock(&rrq->rrq_lock);

	/* Scan bmaps to see if the inode should disappear. */
	for (n = 0; n < REPLRQ_NBMAPS(rrq); n++) {
		if (mds_bmap_load(REPLRQ_FCMH(rrq), n, &bcm))
			continue;
		BMAP_LOCK(bcm);
		rc = mds_repl_bmap_walk(bcm, NULL,
		    retifset, REPL_WALKF_SCIRCUIT, NULL, 0);
		mds_repl_bmap_rel(bcm);
		if (rc)
			goto out;
	}

	spinlock(&replrq_tree_lock);
	spinlock(&rrq->rrq_lock);
	/* XXX or if inode new bmap policy == PERSIST or if any bmap policy == PERSIST */
	/* XXX or if someone else is rmqfile()'s this rrq */
	if (rrq->rrq_flags & REPLRQF_REQUEUE) {
		mds_repl_unrefrq(rrq);
		rrq = NULL;
	} else {
		/*
		 * All states are INACTIVE/ACTIVE;
		 * remove it and its persistent link.
		 */
		inum = sl_get_repls_inum();
		rc = snprintf(fn, sizeof(fn),
		    "%016"PRIx64, REPLRQ_FID(rrq));
		if (rc == -1)
			rc = errno;
		else if (rc >= (int)sizeof(fn))
			rc = ENAMETOOLONG;
		else
			rc = zfsslash2_unlink(zfsVfs, inum, fn, &rootcreds);
		SPLAY_XREMOVE(replrqtree, &replrq_tree, rrq);
	}
	freelock(&replrq_tree_lock);

 out:
	if (rrq) {
		reqlock(&rrq->rrq_lock);
		psc_atomic32_dec(&rrq->rrq_refcnt);	/* removed from tree */
		rrq->rrq_flags |= REPLRQF_DIE;
		rrq->rrq_flags &= ~REPLRQF_BUSY;

		while (psc_atomic32_read(&rrq->rrq_refcnt) > 1) {
			psc_waitq_wakeall(&rrq->rrq_waitq);
			psc_waitq_wait(&rrq->rrq_waitq, &rrq->rrq_lock);
			spinlock(&rrq->rrq_lock);
		}

		atomic_dec(&fcmh_2_fmdsi(REPLRQ_FCMH(rrq))->fmdsi_ref);
		fidc_membh_dropref(REPLRQ_FCMH(rrq));

		/* SPLAY_REMOVE() does not NULL out the field */
		INIT_PSCLIST_ENTRY(&rrq->rrq_lentry);
		psc_pool_return(replrq_pool, rrq);
	}
}

int
mds_repl_delrq(struct slash_fidgen *fgp, sl_blkno_t bmapno,
    sl_replica_t *iosv, int nios)
{
	int iosidx[SL_MAX_REPLICAS], locked, rc, tract[4], retifset[4];
	struct bmapc_memb *bcm;
	struct sl_replrq *rrq;
	sl_blkno_t n;

	if (nios < 1 || nios > SL_MAX_REPLICAS)
		return (EINVAL);

	rrq = mds_repl_findrq(fgp, &locked);
	if (rrq == NULL)
		return (ENOENT);

	/* Find replica IOS indexes */
	rc = mds_repl_iosv_lookup_add(rrq->rrq_inoh,
	    iosv, iosidx, nios);
	if (rc) {
		mds_repl_unrefrq(rrq);
		return (rc);
	}

	tract[SL_REPL_INACTIVE] = -1;
	tract[SL_REPL_ACTIVE] = SL_REPL_INACTIVE;
	tract[SL_REPL_OLD] = SL_REPL_INACTIVE;
	tract[SL_REPL_SCHED] = SL_REPL_INACTIVE;

	if (bmapno == (sl_blkno_t)-1) {
		retifset[SL_REPL_INACTIVE] = 0;
		retifset[SL_REPL_ACTIVE] = 1;
		retifset[SL_REPL_OLD] = 1;
		retifset[SL_REPL_SCHED] = 1;

		rc = SLERR_REPLS_ALL_INACT;
		for (n = 0; n < REPLRQ_NBMAPS(rrq); n++) {
			if (mds_bmap_load(REPLRQ_FCMH(rrq), n, &bcm))
				continue;
			BMAP_LOCK(bcm);
			if (mds_repl_bmap_walk(bcm, tract,
			    retifset, 0, iosidx, nios))
				rc = 0;
			mds_repl_bmap_rel(bcm);
		}
	} else if (mds_bmap_valid(REPLRQ_FCMH(rrq), bmapno)) {
		retifset[SL_REPL_INACTIVE] = SLERR_REPL_ALREADY_INACT;
		retifset[SL_REPL_ACTIVE] = 0;
		retifset[SL_REPL_OLD] = 0;
		retifset[SL_REPL_SCHED] = 0;
		rc = mds_bmap_load(REPLRQ_FCMH(rrq), bmapno, &bcm);
		if (rc == 0) {
			BMAP_LOCK(bcm);
			rc = mds_repl_bmap_walk(bcm,
			    tract, retifset, 0, iosidx, nios);
			mds_repl_bmap_rel(bcm);
		}
	} else
		rc = SLERR_INVALID_BMAP;

	mds_repl_tryrmqfile(rrq);
	return (0);
}

void
mds_repl_scandir(void)
{
	struct fidc_membh *fcmh;
	struct slash_fidgen fg;
	struct fuse_dirent *d;
	struct sl_replrq *rrq;
	struct stat stb;
	size_t siz, tsiz;
	off64_t off, toff;
	uint64_t inum;
	void *data;
	char *buf;
	int rc;

	off = 0;
	siz = 8 * 1024;
	buf = PSCALLOC(siz);

	inum = sl_get_repls_inum();
	rc = zfsslash2_opendir(zfsVfs, inum,
	    &rootcreds, &fg, &stb, &data);
	if (rc) {
		if (rc == ENOENT) {
			rc = zfsslash2_mkdir(zfsVfs, SL_ROOT_INUM,
			    SL_PATH_REPLS, 0700, &rootcreds, NULL, NULL, 1);
			if (rc == -1)
				psc_fatal("zfs_mkdir %s", SL_PATH_REPLS);
			return;
		}
		psc_fatalx("opendir %s: %s", SL_PATH_REPLS,
		    slstrerror(rc));
	}
	for (;;) {
		rc = zfsslash2_readdir(zfsVfs, inum, &rootcreds,
		    siz, off, buf, &tsiz, NULL, 0, data);
		if (rc)
			psc_fatalx("readdir %s: %s", SL_PATH_REPLS,
			    slstrerror(rc));
		if (tsiz == 0)
			break;
		for (toff = 0; toff < (off64_t)tsiz;
		    toff += FUSE_DIRENT_SIZE(d)) {
			d = (void *)(buf + toff);
			off = d->off;

			if (d->name[0] == '.')
				continue;
			fg.fg_fid = d->ino;
			fg.fg_gen = FIDGEN_ANY;
			rc = mds_repl_loadino(&fg, &fcmh);
			rc = fidc_lookup_load_fg(&fg, &rootcreds, &fcmh);
			if (rc)
				/* XXX if ENOENT, remove from repldir and continue */
				psc_fatal("fidc_lookup: %s", slstrerror(rc));

			rrq = psc_pool_get(replrq_pool);
			memset(rrq, 0, sizeof(*rrq));
			LOCK_INIT(&rrq->rrq_lock);
			psc_waitq_init(&rrq->rrq_waitq);
			psc_atomic32_set(&rrq->rrq_refcnt, 1);
			rrq->rrq_inoh = fcmh_2_inoh(fcmh);
			SPLAY_INSERT(replrqtree, &replrq_tree, rrq);
		}
		off += tsiz;
	}
	rc = zfsslash2_release(zfsVfs, inum, &rootcreds, data);
	if (rc)
		psc_fatalx("release %s: %s", SL_PATH_REPLS,
		    slstrerror(rc));

	free(buf);
}

/*
 * The replication busy table is a bitmap to allow quick lookups of
 * communication status between arbitrary IONs.  Each resm has a unique
 * busyid:
 *
 *	 busyid +---+---+---+---+---+---+	n | off, sz=6	| diff
 *	      0	| 1 | 2 | 3 | 4 | 5 | 6 |	--+-------------------
 *		+---+---+---+---+---+---+	0 |  0		|
 *	      1	| 2 | 3 | 4 | 5 | 6 |		1 |  6		| 6
 *		+---+---+---+---+---+		2 | 11		| 5
 *	      2	| 3 | 4 | 5 | 6 |		3 | 15		| 4
 *		+---+---+---+---+		4 | 18		| 3
 *	      3	| 4 | 5 | 6 |			5 | 20		| 2
 *		+---+---+---+			--+-------------------
 *	      4	| 5 | 6 |			n | n * (sz - (n-1)/2)
 *		+---+---+
 *	      5	| 6 |
 *		+---+
 *
 * For checking if communication exists between resources with busyid 1
 * and 2, we test the bit:
 *
 *	1 * (sz - (1-1)/2) + (2 - 1 - 1)
 *	n * (sz - (n-1)/2) + (m - n - 1)
 */
#define MDS_REPL_BUSYNODES(nnodes, min, max)				\
	(((min) * ((nnodes) - ((min) - 1) / 2)) + ((max) - (min) - 1))

int
mds_repl_nodes_getbusy(struct mds_resm_info *ma, struct mds_resm_info *mb)
{
	struct mds_resm_info *min, *max;
	int rc, locked;

	psc_assert(ma->mri_busyid != mb->mri_busyid);

	if (ma->mri_busyid < mb->mri_busyid) {
		min = ma;
		max = mb;
	} else {
		min = mb;
		max = ma;
	}

	locked = reqlock(&repl_busytable_lock);
	rc = psc_vbitmap_get(repl_busytable,
	    MDS_REPL_BUSYNODES(repl_busytable_nents,
	    min->mri_busyid, max->mri_busyid));
	ureqlock(&repl_busytable_lock, locked);
	return (rc);
}

int
mds_repl_nodes_setbusy(struct mds_resm_info *ma,
    struct mds_resm_info *mb, int busy)
{
	struct mds_resm_info *min, *max;
	int locked, rc;

	psc_assert(ma->mri_busyid != mb->mri_busyid);

	if (ma->mri_busyid < mb->mri_busyid) {
		min = ma;
		max = mb;
	} else {
		min = mb;
		max = ma;
	}

	locked = reqlock(&repl_busytable_lock);
	rc = psc_vbitmap_xsetval(repl_busytable,
	    MDS_REPL_BUSYNODES(repl_busytable_nents,
	    min->mri_busyid, max->mri_busyid), busy);
	ureqlock(&repl_busytable_lock, locked);
	return (rc);
}

void
mds_repl_buildbusytable(void)
{
	struct mds_resm_info *mri;
	struct sl_resource *r;
	struct sl_resm *resm;
	struct sl_site *s;
	uint32_t j;
	int n;

	/* count # resm's (IONs) and assign each a busy identifier */
	PLL_LOCK(&globalConfig.gconf_sites);
	spinlock(&repl_busytable_lock);
	repl_busytable_nents = 0;
	PLL_FOREACH(s, &globalConfig.gconf_sites)
		for (n = 0; n < s->site_nres; n++) {
			r = s->site_resv[n];
			for (j = 0; j < r->res_nnids; j++) {
				resm = libsl_nid2resm(r->res_nids[j]);
				mri = resm->resm_pri;
				mri->mri_busyid = repl_busytable_nents++;
			}
		}
	PLL_ULOCK(&globalConfig.gconf_sites);

	if (repl_busytable)
		vbitmap_free(repl_busytable);
	repl_busytable = vbitmap_new(repl_busytable_nents *
	    (repl_busytable_nents - 1) / 2);
	freelock(&repl_busytable_lock);
}

void
mds_repl_init(void)
{
	psc_poolmaster_init(&replrq_poolmaster, struct sl_replrq,
	    rrq_lentry, PPMF_AUTO, 256, 256, 0, NULL, NULL, NULL,
	    "replrq");
	replrq_pool = psc_poolmaster_getmgr(&replrq_poolmaster);

	mds_repl_buildbusytable();
	mds_repl_scandir();
}
