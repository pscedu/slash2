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

/*
 * This file contains routines for accessing the backing store of the MDS
 * file system, where each file in existence here actually contains the
 * SLASH file's metadata.
 */

#include "psc_util/lock.h"

#include "bmap.h"
#include "fid.h"
#include "fidc_mds.h"
#include "fidcache.h"
#include "inode.h"
#include "inodeh.h"
#include "mdsio.h"
#include "slashd.h"
#include "slerr.h"

#include "zfs-fuse/zfs_slashlib.h"

void
mdsio_init(void)
{
	zfs_init();
}

void
mdsio_exit(void)
{
	zfs_exit();
}

static inline void *
bmap_2_zfs_fh(struct bmapc_memb *bmap)
{
	struct fcmh_mds_info *fmi;

	fmi = fcmh_2_fmi(bmap->bcm_fcmh);
	psc_assert(fmi->fmi_mdsio_data);
	return (fmi->fmi_mdsio_data);
}

int
mdsio_apply_fcmh_size(struct fidc_membh *f, size_t size)
{
	int locked;

	locked = reqlock(&f->fcmh_lock);
	fcmh_2_fsz(f) = size;
	ureqlock(&f->fcmh_lock, locked);

	return (zfsslash2_setattr(fcmh_2_fid(f), &f->fcmh_sstb,
	    SRM_SETATTRF_FSIZE, &rootcreds, NULL,
	    fcmh_2_fmi(f)->fmi_mdsio_data, (sl_jlog_cb)NULL));
}

int
mdsio_bmap_read(struct bmapc_memb *bmap)
{
	size_t nb;
	int rc;

	rc = zfsslash2_read(&rootcreds, bmap->bcm_od, BMAP_OD_SZ, &nb,
	    (off_t)((BMAP_OD_SZ * bmap->bcm_blkno) + SL_BMAP_START_OFF),
	    bmap_2_zfs_fh(bmap));
	if (rc == 0 && nb != BMAP_OD_SZ)
		rc = SLERR_SHORTIO;

	DEBUG_BMAP(PLL_TRACE, bmap, "read bmap (rc=%d)",rc);
	return (rc);
}

int
mdsio_bmap_write(struct bmapc_memb *bmap)
{
	size_t nb;
	int rc;

	rc = zfsslash2_write(&rootcreds, bmap->bcm_od, BMAP_OD_SZ, &nb,
	    (off_t)((BMAP_OD_SZ * bmap->bcm_blkno) + SL_BMAP_START_OFF),
	    bmap_2_zfs_fh(bmap));

	if (rc) {
		DEBUG_BMAP(PLL_ERROR, bmap, "zfsslash2_write: error (rc=%d)",
			   rc);
	} else if (nb != BMAP_OD_SZ) {
		DEBUG_BMAP(PLL_ERROR, bmap, "zfsslash2_write: short I/O");
		rc = SLERR_SHORTIO;
	} else {
		rc = zfsslash2_fsync(&rootcreds, 1, bmap_2_zfs_fh(bmap));
		if (rc == -1)
			psc_fatal("zfsslash2_fsync");
	}
	DEBUG_BMAP(PLL_TRACE, bmap, "wrote bmap (rc=%d)",rc);

	return (rc);
}

int
mdsio_inode_read(struct slash_inode_handle *i)
{
	size_t nb;
	int rc;

	INOH_LOCK_ENSURE(i);
	rc = zfsslash2_read(&rootcreds, &i->inoh_ino, INO_OD_SZ, &nb,
	    SL_INODE_START_OFF, inoh_2_mdsio_data(i));

	if (rc) {
		DEBUG_INOH(PLL_ERROR, i, "inode read error %d", rc);
	} else if (nb != INO_OD_SZ) {
		DEBUG_INOH(PLL_NOTICE, i, "short read I/O (%zd vs %zd)",
		    nb, INO_OD_SZ);
		rc = SLERR_SHORTIO;
	} else {
		DEBUG_INOH(PLL_TRACE, i, "read inode data=%p",
		    inoh_2_mdsio_data(i));
//		rc = zfsslash2_getattr(inoh_2_fid(i),
//		    &inoh_2_fsz(i), inoh_2_mdsio_data(i));
	}
	return (rc);
}

int
mdsio_inode_write(struct slash_inode_handle *i)
{
	size_t nb;
	int rc;

	rc = zfsslash2_write(&rootcreds, &i->inoh_ino, INO_OD_SZ, &nb,
	    SL_INODE_START_OFF, inoh_2_mdsio_data(i));

	if (rc) {
		DEBUG_INOH(PLL_ERROR, i, "zfsslash2_write: error (rc=%d)",
			   rc);
	} else if (nb != INO_OD_SZ) {
		DEBUG_INOH(PLL_ERROR, i, "zfsslash2_write: short I/O");
		rc = SLERR_SHORTIO;
	} else {
		DEBUG_INOH(PLL_TRACE, i, "wrote inode (rc=%d) data=%p",
		    rc, inoh_2_mdsio_data(i));
#ifdef SHITTY_PERFORMANCE
		rc = zfsslash2_fsync(&rootcreds, 1, inoh_2_mdsio_data(i));
		if (rc == -1)
			psc_fatal("zfsslash2_fsync");
#endif
	}
	return (rc);
}

int
mdsio_inode_extras_read(struct slash_inode_handle *i)
{
	size_t nb;
	int rc;

	psc_assert(i->inoh_extras);
	rc = zfsslash2_read(&rootcreds, i->inoh_extras, INOX_OD_SZ, &nb,
	    SL_EXTRAS_START_OFF, inoh_2_mdsio_data(i));
	if (rc)
		DEBUG_INOH(PLL_ERROR, i, "zfsslash2_read: error (rc=%d)", rc);
	else if (nb != INOX_OD_SZ) {
		DEBUG_INOH(PLL_ERROR, i, "zfsslash2_read: short I/O");
		rc = SLERR_SHORTIO;
	}
	return (rc);
}

int
mdsio_inode_extras_write(struct slash_inode_handle *i)
{
	size_t nb;
	int rc;

	psc_assert(i->inoh_extras);
	rc = zfsslash2_write(&rootcreds, i->inoh_extras, INOX_OD_SZ, &nb,
	    SL_EXTRAS_START_OFF, inoh_2_mdsio_data(i));

	if (rc) {
		DEBUG_INOH(PLL_ERROR, i, "zfsslash2_write: error (rc=%d)",
			   rc);
	} else if (nb != INOX_OD_SZ) {
		DEBUG_INOH(PLL_ERROR, i, "zfsslash2_write: short I/O");
		rc = SLERR_SHORTIO;
	} else {
#ifdef SHITTY_PERFORMANCE
		rc = zfsslash2_fsync(&rootcreds, 1, inoh_2_mdsio_data(i));
		if (rc == -1)
			psc_fatal("zfsslash2_fsync");
#endif
	}
	return (rc);
}

int
mdsio_release(const struct slash_creds *cr, void *mdsio_data)
{
	return (zfsslash2_release(cr, mdsio_data));
}

int
mdsio_access(mdsio_fid_t ino, int mask, const struct slash_creds *cr)
{
	return (zfsslash2_access(ino, mask, cr));
}

int
mdsio_getattr(mdsio_fid_t ino, const struct slash_creds *cr,
    struct srt_stat *sstb)
{
	return (zfsslash2_getattr(ino, cr, sstb));
}

int
mdsio_readlink(mdsio_fid_t ino, void *buf, const struct slash_creds *cr)
{
	return (zfsslash2_readlink(ino, buf, cr));
}

int
mdsio_statfs(struct statvfs *stbv)
{
	return (zfsslash2_statfs(stbv));
}

int
mdsio_opencreate(mdsio_fid_t pino, const struct slash_creds *cr,
    int flags, mode_t mode, const char *fn, struct slash_fidgen *fgp,
    mdsio_fid_t *mfp, struct srt_stat *sstb, void *mdsio_datap,
    sl_jlog_cb logfunc, sl_getslfid_cb getslfid)
{
	return (zfsslash2_opencreate(pino, cr, flags, mode,
	    fn, fgp, mfp, sstb, mdsio_datap, logfunc, getslfid));
}

int
mdsio_link(mdsio_fid_t ino, mdsio_fid_t pino, const char *fn,
    struct slash_fidgen *fgp, const struct slash_creds *cr,
    struct srt_stat *sstb)
{
	return (zfsslash2_link(ino, pino, fn, fgp, cr, sstb));
}

int
mdsio_lookup(mdsio_fid_t pino, const char *cpn, struct slash_fidgen *fgp,
    mdsio_fid_t *mfp, const struct slash_creds *cr, struct srt_stat *sstb)
{
	return (zfsslash2_lookup(pino, cpn, fgp, mfp, cr, sstb));
}

int
mdsio_lookup_slfid(slfid_t fid, const struct slash_creds *crp,
    struct srt_stat *sstb, mdsio_fid_t *mfp)
{
	return (zfsslash2_lookup_slfid(fid, crp, sstb, mfp));
}

int
mdsio_opendir(mdsio_fid_t ino, const struct slash_creds *cr,
    struct slash_fidgen *fgp, struct srt_stat *sstb, void *mdsio_datap)
{
	return (zfsslash2_opendir(ino, cr, fgp, sstb, mdsio_datap));
}

int
mdsio_mkdir(mdsio_fid_t pino, const char *cpn, mode_t mode,
    const struct slash_creds *cr, struct srt_stat *sstb,
    struct slash_fidgen *fgp, mdsio_fid_t *mfp, 
    sl_jlog_cb logfunc, sl_getslfid_cb getslfid)
{
	return (zfsslash2_mkdir(pino, cpn, mode, cr, sstb,
	    fgp, mfp, logfunc, getslfid));
}

int
mdsio_readdir(const struct slash_creds *cr, size_t siz,
    off_t off, void *buf, size_t *outlen, void *attrs, int nprefetch,
    void *mdsio_data)
{
	return (zfsslash2_readdir(cr, siz, off, buf,
	    outlen, attrs, nprefetch, mdsio_data));
}

int
mdsio_rename(mdsio_fid_t opino, const char *ocpn, mdsio_fid_t npino,
    const char *ncpn, const struct slash_creds *cr)
{
	return (zfsslash2_rename(opino, ocpn, npino, ncpn, cr));
}

int
mdsio_setattr(mdsio_fid_t ino, struct srt_stat *sstb_in, int to_set,
    const struct slash_creds *cr, struct srt_stat *sstb_out,
    void *mdsio_data, sl_jlog_cb logfunc)
{
	return (zfsslash2_setattr(ino, sstb_in, to_set, cr,
	    sstb_out, mdsio_data, logfunc));
}

int
mdsio_symlink(const char *target, mdsio_fid_t pino, const char *cpn,
    const struct slash_creds *cr, struct srt_stat *sstb,
    struct slash_fidgen *fgp, mdsio_fid_t *mfp, sl_getslfid_cb getslfid)
{
	return (zfsslash2_symlink(target, pino, cpn, cr, sstb,
	    fgp, mfp, getslfid));
}

int
mdsio_unlink(mdsio_fid_t pino, const char *cpn, const struct slash_creds *cr)
{
	return (zfsslash2_unlink(pino, cpn, cr));
}

int
mdsio_rmdir(mdsio_fid_t pino, const char *cpn, const struct slash_creds *cr)
{
	return (zfsslash2_rmdir(pino, cpn, cr));
}

/*
 * Replay the namespace create operation performed on the remote MDS.
 */
int
mdsio_replay_create(uint64_t parent_s2id, uint64_t target_s2id, int type, 
	int32_t uid, int32_t gid, int mode, char *name)
{
	int rc;
	switch (type) {
	    case SJ_NAMESPACE_TYPE_DIR:
		rc = zfsslash2_replay_mkdir(parent_s2id, target_s2id, mode, name);
		break;
	    case SJ_NAMESPACE_TYPE_FILE:
		rc = zfsslash2_replay_create(parent_s2id, target_s2id, uid, gid, mode, name);
		break;
	    case SJ_NAMESPACE_TYPE_LINK:
		rc = zfsslash2_replay_link(parent_s2id, target_s2id, mode, name);
		break;
	    case SJ_NAMESPACE_TYPE_SYMLINK:
		rc = zfsslash2_replay_symlink(parent_s2id, target_s2id, mode, name);
		break;
	    default:
		psc_errorx("mdsio_relay_create(): invalid type %d\n", type);
		rc = EINVAL;
	}
	return (rc);
}

/*
 * Replay the namespace remove operation performed on the remote MDS.
 */
int
mdsio_replay_remove(__unusedx uint64_t parent_s2id, __unusedx uint64_t target_s2id, 
	int type, __unusedx char *name)
{
	int rc;
	switch (type) {
	    case SJ_NAMESPACE_TYPE_DIR:
		rc = zfsslash2_replay_rmdir(parent_s2id, target_s2id, name);
		break;
	    case SJ_NAMESPACE_TYPE_FILE:
		rc = zfsslash2_replay_unlink(parent_s2id, target_s2id, name);
		break;
	}
	return (rc);
}

/*
 * Replay the namespace attribute update operation performed on the remote MDS.
 */
int
mdsio_replay_attrib(uint64_t target_s2id, struct srt_stat *stat, uint mask)
{
	int rc;
	rc = zfsslash2_replay_setattr(target_s2id, stat, mask);
	return (rc);
}
