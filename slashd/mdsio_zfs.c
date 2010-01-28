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

void *zfsVfs;

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
	struct fidc_mds_info *fmdsi;

	psc_assert(bmap->bcm_fcmh);

	//fmdsi = fidc_fcmh2fmdsi(bmap->bcm_fcmh);
	fmdsi = fcmh_2_fmdsi(bmap->bcm_fcmh);

	psc_assert(fmdsi);
	psc_assert(fmdsi->fmdsi_data);

	return (fmdsi->fmdsi_data);
}

int
mdsio_release(struct slash_inode_handle *i)
{
	struct fidc_mds_info *fmdsi;

	psc_assert(i->inoh_fcmh);
	psc_assert(i->inoh_fcmh->fcmh_fcoo);
	psc_assert(i->inoh_fcmh->fcmh_state & FCMH_FCOO_CLOSING);

	fmdsi = fcmh_2_fmdsi(i->inoh_fcmh);
	psc_assert(!atomic_read(&fmdsi->fmdsi_ref));

	/*
	 * XXX should we pass the same creds here as the
	 * file was opened with?  do we even have them?
	 */
	return (zfsslash2_release(zfsVfs, fcmh_2_fid(i->inoh_fcmh),
	    &rootcreds, fmdsi->fmdsi_data));
}

int
mdsio_apply_fcmh_size(struct fidc_membh *f, off64_t size)
{
	struct fidc_mds_info *fmdsi;

	FCMH_LOCK(f);

	if (size <= fcmh_2_fsz(f)) {
		FCMH_ULOCK(f);
		return (0);
	}

	DEBUG_FCMH(PLL_INFO, f, "sz=%"PRId64, size);
	fmdsi = fcmh_2_fmdsi(f);
	fcmh_2_fsz(f) = size;

	FCMH_ULOCK(f);

	return (zfsslash2_sets2szattr(zfsVfs, fcmh_2_fid(f), size,
				      fmdsi->fmdsi_data));
}

int
mdsio_bmap_read(struct bmapc_memb *bmap)
{
	struct bmap_mds_info *bmdsi;
	size_t nb;
	int rc;

	bmdsi = bmap->bcm_pri;

	rc = zfsslash2_read(zfsVfs, fcmh_2_fid(bmap->bcm_fcmh), &rootcreds,
	    bmdsi->bmdsi_od, BMAP_OD_SZ, &nb,
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
	struct bmap_mds_info *bmdsi;
	size_t nb;
	int rc;

	bmdsi = bmap->bcm_pri;
	rc = zfsslash2_write(zfsVfs, fcmh_2_fid(bmap->bcm_fcmh), &rootcreds,
	    bmdsi->bmdsi_od, BMAP_OD_SZ, &nb,
	    (off_t)((BMAP_OD_SZ * bmap->bcm_blkno) + SL_BMAP_START_OFF),
	    bmap_2_zfs_fh(bmap));

	if (rc) {
		DEBUG_BMAP(PLL_ERROR, bmap, "zfsslash2_write() error (rc=%d)",
			   rc);
	} else if (nb != BMAP_OD_SZ) {
		DEBUG_BMAP(PLL_ERROR, bmap, "zfsslash2_write() short I/O");
		rc = SLERR_SHORTIO;
	} else {
		rc = zfsslash2_fsync(zfsVfs, fcmh_2_fid(bmap->bcm_fcmh),
		    &rootcreds, 1, bmap_2_zfs_fh(bmap));
		if (rc == -1)
			psc_fatal("zfsslash2_fsync() failed");
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
	rc = zfsslash2_read(zfsVfs, fcmh_2_fid(i->inoh_fcmh),
	    &rootcreds, &i->inoh_ino, INO_OD_SZ, &nb,
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
		rc = zfsslash2_gets2szattr(zfsVfs, inoh_2_fid(i),
		    &inoh_2_fsz(i), inoh_2_mdsio_data(i));
	}
	return (rc);
}

int
mdsio_inode_write(struct slash_inode_handle *i)
{
	size_t nb;
	int rc;

	rc = zfsslash2_write(zfsVfs, fcmh_2_fid(i->inoh_fcmh),
	    &rootcreds, &i->inoh_ino, INO_OD_SZ, &nb,
	    SL_INODE_START_OFF, inoh_2_mdsio_data(i));

	if (rc) {
		DEBUG_INOH(PLL_ERROR, i, "zfsslash2_write() error (rc=%d)",
			   rc);
	} else if (nb != INO_OD_SZ) {
		DEBUG_INOH(PLL_ERROR, i, "zfsslash2_write() short I/O");
		rc = SLERR_SHORTIO;
	} else {
		DEBUG_INOH(PLL_TRACE, i, "wrote inode (rc=%d) data=%p",
		    rc, inoh_2_mdsio_data(i));
#ifdef SHITTY_PERFORMANCE
		rc = zfsslash2_fsync(zfsVfs, fcmh_2_fid(i->inoh_fcmh), &rootcreds,
			     1, inoh_2_mdsio_data(i));
		if (rc == -1)
			psc_fatal("zfsslash2_fsync() failed");
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
	rc = zfsslash2_read(zfsVfs, fcmh_2_fid(i->inoh_fcmh),
	    &rootcreds, i->inoh_extras, INOX_OD_SZ, &nb,
	    SL_EXTRAS_START_OFF, inoh_2_mdsio_data(i));
	if (rc)
		DEBUG_INOH(PLL_ERROR, i, "zfsslash2_read() error (rc=%d)", rc);
	else if (nb != INOX_OD_SZ) {
		DEBUG_INOH(PLL_ERROR, i, "zfsslash2_read() short I/O");
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
	rc = zfsslash2_write(zfsVfs, fcmh_2_fid(i->inoh_fcmh),
	    &rootcreds, i->inoh_extras, INOX_OD_SZ, &nb,
	    SL_EXTRAS_START_OFF, inoh_2_mdsio_data(i));

	if (rc) {
		DEBUG_INOH(PLL_ERROR, i, "zfsslash2_write() error (rc=%d)",
			   rc);
	} else if (nb != INOX_OD_SZ) {
		DEBUG_INOH(PLL_ERROR, i, "zfsslash2_write() short I/O");
		rc = SLERR_SHORTIO;
	} else {
#ifdef SHITTY_PERFORMANCE
		rc = zfsslash2_fsync(zfsVfs, fcmh_2_fid(i->inoh_fcmh), &rootcreds,
			     1, inoh_2_mdsio_data(i));
		if (rc == -1)
			psc_fatal("zfsslash2_fsync() failed");
#endif
	}
	return (rc);
}

int
mdsio_frelease(slfid_t fid, struct slash_creds *cr, void *finfo)
{
	return (zfsslash2_release(zfsVfs, fid, cr, finfo));
}

int
mdsio_access(slfid_t fid, int mask, struct slash_creds *cr)
{
	return (zfsslash2_access(zfsVfs, fid, mask, cr));
}

int
mdsio_getattr(slfid_t fid, struct slash_creds *cr, struct stat *stb,
    slfgen_t *gen)
{
	return (zfsslash2_getattr(zfsVfs, fid, cr, stb, gen));
}

int
mdsio_readlink(slfid_t fid, void *buf, struct slash_creds *cr)
{
	return (zfsslash2_readlink(zfsVfs, fid, buf, cr));
}

int
mdsio_statfs(struct statvfs *stbv)
{
	return (zfsslash2_statfs(zfsVfs, stbv, 1));
}

#ifdef BTREE

#else
int
mdsio_opencreate(slfid_t pfid, struct slash_creds *cr, int flags,
    mode_t mode, const char *fn, struct slash_fidgen *fg,
    struct stat *stb, void *finfop)
{
	return (zfsslash2_opencreate(zfsVfs, pfid, cr, flags, mode,
	    fn, fg, stb, finfop));
}

int
mdsio_link(slfid_t fid, slfid_t pfid, const char *fn,
    struct slash_fidgen *fgp, struct slash_creds *cr, struct stat *stb)
{
	return (zfsslash2_link(zfsVfs, fid, pfid, fn, fgp, cr, stb));
}

int
mdsio_lookup(slfid_t pfid, const char *cpn, struct slash_fidgen *fgp,
    struct slash_creds *cr, struct stat *stb)
{
	return (zfsslash2_lookup(zfsVfs, pfid, cpn, fgp, cr, stb));
}

int
mdsio_opendir(slfid_t fid, struct slash_creds *cr,
    struct slash_fidgen *fgp, struct stat *stb, void *finfop)
{
	return (zfsslash2_opendir(zfsVfs, fid, cr, fgp, stb, finfop));
}

int
mdsio_mkdir(slfid_t pfid, const char *cpn, mode_t mode,
    struct slash_creds *cr, struct stat *stb, struct slash_fidgen *fgp,
    int supress_fidlink)
{
	return (zfsslash2_mkdir(zfsVfs, pfid, cpn, mode, cr, stb, fgp,
	    supress_fidlink));
}

int
mdsio_readdir(slfid_t fid, struct slash_creds *cr, size_t siz,
    off_t off, void *buf, size_t *outlen, void *attrs, int nprefetch,
    void *finfo)
{
	return (zfsslash2_readdir(zfsVfs, fid, cr, siz, off, buf,
	    outlen, attrs, nprefetch, finfo));
}

int
mdsio_rename(slfid_t opfid, const char *ocpn, slfid_t npfid,
    const char *ncpn, struct slash_creds *cr)
{
	return (zfsslash2_rename(zfsVfs, opfid, ocpn, npfid, ncpn, cr));
}

int
mdsio_setattr(slfid_t fid, struct stat *in_stb, int to_set,
    struct slash_creds *cr, struct stat *out_stb, void *finfo)
{
	return (zfsslash2_setattr(zfsVfs, fid, in_stb, to_set, cr,
	    out_stb, finfo));
}

int
mdsio_symlink(const char *target, slfid_t pfid, const char *cpn,
    struct slash_creds *cr, struct stat *stb, struct slash_fidgen *fgp)
{
	return (zfsslash2_symlink(zfsVfs, target, pfid, cpn, cr, stb, fgp));
}

int
mdsio_unlink(slfid_t pfid, const char *cpn, struct slash_creds *cr)
{
	return (zfsslash2_unlink(zfsVfs, pfid, cpn, cr));
}

int
mdsio_rmdir(slfid_t pfid, const char *cpn, struct slash_creds *cr)
{
	return (zfsslash2_rmdir(zfsVfs, pfid, cpn, cr));
}
#endif
