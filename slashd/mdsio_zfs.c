/* $Id$ */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "psc_util/lock.h"

#include "bmap.h"
#include "fidc_mds.h"
#include "fidcache.h"
#include "inode.h"
#include "inodeh.h"
#include "slashdthr.h"

#include "zfs-fuse/zfs_slashlib.h"

void *zfsVfs;

static inline void *
inoh_2_zfs_fh(const struct slash_inode_handle *i)
{
	psc_assert(i->inoh_fcmh);
	psc_assert(i->inoh_fcmh->fcmh_fcoo->fcoo_pri);
	/* Note:  don't use the fidc_fcmh2fmdsi() call here because we're
	 *  most likely being invoked by the open procedure.
	 */
	return (fcmh_2_data(i->inoh_fcmh));
}

static inline void *
bmap_2_zfs_fh(struct bmapc_memb *bmap)
{
	struct fidc_mds_info *fmdsi;

	psc_assert(bmap->bcm_fcmh);

	fmdsi = fidc_fcmh2fmdsi(bmap->bcm_fcmh);

	psc_assert(fmdsi);
	psc_assert(fmdsi->fmdsi_data);

	return (fmdsi->fmdsi_data);
}

int
mdsio_zfs_release(struct slash_inode_handle *i)
{
	struct fidc_mds_info *fmdsi;

	psc_assert(i->inoh_fcmh);
	psc_assert(i->inoh_fcmh->fcmh_fcoo);
	psc_assert(i->inoh_fcmh->fcmh_state & FCMH_FCOO_CLOSING);

	fmdsi = fcmh_2_fmdsi(i->inoh_fcmh);
	psc_assert(!atomic_read(&fmdsi->fmdsi_ref));

	return (zfsslash2_release(zfsVfs, fcmh_2_fid(i->inoh_fcmh), &rootcreds,
				  fmdsi->fmdsi_data));
}

int
mds_fcmh_apply_fsize(struct fidc_membh *f, uint64_t size)
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
mdsio_zfs_bmap_read(struct bmapc_memb *bmap)
{
	struct bmap_mds_info *bmdsi;
	int rc;

	bmdsi = bmap->bcm_pri;

	rc = zfsslash2_read(zfsVfs, fcmh_2_fid(bmap->bcm_fcmh), &rootcreds,
	    bmdsi->bmdsi_od, BMAP_OD_SZ,
	    (off_t)((BMAP_OD_SZ * bmap->bcm_blkno) + SL_BMAP_START_OFF),
	    bmap_2_zfs_fh(bmap));

	DEBUG_BMAP(PLL_TRACE, bmap, "read bmap (rc=%d)",rc);
	return (rc);
}

int
mdsio_zfs_bmap_write(struct bmapc_memb *bmap)
{
	struct bmap_mds_info *bmdsi;
	int rc;

	bmdsi = bmap->bcm_pri;
	rc = zfsslash2_write(zfsVfs, fcmh_2_fid(bmap->bcm_fcmh), &rootcreds,
	    bmdsi->bmdsi_od, BMAP_OD_SZ,
	    (off_t)((BMAP_OD_SZ * bmap->bcm_blkno) + SL_BMAP_START_OFF),
	     bmap_2_zfs_fh(bmap));

	if (rc) {
		DEBUG_BMAP(PLL_ERROR, bmap, "zfsslash2_write() error (rc=%d)",
			   rc);
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
mdsio_zfs_inode_read(struct slash_inode_handle *i)
{
	int rc;

	INOH_LOCK_ENSURE(i);
	rc = zfsslash2_read(zfsVfs, fcmh_2_fid(i->inoh_fcmh), &rootcreds,
		    &i->inoh_ino, (size_t)INO_OD_SZ,
		    (off_t)SL_INODE_START_OFF, inoh_2_zfs_fh(i));
	DEBUG_INOH(PLL_TRACE, i, "read inode (rc=%d) data=%p",
		   rc, inoh_2_zfs_fh(i));

	if (rc < 0)
		rc = -errno;

	return (rc);
}

int
mdsio_zfs_inode_write(struct slash_inode_handle *i)
{
	int rc;

	rc = zfsslash2_write(zfsVfs, fcmh_2_fid(i->inoh_fcmh), &rootcreds,
		     &i->inoh_ino, (size_t)INO_OD_SZ,
		     (off_t)SL_INODE_START_OFF, inoh_2_zfs_fh(i));

	if (rc) {
		DEBUG_INOH(PLL_ERROR, i, "zfsslash2_write() error (rc=%d)",
			   rc);
	} else {
		rc = zfsslash2_fsync(zfsVfs, fcmh_2_fid(i->inoh_fcmh), &rootcreds,
			     1, inoh_2_zfs_fh(i));
		if (rc == -1)
			psc_fatal("zfsslash2_fsync() failed");
	}

	DEBUG_INOH(PLL_TRACE, i, "wrote inode (rc=%d) data=%p",
		   rc, inoh_2_zfs_fh(i));

	return (rc);
}

int
mdsio_zfs_inode_extras_read(struct slash_inode_handle *i)
{
	int rc;

	psc_assert(i->inoh_flags & INOH_LOAD_EXTRAS);
	psc_assert(i->inoh_extras);
	rc = zfsslash2_read(zfsVfs, fcmh_2_fid(i->inoh_fcmh), &rootcreds,
		    i->inoh_extras, (size_t)INOX_OD_SZ,
		    (off_t)SL_EXTRAS_START_OFF, inoh_2_zfs_fh(i));
	if (rc)
		DEBUG_INOH(PLL_ERROR, i, "zfsslash2_write() error (rc=%d)",
			   rc);
	return (rc);
}

int
mdsio_zfs_inode_extras_write(struct slash_inode_handle *i)
{
	int rc;

	psc_assert(i->inoh_extras);
	rc = zfsslash2_write(zfsVfs, fcmh_2_fid(i->inoh_fcmh), &rootcreds,
		     i->inoh_extras, (size_t)INOX_OD_SZ,
		     (off_t)SL_EXTRAS_START_OFF, inoh_2_zfs_fh(i));

	if (rc) {
		DEBUG_INOH(PLL_ERROR, i, "zfsslash2_write() error (rc=%d)",
			   rc);
	} else {
		rc = zfsslash2_fsync(zfsVfs, fcmh_2_fid(i->inoh_fcmh), &rootcreds,
			     1, inoh_2_zfs_fh(i));
		if (rc == -1)
			psc_fatal("zfsslash2_fsync() failed");
	}
	return (rc);
}
