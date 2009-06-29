/* $Id$ */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "fidcache.h"
#include "fidc_mds.h"
#include "bmap.h"
#include "slashdthr.h"

#include "zfs-fuse/zfs_slashlib.h"

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
mdsio_zfs_bmap_read(struct bmapc_memb *bmap)
{
	struct slash_creds cred = { 0, 0 };
	struct bmap_mds_info *bmdsi;
	int rc;

	bmdsi = bmap->bcm_pri;

#if 0
	rc = pread(bmap->bcm_fcmh->fcmh_fd, bmdsi->bmdsi_od,
	    BMAP_OD_SZ, (off_t)(BMAP_OD_SZ * bmap->bcm_blkno));
#else
	rc = zfsslash2_read(zfsVfs, fcmh_2_fid(bmap->bcm_fcmh), &cred,
	    (void *)bmap_2_bmdsiod(bmap), sizeof(*bmdsi->bmdsi_od),
	    (off_t)(BMAP_OD_SZ * bmap->bcm_blkno), bmap_2_zfs_fh(bmap));
#endif
	return (rc);
}

int
mdsio_zfs_bmap_write(struct bmapc_memb *bmap)
{
	struct slash_creds cred = { 0, 0 };
	struct bmap_mds_info *bmdsi;
	int rc;

	bmdsi = bmap->bcm_pri;
#if 0
	rc = pwrite(bmap->bcm_fcmh->fcmh_fd, bmdsi->bmdsi_od,
	    BMAP_OD_SZ, (off_t)(BMAP_OD_SZ * bmap->bcm_blkno));
	if (rc == BMAP_OD_SZ) {
		rc = fsync(bmap->bcm_fcmh->fcmh_fd);
		if (rc == -1)
			psc_fatal("fsync() failed");
	}
#else
	rc = zfsslash2_write(zfsVfs, fcmh_2_fid(bmap->bcm_fcmh), &cred,
	    (void *)bmap_2_bmdsiod(bmap), sizeof(*bmdsi->bmdsi_od),
	    (off_t)(BMAP_OD_SZ * bmap->bcm_blkno), bmap_2_zfs_fh(bmap));
	if (rc == BMAP_OD_SZ) {
		rc = zfsslash2_fsync(zfsVfs, fcmh_2_fid(bmap->bcm_fcmh),
		    &cred, 1, bmap_2_zfs_fh(bmap));
		if (rc == -1)
			psc_fatal("zfsslash2_fsync() failed");
	}
#endif
	return (rc);
}
