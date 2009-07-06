/* $Id$ */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "psc_util/lock.h"

#include "fidcache.h"
#include "fidc_mds.h"
#include "bmap.h"
#include "slashdthr.h"
#include "inode.h"
#include "inodeh.h"

#include "zfs-fuse/zfs_slashlib.h"

struct slash_inode_od null_inode_od;

static inline void * 
inoh_2_zfs_fh(const struct slash_inode_handle *i)
{
	struct fidc_mds_info *fmdsi;

	psc_assert(i->inoh_fcmh);

	fmdsi = fidc_fcmh2fmdsi(i->inoh_fcmh);
	return (fmdsi->fmdsi_data);
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
	rc = (rc != BMAP_OD_SZ) ? -errno : 0;

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
 
int
mdsio_zfs_inode_read(struct slash_inode_handle *i)
{
	struct slash_creds cred = { 0, 0 };
	int rc, locked;
	psc_crc_t crc;

#if 0
	rc = pread(i->inoh_fcmh->fcmh_fd, &i->inoh_ino,
	    INO_OD_SZ, (off_t)SL_INODE_START_OFF);
#else
	rc = zfsslash2_read(zfsVfs, fcmh_2_fid(i->inoh_fcmh), &cred,
		    (void *)&i->inoh_ino, (size_t)INO_OD_SZ, 
		    (off_t)SL_INODE_START_OFF, inoh_2_zfs_fh(i));
#endif
	rc = (rc != INO_OD_SZ) ? -errno : 0;
	
	locked = reqlock(&i->inoh_lock);	
	psc_assert(i->inoh_flags & INOH_INO_NOTLOADED);

	i->inoh_flags &= ~INOH_INO_NOTLOADED;	

	/* Check for a NULL CRC, which can happen when
	 *  bmaps are gaps that have not been written yet.
	 */
	if ((!i->inoh_ino.ino_crc) && 
	    (!memcmp(&i->inoh_ino, &null_inode_od, sizeof(null_inode_od)))) {
		slash_inode_od_initnew(i);
		return (0);
	}	
	/* Calculate and check the CRC now 
	 */
	PSC_CRC_CALC(crc, &i->inoh_ino, INO_OD_CRCSZ);
	if (crc == i->inoh_ino.ino_crc)
		return (0);

	DEBUG_INOH(PLL_WARN, i, "CRC failed want=%"PRIx64", got=%"PRIx64, 
		   i->inoh_ino.ino_crc, crc);
	return (-EIO);
}


int
mdsio_zfs_inode_write(struct slash_inode_handle *i)
{
	struct slash_creds cred = { 0, 0 };
	int rc;

#if 0
	rc = pwrite(i->inoh_fcmh->fcmh_fd, &i->inoh_ino,
		    INO_OD_SZ, (off_t)SL_INODE_START_OFF);
	if (rc == BMAP_OD_SZ) {
		rc = fsync(bmap->bcm_fcmh->fcmh_fd);
		if (rc == -1)
			psc_fatal("fsync() failed");
	}
#else
	rc = zfsslash2_write(zfsVfs, fcmh_2_fid(i->inoh_fcmh), &cred,
		     (void *)&i->inoh_ino, (size_t)INO_OD_SZ,
		     (off_t)SL_INODE_START_OFF, inoh_2_zfs_fh(i));
	if (rc == INO_OD_SZ) {
		rc = zfsslash2_fsync(zfsVfs, fcmh_2_fid(i->inoh_fcmh),
		    &cred, 1, inoh_2_zfs_fh(i));
		if (rc == -1)
			psc_fatal("zfsslash2_fsync() failed");
	}
#endif
	return (rc);
}

int
mdsio_zfs_inode_extras_read(struct slash_inode_handle *i)
{
	struct slash_creds cred = { 0, 0 };
	int rc;

	psc_assert(i->inoh_flags & INOH_LOAD_EXTRAS);
	psc_assert(i->inoh_extras);
#if 0
	rc = pread(i->inoh_fcmh->fcmh_fd, (void *)i->inoh_extras,
		   (size_t)INOX_OD_SZ, (off_t)SL_EXTRAS_START_OFF);
#else
	rc = zfsslash2_read(zfsVfs, fcmh_2_fid(i->inoh_fcmh), &cred,
		    (void *)i->inoh_extras, (size_t)INOX_OD_SZ, 
		    (off_t)SL_EXTRAS_START_OFF, inoh_2_zfs_fh(i));
#endif	
	rc = (rc != INOX_OD_SZ) ? -errno : 0;

	return (rc);
}


int
mdsio_zfs_inode_extras_write(struct slash_inode_handle *i)
{
	struct slash_creds cred = { 0, 0 };
	int rc;

	psc_assert(i->inoh_extras);
#if 0
	rc = pwrite(i->inoh_fcmh->fcmh_fd, &i->inoh_ino,
		    INO_OD_SZ, (off_t)SL_INODE_START_OFF);
	if (rc == BMAP_OD_SZ) {
		rc = fsync(bmap->bcm_fcmh->fcmh_fd);
		if (rc == -1)
			psc_fatal("fsync() failed");
	}
#else
	rc = zfsslash2_write(zfsVfs, fcmh_2_fid(i->inoh_fcmh), &cred,
		     (void *)i->inoh_extras, (size_t)INOX_OD_SZ,
		     (off_t)SL_EXTRAS_START_OFF, inoh_2_zfs_fh(i));

	if (rc == INOX_OD_SZ) {
		rc = zfsslash2_fsync(zfsVfs, fcmh_2_fid(i->inoh_fcmh),
		    &cred, 1, inoh_2_zfs_fh(i));
		if (rc == -1)
			psc_fatal("zfsslash2_fsync() failed");
	}
#endif
	return (rc);
}
