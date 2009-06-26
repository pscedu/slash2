/* $Id$ */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/fsuid.h>
#include <sys/vfs.h>

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <unistd.h>

#include "psc_util/lock.h"

#include "fidcache.h"
#include "inode.h"
#include "bmap.h"
#include "mds.h"
#include "fidc_mds.h"
#include "mdsexpc.h"

#include "zfs-fuse/zfs_slashlib.h"

extern void *zfsVfs;

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

#define mdsio_zfs_bmap_read(b) mdsio_zfs_bmap_io(b, 1)
#define mdsio_zfs_bmap_write(b) mdsio_zfs_bmap_io(b, 0)

int
mdsio_zfs_bmap_io(struct bmapc_memb *bmap, int rw)
{
	int rc;
	struct slash_creds cred = {0,0};

	if (rw)
		rc = zfsslash2_read(zfsVfs, fcmh_2_fid(bmap->bcm_fcmh), &cred,
			    (void *)bmap_2_bmdsiod(bmap), 
			    sizeof(struct slash_bmap_od),
			    (off_t)(BMAP_OD_SZ * bmap->bcm_blkno),
			    bmap_2_zfs_fh(bmap));

	else
		rc = zfsslash2_write(zfsVfs, fcmh_2_fid(bmap->bcm_fcmh), &cred,
			     (void *)bmap_2_bmdsiod(bmap), 
			     sizeof(struct slash_bmap_od),
			     (off_t)(BMAP_OD_SZ * bmap->bcm_blkno),
			     bmap_2_zfs_fh(bmap));	
	return (rc);
}
