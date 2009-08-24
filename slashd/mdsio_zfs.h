/* $Id$ */

#ifndef _SLASHD_MDSIO_H_
#define _SLASHD_MDSIO_H_

#include "bmap.h"
#include "inodeh.h"

extern void *zfsVfs;

extern int
mdsio_zfs_bmap_read(struct bmapc_memb *bmap);

extern int
mdsio_zfs_bmap_write(struct bmapc_memb *bmap);

extern int
mdsio_zfs_inode_read(struct slash_inode_handle *i);

extern int
mdsio_zfs_inode_write(struct slash_inode_handle *i);

extern int
mdsio_zfs_inode_extras_read(struct slash_inode_handle *i);

extern int
mdsio_zfs_inode_extras_write(struct slash_inode_handle *i);

#endif
