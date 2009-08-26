/* $Id$ */

#ifndef _SLASHD_MDSIO_H_
#define _SLASHD_MDSIO_H_

struct bmapc_memb;
struct slash_inode_handle;

int mdsio_zfs_bmap_read(struct bmapc_memb *);
int mdsio_zfs_bmap_write(struct bmapc_memb *);
int mdsio_zfs_inode_extras_read(struct slash_inode_handle *);
int mdsio_zfs_inode_extras_write(struct slash_inode_handle *);
int mdsio_zfs_inode_read(struct slash_inode_handle *);
int mdsio_zfs_inode_write(struct slash_inode_handle *);
int mdsio_zfs_release(struct slash_inode_handle *);

#endif
