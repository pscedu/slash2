/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2009, Pittsburgh Supercomputing Center (PSC).
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
