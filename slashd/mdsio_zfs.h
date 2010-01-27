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

#include <sys/types.h>

#include "sltypes.h"

struct bmapc_memb;
struct slash_creds;
struct slash_fidgen;
struct slash_inode_handle;
struct stat;

int mdsio_bmap_read(struct bmapc_memb *);
int mdsio_bmap_write(struct bmapc_memb *);
int mdsio_inode_extras_read(struct slash_inode_handle *);
int mdsio_inode_extras_write(struct slash_inode_handle *);
int mdsio_inode_read(struct slash_inode_handle *);
int mdsio_inode_write(struct slash_inode_handle *);
int mdsio_release(struct slash_inode_handle *);
int mdsio_apply_fcmh_size(struct fidc_membh *, off64_t);

int mdsio_frelease(slfid_t, struct slash_creds *, void *);
int mdsio_opencreate(slfid_t, struct slash_creds *, int, mode_t,
	const char *, struct slash_fidgen *, struct stat *, void *);
int mdsio_link(slfid_t, slfid_t, const char *, struct slash_fidgen *,
	struct slash_creds *, struct stat *);
int mdsio_unlink(slfid_t, const char *, struct slash_creds *);
int mdsio_lookup(slfid_t, const char *, struct slash_fidgen *,
	struct slash_creds *, struct stat *);
int mdsio_opendir(slfid_t, struct slash_creds *, struct slash_fidgen *,
	struct stat *, void *);
int mdsio_mkdir(slfid_t, const char *, mode_t, struct slash_creds *,
	struct stat *, struct slash_fidgen *, int);
int mdsio_readdir(slfid_t, struct slash_creds *, size_t, off_t, void *,
	size_t *, void *, int, void *);

#endif
