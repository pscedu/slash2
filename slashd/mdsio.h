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

#ifndef _SLASHD_MDSIO_H_
#define _SLASHD_MDSIO_H_

#include <sys/types.h>

#include "fid.h"
#include "sltypes.h"

struct statvfs;

struct bmapc_memb;
struct fidc_membh;
struct slash_creds;
struct slash_fidgen;
struct slash_inode_handle;
struct srt_stat;

int mdsio_bmap_read(struct bmapc_memb *);
int mdsio_bmap_write(struct bmapc_memb *);
int mdsio_inode_extras_read(struct slash_inode_handle *);
int mdsio_inode_extras_write(struct slash_inode_handle *);
int mdsio_inode_read(struct slash_inode_handle *);
int mdsio_inode_write(struct slash_inode_handle *);
int mdsio_release(struct slash_inode_handle *);
int mdsio_apply_fcmh_size(struct fidc_membh *, size_t);

void mdsio_init(void);
void mdsio_exit(void);

int mdsio_access(slfid_t, int, const struct slash_creds *);
int mdsio_frelease(slfid_t, const struct slash_creds *, void *);
int mdsio_getattr(slfid_t, const struct slash_creds *, struct srt_stat *, slfgen_t *);
int mdsio_readlink(slfid_t, void *, const struct slash_creds *);
int mdsio_statfs(struct statvfs *);

int mdsio_opencreate(slfid_t, const struct slash_creds *, int, mode_t,
	const char *, struct slash_fidgen *, struct srt_stat *, void *);
int mdsio_link(slfid_t, slfid_t, const char *, struct slash_fidgen *,
	const struct slash_creds *, struct srt_stat *);
int mdsio_unlink(slfid_t, const char *, const struct slash_creds *);
int mdsio_lookup(slfid_t, const char *, struct slash_fidgen *,
	const struct slash_creds *, struct srt_stat *);
int mdsio_opendir(slfid_t, const struct slash_creds *, struct slash_fidgen *,
	struct srt_stat *, void *);
int mdsio_mkdir(slfid_t, const char *, mode_t, const struct slash_creds *,
	struct srt_stat *, struct slash_fidgen *, int);
int mdsio_readdir(slfid_t, const struct slash_creds *, size_t, off_t, void *,
	size_t *, void *, int, void *);
int mdsio_rename(slfid_t, const char *, slfid_t, const char *,
	const struct slash_creds *);
int mdsio_setattr(slfid_t, struct srt_stat *, int, const struct slash_creds *,
	struct srt_stat *, void *);
int mdsio_symlink(const char *, slfid_t, const char *,
	const struct slash_creds *, struct srt_stat *, struct slash_fidgen *);
int mdsio_rmdir(slfid_t, const char *, const struct slash_creds *);

#endif
