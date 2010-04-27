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

/* journal log callback operations */
#define	MDS_NAMESPACE_OP_CREATE		1
#define	MDS_NAMESPACE_OP_REMOVE		2
#define	MDS_NAMESPACE_OP_ATTRIB		3

#define	MDS_NAMESPACE_TYPE_DIR		1
#define	MDS_NAMESPACE_TYPE_FILE		2
#define	MDS_NAMESPACE_TYPE_LINK		3
#define	MDS_NAMESPACE_TYPE_SYMLINK	4

/* predefined mdsio layer "fids" */
#define MDSIO_FID_ROOT	3

struct statvfs;

struct bmapc_memb;
struct fidc_membh;
struct slash_creds;
struct slash_fidgen;
struct slash_inode_handle;
struct srt_stat;

typedef uint64_t mdsio_fid_t;
typedef slfid_t (*sl_getslfid_cb)(void);
typedef void (*sl_jlog_cb)(int, int, int, uint64_t, uint64_t, const char *);

int mdsio_apply_fcmh_size(struct fidc_membh *, size_t);
int mdsio_bmap_read(struct bmapc_memb *);
int mdsio_bmap_write(struct bmapc_memb *);
int mdsio_inode_extras_read(struct slash_inode_handle *);
int mdsio_inode_extras_write(struct slash_inode_handle *);
int mdsio_inode_read(struct slash_inode_handle *);
int mdsio_inode_write(struct slash_inode_handle *);

void mdsio_init(void);
void mdsio_exit(void);

int mdsio_access(mdsio_fid_t, int, const struct slash_creds *);
int mdsio_release(const struct slash_creds *, void *);
int mdsio_getattr(mdsio_fid_t, const struct slash_creds *, struct srt_stat *);
int mdsio_readlink(mdsio_fid_t, void *, const struct slash_creds *);
int mdsio_statfs(struct statvfs *);

int mdsio_link(mdsio_fid_t, mdsio_fid_t, const char *, struct slash_fidgen *, const struct slash_creds *, struct srt_stat *);
int mdsio_lookup(mdsio_fid_t, const char *, struct slash_fidgen *, mdsio_fid_t *, const struct slash_creds *, struct srt_stat *);
int mdsio_lookup_slfid(slfid_t, const struct slash_creds *, struct srt_stat *, mdsio_fid_t *);
int mdsio_mkdir(mdsio_fid_t, const char *, mode_t, const struct slash_creds *, struct srt_stat *, struct slash_fidgen *, mdsio_fid_t *, sl_getslfid_cb);
int mdsio_opencreate(mdsio_fid_t, const struct slash_creds *, int, mode_t, const char *, struct slash_fidgen *, mdsio_fid_t *, struct srt_stat *, void *, sl_jlog_cb, sl_getslfid_cb);
int mdsio_opendir(mdsio_fid_t, const struct slash_creds *, struct slash_fidgen *, struct srt_stat *, void *);
int mdsio_readdir(const struct slash_creds *, size_t, off_t, void *, size_t *, void *, int, void *);
int mdsio_rename(mdsio_fid_t, const char *, mdsio_fid_t, const char *, const struct slash_creds *);
int mdsio_rmdir(mdsio_fid_t, const char *, const struct slash_creds *);
int mdsio_setattr(mdsio_fid_t, struct srt_stat *, int, const struct slash_creds *, struct srt_stat *, void *);
int mdsio_symlink(const char *, mdsio_fid_t, const char *, const struct slash_creds *, struct srt_stat *, struct slash_fidgen *, mdsio_fid_t *, sl_getslfid_cb);
int mdsio_unlink(mdsio_fid_t, const char *, const struct slash_creds *);

int mdsio_replay_create(uint64_t, uint64_t, int, int, char *);
int mdsio_replay_remove(uint64_t, uint64_t, char *);
int mdsio_replay_attrib(void);

#endif
