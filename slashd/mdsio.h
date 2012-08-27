/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2012, Pittsburgh Supercomputing Center (PSC).
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

#include <stdint.h>

#include "pfl/hashtbl.h"

#include "fid.h"
#include "sltypes.h"

struct statvfs;
struct iovec;

struct bmapc_memb;
struct fidc_membh;
struct slash_creds;
struct slash_inode_handle;
struct srt_stat;

typedef uint64_t mdsio_fid_t;

struct mdsio_fh {
	void *fh;
};

/* callback to get a SLASH2 FID */
typedef int (*sl_getslfid_cb_t)(slfid_t *);

/* callback to log writes to bmap */
typedef void (*sl_log_write_t)(void *, uint64_t, int);

/* callback to log updates to namespace */
typedef void (*sl_log_update_t)(int, uint64_t, uint64_t, uint64_t,
    const struct srt_stat *, int, const char *, const char *, void *);

/* predefined mdsio layer "fids" */
#define MDSIO_FID_ROOT		3		/* XXX FUSE_ROOT_ID? */

/* opencreatef() flags */
#define MDSIO_OPENCRF_NOLINK	(1 << 0)	/* do not create links in FID namespace */
#define MDSIO_OPENCRF_NOMTIM	(1 << 1)	/* do not update st_mtim */

#define mdsio_opencreate(vfs, pino, crp, fflags, mode, fn, mfp, sstb,	\
	    mdsio_datap, logfunc, getslfid, slfid)			\
	mdsio_opencreatef((vfs), (pino), (crp), (fflags), 0, (mode),	\
	    (fn), (mfp), (sstb), (mdsio_datap), (logfunc), (getslfid), (slfid))

#define MDSIO_FOREACH_DIRENT(dh, credp, buf, bufsiz, ip, d, rc)		\
	for ((ip)->mdi_off = 0; ((rc) = mdsio_readdir((credp),		\
	    (bufsiz), (ip)->mdi_off, (buf), &(ip)->mdi_tsiz, NULL,	\
	    NULL, 0, (dh))) == 0 && (ip)->mdi_tsiz;			\
	    (ip)->mdi_off += (ip)->mdi_tsiz)				\
		for ((ip)->mdi_toff = 0;				\
		     (ip)->mdi_toff < (off64_t)(ip)->mdi_tsiz &&	\
		      ((d) = (void *)((buf) + (ip)->mdi_toff)) &&	\
		      ((ip)->mdi_off = (d)->pfd_off);			\
		     (ip)->mdi_toff += PFL_DIRENT_SIZE((d)->pfd_namelen))

/* high-level interface */
int	mdsio_fcmh_refreshattr(struct fidc_membh *, struct srt_stat *);
int	mdsio_write_cursor(int, void *, size_t, void *, sl_log_write_t);

struct mdsio_ops {
	/* control interface */
	int	(*mio_init)(void);
	void	(*mio_exit)(void);

	/* utility interface */
	int	(*mio_setattrmask_2_slflags)(uint);
	uint	(*mio_slflags_2_setattrmask)(int);

	/* low-level file system interface */
	int	(*mio_access)(int, mdsio_fid_t, int, const struct slash_creds *);
	int	(*mio_fsync)(int, const struct slash_creds *, int, void *);
	int	(*mio_getattr)(int, mdsio_fid_t, void *, const struct slash_creds *, struct srt_stat *);
	int	(*mio_link)(int, mdsio_fid_t, mdsio_fid_t, const char *, const struct slash_creds *, struct srt_stat *, sl_log_update_t);
	int	(*mio_lookup)(int, mdsio_fid_t, const char *, mdsio_fid_t *, const struct slash_creds *, struct srt_stat *);
	int	(*mio_lookup_slfid)(int, slfid_t, const struct slash_creds *, struct srt_stat *, mdsio_fid_t *);
	int	(*mio_mkdir)(int, mdsio_fid_t, const char *, const struct srt_stat *, int, int, struct srt_stat *, mdsio_fid_t *,
			sl_log_update_t, sl_getslfid_cb_t, slfid_t);
	int	(*mio_mknod)(int, mdsio_fid_t, const char *, mode_t, const struct slash_creds *, struct srt_stat *, mdsio_fid_t *,
			sl_log_update_t, sl_getslfid_cb_t);
	int	(*mio_opencreatef)(int, mdsio_fid_t, const struct slash_creds *, int, int, mode_t, const char *, mdsio_fid_t *,
			struct srt_stat *, void *, sl_log_update_t, sl_getslfid_cb_t, slfid_t);
	int	(*mio_opendir)(int, mdsio_fid_t, const struct slash_creds *, struct slash_fidgen *, void *);
	int	(*mio_preadv)(int, const struct slash_creds *, struct iovec *, int, size_t *, off_t, void *);
	int	(*mio_pwritev)(int, const struct slash_creds *, const struct iovec *, int, size_t *, off_t, int, void *,
			sl_log_write_t, void *);
	int	(*mio_read)(int, const struct slash_creds *, void *, size_t, size_t *, off_t, void *);
	int	(*mio_readdir)(int, const struct slash_creds *, size_t, off_t, void *, size_t *, size_t *, void *, int, void *);
	int	(*mio_readlink)(int, mdsio_fid_t, char *, const struct slash_creds *);
	int	(*mio_release)(int, const struct slash_creds *, void *);
	int	(*mio_rename)(int, mdsio_fid_t, const char *, mdsio_fid_t, const char *, const struct slash_creds *,
			sl_log_update_t, void *);
	int	(*mio_rmdir)(int, mdsio_fid_t, slfid_t *, const char *, const struct slash_creds *, sl_log_update_t);
	int	(*mio_setattr)(int, mdsio_fid_t, const struct srt_stat *, int, const struct slash_creds *, struct srt_stat *,
			void *, sl_log_update_t);
	int	(*mio_statfs)(int, struct statvfs *);
	int	(*mio_symlink)(int, const char *, mdsio_fid_t, const char *, const struct slash_creds *, struct srt_stat *,
			mdsio_fid_t *, sl_log_update_t, sl_getslfid_cb_t, slfid_t);
	int	(*mio_unlink)(int, mdsio_fid_t, slfid_t *, const char *, const struct slash_creds *, sl_log_update_t, void *);
	int	(*mio_write)(int, const struct slash_creds *, const void *, size_t, size_t *, off_t, int, void *,
			sl_log_write_t, void *);

	int	(*mio_listxattr)(int, const struct slash_creds *, void *, size_t, size_t *, mdsio_fid_t);
	int	(*mio_setxattr)(int, const struct slash_creds *, const char *, const char *, size_t, mdsio_fid_t);
	int	(*mio_getxattr)(int, const struct slash_creds *, const char *, char *, size_t, size_t *, mdsio_fid_t);
	int	(*mio_removexattr)(int, const struct slash_creds *, const char *, mdsio_fid_t);

	/* replay interface */
	int	(*mio_redo_create)(int, slfid_t, char *, struct srt_stat *);
	int	(*mio_redo_fidlink)(int,slfid_t, const struct slash_creds *);
	int	(*mio_redo_link)(int, slfid_t, slfid_t, char *, struct srt_stat *);
	int	(*mio_redo_mkdir)(int, slfid_t, char *, struct srt_stat *);
	int	(*mio_redo_rename)(int, slfid_t, const char *, slfid_t, const char *, struct srt_stat *);
	int	(*mio_redo_rmdir)(int, slfid_t, slfid_t, char *);
	int	(*mio_redo_setattr)(int, slfid_t, uint, struct srt_stat *);
	int	(*mio_redo_symlink)(int, slfid_t, slfid_t, char *, char *, struct srt_stat *);
	int	(*mio_redo_unlink)(int, slfid_t, slfid_t, char *);

	int	(*mio_redo_setxattr)(int, slfid_t, const char *, const char *, size_t size);
	int	(*mio_redo_removexattr)(int, slfid_t, const char *);
};

#define mdsio_init		mdsio_ops.mio_init			/* zfsslash2_init() */
#define mdsio_exit		mdsio_ops.mio_exit			/* libzfs_exit() */

#define mdsio_setattrmask_2_slflags mdsio_ops.mio_setattrmask_2_slflags	/* zfsslash2_setattrmask_2_slflags() */
#define mdsio_slflags_2_setattrmask mdsio_ops.mio_slflags_2_setattrmask	/* zfsslash2_slflags_2_setattrmask() */

#define mdsio_access		mdsio_ops.mio_access			/* zfsslash2_access() */
#define mdsio_fsync		mdsio_ops.mio_fsync			/* zfsslash2_fsync() */
#define mdsio_getattr		mdsio_ops.mio_getattr			/* zfsslash2_getattr() */
#define mdsio_link		mdsio_ops.mio_link			/* zfsslash2_link() */
#define mdsio_lookup		mdsio_ops.mio_lookup			/* zfsslash2_lookup() */
#define mdsio_lookup_slfid	mdsio_ops.mio_lookup_slfid		/* zfsslash2_lookup_slfid() */
#define mdsio_mkdir		mdsio_ops.mio_mkdir			/* zfsslash2_mkdir() */
#define mdsio_mknod		mdsio_ops.mio_mknod			/* zfsslash2_mknod() */
#define mdsio_opencreatef	mdsio_ops.mio_opencreatef		/* zfsslash2_opencreate() */
#define mdsio_opendir		mdsio_ops.mio_opendir			/* zfsslash2_opendir() */
#define mdsio_preadv		mdsio_ops.mio_preadv			/* zfsslash2_preadv() */
#define mdsio_pwritev		mdsio_ops.mio_pwritev			/* zfsslash2_pwritev() */
#define mdsio_read		mdsio_ops.mio_read			/* zfsslash2_read() */
#define mdsio_readdir		mdsio_ops.mio_readdir			/* zfsslash2_readdir() */
#define mdsio_readlink		mdsio_ops.mio_readlink			/* zfsslash2_readlink() */
#define mdsio_release		mdsio_ops.mio_release			/* zfsslash2_release() */
#define mdsio_rename		mdsio_ops.mio_rename			/* zfsslash2_rename() */
#define mdsio_rmdir		mdsio_ops.mio_rmdir			/* zfsslash2_rmdir() */
#define mdsio_setattr		mdsio_ops.mio_setattr			/* zfsslash2_setattr() */
#define mdsio_statfs		mdsio_ops.mio_statfs			/* zfsslash2_statfs() */
#define mdsio_symlink		mdsio_ops.mio_symlink			/* zfsslash2_symlink() */
#define mdsio_unlink		mdsio_ops.mio_unlink			/* zfsslash2_unlink() */
#define mdsio_write		mdsio_ops.mio_write			/* zfsslash2_write() */
#define mdsio_listxattr		mdsio_ops.mio_listxattr			/* zfsslash2_listxattr() */
#define mdsio_setxattr		mdsio_ops.mio_setxattr			/* zfsslash2_setxattr() */
#define mdsio_getxattr		mdsio_ops.mio_getxattr			/* zfsslash2_getxattr() */
#define mdsio_removexattr	mdsio_ops.mio_removexattr		/* zfsslash2_removexattr() */

#define mdsio_redo_create	mdsio_ops.mio_redo_create		/* zfsslash2_replay_create() */
#define mdsio_redo_fidlink	mdsio_ops.mio_redo_fidlink		/* zfsslash2_replay_fidlink() */
#define mdsio_redo_link		mdsio_ops.mio_redo_link			/* zfsslash2_replay_link() */
#define mdsio_redo_mkdir	mdsio_ops.mio_redo_mkdir		/* zfsslash2_replay_mkdir() */
#define mdsio_redo_rename	mdsio_ops.mio_redo_rename		/* zfsslash2_replay_rename() */
#define mdsio_redo_rmdir	mdsio_ops.mio_redo_rmdir		/* zfsslash2_replay_rmdir() */
#define mdsio_redo_setattr	mdsio_ops.mio_redo_setattr		/* zfsslash2_replay_setattr() */
#define mdsio_redo_symlink	mdsio_ops.mio_redo_symlink		/* zfsslash2_replay_symlink() */
#define mdsio_redo_unlink	mdsio_ops.mio_redo_unlink		/* zfsslash2_replay_unlink() */
#define mdsio_redo_setxattr	mdsio_ops.mio_redo_setxattr		/* zfsslash2_replay_setxattr() */
#define mdsio_redo_removexattr	mdsio_ops.mio_redo_removexattr		/* zfsslash2_replay_removexattr() */

extern struct mdsio_ops	mdsio_ops;
extern mdsio_fid_t	mds_metadir_inum[];
extern mdsio_fid_t	mds_fidnsdir_inum[];
extern mdsio_fid_t	mds_tmpdir_inum[];

struct rootNames {
	char			 rn_name[MAXPATHLEN];
	int			 rn_vfsid;
	struct psc_hashent	 rn_hentry;
};

#endif /* _SLASHD_MDSIO_H_ */
