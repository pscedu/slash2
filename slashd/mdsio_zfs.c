/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015, Google, Inc.
 * Copyright (c) 2008-2015, Pittsburgh Supercomputing Center (PSC).
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or (at
 * your option) any later version.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License contained in the file
 * `COPYING-GPL' at the top of this distribution or at
 * https://www.gnu.org/licenses/gpl-2.0.html for more details.
 * ---------------------------------------------------------------------
 * %END_LICENSE%
 */

/*
 * This file contains routines for accessing the backing store of the MDS
 * file system, where each file in existence here actually contains the
 * SLASH2 file's metadata.
 */

#include <poll.h>

#include "pfl/fs.h"
#include "pfl/fsmod.h"
#include "pfl/journal.h"
#include "pfl/lock.h"

#include "bmap.h"
#include "bmap_mds.h"
#include "fid.h"
#include "fidc_mds.h"
#include "fidcache.h"
#include "inode.h"
#include "journal_mds.h"
#include "mdsio.h"
#include "pathnames.h"
#include "slashd.h"
#include "slerr.h"

#include "zfs-fuse/zfs_slashlib.h"

mdsio_fid_t		 mds_metadir_inum[MAX_FILESYSTEMS];
mdsio_fid_t		 mds_fidnsdir_inum[MAX_FILESYSTEMS];
mdsio_fid_t		 mds_tmpdir_inum[MAX_FILESYSTEMS];

int
mdsio_fcmh_refreshattr(struct fidc_membh *f, struct srt_stat *out_sstb)
{
	int locked, rc, vfsid;
	pthread_t pthr;

	pthr = pthread_self();
	locked = FCMH_RLOCK(f);
	fcmh_wait_locked(f, (f->fcmh_flags & FCMH_BUSY) &&
	    f->fcmh_owner != pthr);
	rc = slfid_to_vfsid(fcmh_2_fid(f), &vfsid);
	psc_assert(rc == 0);
	rc = mdsio_getattr(vfsid, fcmh_2_mfid(f),
	    fcmh_2_mfh(f), &rootcreds, &f->fcmh_sstb);
	psc_assert(rc == 0);
	if (out_sstb)
		*out_sstb = f->fcmh_sstb;
	FCMH_URLOCK(f, locked);
	return (rc);
}

void
slmzfskstatmthr_main(__unusedx struct psc_thread *thr)
{
	pscfs_main(PFL_THRT_FS, "slmzk");
}

#define _PATH_KSTAT "/zfs-kstat"

void
slm_unmount_kstat(void)
{
	char buf[BUFSIZ];
	int rc;

	rc = snprintf(buf, sizeof(buf), "umount %s", _PATH_KSTAT);
	if (rc == -1)
		psc_fatal("snprintf: umount %s", _PATH_KSTAT);
	if (rc >= (int)sizeof(buf))
		psc_fatalx("snprintf: umount %s: too long", _PATH_KSTAT);
	if (system(buf) == -1)
		psclog_warn("system(%s)", buf);
}

int
zfsslash2_init(void)
{
	struct pscfs_args args = PSCFS_ARGS_INIT(0, NULL);
	extern struct fuse_lowlevel_ops pscfs_fuse_ops;
	extern struct fuse_session *fuse_session;
	extern struct pollfd pscfs_fds[];
	extern int newfs_fd[2], pscfs_nfds;
	extern char *fuse_mount_options;
	char buf[BUFSIZ];
	int rc;

	rc = snprintf(buf, sizeof(buf), "umount %s", _PATH_KSTAT);
	if (rc == -1)
		psc_fatal("snprintf: umount %s", _PATH_KSTAT);
	if (rc >= (int)sizeof(buf))
		psc_fatalx("snprintf: umount %s: too long", _PATH_KSTAT);
	if (system(buf) == -1)
		psclog_warn("system(%s)", buf);

	if (pipe(newfs_fd) == -1)
		psc_fatal("pipe");

	pscfs_fds[0].fd = newfs_fd[0];
	pscfs_fds[0].events = POLLIN;
	pscfs_nfds = 1;

	fuse_session = fuse_lowlevel_new(&args.pfa_av, &pscfs_fuse_ops,
	    sizeof(pscfs_fuse_ops), NULL);

	pscthr_init(SLMTHRT_ZFS_KSTAT, slmzfskstatmthr_main, NULL, 0,
	    "slmzfskstatmthr");

	fuse_mount_options = "";
	rc = libzfs_init_fusesocket();
	if (rc == 0)
		rc = libzfs_init();
	atexit(slm_unmount_kstat);
	return (rc);
}

struct mdsio_ops mdsio_ops = {
	zfsslash2_init,
	libzfs_exit,

	zfsslash2_setattrmask_2_slflags,
	zfsslash2_slflags_2_setattrmask,
	zfsslash2_getfidlinkdir,
	zfsslash2_write_cursor,

	zfsslash2_access,
	zfsslash2_fsync,
	zfsslash2_getattr,
	zfsslash2_link,
	zfsslash2_lookup,
	zfsslash2_lookup_slfid,
	zfsslash2_mkdir,
	zfsslash2_mknod,
	zfsslash2_opencreate,
	zfsslash2_opendir,
	zfsslash2_preadv,
	zfsslash2_pwritev,
	zfsslash2_read,
	zfsslash2_readdir,
	zfsslash2_readlink,
	zfsslash2_release,
	zfsslash2_rename,
	zfsslash2_rmdir,
	zfsslash2_setattr,
	zfsslash2_statfs,
	zfsslash2_symlink,
	zfsslash2_unlink,
	zfsslash2_write,

	zfsslash2_hasxattrs,
	zfsslash2_listxattr,
	zfsslash2_setxattr,
	zfsslash2_getxattr,
	zfsslash2_removexattr,

	zfsslash2_replay_create,
	zfsslash2_replay_link,
	zfsslash2_replay_mkdir,
	zfsslash2_replay_rename,
	zfsslash2_replay_rmdir,
	zfsslash2_replay_setattr,
	zfsslash2_replay_symlink,
	zfsslash2_replay_unlink,

	zfsslash2_replay_setxattr,
	zfsslash2_replay_removexattr
};
