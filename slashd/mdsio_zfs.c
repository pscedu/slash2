/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2011, Pittsburgh Supercomputing Center (PSC).
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

/*
 * This file contains routines for accessing the backing store of the MDS
 * file system, where each file in existence here actually contains the
 * SLASH file's metadata.
 */

#include <poll.h>

#include "pfl/fs.h"
#include "pfl/fsmod.h"
#include "psc_util/lock.h"
#include "psc_util/journal.h"

#include "bmap.h"
#include "bmap_mds.h"
#include "fid.h"
#include "fidc_mds.h"
#include "fidcache.h"
#include "inode.h"
#include "mdsio.h"
#include "pathnames.h"
#include "slashd.h"
#include "slerr.h"

#include "sljournal.h"
#include "zfs-fuse/zfs_slashlib.h"

mdsio_fid_t		 mds_metadir_inum;
mdsio_fid_t		 mds_upschdir_inum;
mdsio_fid_t		 mds_fidnsdir_inum;

static __inline void *
bmap_2_zfs_fh(struct bmapc_memb *bmap)
{
	struct fcmh_mds_info *fmi;

	fmi = fcmh_2_fmi(bmap->bcm_fcmh);
	psc_assert(fmi->fmi_mdsio_data);
	return (fmi->fmi_mdsio_data);
}

int
mdsio_fcmh_setattr(struct fidc_membh *f, int setattrflags)
{
	return (mdsio_setattr(fcmh_2_mdsio_fid(f), &f->fcmh_sstb,
	    setattrflags, &rootcreds, NULL,
	    fcmh_2_fmi(f)->fmi_mdsio_data, NULL)); /* XXX mds_namespace_log */
}

int
mdsio_fcmh_refreshattr(struct fidc_membh *f, struct srt_stat *out_sstb)
{
	int locked, rc;

	locked = FCMH_RLOCK(f);
	rc = mdsio_getattr(fcmh_2_mdsio_fid(f), fcmh_2_mdsio_data(f),
	    &rootcreds, &f->fcmh_sstb);

	if (rc)
		abort();

	if (out_sstb)
		*out_sstb = f->fcmh_sstb;
	FCMH_URLOCK(f, locked);

	return (rc);
}

int
mdsio_bmap_read(struct bmapc_memb *bmap)
{
	size_t nb;
	int rc;

	rc = mdsio_read(&rootcreds, bmap_2_ondisk(bmap), BMAP_OD_SZ,
	    &nb, (off_t)BMAP_OD_SZ * bmap->bcm_bmapno +
	    SL_BMAP_START_OFF, bmap_2_zfs_fh(bmap));
	if (rc == 0 && nb != BMAP_OD_SZ)
		rc = SLERR_SHORTIO;

	DEBUG_BMAP(PLL_INFO, bmap, "read bmap (rc=%d)", rc);
	return (rc);
}

int
mdsio_bmap_write(struct bmapc_memb *bmap, int update_mtime, void *logf,
    void *logarg)
{
	size_t nb;
	int rc;

	BMAPOD_REQRDLOCK(bmap_2_bmdsi(bmap));
	mds_bmap_ensure_valid(bmap);

	if (logf)
		mds_reserve_slot();
	rc = mdsio_write(&rootcreds, bmap_2_ondisk(bmap), BMAP_OD_SZ,
	    &nb, (off_t)((BMAP_OD_SZ * bmap->bcm_bmapno) +
	    SL_BMAP_START_OFF), update_mtime, bmap_2_zfs_fh(bmap), logf,
	    logarg);
	if (logf)
		mds_unreserve_slot();

	if (rc) {
		DEBUG_BMAP(PLL_ERROR, bmap,
		    "mdsio_write: error (rc=%d)", rc);
	} else if (nb != BMAP_OD_SZ) {
		DEBUG_BMAP(PLL_ERROR, bmap, "mdsio_write: short I/O");
		rc = SLERR_SHORTIO;
	}
	DEBUG_BMAP(PLL_INFO, bmap, "wrote bmap: fid="SLPRI_FID" bmapno=%u rc=%d",
	    fcmh_2_fid(bmap->bcm_fcmh), bmap->bcm_bmapno, rc);
	BMAPOD_READ_DONE(bmap, 0);
	return (rc);
}

int
mdsio_inode_read(struct slash_inode_handle *ih)
{
	size_t nb;
	int rc;

	INOH_LOCK_ENSURE(ih);
	rc = mdsio_read(&rootcreds, &ih->inoh_ino, INO_OD_SZ, &nb,
	    SL_INODE_START_OFF, inoh_2_mdsio_data(ih));

	if (rc) {
		DEBUG_INOH(PLL_ERROR, ih, "inode read error %d", rc);
	} else if (nb != INO_OD_SZ) {
		DEBUG_INOH(PLL_INFO, ih, "short read I/O (%zd vs %zd)",
		    nb, INO_OD_SZ);
		rc = SLERR_SHORTIO;
	} else {
		DEBUG_INOH(PLL_INFO, ih, "read inode data=%p",
		    inoh_2_mdsio_data(ih));
//		rc = mdsio_getattr(inoh_2_fid(ih), &inoh_2_fsz(ih),
//		inoh_2_mdsio_data(ih));
	}
	return (rc);
}

int
mdsio_inode_write(struct slash_inode_handle *ih, void *logf, void *arg)
{
	size_t nb;
	int rc;

	rc = mdsio_write(&rootcreds, &ih->inoh_ino, INO_OD_SZ, &nb,
	    SL_INODE_START_OFF, 0, inoh_2_mdsio_data(ih), logf, arg);

	if (rc) {
		DEBUG_INOH(PLL_ERROR, ih,
		    "mdsio_write: error (rc=%d)", rc);
	} else if (nb != INO_OD_SZ) {
		DEBUG_INOH(PLL_ERROR, ih, "mdsio_write: short I/O");
		rc = SLERR_SHORTIO;
	} else {
		DEBUG_INOH(PLL_INFO, ih, "wrote inode (rc=%d) data=%p",
		    rc, inoh_2_mdsio_data(ih));
#ifdef SHITTY_PERFORMANCE
		rc = mdsio_fsync(&rootcreds, 1, inoh_2_mdsio_data(ih));
		if (rc == -1)
			psc_fatal("mdsio_fsync");
#endif
	}
	return (rc);
}

int
mdsio_inode_extras_read(struct slash_inode_handle *ih)
{
	size_t nb;
	int rc;

	psc_assert(ih->inoh_extras);
	rc = mdsio_read(&rootcreds, ih->inoh_extras, INOX_OD_SZ, &nb,
	    SL_EXTRAS_START_OFF, inoh_2_mdsio_data(ih));
	if (rc)
		DEBUG_INOH(PLL_ERROR, ih, "mdsio_read: error (rc=%d)",
		    rc);
	else if (nb != INOX_OD_SZ) {
		DEBUG_INOH(PLL_ERROR, ih, "mdsio_read: short I/O");
		rc = SLERR_SHORTIO;
	}
	return (rc);
}

int
mdsio_inode_extras_write(struct slash_inode_handle *ih, void *logf,
    void *arg)
{
	size_t nb;
	int rc;

	psc_assert(ih->inoh_extras);
	rc = mdsio_write(&rootcreds, ih->inoh_extras, INOX_OD_SZ,
	    &nb, SL_EXTRAS_START_OFF, 0, inoh_2_mdsio_data(ih), logf,
	    arg);

	if (rc) {
		DEBUG_INOH(PLL_ERROR, ih, "mdsio_write: error (rc=%d)",
		    rc);
	} else if (nb != INOX_OD_SZ) {
		DEBUG_INOH(PLL_ERROR, ih, "mdsio_write: short I/O");
		rc = SLERR_SHORTIO;
	} else {
#ifdef SHITTY_PERFORMANCE
		rc = mdsio_fsync(&rootcreds, 1, inoh_2_mdsio_data(ih));
		if (rc == -1)
			psc_fatal("mdsio_fsync");
#endif
	}
	return (rc);
}

void
slmzfskstatmthr_main(__unusedx struct psc_thread *thr)
{
	pscfs_main();
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
		psc_warn("system(%s)", buf);

	if (pipe(newfs_fd) == -1)
		psc_fatal("pipe");

	pscfs_fds[0].fd = newfs_fd[0];
	pscfs_fds[0].events = POLLIN;
	pscfs_nfds = 1;

	fuse_session = fuse_lowlevel_new(&args.pfa_av, &pscfs_fuse_ops,
	    sizeof(pscfs_fuse_ops), NULL);

	pscthr_init(SLMTHRT_ZFS_KSTAT, 0, slmzfskstatmthr_main, NULL, 0,
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

	zfsslash2_access,
	zfsslash2_getattr,
	zfsslash2_link,
	zfsslash2_lookup,
	zfsslash2_lookup_slfid,
	zfsslash2_mkdir,
	zfsslash2_mknod,
	zfsslash2_opencreate,
	zfsslash2_opendir,
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
	zfsslash2_fsync,

	zfsslash2_replay_create,
	zfsslash2_replay_link,
	zfsslash2_replay_mkdir,
	zfsslash2_replay_rename,
	zfsslash2_replay_rmdir,
	zfsslash2_replay_setattr,
	zfsslash2_replay_symlink,
	zfsslash2_replay_unlink,
};

int
mdsio_write_cursor(void *buf, size_t size, void *finfo,
    sl_log_write_t funcp)
{
	return (zfsslash2_write_cursor(buf, size, finfo, funcp));
}
