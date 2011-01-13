/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2010, Pittsburgh Supercomputing Center (PSC).
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

#include "pfl/fs.h"
#include "psc_util/lock.h"
#include "psc_util/journal.h"

#include "bmap.h"
#include "bmap_mds.h"
#include "fid.h"
#include "fidc_mds.h"
#include "fidcache.h"
#include "inode.h"
#include "inodeh.h"
#include "mdsio.h"
#include "slashd.h"
#include "slerr.h"

#include "sljournal.h"
#include "zfs-fuse/zfs_slashlib.h"

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
	return (zfsslash2_setattr(fcmh_2_mdsio_fid(f), &f->fcmh_sstb,
	    setattrflags, &rootcreds, NULL,
	    fcmh_2_fmi(f)->fmi_mdsio_data, NULL));
}

int
mdsio_fcmh_refreshattr(struct fidc_membh *f, struct srt_stat *out_sstb)
{
	int locked, rc=0;

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

	rc = zfsslash2_read(&rootcreds, bmap_2_ondisk(bmap), BMAP_OD_SZ,
	    &nb, (off_t)BMAP_OD_SZ * bmap->bcm_bmapno +
	    SL_BMAP_START_OFF, bmap_2_zfs_fh(bmap));
	if (rc == 0 && nb != BMAP_OD_SZ)
		rc = SLERR_SHORTIO;

	DEBUG_BMAP(PLL_TRACE, bmap, "read bmap (rc=%d)", rc);
	return (rc);
}

/**
 * mdsio_bmap_crc_update - Handle CRC updates for one bmap by pushing the updates
 *     to ZFS and then log it.
 */
int
mds_bmap_crc_update(struct bmapc_memb *bmap, struct srm_bmap_crcup *crcup)
{
	struct bmap_mds_info *bmdsi = bmap_2_bmdsi(bmap);
	struct sl_mds_crc_log crclog;
	uint32_t utimgen, i;
	size_t nb;
	int rc;

	psc_assert(bmap->bcm_flags & BMAP_MDS_CRC_UP);

	FCMH_LOCK(bmap->bcm_fcmh);
	if (fcmh_2_fsz(bmap->bcm_fcmh) != crcup->fsize) {
		DEBUG_FCMH(PLL_INFO, bmap->bcm_fcmh, "new fsize %"PRId64, crcup->fsize);
		fcmh_2_fsz(bmap->bcm_fcmh) = crcup->fsize;
	}
	utimgen = bmap->bcm_fcmh->fcmh_sstb.sst_utimgen;
	FCMH_ULOCK(bmap->bcm_fcmh);

	rc = mdsio_fcmh_setattr(bmap->bcm_fcmh, PSCFS_SETATTRF_DATASIZE);

	if (utimgen < crcup->utimgen)
		DEBUG_FCMH(PLL_ERROR, bmap->bcm_fcmh,
		   "utimgen %d < crcup->utimgen %d", utimgen, crcup->utimgen);

	crclog.scl_bmap = bmap;
	crclog.scl_crcup = crcup;

	BMAPOD_WRLOCK(bmdsi);
	for (i = 0; i < crcup->nups; i++) {
		bmap_2_crcs(bmap, crcup->crcs[i].slot) =
			crcup->crcs[i].crc;

		bmap->bcm_crcstates[crcup->crcs[i].slot] =
			BMAP_SLVR_DATA | BMAP_SLVR_CRC;

		DEBUG_BMAP(PLL_DEBUG, bmap, "slot(%d) crc(%"PRIx64")",
			   crcup->crcs[i].slot, crcup->crcs[i].crc);
	}
	BMAPOD_ULOCK(bmdsi);

	mds_reserve_slot();
	rc = zfsslash2_write(&rootcreds, bmap_2_ondisk(bmap),
	    BMAP_OD_SZ, &nb, (off_t)((BMAP_OD_SZ * bmap->bcm_bmapno) +
	    SL_BMAP_START_OFF), (utimgen == crcup->utimgen),
	    bmap_2_zfs_fh(bmap), mds_bmap_crc_log, &crclog);
	mds_unreserve_slot();

	if (rc) {
		DEBUG_BMAP(PLL_ERROR, bmap, "zfsslash2_write: error (rc=%d)",
			   rc);
	} else if (nb != BMAP_OD_SZ) {
		DEBUG_BMAP(PLL_ERROR, bmap, "zfsslash2_write: short I/O");
		rc = SLERR_SHORTIO;
	}
	DEBUG_BMAP(PLL_TRACE, bmap, "wrote bmap (rc=%d)",rc);

	return (rc);
}

int
mds_bmap_repl_update(struct bmapc_memb *bmap)
{
	int rc, logchg = 0;
	size_t nb;

	BMAPOD_RDREQLOCK(bmap_2_bmdsi(bmap));
	BMDSI_LOGCHG_CHECK(bmap, logchg);
	if (!logchg) {
		BMAPOD_READ_DONE(bmap);
		return (0);
	}

	mds_reserve_slot();
	rc = zfsslash2_write(&rootcreds, bmap_2_ondisk(bmap),
	    BMAP_OD_SZ, &nb, (off_t)((BMAP_OD_SZ * bmap->bcm_bmapno) +
	    SL_BMAP_START_OFF), 0, bmap_2_zfs_fh(bmap),
	    mds_bmap_repl_log, bmap);
	mds_unreserve_slot();

	if (rc) {
		DEBUG_BMAP(PLL_ERROR, bmap, "zfsslash2_write: error (rc=%d)",
			   rc);
	} else if (nb != BMAP_OD_SZ) {
		DEBUG_BMAP(PLL_ERROR, bmap, "zfsslash2_write: short I/O");
		rc = SLERR_SHORTIO;
	}
	DEBUG_BMAP(PLL_TRACE, bmap, "wrote bmap (rc=%d)",rc);
	BMAPOD_READ_DONE(bmap);
	return (rc);
}

int
mds_inode_addrepl_update(struct slash_inode_handle *inoh,
    sl_ios_id_t ios, uint32_t pos)
{
	size_t nb;
	int locked, rc = 0;
	struct slmds_jent_ino_addrepl jrir;

	jrir.sjir_fid = fcmh_2_fid(inoh->inoh_fcmh);
	jrir.sjir_ios = ios;
	jrir.sjir_pos = pos;

	locked = reqlock(&inoh->inoh_lock);

	jrir.sjir_nrepls = inoh->inoh_ino.ino_nrepls;

	psc_assert((inoh->inoh_flags & INOH_INO_DIRTY) ||
		   (inoh->inoh_flags & INOH_EXTRAS_DIRTY));

	mds_reserve_slot();
	if (inoh->inoh_flags & INOH_INO_DIRTY) {
		psc_crc64_calc(&inoh->inoh_ino.ino_crc, &inoh->inoh_ino,
		    INO_OD_CRCSZ);
		rc = zfsslash2_write(&rootcreds, &inoh->inoh_ino,
		    INO_OD_SZ, &nb, SL_INODE_START_OFF, 0,
		    inoh_2_mdsio_data(inoh), mds_inode_addrepl_log,
		    &jrir);

		if (!rc && nb != INO_OD_SZ)
			rc = SLERR_SHORTIO;
		if (rc)
			DEBUG_INOH(PLL_FATAL, inoh, "rc=%d sync fail", rc);

		inoh->inoh_flags &= ~INOH_INO_DIRTY;
		if (inoh->inoh_flags & INOH_INO_NEW) {
			inoh->inoh_flags &= ~INOH_INO_NEW;
			//inoh->inoh_flags |= INOH_EXTRAS_DIRTY;
		}
	}

	if (inoh->inoh_flags & INOH_EXTRAS_DIRTY) {
		psc_crc64_calc(&inoh->inoh_extras->inox_crc,
		    inoh->inoh_extras, INOX_OD_CRCSZ);
		rc = zfsslash2_write(&rootcreds, &inoh->inoh_extras,
		    INOX_OD_SZ, &nb, SL_EXTRAS_START_OFF, 0,
		    inoh_2_mdsio_data(inoh), mds_inode_addrepl_log,
		    &jrir);

		if (!rc && nb != INO_OD_SZ)
			rc = SLERR_SHORTIO;
		if (rc)
			DEBUG_INOH(PLL_FATAL, inoh, "rc=%d sync fail", rc);

		inoh->inoh_flags &= ~INOH_EXTRAS_DIRTY;
	}
	mds_unreserve_slot();

	ureqlock(&inoh->inoh_lock, locked);
	return (rc);
}

int
mdsio_bmap_write(struct bmapc_memb *bmap)
{
	size_t nb;
	int rc;

	rc = zfsslash2_write(&rootcreds, bmap_2_ondisk(bmap),
	    BMAP_OD_SZ, &nb, (off_t)((BMAP_OD_SZ * bmap->bcm_bmapno) +
	    SL_BMAP_START_OFF), 0, bmap_2_zfs_fh(bmap), NULL, NULL);

	if (rc) {
		DEBUG_BMAP(PLL_ERROR, bmap, "zfsslash2_write: error (rc=%d)",
			   rc);
	} else if (nb != BMAP_OD_SZ) {
		DEBUG_BMAP(PLL_ERROR, bmap, "zfsslash2_write: short I/O");
		rc = SLERR_SHORTIO;
	}
	DEBUG_BMAP(PLL_TRACE, bmap, "wrote bmap (rc=%d)",rc);
	return (rc);
}

int
mdsio_inode_read(struct slash_inode_handle *i)
{
	size_t nb;
	int rc;

	INOH_LOCK_ENSURE(i);
	rc = zfsslash2_read(&rootcreds, &i->inoh_ino, INO_OD_SZ, &nb,
	    SL_INODE_START_OFF, inoh_2_mdsio_data(i));

	if (rc) {
		DEBUG_INOH(PLL_ERROR, i, "inode read error %d", rc);
	} else if (nb != INO_OD_SZ) {
		DEBUG_INOH(PLL_NOTICE, i, "short read I/O (%zd vs %zd)",
		    nb, INO_OD_SZ);
		rc = SLERR_SHORTIO;
	} else {
		DEBUG_INOH(PLL_TRACE, i, "read inode data=%p",
		    inoh_2_mdsio_data(i));
//		rc = zfsslash2_getattr(inoh_2_fid(i),
//		    &inoh_2_fsz(i), inoh_2_mdsio_data(i));
	}
	return (rc);
}

int
mdsio_inode_write(struct slash_inode_handle *i)
{
	size_t nb;
	int rc;

	rc = zfsslash2_write(&rootcreds, &i->inoh_ino, INO_OD_SZ, &nb,
	     SL_INODE_START_OFF, 0, inoh_2_mdsio_data(i), NULL, NULL);

	if (rc) {
		DEBUG_INOH(PLL_ERROR, i, "zfsslash2_write: error (rc=%d)",
			   rc);
	} else if (nb != INO_OD_SZ) {
		DEBUG_INOH(PLL_ERROR, i, "zfsslash2_write: short I/O");
		rc = SLERR_SHORTIO;
	} else {
		DEBUG_INOH(PLL_TRACE, i, "wrote inode (rc=%d) data=%p",
		    rc, inoh_2_mdsio_data(i));
#ifdef SHITTY_PERFORMANCE
		rc = zfsslash2_fsync(&rootcreds, 1, inoh_2_mdsio_data(i));
		if (rc == -1)
			psc_fatal("zfsslash2_fsync");
#endif
	}
	return (rc);
}

int
mdsio_inode_extras_read(struct slash_inode_handle *i)
{
	size_t nb;
	int rc;

	psc_assert(i->inoh_extras);
	rc = zfsslash2_read(&rootcreds, i->inoh_extras, INOX_OD_SZ, &nb,
	    SL_EXTRAS_START_OFF, inoh_2_mdsio_data(i));
	if (rc)
		DEBUG_INOH(PLL_ERROR, i, "zfsslash2_read: error (rc=%d)", rc);
	else if (nb != INOX_OD_SZ) {
		DEBUG_INOH(PLL_ERROR, i, "zfsslash2_read: short I/O");
		rc = SLERR_SHORTIO;
	}
	return (rc);
}

int
mdsio_inode_extras_write(struct slash_inode_handle *i)
{
	size_t nb;
	int rc;

	psc_assert(i->inoh_extras);
	rc = zfsslash2_write(&rootcreds, i->inoh_extras, INOX_OD_SZ, &nb,
	     SL_EXTRAS_START_OFF, 0, inoh_2_mdsio_data(i), NULL, NULL);

	if (rc) {
		DEBUG_INOH(PLL_ERROR, i, "zfsslash2_write: error (rc=%d)",
			   rc);
	} else if (nb != INOX_OD_SZ) {
		DEBUG_INOH(PLL_ERROR, i, "zfsslash2_write: short I/O");
		rc = SLERR_SHORTIO;
	} else {
#ifdef SHITTY_PERFORMANCE
		rc = zfsslash2_fsync(&rootcreds, 1, inoh_2_mdsio_data(i));
		if (rc == -1)
			psc_fatal("zfsslash2_fsync");
#endif
	}
	return (rc);
}

int
zfsslash2_init(void)
{
	extern char *fuse_mount_options;
	int rc;

	fuse_mount_options = "";
	rc = libzfs_init_fusesocket();
	if (rc == 0)
		rc = libzfs_init();
	return (rc);
}

struct mdsio_ops mdsio_ops = {
	zfsslash2_init,
	libzfs_exit,

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
mdsio_write_cursor(void *buf, size_t size, void *finfo, sl_log_write_t funcp)
{
	return (zfsslash2_write_cursor(buf, size, finfo, funcp));
}
