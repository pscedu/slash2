/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2011, Pittsburgh Supercomputing Center (PSC).
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

#include "pfl/cdefs.h"
#include "pfl/fs.h"

#include "bmap_mds.h"
#include "fidc_mds.h"
#include "inode.h"
#include "mdsio.h"
#include "mdslog.h"
#include "slashd.h"
#include "slerr.h"
#include "sljournal.h"

int
mdsio_inode_dump(struct slash_inode_handle *ih, void *readh)
{
	struct fidc_membh *f;
	struct bmapc_memb *b;
	sl_bmapno_t i;
	void *th;
	int rc;

	f = ih->inoh_fcmh;
	th = inoh_2_mdsio_data(ih);

	for (i = 0; i < fcmh_2_nbmaps(f) + fcmh_2_nxbmaps(f); i++) {
		inoh_2_mdsio_data(ih) = readh;
		rc = mds_bmap_load(f, i, &b);
		inoh_2_mdsio_data(ih) = th;
		if (rc)
			return (rc);

		rc = mdsio_bmap_write(b, 0, NULL, NULL);
		bmap_op_done_type(b, BMAP_OPCNT_LOOKUP);
		if (rc)
			return (rc);
	}

	rc = mdsio_inode_extras_write(ih, NULL, NULL);
	if (rc)
		return (rc);

	return (mdsio_inode_write(ih, NULL, NULL));
}

int
mds_inode_update(struct slash_inode_handle *ih, int old_version)
{
	char fn[NAME_MAX + 1];
	struct sl_ino_compat *sic;
	struct srt_stat sstb;
	void *h = NULL, *th;
	int rc;

	sic = &sl_ino_compat_table[old_version];
	rc = sic->sic_read_ino(ih);
	if (rc)
		return (rc);
	DEBUG_INOH(PLL_INFO, ih, "updating old inode (v %d)",
	    old_version);

	snprintf(fn, sizeof(fn), "%016lx.update",
	    fcmh_2_fid(ih->inoh_fcmh));
	rc = mdsio_opencreate(mds_tmpdir_inum, &rootcreds, O_RDWR |
	    O_CREAT | O_TRUNC, 0644, fn, NULL, NULL, &h, NULL, NULL);
	if (rc)
		goto out;

	/* convert old structures into new into temp file */
	rc = sic->sic_read_inox(ih);
	if (rc)
		goto out;

	th = inoh_2_mdsio_data(ih);
	inoh_2_mdsio_data(ih) = h;
	rc = mdsio_inode_dump(ih, th);
	if (rc)
		goto out;

	/* move new structures to inode meta file */
	inoh_2_mdsio_data(ih) = th;
	memset(&sstb, 0, sizeof(sstb));
	rc = mdsio_setattr(0, &sstb, SL_SETATTRF_METASIZE, &rootcreds,
	    NULL, th, NULL);
	if (rc)
		goto out;

	rc = mdsio_inode_dump(ih, h);
	if (rc)
		goto out;

	mdsio_unlink(mds_tmpdir_inum, fn, &rootcreds, NULL);

 out:
	if (h)
		mdsio_release(&rootcreds, h);
	if (rc) {
		mdsio_unlink(mds_tmpdir_inum, fn, &rootcreds, NULL);
		DEBUG_INOH(PLL_ERROR, ih, "error updating old inode");
	}
	return (rc);
}

int
mds_inode_update_interrupted(struct slash_inode_handle *ih, int *rc)
{
	char fn[NAME_MAX + 1];
	struct srt_stat sstb;
	void *h = NULL, *th;
	int exists = 0;
	uint64_t crc;
	size_t nb;

	th = inoh_2_mdsio_data(ih);

	snprintf(fn, sizeof(fn), "%016lx.update",
	    fcmh_2_fid(ih->inoh_fcmh));
	*rc = mdsio_opencreate(mds_tmpdir_inum, &rootcreds, O_RDONLY,
	    0644, fn, NULL, NULL, &h, NULL, NULL);
	if (*rc)
		goto out;
	exists = 1;

	inoh_2_mdsio_data(ih) = h;

	*rc = mdsio_read(&rootcreds, &ih->inoh_ino, INO_OD_SZ, &nb, 0,
	    h);
	if (*rc)
		goto out;

	psc_crc64_calc(&crc, &ih->inoh_ino, INO_OD_CRCSZ);
	if (crc != ih->inoh_ino.ino_crc) {
		*rc = EIO;
		goto out;
	}

	*rc = mds_inox_ensure_loaded(ih);
	if (*rc)
		goto out;

	inoh_2_mdsio_data(ih) = th;

	memset(&sstb, 0, sizeof(sstb));
	*rc = mdsio_setattr(0, &sstb, SL_SETATTRF_METASIZE, &rootcreds,
	    NULL, th, NULL);
	if (*rc)
		goto out;

	*rc = mdsio_inode_dump(ih, h);
	if (*rc)
		goto out;

	mdsio_unlink(mds_tmpdir_inum, fn, &rootcreds, NULL);

 out:
	if (exists && *rc)
		mdsio_unlink(mds_tmpdir_inum, fn, &rootcreds, NULL);
	inoh_2_mdsio_data(ih) = th;
	return (exists);
}

int
mds_ino_read_v1(struct slash_inode_handle *ih)
{
	struct {
		uint16_t	ino_version;
		uint16_t	ino_flags;
		uint32_t	ino_bsz;
		uint32_t	ino_nrepls;
		uint32_t	ino_replpol;
		sl_replica_t	ino_repls[4];
		uint64_t	ino_crc;
	} ino;
	uint64_t crc;
	size_t nb;
	int rc;

	rc = mdsio_read(&rootcreds, &ino, sizeof(ino), &nb, 0,
	    inoh_2_mdsio_data(ih));

	if (rc)
		return (rc);

	psc_crc64_calc(&crc, &ino, sizeof(ino) - sizeof(crc));
	if (crc != ino.ino_crc)
		return (EIO);
	ih->inoh_ino.ino_version = ino.ino_version;
	ih->inoh_ino.ino_bsz = ino.ino_bsz;
	ih->inoh_ino.ino_nrepls = ino.ino_nrepls;
	ih->inoh_ino.ino_replpol = ino.ino_replpol;
	ih->inoh_ino.ino_replpol = ino.ino_replpol;
	memcpy(ih->inoh_ino.ino_repls, ino.ino_repls,
	    sizeof(ino.ino_repls));
	return (0);
}

int
mds_inox_read_v1(struct slash_inode_handle *ih)
{
	struct {
		sl_snap_t	inox_snaps[1];
		sl_replica_t	inox_repls[60];
		uint64_t	inox_crc;
	} inox;
	uint64_t crc;
	size_t nb;
	int rc;

	rc = mdsio_read(&rootcreds, &inox, sizeof(inox), &nb, 0,
	    inoh_2_mdsio_data(ih));

	if (rc)
		return (rc);

	psc_crc64_calc(&crc, &inox, sizeof(inox) - sizeof(crc));
	if (crc != inox.inox_crc)
		return (EIO);
	memcpy(ih->inoh_extras->inox_snaps, inox.inox_snaps,
	    sizeof(inox.inox_snaps));
	memcpy(ih->inoh_extras->inox_repls, inox.inox_repls,
	    sizeof(inox.inox_repls));
	return (0);
}

struct sl_ino_compat sl_ino_compat_table[] = {
/* 1 */	{ mds_ino_read_v1, mds_inox_read_v1, 0x400, 0x1000 },
/* 2 */	{ NULL, NULL, SL_EXTRAS_START_OFF, SL_BMAP_START_OFF },
};
