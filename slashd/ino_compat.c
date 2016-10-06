/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015, Google, Inc.
 * Copyright 2011-2016, Pittsburgh Supercomputing Center
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

#define PSC_SUBSYS SLSS_FCMH
#include "slsubsys.h"

#include "pfl/cdefs.h"
#include "pfl/fs.h"

#include "bmap_mds.h"
#include "fidc_mds.h"
#include "inode.h"
#include "journal_mds.h"
#include "mdsio.h"
#include "slashd.h"
#include "slerr.h"

#include "zfs-fuse/zfs_slashlib.h"

int
mds_inode_dump(int vfsid, struct sl_ino_compat *sic,
    struct slash_inode_handle *ih, void *readh)
{
	struct fidc_membh *f;
	struct bmapc_memb *b;
	struct mio_fh *fh;
	sl_bmapno_t i;
	int rc, fl;
	void *th;

	f = inoh_2_fcmh(ih);
	th = inoh_2_mfh(ih);
	fh = inoh_2_mfh(ih);

	psclog_warnx("Calling obsolete function %s", __func__);

	fl = BMAPGETF_CREATE | BMAPGETF_NOAUTOINST;
	if (sic)
		fl |= BMAPGETF_NORETRIEVE;

	for (i = 0; ; i++) {
		fh->fh = readh;

		rc = bmap_getf(f, i, SL_WRITE, fl, &b);
		if (sic && !rc)
			b->bcm_flags |= BMAPF_LOADED;
		fh->fh = th;

		if (rc == SLERR_BMAP_INVALID)
			break;

		if (rc)
			return (rc);

		if (sic) {
			rc = sic->sic_read_bmap(b, readh);
			if (rc) {
				bmap_op_done(b);
				(void)INOH_RLOCK(ih);
				if (rc == SLERR_BMAP_INVALID)
					break;
				return (rc);
			}
		}

		rc = mds_bmap_write(b, NULL, NULL);
		bmap_op_done(b);
		if (rc)
			return (rc);
	}

	INOH_LOCK(ih);
	rc = mds_inox_write(vfsid, ih, NULL, NULL);
	if (rc) {
		INOH_ULOCK(ih);
		return (rc);
	}

	rc = mds_inode_write(vfsid, ih, NULL, NULL);
	if (rc) {
		INOH_ULOCK(ih);
		return (rc);
	}

	INOH_ULOCK(ih);
	mdsio_fsync(vfsid, &rootcreds, 1, th);
	return (0);
}

int
mds_inode_update(int vfsid, struct slash_inode_handle *ih,
    int old_version)
{
	char fn[NAME_MAX + 1];
	struct sl_ino_compat *sic;
	struct fidc_membh *f;
	struct srt_stat sstb;
	void *h = NULL, *th;
	int rc, level;
	char buf[LINE_MAX];

	OPSTAT_INCR("inode-update");
	sic = &sl_ino_compat_table[old_version];
	rc = sic->sic_read_ino(ih);
	if (rc)
		return (rc);

	level = debug_ondisk_inode ? PLL_MAX : PLL_WARN;
	DEBUG_INOH(level, ih, buf, "updating old inode (v %d)", old_version);

	f = inoh_2_fcmh(ih);
	/* 
	 * Introduced by commit 85f8cf4f751fe8348e1dc997d6f73f99a1d37938
	 */
	snprintf(fn, sizeof(fn), "%016"PRIx64".update", fcmh_2_fid(f));
	rc = mdsio_opencreatef(vfsid, mds_tmpdir_inum[vfsid],
	    &rootcreds, O_RDWR | O_CREAT | O_TRUNC,
	    MDSIO_OPENCRF_NOLINK, 0644, fn, NULL, NULL, &h, NULL, NULL,
	    0);
	if (rc)
		PFL_GOTOERR(out, rc);

	psc_assert(ih->inoh_extras == NULL);
	ih->inoh_extras = PSCALLOC(INOX_SZ);

	/* convert old structures into new into temp file */
	rc = sic->sic_read_inox(ih);
	if (rc)
		PFL_GOTOERR(out, rc);

	th = inoh_2_mfhp(ih)->fh;
	inoh_2_mfhp(ih)->fh = h;
	rc = mds_inode_dump(vfsid, sic, ih, th);
	inoh_2_mfhp(ih)->fh = th;
	if (rc)
		PFL_GOTOERR(out, rc);

	/* move new structures to inode meta file */
	memset(&sstb, 0, sizeof(sstb));
	rc = mdsio_setattr(vfsid, 0, &sstb, SL_SETATTRF_METASIZE,
	    &rootcreds, NULL, th, NULL);
	if (rc)
		PFL_GOTOERR(out, rc);

	rc = mds_inode_dump(vfsid, NULL, ih, h);
	if (rc)
		PFL_GOTOERR(out, rc);

	mdsio_unlink(vfsid, mds_tmpdir_inum[vfsid], NULL, fn,
	    &rootcreds, NULL, NULL);

 out:
	if (h)
		mdsio_release(vfsid, &rootcreds, h);
	if (rc) {
		mdsio_unlink(vfsid, mds_tmpdir_inum[vfsid], NULL, fn,
		    &rootcreds, NULL, NULL);
		DEBUG_INOH(PLL_ERROR, ih, buf, "error updating old inode "
		    "rc=%d", rc);
	}
	return (rc);
}

int
mds_inode_update_interrupted(int vfsid, struct slash_inode_handle *ih,
    int *rc)
{
	char fn[NAME_MAX + 1];
	struct srt_stat sstb;
	struct iovec iovs[2];
	void *h = NULL, *th;
	mdsio_fid_t inum;
	int exists = 0;
	size_t nb;
	uint64_t od_crc;
	uint64_t crc;
	char buf[LINE_MAX];

	th = inoh_2_mfh(ih);

	snprintf(fn, sizeof(fn), "%016"PRIx64".update",
	    inoh_2_fid(ih));

	*rc = mdsio_lookup(vfsid, mds_tmpdir_inum[vfsid], fn, &inum,
	    &rootcreds, NULL);
	if (*rc)
		PFL_GOTOERR(out, *rc);

	*rc = mdsio_opencreatef(vfsid, inum, &rootcreds, O_RDONLY,
	    MDSIO_OPENCRF_NOLINK, 0644, NULL, NULL, NULL, &h, NULL,
	    NULL, 0);
	if (*rc)
		PFL_GOTOERR(out, *rc);

	iovs[0].iov_base = &ih->inoh_ino;
	iovs[0].iov_len = sizeof(ih->inoh_ino);
	iovs[1].iov_base = &od_crc;
	iovs[1].iov_len = sizeof(od_crc);
	*rc = mdsio_preadv(vfsid, &rootcreds, iovs, nitems(iovs), &nb, 0, h);
	if (*rc)
		PFL_GOTOERR(out, *rc);

	psc_crc64_calc(&crc, &ih->inoh_ino, sizeof(ih->inoh_ino));
	if (crc != od_crc && slm_crc_check) {
		OPSTAT_INCR("badcrc");
		*rc = PFLERR_BADCRC;
		DEBUG_INOH(PLL_WARN, ih, buf, "CRC failed "
		    "want=%"PSCPRIxCRC64", got=%"PSCPRIxCRC64,
		    od_crc, crc);
		PFL_GOTOERR(out, *rc);
	}

	exists = 1;

	psc_assert(ih->inoh_extras == NULL);
	ih->inoh_extras = PSCALLOC(INOX_SZ);

	inoh_2_mfh(ih) = h;
	*rc = mds_inox_ensure_loaded(ih);
	if (*rc)
		PFL_GOTOERR(out, *rc);

	inoh_2_mfh(ih) = th;

	memset(&sstb, 0, sizeof(sstb));
	*rc = mdsio_setattr(vfsid, 0, &sstb, SL_SETATTRF_METASIZE,
	    &rootcreds, NULL, th, NULL);
	if (*rc)
		PFL_GOTOERR(out, *rc);

	*rc = mds_inode_dump(vfsid, NULL, ih, h);
	if (*rc)
		PFL_GOTOERR(out, *rc);

	mdsio_unlink(vfsid, mds_tmpdir_inum[vfsid], NULL, fn,
	    &rootcreds, NULL, NULL);

 out:
	if (h)
		mdsio_release(vfsid, &rootcreds, h);
	if (*rc)
		mdsio_unlink(vfsid, mds_tmpdir_inum[vfsid], NULL, fn,
		    &rootcreds, NULL, NULL);
	inoh_2_mfh(ih) = th;
	return (exists);
}

/* called by sic_read_ino() */
int
mds_ino_read_v1(struct slash_inode_handle *ih)
{
	struct {
		uint16_t	version;
		uint16_t	flags;
		uint32_t	bsz;
		uint32_t	nrepls;
		uint32_t	replpol;
		sl_replica_t	repls[4];
	} ino;
	struct fidc_membh *f;
	struct iovec iovs[2];
	uint64_t crc, od_crc;
	int i, rc, vfsid;
	size_t nb;

	iovs[0].iov_base = &ino;
	iovs[0].iov_len = sizeof(ino);
	iovs[1].iov_base = &od_crc;
	iovs[1].iov_len = sizeof(od_crc);

	f = inoh_2_fcmh(ih);
	slfid_to_vfsid(fcmh_2_fid(f), &vfsid);
	rc = mdsio_preadv(vfsid, &rootcreds, iovs, nitems(iovs), &nb, 0,
	    inoh_2_mfh(ih));

	if (rc)
		return (rc);
	if (nb != sizeof(ino) + sizeof(od_crc))
		return (SLERR_SHORTIO);

	psc_crc64_calc(&crc, &ino, sizeof(ino));
	if (crc != od_crc) {
		OPSTAT_INCR("badcrc");
		return (PFLERR_BADCRC);
	}
	ih->inoh_ino.ino_version = INO_VERSION;
	ih->inoh_ino.ino_bsz = ino.bsz;
	ih->inoh_ino.ino_nrepls = ino.nrepls;
	ih->inoh_ino.ino_replpol = ino.replpol;
	for (i = 0; i < 4; i++)
		ih->inoh_ino.ino_repls[i] = ino.repls[i];
	return (0);
}

/* called by sic_read_inox() */
int
mds_inox_read_v1(struct slash_inode_handle *ih)
{
	struct {
		sl_snap_t	snaps[1];
		sl_replica_t	repls[60];
	} inox;
	struct fidc_membh *f;
	struct iovec iovs[2];
	uint64_t crc, od_crc;
	int i, rc, vfsid;
	size_t nb;

	memset(&inox, 0, sizeof(inox));

	iovs[0].iov_base = &inox;
	iovs[0].iov_len = sizeof(inox);
	iovs[1].iov_base = &od_crc;
	iovs[1].iov_len = sizeof(od_crc);

	f = inoh_2_fcmh(ih);
	slfid_to_vfsid(fcmh_2_fid(f), &vfsid);
	rc = mdsio_preadv(vfsid, &rootcreds, iovs, nitems(iovs), &nb,
	    0x400, inoh_2_mfh(ih));

	if (rc)
		return (rc);
	if (pfl_memchk(&inox, 0, sizeof(inox)) && od_crc == 0)
		return (0);
	if (nb != sizeof(inox) + sizeof(od_crc))
		return (SLERR_SHORTIO);

	psc_crc64_calc(&crc, &inox, sizeof(inox));
	if (crc != od_crc) {
		OPSTAT_INCR("badcrc");
		return (PFLERR_BADCRC);
	}
	for (i = 0; i < 60; i++)
		ih->inoh_extras->inox_repls[i] = inox.repls[i];
	return (0);
}

/* called by sic_read_bmap() */
int
mds_bmap_read_v1(struct bmapc_memb *b, void *readh)
{
	struct {
		uint8_t		crcstates[128];
		uint8_t		repls[24];
		uint64_t	crcs[128];
		uint32_t	gen;
		uint32_t	replpol;
	} bod;
	struct fidc_membh *f;
	struct iovec iovs[2];
	uint64_t crc, od_crc;
	int i, rc, vfsid;
	size_t nb, bsz;
	struct bmap_mds_info *bmi = bmap_2_bmi(b);

	bsz = sizeof(bod) + sizeof(crc);

	iovs[0].iov_base = &bod;
	iovs[0].iov_len = sizeof(bod);
	iovs[1].iov_base = &od_crc;
	iovs[1].iov_len = sizeof(od_crc);
	f = b->bcm_fcmh;
	slfid_to_vfsid(fcmh_2_fid(f), &vfsid);
	rc = mdsio_preadv(vfsid, &rootcreds, iovs, nitems(iovs), &nb,
	    bsz * b->bcm_bmapno + 0x1000, readh);

	if (rc)
		return (rc);
	if (nb == 0)
		return (SLERR_BMAP_INVALID);
	if (nb != bsz)
		return (SLERR_SHORTIO);

	psc_crc64_calc(&crc, &bod, sizeof(bod));
	if (crc != od_crc && slm_crc_check) {
		OPSTAT_INCR("badcrc");
		return (PFLERR_BADCRC);
	}
	for (i = 0; i < 128; i++)
		bmi->bmi_crcstates[i] = bod.crcstates[i];
	for (i = 0; i < 24; i++)
		bmi->bmi_repls[i] = bod.repls[i];
	for (i = 0; i < 128; i++)
		bmap_2_crcs(b, i) = bod.crcs[i];
	bmap_2_bgen(b) = bod.gen;
	bmap_2_replpol(b) = bod.replpol;
	return (0);
}

struct sl_ino_compat sl_ino_compat_table[] = {
/* 0 */	{ NULL, NULL, NULL },
/* 1 */	{ mds_ino_read_v1, mds_inox_read_v1, mds_bmap_read_v1 },
/* 2 */	{ NULL, NULL, NULL }
};
