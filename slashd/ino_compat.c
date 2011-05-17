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
mds_inode_dump(struct sl_ino_compat *sic, struct slash_inode_handle *ih,
    void *readh)
{
	struct fidc_membh *f;
	struct bmapc_memb *b;
	sl_bmapno_t i;
	int rc, fl;
	void *th;

	f = ih->inoh_fcmh;
	th = inoh_2_mdsio_data(ih);

	fl = BMAPGETF_LOAD | BMAPGETF_NOAUTOINST;
	if (sic)
		fl |= BMAPGETF_NORETRIEVE;

	for (i = 0; ; i++) {
		inoh_2_mdsio_data(ih) = readh;
		rc = bmap_getf(f, i, SL_WRITE, fl, &b);
		inoh_2_mdsio_data(ih) = th;

		if (rc == SLERR_BMAP_INVALID)
			break;

		if (rc)
			return (rc);

		if (sic) {
			rc = sic->sic_read_bmap(b, readh);
			if (rc) {
				bmap_op_done_type(b, BMAP_OPCNT_LOOKUP);
				if (rc == SLERR_BMAP_INVALID)
					break;
				return (rc);
			}
		}

		rc = mds_bmap_write(b, 0, NULL, NULL);
		bmap_op_done_type(b, BMAP_OPCNT_LOOKUP);
		if (rc)
			return (rc);
	}

	rc = mds_inox_write(ih, NULL, NULL);
	if (rc)
		return (rc);

	rc = mds_inode_write(ih, NULL, NULL);
	if (rc)
		return (rc);

	mdsio_fsync(&rootcreds, 1, th);
	return (0);
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

	snprintf(fn, sizeof(fn), "%016"PRIx64".update",
	    fcmh_2_fid(ih->inoh_fcmh));
	rc = mdsio_opencreate(mds_tmpdir_inum, &rootcreds, O_RDWR |
	    O_CREAT | O_TRUNC, 0644, fn, NULL, NULL, &h, NULL, NULL, 0);
	if (rc)
		goto out;

	psc_assert(ih->inoh_extras == NULL);
	ih->inoh_extras = PSCALLOC(sizeof(*ih->inoh_extras));
	ih->inoh_flags |= INOH_HAVE_EXTRAS;

	/* convert old structures into new into temp file */
	rc = sic->sic_read_inox(ih);
	if (rc)
		goto out;

	th = inoh_2_mdsio_data(ih);
	inoh_2_mdsio_data(ih) = h;
	rc = mds_inode_dump(sic, ih, th);
	inoh_2_mdsio_data(ih) = th;
	if (rc)
		goto out;

	/* move new structures to inode meta file */
	memset(&sstb, 0, sizeof(sstb));
	rc = mdsio_setattr(0, &sstb, SL_SETATTRF_METASIZE, &rootcreds,
	    NULL, th, NULL);
	if (rc)
		goto out;

	rc = mds_inode_dump(NULL, ih, h);
	if (rc)
		goto out;

	mdsio_unlink(mds_tmpdir_inum, NULL, fn, &rootcreds, NULL);

 out:
	if (h)
		mdsio_release(&rootcreds, h);
	if (rc) {
		mdsio_unlink(mds_tmpdir_inum, NULL, fn, &rootcreds, NULL);
		DEBUG_INOH(PLL_ERROR, ih, "error updating old inode "
		    "rc=%d", rc);
	}
	return (rc);
}

int
mds_inode_update_interrupted(struct slash_inode_handle *ih, int *rc)
{
	char fn[NAME_MAX + 1];
	struct srt_stat sstb;
	struct iovec iovs[2];
	uint64_t crc, od_crc;
	void *h = NULL, *th;
	mdsio_fid_t inum;
	int exists = 0;
	size_t nb;

	th = inoh_2_mdsio_data(ih);

	snprintf(fn, sizeof(fn), "%016"PRIx64".update",
	    fcmh_2_fid(ih->inoh_fcmh));

	*rc = mdsio_lookup(mds_tmpdir_inum, fn, &inum, &rootcreds, NULL);
	if (*rc)
		goto out;

	*rc = mdsio_opencreatef(inum, &rootcreds, O_RDONLY,
	    MDSIO_OPENCRF_NOLINK, 0644, NULL, NULL, NULL, &h, NULL,
	    NULL, 0);
	if (*rc)
		goto out;

	iovs[0].iov_base = &ih->inoh_ino;
	iovs[0].iov_len = sizeof(ih->inoh_ino);
	iovs[1].iov_base = &od_crc;
	iovs[1].iov_len = sizeof(od_crc);
	*rc = mdsio_preadv(&rootcreds, iovs, nitems(iovs), &nb, 0, h);
	if (*rc)
		goto out;

	psc_crc64_calc(&crc, &ih->inoh_ino, sizeof(ih->inoh_ino));
	if (crc != od_crc) {
		*rc = SLERR_BADCRC;
		goto out;
	}

	exists = 1;

	psc_assert(ih->inoh_extras == NULL);
	ih->inoh_extras = PSCALLOC(sizeof(*ih->inoh_extras));
	ih->inoh_flags |= INOH_HAVE_EXTRAS;

	inoh_2_mdsio_data(ih) = h;
	*rc = mds_inox_ensure_loaded(ih);
	if (*rc)
		goto out;

	inoh_2_mdsio_data(ih) = th;

	memset(&sstb, 0, sizeof(sstb));
	*rc = mdsio_setattr(0, &sstb, SL_SETATTRF_METASIZE, &rootcreds,
	    NULL, th, NULL);
	if (*rc)
		goto out;

	*rc = mds_inode_dump(NULL, ih, h);
	if (*rc)
		goto out;

	mdsio_unlink(mds_tmpdir_inum, NULL, fn, &rootcreds, NULL);

 out:
	if (h)
		mdsio_release(&rootcreds, h);
	mdsio_unlink(mds_tmpdir_inum, NULL, fn, &rootcreds, NULL);
	inoh_2_mdsio_data(ih) = th;
	return (exists);
}

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
	uint64_t crc, od_crc;
	struct iovec iovs[2];
	size_t nb;
	int i, rc;

	iovs[0].iov_base = &ino;
	iovs[0].iov_len = sizeof(ino);
	iovs[1].iov_base = &od_crc;
	iovs[1].iov_len = sizeof(od_crc);
	rc = mdsio_preadv(&rootcreds, iovs, nitems(iovs), &nb, 0,
	    inoh_2_mdsio_data(ih));

	if (rc)
		return (rc);
	if (nb != sizeof(ino) + sizeof(od_crc))
		return (SLERR_SHORTIO);

	psc_crc64_calc(&crc, &ino, sizeof(ino));
	if (crc != od_crc)
		return (SLERR_BADCRC);
	ih->inoh_ino.ino_version = ino.version;
	ih->inoh_ino.ino_bsz = ino.bsz;
	ih->inoh_ino.ino_nrepls = ino.nrepls;
	ih->inoh_ino.ino_replpol = ino.replpol;
	for (i = 0; i < 4; i++)
		ih->inoh_ino.ino_repls[i] = ino.repls[i];
	return (0);
}

int
mds_inox_read_v1(struct slash_inode_handle *ih)
{
	struct {
		sl_snap_t	snaps[1];
		sl_replica_t	repls[60];
	} inox;
	uint64_t crc, od_crc;
	struct iovec iovs[2];
	size_t nb;
	int i, rc;

	memset(&inox, 0, sizeof(inox));

	iovs[0].iov_base = &inox;
	iovs[0].iov_len = sizeof(inox);
	iovs[1].iov_base = &od_crc;
	iovs[1].iov_len = sizeof(od_crc);
	rc = mdsio_preadv(&rootcreds, iovs, nitems(iovs), &nb, 0x400,
	    inoh_2_mdsio_data(ih));

	if (rc)
		return (rc);
	if (pfl_memchk(&inox, 0, sizeof(inox)) && od_crc == 0)
		return (0);
	if (nb != sizeof(inox) + sizeof(od_crc))
		return (SLERR_SHORTIO);

	psc_crc64_calc(&crc, &inox, sizeof(inox));
	if (crc != od_crc)
		return (SLERR_BADCRC);
	for (i = 0; i < 60; i++)
		ih->inoh_extras->inox_repls[i] = inox.repls[i];
	return (0);
}

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
	uint64_t crc, od_crc;
	struct iovec iovs[2];
	size_t nb, bsz;
	int i, rc;

	bsz = sizeof(bod) + sizeof(crc);

	iovs[0].iov_base = &bod;
	iovs[0].iov_len = sizeof(bod);
	iovs[1].iov_base = &od_crc;
	iovs[1].iov_len = sizeof(od_crc);
	rc = mdsio_preadv(&rootcreds, iovs, nitems(iovs), &nb,
	    bsz * b->bcm_bmapno + 0x1000, readh);

	if (rc)
		return (rc);
	if (nb == 0)
		return (SLERR_BMAP_INVALID);
	if (nb != bsz)
		return (SLERR_SHORTIO);

	psc_crc64_calc(&crc, &bod, sizeof(bod));
	if (crc != od_crc)
		return (SLERR_BADCRC);
	for (i = 0; i < 128; i++)
		b->bcm_crcstates[i] = bod.crcstates[i];
	for (i = 0; i < 24; i++)
		b->bcm_repls[i] = bod.repls[i];
	for (i = 0; i < 128; i++)
		bmap_2_crcs(b, i) = bod.crcs[i];
	bmap_2_bgen(b) = bod.gen;
	bmap_2_replpol(b) = bod.replpol;
	return (0);
}

struct sl_ino_compat sl_ino_compat_table[] = {
/* 0 */	{ NULL, NULL, NULL },
/* 1 */	{ NULL, NULL, NULL },
};
