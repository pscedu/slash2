/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015, Google, Inc.
 * Copyright (c) 2006-2014, Pittsburgh Supercomputing Center (PSC).
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

#include "fidc_mds.h"
#include "inode.h"
#include "journal_mds.h"
#include "mdsio.h"
#include "slashd.h"
#include "slerr.h"

#include "zfs-fuse/zfs_slashlib.h"

__static void
mds_inode_od_initnew(struct slash_inode_handle *ih)
{
	ih->inoh_flags = INOH_INO_NEW;

	/* For now this is a fixed size. */
	ih->inoh_ino.ino_bsz = SLASH_BMAP_SIZE;
	ih->inoh_ino.ino_version = INO_VERSION;
}

int
mds_inode_read(struct slash_inode_handle *ih)
{
	uint64_t crc, od_crc = 0;
	struct fidc_membh *f;
	struct iovec iovs[2];
	int rc, locked, vfsid;
	uint16_t vers;
	size_t nb;
	char buf[LINE_MAX];

	f = inoh_2_fcmh(ih);
	rc = slfid_to_vfsid(fcmh_2_fid(f), &vfsid);
	if (rc)
		return (-rc);

	locked = INOH_RLOCK(ih); /* XXX bad on slow archiver */
	psc_assert(ih->inoh_flags & INOH_INO_NOTLOADED);

	memset(&ih->inoh_ino, 0, sizeof(ih->inoh_ino));

	iovs[0].iov_base = &ih->inoh_ino;
	iovs[0].iov_len = sizeof(ih->inoh_ino);
	iovs[1].iov_base = &od_crc;
	iovs[1].iov_len = sizeof(od_crc);

	rc = mdsio_preadv(vfsid, &rootcreds, iovs, nitems(iovs), &nb, 0,
	    inoh_2_mfh(ih));

	if (rc == 0 && nb != sizeof(ih->inoh_ino) + sizeof(od_crc))
		rc = SLERR_SHORTIO;

	if (rc == SLERR_SHORTIO && od_crc == 0 &&
	    pfl_memchk(&ih->inoh_ino, 0, sizeof(ih->inoh_ino))) {
		if (!mds_inode_update_interrupted(vfsid, ih, &rc)) {
			DEBUG_INOH(PLL_INFO, ih, buf, "detected a new inode");
			mds_inode_od_initnew(ih);
			rc = 0;
		}
	} else if (rc && rc != SLERR_SHORTIO) {
		DEBUG_INOH(PLL_ERROR, ih, buf, "inode read error %d", rc);
	} else {
		psc_crc64_calc(&crc, &ih->inoh_ino, sizeof(ih->inoh_ino));
		if (crc != od_crc && slm_crc_check) {
			vers = ih->inoh_ino.ino_version;
			memset(&ih->inoh_ino, 0, sizeof(ih->inoh_ino));

			if (mds_inode_update_interrupted(vfsid, ih, &rc))
				;
			else if (vers && vers < INO_VERSION)
				rc = mds_inode_update(vfsid, ih, vers);
			else if (rc == SLERR_SHORTIO)
				DEBUG_INOH(PLL_INFO, ih, buf,
				    "short read I/O (%zd vs %zd)",
				    nb, sizeof(ih->inoh_ino) +
				    sizeof(od_crc));
			else {
				DEBUG_INOH(PLL_WARN, ih, buf, 
				    "CRC failed (rc = %d) "
				    "want=%"PSCPRIxCRC64", got=%"PSCPRIxCRC64,
				    rc, od_crc, crc);
				rc = EIO;
			}
		}
		if (rc == 0) {
			ih->inoh_flags &= ~INOH_INO_NOTLOADED;
			DEBUG_INOH(PLL_INFO, ih, buf, "successfully loaded inode od");
		}
	}
	INOH_URLOCK(ih, locked);
	return (rc);
}

int
mds_inode_write(int vfsid, struct slash_inode_handle *ih, void *logf,
    void *arg)
{
	int rc, wasbusy, waslocked;
	struct fidc_membh *f;
	struct iovec iovs[2];
	uint64_t crc;
	size_t nb;
	char buf[LINE_MAX];

	INOH_LOCK_ENSURE(ih);

	f = inoh_2_fcmh(ih);
	wasbusy = FCMH_REQ_BUSY(f, &waslocked);

	psc_crc64_calc(&crc, &ih->inoh_ino, sizeof(ih->inoh_ino));

	iovs[0].iov_base = &ih->inoh_ino;
	iovs[0].iov_len = sizeof(ih->inoh_ino);
	iovs[1].iov_base = &crc;
	iovs[1].iov_len = sizeof(crc);

	INOH_ULOCK(ih);

	if (logf)
		mds_reserve_slot(1);
	rc = mdsio_pwritev(vfsid, &rootcreds, iovs, nitems(iovs), &nb,
	    0, inoh_2_mfh(ih), logf, arg);
	if (logf)
		mds_unreserve_slot(1);

	INOH_LOCK(ih);

	if (rc == 0 && nb != sizeof(ih->inoh_ino) + sizeof(crc))
		rc = SLERR_SHORTIO;

	if (rc)
		DEBUG_INOH(PLL_ERROR, ih, buf,
		    "mdsio_pwritev: error (resid=%zd nb=%zd rc=%d)",
		    sizeof(ih->inoh_ino) + sizeof(crc), nb, rc);
	else {
		DEBUG_INOH(PLL_INFO, ih, buf, "wrote inode, "
		    "flags=%x size=%"PRIu64" data=%p",
		    ih->inoh_flags, inoh_2_fsz(ih),
		    inoh_2_mfh(ih));
		if (ih->inoh_flags & INOH_INO_NEW)
			ih->inoh_flags &= ~INOH_INO_NEW;
	}
	FCMH_UREQ_BUSY(f, wasbusy, waslocked);
	return (rc);
}

int
mds_inox_write(int vfsid, struct slash_inode_handle *ih, void *logf,
    void *arg)
{
	int rc, wasbusy, waslocked;
	struct fidc_membh *f;
	struct iovec iovs[2];
	uint64_t crc;
	size_t nb;
	char buf[LINE_MAX];

	INOH_LOCK_ENSURE(ih);

	psc_assert(ih->inoh_extras);

	f = inoh_2_fcmh(ih);
	wasbusy = FCMH_REQ_BUSY(f, &waslocked);

	psc_crc64_calc(&crc, ih->inoh_extras, INOX_SZ);

	iovs[0].iov_base = ih->inoh_extras;
	iovs[0].iov_len = INOX_SZ;
	iovs[1].iov_base = &crc;
	iovs[1].iov_len = sizeof(crc);

	INOH_ULOCK(ih);

	if (logf)
		mds_reserve_slot(1);
	rc = mdsio_pwritev(vfsid, &rootcreds, iovs, nitems(iovs), &nb,
	    SL_EXTRAS_START_OFF, inoh_2_mfh(ih), logf, arg);
	if (logf)
		mds_unreserve_slot(1);

	INOH_LOCK(ih);

	if (rc == 0 && nb != INOX_SZ + sizeof(crc))
		rc = SLERR_SHORTIO;
	if (rc)
		DEBUG_INOH(PLL_ERROR, ih, buf, "mdsio_pwritev: error (rc=%d)",
		    rc);

	FCMH_UREQ_BUSY(f, wasbusy, waslocked);
	return (rc);
}

int
mds_inox_load_locked(struct slash_inode_handle *ih)
{
	struct fidc_membh *f;
	struct iovec iovs[2];
	uint64_t crc, od_crc;
	int rc, vfsid;
	size_t nb;
	char buf[LINE_MAX];

	INOH_LOCK_ENSURE(ih);

	psc_assert(ih->inoh_extras == NULL);

	ih->inoh_extras = PSCALLOC(INOX_SZ);

	iovs[0].iov_base = ih->inoh_extras;
	iovs[0].iov_len = INOX_SZ;
	iovs[1].iov_base = &od_crc;
	iovs[1].iov_len = sizeof(od_crc);

	f = inoh_2_fcmh(ih);
	slfid_to_vfsid(fcmh_2_fid(f), &vfsid);
	rc = mdsio_preadv(vfsid, &rootcreds, iovs, nitems(iovs), &nb,
	    SL_EXTRAS_START_OFF, inoh_2_mfh(ih));
	if (rc == 0 && od_crc == 0 &&
	    pfl_memchk(ih->inoh_extras, 0, INOX_SZ)) {
		rc = 0;
	} else if (rc) {
		rc = -abs(rc);
		DEBUG_INOH(PLL_ERROR, ih, buf, "read inox: %d", rc);
	} else if (nb != INOX_SZ + sizeof(od_crc)) {
		rc = -SLERR_SHORTIO;
		DEBUG_INOH(PLL_ERROR, ih, buf, "read inox: %d nb=%zu", rc, nb);
	} else {
		psc_crc64_calc(&crc, ih->inoh_extras, INOX_SZ);
		if (crc != od_crc) {
			psclog_errorx("inox CRC fail (rc=%d) "
			    "disk=%"PSCPRIxCRC64" mem=%"PSCPRIxCRC64,
			    rc, od_crc, crc);
			OPSTAT_INCR("badcrc");
			rc = -PFLERR_BADCRC;
		}
	}
	if (rc) {
		PSCFREE(ih->inoh_extras);
		ih->inoh_extras = NULL;
	}
	return (rc);
}

int
mds_inox_ensure_loaded(struct slash_inode_handle *ih)
{
	int locked, rc = 0;

	locked = INOH_RLOCK(ih);
	if (ih->inoh_extras == NULL)
		rc = mds_inox_load_locked(ih);
	INOH_URLOCK(ih, locked);
	return (rc);
}

int
mds_inodes_odsync(int vfsid, struct fidc_membh *f,
    void (*logf)(void *, uint64_t, int))
{
	struct slash_inode_handle *ih = fcmh_2_inoh(f);
	int locked, rc;

	locked = INOH_RLOCK(ih);
	if (ih->inoh_ino.ino_nrepls > SL_DEF_REPLICAS) {
		/*
		 * Don't assume the inox has been loaded.  It's
		 * possible our caller didn't require them (BZ #258).
		 */
		rc = mds_inox_ensure_loaded(ih);
		if (rc) {
			INOH_URLOCK(ih, locked);
			return (rc);
		}
	}

	rc = mds_inode_write(vfsid, ih, logf, f);
	if (rc == 0 && ih->inoh_ino.ino_nrepls > SL_DEF_REPLICAS)
		rc = mds_inox_write(vfsid, ih, NULL, NULL);

	DEBUG_FCMH(PLL_DEBUG, f, "updated inode logf=%p",
	    logf);
	INOH_URLOCK(ih, locked);
	return (rc);
}

char *
_dump_ino(char *buf, size_t siz, const struct slash_inode_od *ino)
{
	char nbuf[16], rbuf[LINE_MAX];
	int nr, j;

	nr = ino->ino_nrepls;
	if (nr < 0)
		nr = 1;
	else if (nr > SL_DEF_REPLICAS)
		nr = SL_DEF_REPLICAS;

	rbuf[0] = '\0';
	for (j = 0; j < nr; j++) {
		if (j)
			strlcat(rbuf, ",", sizeof(rbuf));
		snprintf(nbuf, sizeof(nbuf), "%u",
		    ino->ino_repls[j].bs_id);
		strlcat(rbuf, nbuf, sizeof(rbuf));
	}
	snprintf(buf, siz, "bsz:%u nr:%u nbpol:%u repl:%s",
	    ino->ino_bsz, ino->ino_nrepls, ino->ino_replpol, rbuf);
	return (buf);
}

#if PFL_DEBUG > 0
void
dump_inoh(const struct slash_inode_handle *ih)
{
	char buf[LINE_MAX];

	DEBUG_INOH(PLL_MAX, ih, buf, "");
}

void
dump_ino(const struct slash_inode_od *ino)
{
	char buf[BUFSIZ];

	fprintf(stderr, "%s", _dump_ino(buf, sizeof(buf), ino));
}
#endif
