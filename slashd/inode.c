/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2011, Pittsburgh Supercomputing Center (PSC).
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

#define PSC_SUBSYS SLSS_FCMH
#include "slsubsys.h"

#include "pfl/cdefs.h"

#include "fidc_mds.h"
#include "inode.h"
#include "mdsio.h"
#include "mdslog.h"
#include "slashd.h"
#include "slerr.h"
#include "sljournal.h"

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
	struct iovec iovs[2];
	uint64_t crc, od_crc = 0;
	uint16_t vers;
	int rc, locked;
	size_t nb;

	locked = INOH_RLOCK(ih); /* XXX bad on slow archiver */
	psc_assert(ih->inoh_flags & INOH_INO_NOTLOADED);

	memset(&ih->inoh_ino, 0, sizeof(ih->inoh_ino));

	iovs[0].iov_base = &ih->inoh_ino;
	iovs[0].iov_len = sizeof(ih->inoh_ino);
	iovs[1].iov_base = &od_crc;
	iovs[1].iov_len = sizeof(od_crc);
	rc = mdsio_preadv(&rootcreds, iovs, nitems(iovs), &nb, 0,
	    inoh_2_mdsio_data(ih));

	if (rc == 0 && nb != sizeof(ih->inoh_ino) + sizeof(od_crc))
		rc = SLERR_SHORTIO;

	if (rc == SLERR_SHORTIO && od_crc == 0 &&
	    pfl_memchk(&ih->inoh_ino, 0, sizeof(ih->inoh_ino))) {
		if (!mds_inode_update_interrupted(ih, &rc)) {
			DEBUG_INOH(PLL_INFO, ih, "detected a new inode");
			mds_inode_od_initnew(ih);
			rc = 0;
		}
	} else if (rc && rc != SLERR_SHORTIO) {
		DEBUG_INOH(PLL_ERROR, ih, "inode read error %d", rc);
	} else {
		psc_crc64_calc(&crc, &ih->inoh_ino, sizeof(ih->inoh_ino));
		if (crc != od_crc) {
			vers = ih->inoh_ino.ino_version;
			memset(&ih->inoh_ino, 0, sizeof(ih->inoh_ino));

			if (mds_inode_update_interrupted(ih, &rc))
				;
			else if (vers && vers < INO_VERSION)
				rc = mds_inode_update(ih, vers);
			else if (rc == SLERR_SHORTIO)
				DEBUG_INOH(PLL_INFO, ih,
				    "short read I/O (%zd vs %zd)",
				    nb, sizeof(ih->inoh_ino) +
				    sizeof(od_crc));
			else {
				rc = SLERR_BADCRC;
				DEBUG_INOH(PLL_WARN, ih, "CRC failed "
				    "want=%"PSCPRIxCRC64", got=%"PSCPRIxCRC64,
				    od_crc, crc);
			}
		}
		if (rc == 0) {
			ih->inoh_flags &= ~INOH_INO_NOTLOADED;
			DEBUG_INOH(PLL_INFO, ih, "successfully loaded inode od");
		}
	}
	INOH_URLOCK(ih, locked);
	return (rc);
}

int
mds_inode_write(struct slash_inode_handle *ih, void *logf, void *arg)
{
	struct iovec iovs[2];
	uint64_t crc;
	size_t nb;
	int rc;

	INOH_LOCK_ENSURE(ih);

	while (ih->inoh_flags & INOH_IN_IO) {
		usleep(1);
		INOH_LOCK(ih);
	}

	psc_crc64_calc(&crc, &ih->inoh_ino, sizeof(ih->inoh_ino));

	iovs[0].iov_base = &ih->inoh_ino;
	iovs[0].iov_len = sizeof(ih->inoh_ino);
	iovs[1].iov_base = &crc;
	iovs[1].iov_len = sizeof(crc);

	ih->inoh_flags |= INOH_IN_IO;
	INOH_ULOCK(ih);

	if (logf)
		mds_reserve_slot();
	rc = mdsio_pwritev(&rootcreds, iovs, nitems(iovs), &nb, 0, 0,
	    inoh_2_mdsio_data(ih), logf, arg);
	if (logf)
		mds_unreserve_slot();

	INOH_LOCK(ih);
	ih->inoh_flags &= ~INOH_IN_IO;

	if (rc == 0 && nb != sizeof(ih->inoh_ino) + sizeof(crc))
		rc = SLERR_SHORTIO;

	if (rc)
		DEBUG_INOH(PLL_ERROR, ih,
		    "mdsio_pwritev: error (resid=%d nb=%d rc=%d)",
		    sizeof(ih->inoh_ino) + sizeof(crc), nb, rc);
	else {
		DEBUG_INOH(PLL_INFO, ih, "wrote inode (rc=%d) data=%p",
		    rc, inoh_2_mdsio_data(ih));
#ifdef SHITTY_PERFORMANCE
		rc = mdsio_fsync(&rootcreds, 1, inoh_2_mdsio_data(ih));
		if (rc == -1)
			psc_fatal("mdsio_fsync");
#endif
		if (ih->inoh_flags & INOH_INO_NEW)
			ih->inoh_flags &= ~INOH_INO_NEW;
	}
	return (rc);
}

int
mds_inox_write(struct slash_inode_handle *ih, void *logf, void *arg)
{
	struct iovec iovs[2];
	uint64_t crc;
	size_t nb;
	int rc;

	INOH_LOCK_ENSURE(ih);

	psc_assert(ih->inoh_extras);
	psc_crc64_calc(&crc, ih->inoh_extras, sizeof(*ih->inoh_extras));

	iovs[0].iov_base = ih->inoh_extras;
	iovs[0].iov_len = sizeof(*ih->inoh_extras);
	iovs[1].iov_base = &crc;
	iovs[1].iov_len = sizeof(crc);

	if (logf)
		mds_reserve_slot();
	rc = mdsio_pwritev(&rootcreds, iovs, nitems(iovs), &nb,
	    SL_EXTRAS_START_OFF, 0, inoh_2_mdsio_data(ih), logf, arg);
	if (logf)
		mds_unreserve_slot();

	if (rc == 0 && nb != sizeof(*ih->inoh_extras) + sizeof(crc))
		rc = SLERR_SHORTIO;

	if (rc) {
		DEBUG_INOH(PLL_ERROR, ih, "mdsio_pwritev: error (rc=%d)",
		    rc);
	} else {
#ifdef SHITTY_PERFORMANCE
		rc = mdsio_fsync(&rootcreds, 1, inoh_2_mdsio_data(ih));
		if (rc == -1)
			psc_fatal("mdsio_fsync");
#endif
	}
	return (rc);
}

int
mds_inox_load_locked(struct slash_inode_handle *ih)
{
	struct iovec iovs[2];
	uint64_t crc, od_crc;
	size_t nb;
	int rc;

	INOH_LOCK_ENSURE(ih);

	psc_assert(!(ih->inoh_flags & INOH_HAVE_EXTRAS));

	psc_assert(ih->inoh_extras == NULL);
	ih->inoh_extras = PSCALLOC(sizeof(*ih->inoh_extras));

	iovs[0].iov_base = ih->inoh_extras;
	iovs[0].iov_len = sizeof(*ih->inoh_extras);
	iovs[1].iov_base = &od_crc;
	iovs[1].iov_len = sizeof(od_crc);
	rc = mdsio_preadv(&rootcreds, iovs, nitems(iovs), &nb,
	    SL_EXTRAS_START_OFF, inoh_2_mdsio_data(ih));
	if (rc == 0 && od_crc == 0 &&
	    pfl_memchk(&ih->inoh_extras, 0, sizeof(*ih->inoh_extras))) {
		ih->inoh_flags |= INOH_HAVE_EXTRAS;
		rc = 0;
	} else if (rc) {
		DEBUG_INOH(PLL_ERROR, ih, "read inox: %d", rc);
	} else if (nb != sizeof(*ih->inoh_extras) + sizeof(od_crc)) {
		rc = SLERR_SHORTIO;
		DEBUG_INOH(PLL_ERROR, ih, "read inox: %d", rc);
	} else {
		psc_crc64_calc(&crc, ih->inoh_extras,
		    sizeof(*ih->inoh_extras));
		if (crc == od_crc)
			ih->inoh_flags |= INOH_HAVE_EXTRAS;
		else {
			psclog_errorx("inox CRC fail; "
			    "disk=%"PSCPRIxCRC64" mem=%"PSCPRIxCRC64,
			    od_crc, crc);
			rc = SLERR_BADCRC;
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
	if (ATTR_NOTSET(ih->inoh_flags, INOH_HAVE_EXTRAS))
		rc = mds_inox_load_locked(ih);
	INOH_URLOCK(ih, locked);
	return (rc);
}

int
mds_inode_addrepl_update(struct slash_inode_handle *ih,
    sl_ios_id_t ios, uint32_t pos, int log)
{
	struct slmds_jent_ino_addrepl jrir;
	int locked, rc = 0;
	void *logf;

	logf = log ? mds_inode_addrepl_log : NULL;

	locked = INOH_RLOCK(ih);

	if (log) {
		jrir.sjir_fid = fcmh_2_fid(ih->inoh_fcmh);
		jrir.sjir_ios = ios;
		jrir.sjir_pos = pos;
		jrir.sjir_nrepls = ih->inoh_ino.ino_nrepls;
	}
	if (pos < SL_DEF_REPLICAS)
		rc = mds_inode_write(ih, logf, &jrir);
	else
		rc = mds_inox_write(ih, logf, &jrir);
	psclog_info("update: fid="SLPRI_FID" log=%d",
	    fcmh_2_fid(ih->inoh_fcmh), log);
	INOH_URLOCK(ih, locked);
	return (rc);
}
