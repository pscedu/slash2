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

#include "pfl/cdefs.h"

#include "inode.h"
#include "mdsio.h"
#include "mdslog.h"
#include "slerr.h"
#include "sljournal.h"

__static void
mds_inode_od_initnew(struct slash_inode_handle *ih)
{
	ih->inoh_flags = INOH_INO_NEW | INOH_INO_DIRTY;

	/* For now this is a fixed size. */
	ih->inoh_ino.ino_bsz = SLASH_BMAP_SIZE;
	ih->inoh_ino.ino_version = INO_VERSION;
	ih->inoh_ino.ino_nrepls = 0;
}

int
mds_inode_read(struct slash_inode_handle *ih)
{
	uint64_t crc;
	int rc, locked;

	locked = reqlock(&ih->inoh_lock);
	psc_assert(ih->inoh_flags & INOH_INO_NOTLOADED);

	rc = mdsio_inode_read(ih);

	if (rc == SLERR_SHORTIO && ih->inoh_ino.ino_crc == 0 &&
	    pfl_memchk(&ih->inoh_ino, 0, INO_OD_CRCSZ)) {
		DEBUG_INOH(PLL_INFO, ih, "detected a new inode");
		mds_inode_od_initnew(ih);
		rc = 0;

	} else if (rc) {
		DEBUG_INOH(PLL_WARN, ih, "mdsio_inode_read: %d", rc);

	} else {
		psc_crc64_calc(&crc, &ih->inoh_ino, INO_OD_CRCSZ);
		if (crc == ih->inoh_ino.ino_crc) {
			ih->inoh_flags &= ~INOH_INO_NOTLOADED;
			DEBUG_INOH(PLL_INFO, ih, "successfully loaded inode od");
		} else {
			DEBUG_INOH(PLL_WARN, ih, "CRC failed "
			    "want=%"PSCPRIxCRC64", got=%"PSCPRIxCRC64,
			    ih->inoh_ino.ino_crc, crc);
			rc = EIO;
		}
	}
	ureqlock(&ih->inoh_lock, locked);
	return (rc);
}

int
mds_inox_load_locked(struct slash_inode_handle *ih)
{
	uint64_t crc;
	int rc;

	INOH_LOCK_ENSURE(ih);

	psc_assert(!(ih->inoh_flags & INOH_HAVE_EXTRAS));

	psc_assert(ih->inoh_extras == NULL);
	ih->inoh_extras = PSCALLOC(sizeof(*ih->inoh_extras));

	rc = mdsio_inode_extras_read(ih);
	if (rc == SLERR_SHORTIO || (ih->inoh_extras->inox_crc == 0 &&
	    pfl_memchk(&ih->inoh_extras, 0, INOX_OD_CRCSZ))) {
		ih->inoh_flags |= INOH_HAVE_EXTRAS;
		rc = 0;
	} else if (rc) {
		DEBUG_INOH(PLL_WARN, ih, "mdsio_inode_extras_read: %d", rc);
	} else {
		psc_crc64_calc(&crc, ih->inoh_extras, INOX_OD_CRCSZ);
		if (crc == ih->inoh_extras->inox_crc)
			ih->inoh_flags |= INOH_HAVE_EXTRAS;
		else {
			psclog_errorx("inox CRC fail; "
			    "disk=%"PSCPRIxCRC64" mem=%"PSCPRIxCRC64,
			    ih->inoh_extras->inox_crc, crc);
			rc = EIO;
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
mds_inode_addrepl_update(struct slash_inode_handle *inoh,
    sl_ios_id_t ios, uint32_t pos, int log)
{
	struct slmds_jent_ino_addrepl jrir;
	int locked, rc = 0;
	void *logf;

	logf = log ? mds_inode_addrepl_log : NULL;

	locked = reqlock(&inoh->inoh_lock);

	psc_assert((inoh->inoh_flags & INOH_INO_DIRTY) ||
		   (inoh->inoh_flags & INOH_EXTRAS_DIRTY));

	if (log) {
		jrir.sjir_fid = fcmh_2_fid(inoh->inoh_fcmh);
		jrir.sjir_ios = ios;
		jrir.sjir_pos = pos;
		jrir.sjir_nrepls = inoh->inoh_ino.ino_nrepls;
		mds_reserve_slot();
	}
	if (inoh->inoh_flags & INOH_INO_DIRTY) {
		psc_crc64_calc(&inoh->inoh_ino.ino_crc, &inoh->inoh_ino,
		    INO_OD_CRCSZ);

		rc = mdsio_inode_write(inoh, logf, &jrir);

		inoh->inoh_flags &= ~INOH_INO_DIRTY;
		if (inoh->inoh_flags & INOH_INO_NEW) {
			inoh->inoh_flags &= ~INOH_INO_NEW;
			//inoh->inoh_flags |= INOH_EXTRAS_DIRTY;
		}
		psclog_info("fid="SLPRI_FID" crc=%"PSCPRIxCRC64" log=%d",
		    fcmh_2_fid(inoh->inoh_fcmh), inoh->inoh_ino.ino_crc,
		    log);

		logf = NULL;
	}

	if (inoh->inoh_flags & INOH_EXTRAS_DIRTY) {
		psc_crc64_calc(&inoh->inoh_extras->inox_crc,
		    inoh->inoh_extras, INOX_OD_CRCSZ);

		rc = mdsio_inode_extras_write(inoh, logf, &jrir);

		inoh->inoh_flags &= ~INOH_EXTRAS_DIRTY;
		psclog_info("update: fid="SLPRI_FID", extra crc=%"
		    PSCPRIxCRC64", log=%d", fcmh_2_fid(inoh->inoh_fcmh),
		    inoh->inoh_extras->inox_crc, log);
	}
	if (log)
		mds_unreserve_slot();

	ureqlock(&inoh->inoh_lock, locked);
	return (rc);
}
