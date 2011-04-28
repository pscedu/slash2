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

#include "inode.h"
#include "mdsio.h"
#include "mdslog.h"
#include "slerr.h"
#include "sljournal.h"

struct sl_ino_compat sl_ino_compat_table[] = {
/* 1 */	{ mds_ino_read_v1, mds_inox_read_v1, 0x400, 0x1000 },
/* 2 */	{ NULL, NULL, SL_EXTRAS_START_OFF, SL_BMAP_START_OFF },
};

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
	psc_crc64_t crc;
	size_t nb;
	int rc;

	rc = mdsio_read(&rootcreds, &ino, sizeof(ino), &nb, 0,
	    inoh_2_mdsio_data(ih));

	if (rc)
		return (rc);

	psc_crc64_calc(&crc, &ino, sizeof(ino) - sizeof(crc));
	if (crc != ino.ino_crc)
		return (EIO);
	memset(&ih->ih_ino, 0, sizeof(ih->ih_ino));
	ih->ih_ino.ino_version = ino.ino_version;
	ih->ih_ino.ino_bsz = ino.ino_bsz;
	ih->ih_ino.ino_nrepls = ino.ino_nrepls;
	ih->ih_ino.ino_replpol = ino.ino_replpol;
	ih->ih_ino.ino_replpol = ino.ino_replpol;
	memcpy(ih->ih_ino.ino_repls, ino.ino_repls,
	    sizeof(ino.ino_repls));
	return (0);
}

int
mds_inox_read_v1(struct slash_inode_handle *ih)
{
	struct {
		sl_snap_t	inox_snaps[1];	/* snapshot pointers */
		sl_replica_t	inox_repls[60];
		uint64_t	inox_crc;
	} inox;
	psc_crc64_t crc;
	size_t nb;
	int rc;

	rc = mdsio_read(&rootcreds, &inox, sizeof(inox), &nb, 0,
	    inoh_2_mdsio_data(ih));

	if (rc)
		return (rc);

	psc_crc64_calc(&crc, &inox, sizeof(inox) - sizeof(crc));
	if (crc != inox.inox_crc)
		return (EIO);
	memset(&ih->ih_extras, 0, sizeof(*ih->ih_extras));
	memset(&ih->ih_extras->inox_snaps, inox.inox_snaps,
	    sizeof(inox.inox_snaps));
	memset(&ih->ih_extras->inox_repls, inox.inox_repls,
	    sizeof(inox.inox_repls));
	return (0);
}
