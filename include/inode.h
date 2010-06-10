/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
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
 * Metadata about files resident in a SLASH network is stored in inodes.
 */

#ifndef _SLASH_INODE_H_
#define _SLASH_INODE_H_

#include <sys/types.h>
#include <sys/stat.h>

#include "pfl/types.h"
#include "psc_util/crc.h"
#include "psc_util/lock.h"
#include "psc_util/log.h"

#include "cache_params.h"
#include "fid.h"
#include "sltypes.h"

#define SL_DEF_SNAPSHOTS	1
#define SL_MAX_GENS_PER_BLK	4

/* Define metafile offsets
 */
#define SL_INODE_START_OFF	UINT64_C(0x0000)
#define SL_BMAP_START_OFF	UINT64_C(0x1000)
#define SL_EXTRAS_START_OFF	UINT64_C(0x0400)

#define SL_NULL_CRC		UINT64_C(0x436f5d7c450ed606)

/*
 * Point to an offset within the linear metadata file which holds a
 * snapshot.  Snapshots are read-only and their metadata may not be
 * expanded.  Once the offset is established the slash_block structure
 * is used to index up to sn_nblks.
 */
typedef struct slash_snapshot {
	off_t			sn_off;
	size_t			sn_nblks;
	time_t			sn_date;
} sl_snap_t;

#define INO_DEF_NREPLS		SL_DEF_REPLICAS

/*
 * The inode structure lives at the beginning of the metafile and holds
 * the block store array along with snapshot pointers.
 */
struct slash_inode_od {
	struct slash_fidgen	ino_fg;
	uint16_t		ino_version;
	uint16_t		ino_flags;
	uint32_t		ino_bsz;			/* bmap size		*/
	uint32_t		ino_nrepls;			/* if 0, use ino_prepl	*/
	uint32_t		ino_csnap;			/* current snapshot	*/
	sl_replica_t		ino_repls[INO_DEF_NREPLS];	/* embed a few replicas	*/

	/* must be last */
	psc_crc64_t		ino_crc;			/* crc of the inode	*/
};
#define INO_OD_SZ		sizeof(struct slash_inode_od)
#define INO_OD_CRCSZ		(INO_OD_SZ - (sizeof(psc_crc64_t)))

#define INO_VERSION		0x0006

struct slash_inode_extras_od {
	sl_snap_t		inox_snaps[SL_DEF_SNAPSHOTS];	/* snapshot pointers      */
	sl_replica_t		inox_repls[SL_MAX_REPLICAS - INO_DEF_NREPLS]; /* replicas */
	uint32_t		inox_newbmap_policy;		/* see BRP_* values */

	/* must be last */
	psc_crc64_t		inox_crc;
};
#define INOX_OD_SZ		sizeof(struct slash_inode_extras_od)
#define INOX_OD_CRCSZ		(INOX_OD_SZ - (sizeof(psc_crc64_t)))

/* File extended attribute names. */
#define SFX_INODE		"sl-inode"
#define SFX_REPLICAS		"sl-replicas"

#endif /* _SLASH_INODE_H_ */
