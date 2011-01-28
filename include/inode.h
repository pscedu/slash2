/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2007-2011, Pittsburgh Supercomputing Center (PSC).
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

#include "cache_params.h"
#include "fid.h"
#include "sltypes.h"

#define SL_DEF_SNAPSHOTS	1
#define SL_MAX_GENS_PER_BLK	4

/*
 * Define metafile offsets.  At the beginning of the metafile is the
 * SLASH2 inode, which is always loaded.  Start from byte offset 1024
 * (0x400) are extra inode attributes. Block CRCs begin at offset 4096
 * (0x1000).
 */
#define SL_INODE_START_OFF	UINT64_C(0x0000)
#define SL_EXTRAS_START_OFF	UINT64_C(0x0400)
#define SL_BMAP_START_OFF	UINT64_C(0x1000)

/*
 * Point to an offset within the linear metadata file which holds a
 * snapshot.  Snapshots are read-only and their metadata may not be
 * expanded.  Once the offset is established the slash_block structure
 * is used to index up to sn_nblks.
 */
typedef struct slash_snapshot {
	int64_t			sn_off;
	int64_t			sn_nblks;
	int64_t			sn_date;
} sl_snap_t;

/*
 * The inode structure lives at the beginning of the metafile and holds
 * the block store array along with snapshot pointers.
 */
struct slash_inode_od {
	uint16_t		ino_version;
	uint16_t		_ino_pad;
	uint32_t		ino_bsz;			/* bmap size */
	uint32_t		ino_nrepls;			/* if 0, use ino_prepl */
//	uint32_t		ino_csnap;			/* current snapshot */
	uint32_t		ino_replpol;			/* BRP_* policies */
	sl_replica_t		ino_repls[SL_DEF_REPLICAS];	/* embed a few replicas	*/

	/* must be last */
	uint64_t		ino_crc;			/* CRC of the inode */
};
#define INO_OD_SZ		sizeof(struct slash_inode_od)
#define INO_OD_CRCSZ		offsetof(struct slash_inode_od, ino_crc)

#define INO_VERSION		0x0001

struct slash_inode_extras_od {
	sl_snap_t		inox_snaps[SL_DEF_SNAPSHOTS];	/* snapshot pointers */
	sl_replica_t		inox_repls[SL_INOX_NREPLICAS];

	/* must be last */
	uint64_t		inox_crc;
};
#define INOX_OD_SZ		sizeof(struct slash_inode_extras_od)
#define INOX_OD_CRCSZ		offsetof(struct slash_inode_extras_od, inox_crc)

#endif /* _SLASH_INODE_H_ */
