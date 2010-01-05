/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2009, Pittsburgh Supercomputing Center (PSC).
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

/*
 * To save space in the bmaps, replica stores are kept in the sl-replicas
 *   xattr.  Each bmap uses an array of char's as a bitmap to track which
 *   stores the bmap is replicated to.  Additional bits are used to specify
 *   the freshness of the replica bmaps.  '100' would mean that the bmap
 *   is up-to-date, '110' would mean that the bmap is only one generation
 *   back and therefore may take partial updates.  111 means that the bmap
 *   is more than one generation old.
 * '00' - bmap is not replicated to this ios.
 * '01' - bmap is > one generation back.
 * '10' - bmap is one generation back.
 * '11' - bmap is replicated to the ios and current.
 */
#define SL_MAX_REPLICAS		64
#define SL_BITS_PER_REPLICA	2
#define SL_REPLICA_MASK		((uint8_t)((1 << SL_BITS_PER_REPLICA) - 1))
#define SL_REPLICA_NBYTES	((SL_MAX_REPLICAS * SL_BITS_PER_REPLICA) / NBBY)

#define SL_DEF_SNAPSHOTS	1
#define SL_MAX_GENS_PER_BLK	4

#define SL_BMAP_SIZE		SLASH_BMAP_SIZE
#define SL_CRC_SIZE		(1024 * 1024)
#define SL_CRCS_PER_BMAP	(SL_BMAP_SIZE / SL_CRC_SIZE)

/* Define metafile offsets
 */
#define SL_INODE_START_OFF	UINT64_C(0x0000)
#define SL_BMAP_START_OFF	UINT64_C(0x1000)
#define SL_EXTRAS_START_OFF	UINT64_C(0x0400)

#define SL_NULL_CRC		UINT64_C(0x436f5d7c450ed606)

#define SL_REPL_INACTIVE	0
#define SL_REPL_SCHED		1
#define SL_REPL_OLD		2
#define SL_REPL_ACTIVE		3

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

/*
 * Defines a storage system which holds a block or blocks of the
 * respective file.  A number of these structures are statically
 * allocated within the inode and are fixed for the life of the file
 * and apply to snapshots as well as the active file.  This structure
 * saves us from storing the iosystem id within each block at the cost
 * of limiting the number of iosystems which may manage our blocks.
 */
typedef struct slash_replica {
	sl_ios_id_t		bs_id;     /* id of this block store    */
} __packed sl_replica_t;

/*
 * Associate a crc with a generation id for a block.
 */
typedef struct slash_gencrc {
	psc_crc64_t		gc_crc;
} sl_gcrc_t;

struct slash_bmap_cli_wire {
	uint8_t			bw_crcstates[SL_CRCS_PER_BMAP];
	uint8_t			bw_repls[SL_REPLICA_NBYTES];
};

#define INO_DEF_NREPLS		4

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

#define INO_VERSION		0x0003

enum {
	INO_FL_HAVE_EXTRAS = (1<<0)
};

struct slash_inode_extras_od {
	sl_snap_t		inox_snaps[SL_DEF_SNAPSHOTS];	/* snapshot pointers      */
	sl_replica_t		inox_repls[SL_MAX_REPLICAS - INO_DEF_NREPLS]; /* replicas */
	uint32_t		inox_newbmap_policy;		/* see BRP_* values */

	/* must be last */
	psc_crc64_t		inox_crc;
};
#define INOX_OD_SZ		sizeof(struct slash_inode_extras_od)
#define INOX_OD_CRCSZ		(INOX_OD_SZ - (sizeof(psc_crc64_t)))

#define SL_ROOT_INUM		1

/* File extended attribute names. */
#define SFX_INODE		"sl-inode"
#define SFX_REPLICAS		"sl-replicas"

#endif /* _SLASH_INODE_H_ */
