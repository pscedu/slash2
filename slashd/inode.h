/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015, Google, Inc.
 * Copyright 2007-2016, Pittsburgh Supercomputing Center
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

/*
 * Inodes contain part of the metadata about files resident in a SLASH2
 * deployment.  Inode handles are in-memory representations of this
 * metadata.
 */

#ifndef _SLASHD_INODE_H_
#define _SLASHD_INODE_H_

#include <sys/types.h>

#include <inttypes.h>
#include <limits.h>

#include "pfl/cdefs.h"
#include "pfl/str.h"
#include "pfl/lock.h"

#include "cache_params.h"
#include "fid.h"
#include "fidcache.h"
#include "sltypes.h"

#define SL_DEF_SNAPSHOTS	1

/*
 * Define metafile offsets.  At the beginning of the metafile is the
 * basic SLASH2 inode, which is always loaded. After the basic inode, 
 * we have an extra inode, which are followed by block maps for block 
 * 0, 1, 2, ...
 *
 *               SL_EXTRAS_START_OFF             SL_BMAP_START_OFF
 *
 *                     |                             |
 *                     v                             v
 * +-------------+-----+-------------------+---------+-------------------+-----
 * | inode + CRC | gap | extra inode + CRC |   gap   | block 0 map + CRC |
 * +-------------+-----+-------------------+---------+-------------------+-----
 *       72                   752                            1192 
 *                                                     struct bmap_ondisk 
 *
 * CRC are 8 bytes.  For a file with one bmap, the size is 2728 bytes.
 *
 */
#define SL_EXTRAS_START_OFF	((off_t)0x0200)
#define SL_BMAP_START_OFF	((off_t)0x0600)

/*
 * Point to an offset within the linear metadata file which holds a
 * snapshot.  Snapshots are read-only and their metadata may not be
 * expanded.  Once the offset is established the slash_block structure
 * is used to index up to sn_nblks.
 */
typedef struct {
	int64_t			sn_off;
	int64_t			sn_nblks;
	int64_t			sn_date;
} sl_snap_t;

#define INO_VERSION		0x0002

/*
 * The inode structure lives at the beginning of the metafile and holds
 * the block store array along with snapshot pointers.
 *
 * A 64-bit CRC checksum follows this structure on disk. 64 bytes in all.
 */
struct slm_ino_od {
	uint16_t		 ino_version;
	uint16_t		 ino_flags;			/* immutable, etc. */
	uint32_t		 ino_bsz;			/* bmap size */
	/*
 	 * Incremented in _mds_repl_ios_lookup().
 	 */
	uint32_t		 ino_nrepls;			/* number of replicas */
	uint32_t		 ino_replpol;			/* BRPOL_ONETIME or BRPOL_PERSIST */
	sl_replica_t		 ino_repls[SL_DEF_REPLICAS];	/* embed a few replicas	*/
	uint64_t		 ino_repl_nblks[SL_DEF_REPLICAS];/* st_blocks constituents */
};

#define slash_inode_od slm_ino_od

#define INOF_IOS_AFFINITY	(1 << 0)			/* Prefer existing IOS for new bmaps */

/*
 * A 64-bit CRC checksum follows this structure on disk. 744 bytes in all.
 */
struct slm_inox_od {
	sl_snap_t		 inox_snaps[SL_DEF_SNAPSHOTS];	/* snapshot pointers */
	sl_replica_t		 inox_repls[SL_INOX_NREPLICAS];
	uint64_t		 inox_repl_nblks[SL_INOX_NREPLICAS];
};

#define slash_inode_extras_od slm_inox_od

#define INOX_SZ			sizeof(struct slm_inox_od)

/* in memory handle */
struct slm_inoh {
	struct slm_ino_od	 inoh_ino;
	struct slm_inox_od	*inoh_extras;
	int			 inoh_flags;		/* see INOH_* */
};

#define slash_inode_handle	 slm_inoh

/* inoh_flags */
#define INOH_INO_NEW		(1 << 0)		/* not yet written to disk */

/* 
 * not very useful, because if we fail the whole fcmh is freed.
 */
#define INOH_INO_NOTLOADED	(1 << 1)	

#define INOH_GETLOCK(ih)	(&inoh_2_fcmh(ih)->fcmh_lock)
#define INOH_LOCK(ih)		spinlock(INOH_GETLOCK(ih))
#define INOH_ULOCK(ih)		freelock(INOH_GETLOCK(ih))
#define INOH_RLOCK(ih)		reqlock(INOH_GETLOCK(ih))
#define INOH_URLOCK(ih, lk)	ureqlock(INOH_GETLOCK(ih), (lk))
#define INOH_LOCK_ENSURE(ih)	LOCK_ENSURE(INOH_GETLOCK(ih))

#define inoh_2_mfhp(ih)		(fcmh_isdir(inoh_2_fcmh(ih)) ?		\
				 fcmh_2_dino_mfhp(inoh_2_fcmh(ih)) :	\
				 fcmh_2_mfhp(inoh_2_fcmh(ih)))

#define inoh_2_mfh(ih)		inoh_2_mfhp(ih)->fh

#define inoh_2_fsz(ih)		fcmh_2_fsz(inoh_2_fcmh(ih))
#define inoh_2_fid(ih)		fcmh_2_fid(inoh_2_fcmh(ih))
#define inoh_2_fcmh(ih)		fmi_2_fcmh(inoh_2_fmi(ih))
#define inoh_2_fcmh_const(ih)	fmi_2_fcmh_const(inoh_2_fmi_const(ih))

#define DEBUG_INOH(level, ih, buf, fmt, ...)				\
	psclog((level), "inoh@%p fcmh=%p f+g="SLPRI_FG" "		\
	    "fl:%#x:%s%s %s :: " fmt,					\
	    (ih), inoh_2_fcmh_const(ih),				\
	    SLPRI_FG_ARGS(&inoh_2_fcmh_const(ih)->fcmh_fg),		\
	    (ih)->inoh_flags,						\
	    (ih)->inoh_flags & INOH_INO_NEW	  ? "N" : "",		\
	    (ih)->inoh_flags & INOH_INO_NOTLOADED ? "L" : "",		\
	    _dump_ino((buf), LINE_MAX, &(ih)->inoh_ino), ## __VA_ARGS__)

struct sl_ino_compat {
	int			(*sic_read_ino)(struct slash_inode_handle *);
	int			(*sic_read_inox)(struct slash_inode_handle *);
	int			(*sic_read_bmap)(struct bmapc_memb *, void *);
};

int	mds_inode_update(int, struct slash_inode_handle *, int);
int	mds_inode_update_interrupted(int, struct slash_inode_handle *, int *);
int	mds_inode_read(struct slash_inode_handle *);
int	mds_inode_write(int, struct slash_inode_handle *, void *, void *);
int	mds_inox_write(int, struct slash_inode_handle *, void *, void *);

int	mds_inox_load_locked(struct slash_inode_handle *);
int	mds_inox_ensure_loaded(struct slash_inode_handle *);

int	mds_inodes_odsync(int, struct fidc_membh *, void (*logf)(void *, uint64_t, int));

char	*_dump_ino(char *, size_t, const struct slash_inode_od *);

extern struct sl_ino_compat sl_ino_compat_table[];

#endif /* _SLASHD_INODE_H_ */
