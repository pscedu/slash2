/* $Id$ */

#ifndef __SLASH_INODE_H__
#define __SLASH_INODE_H__

#include <sys/types.h>
#include <sys/stat.h>

#include "psc_types.h"
#include "psc_util/assert.h"
#include "psc_util/crc.h"
#include "psc_util/lock.h"

#include "cache_params.h"
#include "fid.h"


/* To save space in the bmaps, replica stores are kept in the sl-replicas 
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
#define SL_MAX_REPLICAS     64
#define SL_BITS_PER_REPLICA 2
#define SL_REPLICA_NBYTES   ((SL_MAX_REPLICAS * SL_BITS_PER_REPLICA) /	\
			     (sizeof(u8)))

#define SL_DEF_SNAPSHOTS    16
#define SL_MAX_GENS_PER_BLK 4

#define SL_SITE_BITS 16
#define SL_RES_BITS  15
#define SL_MDS_BITS  1

#define SL_BMAP_SIZE  SLASH_BMAP_SIZE
#define SL_CRC_SIZE   1048576
#define SL_CRCS_PER_BMAP (SL_BMAP_SIZE / 1048576)

#define SL_NULL_CRC 0x436f5d7c450ed606ULL
#define SL_NULL_BMAPOD_CRC 0xb75884187c18a4f2ULL /* obtained from tests/crc */

#define SL_REPL_INACTIVE 0
#define SL_REPL_TOO_OLD  1
#define SL_REPL_OLD      2
#define SL_REPL_ACTIVE   3

typedef u32 sl_mds_id_t;
typedef u64 sl_inum_t;
typedef u32 sl_blkno_t;  /* block number type */
typedef u32 sl_ios_id_t; /* io server id: 16 bit site id
			  *               15 bit resource id
			  *                1 bit metadata svr bool
			  */

#define IOS_ID_ANY (~(sl_ios_id_t)0)
#define BLKNO_ANY  (~(sl_blkno_t)0)

#define SL_MDS_ID_BITS   8 /* These 8 bits compose the first bits of the inode
			    *  number */
#define SL_MDS_ID_MASK ((2 << SL_MDS_ID_BITS) - 1)

/*
 * sl_global_id_build - produce a unique 32 bit identifier from the
 *	object's site and resource id's.
 * @site_id:  id number of the resource's site
 * @res_id:   id number within the site
 * @mds_bool: is this a metadata server?
 */
static inline sl_ios_id_t
sl_global_id_build(u32 site_id, u32 res_id, u32 mds_bool)
{
	sl_ios_id_t ios_id = 0;

	psc_assert(site_id  <= ((1 << SL_SITE_BITS))-1);
	psc_assert(res_id   <= ((1 << SL_RES_BITS))-1);
	psc_assert(mds_bool <= ((1 << SL_MDS_BITS))-1);

	ios_id = site_id << SL_SITE_BITS;
	ios_id |= (res_id + mds_bool);

	return ios_id;
}

static inline u32
sl_glid_to_resid(sl_ios_id_t glid)
{
	sl_ios_id_t tmp = 0;

	tmp = ((1 << SL_SITE_BITS)-1) << (SL_RES_BITS + SL_MDS_BITS);

	return (u32)(glid & ~tmp);
}

/*
 * Point to an offset within the linear metadata file which holds a
 * snapshot.  Snapshots are read-only and their metadata may not be
 * expanded.  Once the offset is established the slash_block structure
 * is used to index up to sn_nblks.
 */
typedef struct slash_snapshot {
	off_t  sn_off;
	size_t sn_nblks;
	time_t sn_date;
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
	sl_ios_id_t bs_id;     /* id of this block store    */
} sl_replica_t;

/*
 * Associate a crc with a generation id for a block.
 */
typedef struct slash_gencrc {
	psc_crc_t gc_crc;
} sl_gcrc_t;

/*
 * Slim block structure just holds a generation number and a
 * validation bit.  The io server id is held in the block store array.
 */
typedef struct slash_block_gen {
	unsigned int bl_gen; /* generation number     */
} sl_blkgen_t;

/*
 * A block container which holds a bmap.  Included are the bmap's checksums and the replication table.
 * XXX Notes:  use the bh_gen_crc[] to denote holes within the bmap using the CRC of \0's.
 */
typedef struct slash_block_handle {
	sl_blkgen_t bh_gen;                       /* current generation num */
	sl_gcrc_t   bh_crcs[SL_CRCS_PER_BMAP];    /* array of crcs          */
	u8          bh_crcstates[SL_CRCS_PER_BMAP]; /* crc descriptor bits  */
	u8          bh_repls[SL_REPLICA_NBYTES];  /* replica bit map        */
	psc_crc_t   bh_bhcrc;                     /* on-disk bmap crc       */
} sl_blkh_t;

#define BMAP_OD_SZ (sizeof(sl_blkh_t))
#define BMAP_OD_CRCSZ (sizeof(sl_blkh_t)-(sizeof(psc_crc_t)))

/*
 * The inode structure lives at the beginning of the metafile and holds
 * the block store array along with snapshot pointers.
 * 
 * Replica tables are held here as opposed to  
 */
typedef struct slash_inode_store {
	struct slash_fidgen ino_fg;
	off_t         ino_off;                    /* inode metadata offset   */
	size_t        ino_bsz;                    /* file block size         */
	size_t        ino_lblk;                   /* last block              */
	u32           ino_lblk_sz;                /* last block size         */
	sl_snap_t     ino_snaps[SL_DEF_SNAPSHOTS];/* snapshot pointers       */
	u32           ino_csnap;                  /* current snapshot        */
	size_t        ino_nrepls;                 /* if 0, use ino_prepl     */
	sl_replica_t  ino_prepl;                  /* primary replica         */
	sl_replica_t  ino_repls[SL_MAX_REPLICAS]; /* replicas                */
	psc_crc_t     ino_rs_crc;                 /* crc of the replicas     */
	psc_crc_t     ino_crc;                    /* crc of the inode        */
} sl_inode_mds_t;


typedef struct slash_inode_handle {
	sl_inode_mds_t inoh_ino;
	psc_spinlock_t inoh_lock;
	int            inoh_flags;
	sl_replica_t  *inoh_replicas;
} sl_inodeh_t;

#define INOH_LOCK(h) spinlock(&(i)->inoh_lock)
#define INOH_ULOCK(h) freelock(&(i)->inoh_lock)
#define INOH_LOCK_ENSURE(h) LOCK_ENSURE(&(i)->inoh_lock)

enum slash_inode_handle_flags {	
	INOH_INO_DIRTY = (1<<0), /* Inode structures need to be written */
	INOH_REP_DIRTY = (1<<1), /* Replication structures need written */
	INOH_HAVE_REPS = (1<<2)
};

#define FCMH_2_INODEP(f) (&(f)->fcmh_memb.fcm_inodeh.inoh_ino)

#define COPYFID(d, s) 				\
	memcpy((d), (s), sizeof(*(d)))
	

#define INOH_FLAG(field, str) ((field) ? (str) : "")
#define DEBUG_INOH_FLAGS(i)					\
	INOH_FLAG((i)->inoh_flags & INOH_INO_DIRTY, "D"),	\
	INOH_FLAG((i)->inoh_flags & INOH_REP_DIRTY, "d"),	\
	INOH_FLAG((i)->inoh_flags & INOH_HAVE_REPS, "r")

#define INOH_FLAGS_FMT "%s%s%s"

#define DEBUG_INOH(level, i, fmt, ...)					\
	psc_logs(PSS_OTHER, (level),					\
		" inoh@%p f:"FIDFMT" fl:"INOH_FLAGS_FMT			\
		"o:%"_P_U64"x bsz:%zu lb:%zu "				\
		"lbsz:%u cs:%u pr:%u nr:%zu icrc:%"_P_U64"x "		\
		"rcrc:%"_P_U64"x :: "fmt,				\
		(i), FIDFMTARGS(&(i)->inoh_ino.ino_fg),			\
		DEBUG_INOH_FLAGS(i),					\
		(i)->inoh_ino.ino_off, (i)->inoh_ino.ino_bsz,		\
		(i)->inoh_ino.ino_lblk,					\
		(i)->inoh_ino.ino_lblk_sz, (i)->inoh_ino.ino_csnap,	\
		(i)->inoh_ino.ino_prepl.bs_id,				\
		(i)->inoh_ino.ino_nrepls,				\
		(i)->inoh_ino.ino_rs_crc, (i)->inoh_ino.ino_crc,	\
		## __VA_ARGS__)

/* File extended attribute names. */
#define SFX_INODE	"sl-inode"
#define SFX_REPLICAS    "sl-replicas"
#endif /* __SLASH_INODE_H__ */
