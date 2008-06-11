/* $Id$ */

#ifndef __SLASH_INODE_H__
#define __SLASH_INODE_H__

#include <sys/types.h>
#include <sys/stat.h>

#include "psc_types.h"
#include "psc_util/assert.h"
#include "psc_util/crc.h"

#include "fid.h"

#define SL_DEF_REPLICAS     4
#define SL_DEF_SNAPSHOTS    16
#define SL_MAX_GENS_PER_BLK 4

#define SL_SITE_BITS 16
#define SL_RES_BITS  15
#define SL_MDS_BITS  1

typedef u32 sl_inum_t;
typedef u32 sl_blkno_t;  /* block number type */
typedef u32 sl_ios_id_t; /* io server id: 16 bit site id
			  *               15 bit resource id
			  *                1 bit metadata svr bool
			  */

#define IOS_ID_ANY (~(sl_ios_id_t)0)
#define BLKNO_ANY  (~(sl_blkno_t)0)

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
typedef struct slash_block_store {
	sl_ios_id_t bs_id;     /* id of this block store    */
	time_t      bs_lszup;  /* last size update          */
	off_t       bs_lsz;    /* last size seen            */
	int         bs_closed; /* expect no more sz updates */
} sl_bstore_t;

/*
 * Associate a crc with a generation id for a block.
 */
typedef struct slash_gencrc {
	int       gc_gen:31;      /* generation number  */
	int       gc_crc_valid:1; /* generation number  */
	psc_crc_t gc_crc;         /* crc for generation */
} sl_gcrc_t;

/*
 * Slim block structure just holds a generation number and a
 * validation bit.  The io server id is held in the block store array.
 */
typedef struct slash_block_desc {
	unsigned int bl_gen:31; /* generation number     */
	unsigned int bl_inv:1;  /* invalidated via ovwrt */
} sl_blkd_t;

/*
 * A block container which holds blocks, their checksums, and the number of replicas.
 */
typedef struct slash_block_handle {
	u64       bh_magic;                        /* set if i'm not a hole */
	u8        bh_nrepls;                       /* num replicas   */
	sl_gcrc_t bh_gen_crc[SL_MAX_GENS_PER_BLK]; /* array of crcs  */
	sl_blkd_t bh_blks[SL_DEF_REPLICAS];        /* blk structures */
} sl_blkh_t;

/*
 * The inode structure lives at the beginning of the metafile and holds
 * the block store array along with snapshot pointers.
 */
typedef struct slash_inode {
	slfid_t      ino_fid;			 /* lower 48 bits are inum  */
	off_t        ino_off;                    /* inode metadata offset   */
	size_t       ino_bsz;                    /* file block size         */
	size_t       ino_lblk;                   /* last block              */
	u32          ino_lblk_sz;                /* last block size         */
	sl_bstore_t  ino_repls[SL_DEF_REPLICAS]; /* io systems holding blks */
	sl_snap_t    ino_snaps[SL_DEF_SNAPSHOTS];/* snapshot pointers       */
	struct stat  ino_stb;                    /* stat buf, on disk       */
	psc_crc_t    ino_crc;                    /* crc of the inode        */
} sl_inode_t;

#endif /* __SLASH_INODE_H__ */
