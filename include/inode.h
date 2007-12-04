#ifndef _SLASH_INODE_H
#define _SLASH_INODE_H 1

#include "psc_types.h"

#define SL_DEF_REPLICAS     4
#define SL_DEF_SNAPSHOTS    16
#define SL_MAX_GENS_PER_BLK 4 
 
typedef u32 slash_ios_id_t; /* io server id: 16 bit site id
			     *               15 bit resource id 
			     *                1 bit metadata svr bool
			     */
/*
 * Point to an offset within the linear metadata file which holds a snapshot.  Snapshots are read-only and their metadata may not be expanded.  Once the offset is established the slash_block structure is used to index up to sn_nblks.
 */
struct slash_snapshot {
	off_t  sn_off;
	size_t sn_nblks;
	time_t sn_date;
} sl_snap_t;

/* 
 * Defines a storage system which holds a block or blocks of the respective file.  A number of these structures are statically allocated within the inode and are fixed for the life of the file and apply to snapshots as well as the active file.  This structure saves us from storing the iosystem id within each block at the cost of limiting the number of iosystems which may manage our blocks.
 */
struct slash_block_store {
	slash_ios_id_t bs_id;     /* id of this block store    */
	time_t         bs_lszup;  /* last size update          */
	off_t          bs_lsz;    /* last size seen            */
	int            bs_closed; /* expect no more sz updates */
} sl_bstore_t;

/*
 * Associate a crc with a generation id for a block.
 */
struct slash_gencrc {
	int   gc_gen;  /* generation number  */
	crc_t gc_crc;  /* crc for generation */
} sl_gcrc_t;

/*
 * Slim block structure just holds a generation number and a validation bit.  The io server id is held in the block store array.
 */
struct slash_block_desc {
	unsigned int bl_gen:31; /* generation number     */
	unsigned int bl_inv:1;  /* invalidated via ovwrt */
} sl_blkd_t;

/*
 * A block container which holds blocks, their checksums, and the number of replicas.
 */
struct slash_block_handle {
	u8        bh_nrepls;                       /* num replicas   */
	sl_gcrc_t bh_gen_crc[SL_MAX_GENS_PER_BLK]; /* array of crcs  */
	sl_blkd_t bh_blks[SL_DEF_REPLICAS];        /* blk structures */
} sl_blkh_t;

/*
 * The inode structure lives at the beginning of the metafile and holds the block store array along with snapshot pointers.
 */
struct slash_inode {
	u64          ino_fid;                    /* inode number            */
	off_t        ino_off;                    /* inode metadata offset   */
	size_t       ino_bsz;                    /* file block size         */
	size_t       ino_lblk;                   /* last block              */
	u32          ino_lblk_sz;                /* last block size         */
	sl_bstore_t  ino_repls[SL_DEF_REPLICAS]; /* io systems holding blks */
	sl_snap_t    ino_snaps[SL_DEF_SNAPS];    /* snapshot pointers       */
	crc_t        ino_crc;                    /* crc of the inode        */
} sl_inode_t;

#endif
