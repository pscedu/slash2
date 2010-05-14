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

#ifndef _SL_TYPES_H_
#define _SL_TYPES_H_

#include <sys/types.h>
#include <sys/stat.h>

#include <stdint.h>

#include "pfl/cdefs.h"

typedef uint32_t sl_bmapno_t;		/* file block map index */
typedef uint32_t sl_bmapgen_t;		/* file block map generation */

typedef uint16_t sl_siteid_t;
typedef uint32_t sl_ios_id_t;

typedef uint64_t sl_ino_t;

#define BLKNO_ANY		(~(sl_bmapno_t)0)	/* deprecated */
#define BMAPNO_ANY		((sl_bmapno_t)~0U)

#define IOS_ID_ANY		((sl_ios_id_t)~0U)
#define SITE_ID_ANY		((sl_siteid_t)~0U)

#define BMAPSEQ_ANY	 ((uint64_t)~0U)

/* breakdown of I/O system ID: # of bits for each part */
#define SL_SITE_BITS		16
#define SL_RES_BITS		16

#define SL_SITE_MASK		0xffff0000
#define SL_RES_MASK		0x0000ffff	/* resource mask */

/* I/O flags */
enum rw {
	SL_READ			= 42,
	SL_WRITE		= 43
};

enum rw fflags_2_rw(int);

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

#define SL_MAX_REPLICAS		64

/* Slash RPC transportably safe structures. */
struct srt_stat {
	uint64_t		sst_dev;	/* ID of device containing file */
	uint64_t		sst_ino;	/* inode number */
	uint32_t		sst_gen;	/* full truncate generation */
	uint32_t		sst_ptruncgen;	/* partial truncate generation */
	uint32_t		sst_mode;	/* file permissions */
	uint32_t		sst__pad;
	uint64_t		sst_nlink;	/* number of hard links */
	uint32_t		sst_uid;	/* user ID of owner */
	uint32_t		sst_gid;	/* group ID of owner */
	uint64_t		sst_rdev;	/* device ID (if special file) */
	uint64_t		sst_size;	/* total size, in bytes */
	int64_t			sst_blksize;	/* blocksize for file system I/O */
	int64_t			sst_blocks;	/* number of 512B blocks allocated */
	int64_t			sst_atime;	/* time of last access */
	int64_t			sst_mtime;	/* time of last modification */
	int64_t			sst_ctime;	/* time of last status change */
} __packed;

struct srt_statfs {
	uint64_t		sf_bsize;	/* file system block size */
	uint64_t		sf_frsize;	/* fragment size */
	uint64_t		sf_blocks;	/* size of fs in f_frsize units */
	uint64_t		sf_bfree;	/* # free blocks */
	uint64_t		sf_bavail;	/* # free blocks for non-root */
	uint64_t		sf_files;	/* # inodes */
	uint64_t		sf_ffree;	/* # free inodes */
	uint64_t		sf_favail;	/* # free inodes for non-root */
	uint64_t		sf_fsid;	/* file system ID */
	uint64_t		sf_flag;	/* mount flags */
	uint64_t		sf_namemax;	/* maximum filename length */
} __packed;

/* journal log callback operations */
#define	SL_NAMESPACE_OP_CREATE		1
#define	SL_NAMESPACE_OP_REMOVE		2
#define	SL_NAMESPACE_OP_ATTRIB		3

#define	SL_NAMESPACE_TYPE_DIR		1
#define	SL_NAMESPACE_TYPE_FILE		2
#define	SL_NAMESPACE_TYPE_LINK		3
#define	SL_NAMESPACE_TYPE_SYMLINK	4

typedef uint64_t slfid_t;
typedef uint64_t slfgen_t;
typedef uint64_t mdsio_fid_t;

typedef slfid_t (*sl_getslfid_cb)(void);
typedef void (*sl_jlog_cb)(int, int, uint64_t, uint64_t, uint64_t, const struct srt_stat *, uint, const char *);

#endif /* _SL_TYPES_H_ */
