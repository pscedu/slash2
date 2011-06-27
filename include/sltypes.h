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

#ifndef _SL_TYPES_H_
#define _SL_TYPES_H_

#include <sys/types.h>
#include <sys/stat.h>

#include <stdint.h>
#include <pthread.h>

#include "pfl/cdefs.h"
#include "pfl/pfl.h"

#include "cache_params.h"

typedef uint32_t sl_bmapno_t;			/* file block map index */
typedef uint32_t sl_bmapgen_t;			/* file block map generation */

typedef uint16_t sl_siteid_t;
typedef uint32_t sl_ios_id_t;

#define BMAPNO_ANY		((sl_bmapno_t)~0U)

#define IOS_ID_ANY		((sl_ios_id_t)~0U)
#define SITE_ID_ANY		((sl_siteid_t)~0U)

#define BMAPSEQ_ANY		(~UINT64_C(0))

/* breakdown of I/O system ID: # of bits for each part */
#define SL_SITE_BITS		16
#define SL_RES_BITS		16

#define SL_SITE_MASK		0xffff0000
#define SL_RES_MASK		0x0000ffff	/* resource ID mask */

/* thread local storage */
#define SL_TLSIDX_FIDBUF	(PFL_TLSIDX_LASTRESERVED)

#define SL_NAME_MAX		255		/* file name component length */
#define SL_PATH_MAX		4096		/* file path name length */

#define SL_TWO_NAME_MAX		364		/* room for at most two names */

/* I/O flags */
enum rw {
	SL_READ			= 42,
	SL_WRITE		= 43
//	SL_RDWR			= 44
};

/*
 * Defines a storage system which can hold a block or blocks of a file.  A number
 * of these structures are statically allocated within the inode of the file and
 * are fixed for the lifetime of the file.  They apply to snapshots as well as
 * the active file.  Such an arrangement saves us from storing the I/O system id
 * within each block at the cost of limiting the number of I/O systems which may
 * manage the blocks of a given file.
 */
typedef struct {
	sl_ios_id_t		bs_id;		/* ID of this block store    */
} __packed sl_replica_t;

/*
 * The default and the maximum number of storage systems that can hold blocks
 * of any given file.
 */
#define SL_DEF_REPLICAS		4
#define SL_MAX_REPLICAS		64

#define SL_INOX_NREPLICAS	(SL_MAX_REPLICAS - SL_DEF_REPLICAS)

typedef uint64_t slfid_t;
typedef uint64_t slfgen_t;

#define SL_SETATTRF_METASIZE	(_PSCFS_SETATTRF_LAST << 0)	/* metadata file */
#define SL_SETATTRF_PTRUNCGEN	(_PSCFS_SETATTRF_LAST << 1)	/* partial truncates */
#define SL_SETATTRF_GEN		(_PSCFS_SETATTRF_LAST << 2)	/* full truncate */
#define SL_SETATTRF_NBLKS	(_PSCFS_SETATTRF_LAST << 3)	/* st_blocks */

#define SL_SETATTRF_FREPLPOL	SL_SETATTRF_PTRUNCGEN

#define SL_SETATTRF_CLI_ALL						\
	(PSCFS_SETATTRF_MODE | PSCFS_SETATTRF_UID |			\
	 PSCFS_SETATTRF_GID  | PSCFS_SETATTRF_DATASIZE |		\
	 PSCFS_SETATTRF_ATIME | PSCFS_SETATTRF_MTIME | PSCFS_SETATTRF_CTIME)

#define	SLASH2_IGNORE_MTIME	0x400000

struct sl_timespec {
	uint64_t		tv_sec;
	uint64_t		tv_nsec;
};

#define SLPRI_TIMESPEC		"%"PRId64":%06"PRId64
#define SLPRI_TIMESPEC_ARGS(ts)	(ts)->tv_sec, (ts)->tv_nsec

#define SL_GETTIMESPEC(ts)						\
	do {								\
		struct timespec _ts;					\
									\
		PFL_GETTIMESPEC(&_ts);					\
		(ts)->tv_sec = _ts.tv_sec;				\
		(ts)->tv_nsec = _ts.tv_nsec;				\
	} while (0)

#endif /* _SL_TYPES_H_ */
