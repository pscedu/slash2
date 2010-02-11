/* $Id$ */

#include <sys/types.h>

#include <inttypes.h>

#include "psc_util/log.h"

#define DEBUG_STATBUF(level, stb, fmt, ...)					\
	psc_logs((level), PSS_GEN,						\
	    "stb (%p) dev:%"PRIu64" inode:%"PRId64" mode:0%o "			\
	    "nlink:%"PRIu64" uid:%u gid:%u "					\
	    "rdev:%"PRIu64" sz:%"PRId64" blksz:%ld "				\
	    "blkcnt:%"PRId64" atime:%lu mtime:%lu ctime:%lu " fmt,		\
	    (stb), (uint64_t)(stb)->st_dev, (stb)->st_ino, (stb)->st_mode,	\
	    (uint64_t)(stb)->st_nlink, (stb)->st_uid, (stb)->st_gid,		\
	    (uint64_t)(stb)->st_rdev, (stb)->st_size, (stb)->st_blksize,	\
	    (stb)->st_blocks, (stb)->st_atime, (stb)->st_mtime,			\
	    (stb)->st_ctime, ## __VA_ARGS__)

void dump_fflags(int);
void dump_statbuf(int, const struct stat *);
