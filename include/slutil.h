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

#ifndef _SLUTIL_H_
#define _SLUTIL_H_

#include <sys/types.h>

#include <inttypes.h>

struct stat;
struct statvfs;

struct slash_creds;
struct srt_stat;

#define DEBUG_STATBUF(level, stb, fmt, ...)					\
	psc_logs((level), PSS_GEN,						\
	    "stb (%p) dev:%"PRIu64" inode:%"PRId64" mode:0%o "			\
	    "nlink:%"PRIu64" uid:%u gid:%u "					\
	    "rdev:%"PRIu64" sz:%"PRId64" blksz:%"PSCPRI_BLKSIZE_T" "		\
	    "blkcnt:%"PRId64" atime:%lu mtime:%lu ctime:%lu " fmt,		\
	    (stb), (uint64_t)(stb)->st_dev, (stb)->st_ino, (stb)->st_mode,	\
	    (uint64_t)(stb)->st_nlink, (stb)->st_uid, (stb)->st_gid,		\
	    (uint64_t)(stb)->st_rdev, (stb)->st_size, (stb)->st_blksize,	\
	    (stb)->st_blocks, (stb)->st_atime, (stb)->st_mtime,			\
	    (stb)->st_ctime, ## __VA_ARGS__)

void	dump_fflags(int);
void	dump_statbuf(int, const struct stat *);

void	print_flag(const char *, int *);

void	sl_externalize_stat(const struct stat *, struct srt_stat *);
void	sl_internalize_stat(const struct srt_stat *, struct stat *);
void	sl_externalize_statfs(const struct statvfs *, struct srt_statfs *);
void	sl_internalize_statfs(const struct srt_statfs *, struct statvfs *);

int	checkcreds(const struct srt_stat *, const struct slash_creds *, int);

#endif /* _SLUTIL_H_ */
