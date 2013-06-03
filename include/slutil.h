/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2013, Pittsburgh Supercomputing Center (PSC).
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

#include "sltypes.h"

struct stat;
struct statvfs;
struct statfs;

struct srt_stat;
struct srt_statfs;

enum rw	fflags_2_rw(int);

void	sl_externalize_stat(const struct stat *, struct srt_stat *);
void	sl_internalize_stat(const struct srt_stat *, struct stat *);
void	sl_externalize_statfs(const struct statvfs *, struct srt_statfs *);
void	sl_internalize_statfs(const struct srt_statfs *, struct statvfs *);

void	statfs_2_statvfs(const struct statfs *, struct statvfs *);

#endif /* _SLUTIL_H_ */
