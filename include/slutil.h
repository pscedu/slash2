/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015, Google, Inc.
 * Copyright 2006-2016, Pittsburgh Supercomputing Center
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

#endif /* _SLUTIL_H_ */
