/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015, Google, Inc.
 * Copyright (c) 2006-2013, Pittsburgh Supercomputing Center (PSC).
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
 * Simple API for constructing path file names in a POSIX file system.
 */

#ifndef _SL_MKFN_H_
#define _SL_MKFN_H_

#include <limits.h>

int	mkfn(char[PATH_MAX], const char *, ...);
int	mkfnv(char[PATH_MAX], const char *, va_list);
void	xmkfn(char[PATH_MAX], const char *, ...);
void	xmkfnv(char[PATH_MAX], const char *, va_list);

#endif /* _SL_MKFN_H_ */
