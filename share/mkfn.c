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

#include <sys/param.h>

#include <err.h>
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>

#include "mkfn.h"

int
mkfnv(char fn[PATH_MAX], const char *fmt, va_list ap)
{
	int rc;

	rc = vsnprintf(fn, PATH_MAX, fmt, ap);

	if (rc >= PATH_MAX)
		return (ENAMETOOLONG);
	if (rc == -1)
		return (errno);
	return (0);
}

void
xmkfnv(char fn[PATH_MAX], const char *fmt, va_list ap)
{
	va_list apd;

	va_copy(apd, ap);
	if (mkfnv(fn, fmt, ap))
		verr(1, fmt, apd);
}

int
mkfn(char fn[PATH_MAX], const char *fmt, ...)
{
	va_list ap;
	int rc;

	va_start(ap, fmt);
	rc = mkfnv(fn, fmt, ap);
	va_end(ap);
	return (rc);
}

void
xmkfn(char fn[PATH_MAX], const char *fmt, ...)
{
	va_list ap, apd;

	va_start(ap, fmt);
	va_copy(apd, ap);
	if (mkfnv(fn, fmt, ap))
		verr(1, fmt, apd);
	va_end(ap);
}
