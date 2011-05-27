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
