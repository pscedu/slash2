/* $Id$ */

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
	va_list ap;
	int rc;

	va_start(ap, fmt);
	rc = mkfnv(fn, fmt, ap);
	va_end(ap);

	if (rc) {
		va_start(ap, fmt);
		verr(1, fmt, ap);
	}
}
