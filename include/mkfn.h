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
