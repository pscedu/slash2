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

#ifndef _SLASH_CREDS_H_
#define _SLASH_CREDS_H_

#include <stdint.h>

struct passwd;

struct pscfs_creds;

struct srt_stat;

#define SLASH_UID	"_slash"

struct slash_creds {
	uint32_t	scr_uid;
	uint32_t	scr_gid;
};

void	sl_drop_privs(int);
void	sl_getuserpwent(struct passwd **);
int	checkcreds(const struct srt_stat *, const struct pscfs_creds *,
	    int);

#endif /* _SLASH_CREDS_H_ */
