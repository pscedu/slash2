/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2010-2011, Pittsburgh Supercomputing Center (PSC).
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

#include <sys/types.h>

#include <pwd.h>
#include <stdio.h>
#include <unistd.h>

#include "pfl/setresuid.h"
#include "psc_util/log.h"

#include "creds.h"
#include "slerr.h"

void
sl_getuserpwent(struct passwd **pwp)
{
	errno = 0;
	*pwp = getpwnam(SLASH_UID);
	if (*pwp == NULL && errno == 0)
		errno = SLERR_USER_NOTFOUND;
}

void
sl_drop_privs(int allow_root_uid)
{
	struct passwd *pw;

	sl_getuserpwent(&pw);
	if (pw == NULL) {
		if (allow_root_uid)
			psclog_error("unable to setuid %s", SLASH_UID);
		else
			psc_fatal("unable to setuid %s", SLASH_UID);
	} else {
		if (setresgid(pw->pw_gid, pw->pw_gid, pw->pw_gid) == -1)
			psc_fatal("unable to setgid %d", pw->pw_gid);
		if (setresuid(pw->pw_uid, pw->pw_uid, pw->pw_uid) == -1)
			psc_fatal("unable to setuid %d", pw->pw_uid);
	}
}
