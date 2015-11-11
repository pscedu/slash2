/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright (c) 2010-2013, Pittsburgh Supercomputing Center (PSC).
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

#include <sys/types.h>

#include <pwd.h>
#include <stdio.h>
#include <unistd.h>

#include "pfl/setresuid.h"
#include "pfl/log.h"

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
sl_drop_privs(__unusedx int allow_root_uid)
{
	struct passwd *pw;

	sl_getuserpwent(&pw);
	if (pw == NULL) {
//		if (allow_root_uid)
//			psclog_error("unable to setuid %s", SLASH_UID);
//		else
//			psc_fatal("unable to setuid %s", SLASH_UID);
	} else {
		if (setresgid(pw->pw_gid, pw->pw_gid, pw->pw_gid) == -1)
			psc_fatal("unable to setgid %d", pw->pw_gid);
		if (setresuid(pw->pw_uid, pw->pw_uid, pw->pw_uid) == -1)
			psc_fatal("unable to setuid %d", pw->pw_uid);
	}
}
