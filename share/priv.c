/* $Id$ */
/* %PSC_COPYRIGHT% */

#include <sys/types.h>

#include <pwd.h>
#include <stdio.h>
#include <unistd.h>

#include "psc_util/log.h"

#include "creds.h"

void
sl_drop_privs(int allow_root_uid)
{
	struct passwd *pw;

	pw = getpwnam(SLASH_UID);
	if (pw == NULL) {
		if (allow_root_uid)
			psc_error("unable to setuid %s", SLASH_UID);
		else
			psc_fatal("unable to setuid %s", SLASH_UID);
	} else {
		if (setresgid(pw->pw_gid, pw->pw_gid, pw->pw_gid) == -1)
			psc_fatal("unable to setgid %d", pw->pw_gid);
		if (setresuid(pw->pw_uid, pw->pw_uid, pw->pw_uid) == -1)
			psc_fatal("unable to setuid %d", pw->pw_uid);
	}
}
