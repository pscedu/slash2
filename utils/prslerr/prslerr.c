/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright (c) 2006-2015, Pittsburgh Supercomputing Center (PSC).
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

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "pfl/cdefs.h"
#include "pfl/pfl.h"

#include "slerr.h"

__dead void
usage(void)
{
	extern const char *__progname;

	fprintf(stderr, "usage: %s\n", __progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	pfl_init();
	sl_errno_init();

	if (getopt(argc, argv, "") != -1)
		usage();
	argc -= optind;
	if (argc)
		usage();

	/* start custom errnos */
	printf("%4d [PFLERR_BADMSG]: %s\n", PFLERR_BADMSG, sl_strerror(PFLERR_BADMSG));
	printf("%4d [PFLERR_KEYEXPIRED]: %s\n", PFLERR_KEYEXPIRED, sl_strerror(PFLERR_KEYEXPIRED));
	printf("%4d [PFLERR_NOTCONN]: %s\n", PFLERR_NOTCONN, sl_strerror(PFLERR_NOTCONN));
	printf("%4d [PFLERR_ALREADY]: %s\n", PFLERR_ALREADY, sl_strerror(PFLERR_ALREADY));
	printf("%4d [PFLERR_NOTSUP]: %s\n", PFLERR_NOTSUP, sl_strerror(PFLERR_NOTSUP));
	printf("%4d [PFLERR_NOSYS]: %s\n", PFLERR_NOSYS, sl_strerror(PFLERR_NOSYS));
	printf("%4d [PFLERR_CANCELED]: %s\n", PFLERR_CANCELED, sl_strerror(PFLERR_CANCELED));
	printf("%4d [PFLERR_STALE]: %s\n", PFLERR_STALE, sl_strerror(PFLERR_STALE));
	printf("%4d [PFLERR_BADMAGIC]: %s\n", PFLERR_BADMAGIC, sl_strerror(PFLERR_BADMAGIC));
	printf("%4d [PFLERR_NOKEY]: %s\n", PFLERR_NOKEY, sl_strerror(PFLERR_NOKEY));
	printf("%4d [PFLERR_BADCRC]: %s\n", PFLERR_BADCRC, sl_strerror(PFLERR_BADCRC));
	printf("%4d [PFLERR_TIMEDOUT]: %s\n", PFLERR_TIMEDOUT, sl_strerror(PFLERR_TIMEDOUT));
	printf("%4d [PFLERR_WOULDBLOCK]: %s\n", PFLERR_WOULDBLOCK, sl_strerror(PFLERR_WOULDBLOCK));
	printf("%4d [SLERR_REPL_ALREADY_ACT]: %s\n", SLERR_REPL_ALREADY_ACT, sl_strerror(SLERR_REPL_ALREADY_ACT));
	printf("%4d [SLERR_REPL_NOT_ACT]: %s\n", SLERR_REPL_NOT_ACT, sl_strerror(SLERR_REPL_NOT_ACT));
	printf("%4d [SLERR_RPCIO]: %s\n", SLERR_RPCIO, sl_strerror(SLERR_RPCIO));
	printf("%4d [SLERR_REPLICA_STATE_INVALID]: %s\n", SLERR_REPLICA_STATE_INVALID, sl_strerror(SLERR_REPLICA_STATE_INVALID));
	printf("%4d [SLERR_BMAP_INVALID]: %s\n", SLERR_BMAP_INVALID, sl_strerror(SLERR_BMAP_INVALID));
	printf("%4d [SLERR_BMAP_DIOWAIT]: %s\n", SLERR_BMAP_DIOWAIT, sl_strerror(SLERR_BMAP_DIOWAIT));
	printf("%4d [SLERR_BMAP_ZERO]: %s\n", SLERR_BMAP_ZERO, sl_strerror(SLERR_BMAP_ZERO));
	printf("%4d [SLERR_RES_UNKNOWN]: %s\n", SLERR_RES_UNKNOWN, sl_strerror(SLERR_RES_UNKNOWN));
	printf("%4d [SLERR_IOS_UNKNOWN]: %s\n", SLERR_IOS_UNKNOWN, sl_strerror(SLERR_IOS_UNKNOWN));
	printf("%4d [SLERR_ION_UNKNOWN]: %s\n", SLERR_ION_UNKNOWN, sl_strerror(SLERR_ION_UNKNOWN));
	printf("%4d [SLERR_ION_OFFLINE]: %s\n", SLERR_ION_OFFLINE, sl_strerror(SLERR_ION_OFFLINE));
	printf("%4d [SLERR_LASTREPL]: %s\n", SLERR_LASTREPL, sl_strerror(SLERR_LASTREPL));
	printf("%4d [SLERR_XACT_FAIL]: %s\n", SLERR_XACT_FAIL, sl_strerror(SLERR_XACT_FAIL));
	printf("%4d [SLERR_SHORTIO]: %s\n", SLERR_SHORTIO, sl_strerror(SLERR_SHORTIO));
	printf("%4d [SLERR_AUTHBUF_BADMAGIC]: %s\n", SLERR_AUTHBUF_BADMAGIC, sl_strerror(SLERR_AUTHBUF_BADMAGIC));
	printf("%4d [SLERR_AUTHBUF_BADPEER]: %s\n", SLERR_AUTHBUF_BADPEER, sl_strerror(SLERR_AUTHBUF_BADPEER));
	printf("%4d [SLERR_AUTHBUF_BADHASH]: %s\n", SLERR_AUTHBUF_BADHASH, sl_strerror(SLERR_AUTHBUF_BADHASH));
	printf("%4d [SLERR_AUTHBUF_ABSENT]: %s\n", SLERR_AUTHBUF_ABSENT, sl_strerror(SLERR_AUTHBUF_ABSENT));
	printf("%4d [SLERR_USER_NOTFOUND]: %s\n", SLERR_USER_NOTFOUND, sl_strerror(SLERR_USER_NOTFOUND));
	printf("%4d [SLERR_GEN_OLD]: %s\n", SLERR_GEN_OLD, sl_strerror(SLERR_GEN_OLD));
	printf("%4d [SLERR_GEN_INVALID]: %s\n", SLERR_GEN_INVALID, sl_strerror(SLERR_GEN_INVALID));
	printf("%4d [SLERR_BMAP_IN_PTRUNC]: %s\n", SLERR_BMAP_IN_PTRUNC, sl_strerror(SLERR_BMAP_IN_PTRUNC));
	printf("%4d [SLERR_BMAP_PTRUNC_STARTED]: %s\n", SLERR_BMAP_PTRUNC_STARTED, sl_strerror(SLERR_BMAP_PTRUNC_STARTED));
	printf("%4d [SLERR_AIOWAIT]: %s\n", SLERR_AIOWAIT, sl_strerror(SLERR_AIOWAIT));
	printf("%4d [SLERR_REIMPORT_OLD]: %s\n", SLERR_REIMPORT_OLD, sl_strerror(SLERR_REIMPORT_OLD));
	printf("%4d [SLERR_IMPORT_XREPL_DIFF]: %s\n", SLERR_IMPORT_XREPL_DIFF, sl_strerror(SLERR_IMPORT_XREPL_DIFF));
	printf("%4d [SLERR_RES_BADTYPE]: %s\n", SLERR_RES_BADTYPE, sl_strerror(SLERR_RES_BADTYPE));
	printf("%4d [SLERR_CRCABSENT]: %s\n", SLERR_CRCABSENT, sl_strerror(SLERR_CRCABSENT));
	/* end custom errnos */
	exit(0);
}
