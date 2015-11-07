/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
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

#include "slerr.h"

extern char *slash_errstrs[];
const char *progname;

__dead void
usage(void)
{
	fprintf(stderr, "usage: %s\n", progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	progname = argv[0];
	if (getopt(argc, argv, "") != -1)
		usage();
	argc -= optind;
	if (argc)
		usage();

	/* start custom errnos */
	printf("%4d [PFLERR_BADMSG]: %s\n", PFLERR_BADMSG, slstrerror(PFLERR_BADMSG));
	printf("%4d [PFLERR_KEYEXPIRED]: %s\n", PFLERR_KEYEXPIRED, slstrerror(PFLERR_KEYEXPIRED));
	printf("%4d [PFLERR_NOTCONN]: %s\n", PFLERR_NOTCONN, slstrerror(PFLERR_NOTCONN));
	printf("%4d [PFLERR_ALREADY]: %s\n", PFLERR_ALREADY, slstrerror(PFLERR_ALREADY));
	printf("%4d [PFLERR_NOTSUP]: %s\n", PFLERR_NOTSUP, slstrerror(PFLERR_NOTSUP));
	printf("%4d [PFLERR_NOSYS]: %s\n", PFLERR_NOSYS, slstrerror(PFLERR_NOSYS));
	printf("%4d [PFLERR_CANCELED]: %s\n", PFLERR_CANCELED, slstrerror(PFLERR_CANCELED));
	printf("%4d [PFLERR_STALE]: %s\n", PFLERR_STALE, slstrerror(PFLERR_STALE));
	printf("%4d [PFLERR_BADMAGIC]: %s\n", PFLERR_BADMAGIC, slstrerror(PFLERR_BADMAGIC));
	printf("%4d [PFLERR_NOKEY]: %s\n", PFLERR_NOKEY, slstrerror(PFLERR_NOKEY));
	printf("%4d [PFLERR_BADCRC]: %s\n", PFLERR_BADCRC, slstrerror(PFLERR_BADCRC));
	printf("%4d [PFLERR_TIMEDOUT]: %s\n", PFLERR_TIMEDOUT, slstrerror(PFLERR_TIMEDOUT));
	printf("%4d [SLERR_REPL_ALREADY_ACT]: %s\n", SLERR_REPL_ALREADY_ACT, slstrerror(SLERR_REPL_ALREADY_ACT));
	printf("%4d [SLERR_REPL_NOT_ACT]: %s\n", SLERR_REPL_NOT_ACT, slstrerror(SLERR_REPL_NOT_ACT));
	printf("%4d [SLERR_RPCIO]: %s\n", SLERR_RPCIO, slstrerror(SLERR_RPCIO));
	printf("%4d [SLERR_BMAP_INVALID]: %s\n", SLERR_BMAP_INVALID, slstrerror(SLERR_BMAP_INVALID));
	printf("%4d [SLERR_BMAP_DIOWAIT]: %s\n", SLERR_BMAP_DIOWAIT, slstrerror(SLERR_BMAP_DIOWAIT));
	printf("%4d [SLERR_BMAP_ZERO]: %s\n", SLERR_BMAP_ZERO, slstrerror(SLERR_BMAP_ZERO));
	printf("%4d [SLERR_RES_UNKNOWN]: %s\n", SLERR_RES_UNKNOWN, slstrerror(SLERR_RES_UNKNOWN));
	printf("%4d [SLERR_IOS_UNKNOWN]: %s\n", SLERR_IOS_UNKNOWN, slstrerror(SLERR_IOS_UNKNOWN));
	printf("%4d [SLERR_ION_UNKNOWN]: %s\n", SLERR_ION_UNKNOWN, slstrerror(SLERR_ION_UNKNOWN));
	printf("%4d [SLERR_ION_OFFLINE]: %s\n", SLERR_ION_OFFLINE, slstrerror(SLERR_ION_OFFLINE));
	printf("%4d [SLERR_LASTREPL]: %s\n", SLERR_LASTREPL, slstrerror(SLERR_LASTREPL));
	printf("%4d [SLERR_XACT_FAIL]: %s\n", SLERR_XACT_FAIL, slstrerror(SLERR_XACT_FAIL));
	printf("%4d [SLERR_SHORTIO]: %s\n", SLERR_SHORTIO, slstrerror(SLERR_SHORTIO));
	printf("%4d [SLERR_AUTHBUF_BADMAGIC]: %s\n", SLERR_AUTHBUF_BADMAGIC, slstrerror(SLERR_AUTHBUF_BADMAGIC));
	printf("%4d [SLERR_AUTHBUF_BADPEER]: %s\n", SLERR_AUTHBUF_BADPEER, slstrerror(SLERR_AUTHBUF_BADPEER));
	printf("%4d [SLERR_AUTHBUF_BADHASH]: %s\n", SLERR_AUTHBUF_BADHASH, slstrerror(SLERR_AUTHBUF_BADHASH));
	printf("%4d [SLERR_AUTHBUF_ABSENT]: %s\n", SLERR_AUTHBUF_ABSENT, slstrerror(SLERR_AUTHBUF_ABSENT));
	printf("%4d [SLERR_USER_NOTFOUND]: %s\n", SLERR_USER_NOTFOUND, slstrerror(SLERR_USER_NOTFOUND));
	printf("%4d [SLERR_GEN_OLD]: %s\n", SLERR_GEN_OLD, slstrerror(SLERR_GEN_OLD));
	printf("%4d [SLERR_GEN_INVALID]: %s\n", SLERR_GEN_INVALID, slstrerror(SLERR_GEN_INVALID));
	printf("%4d [SLERR_BMAP_IN_PTRUNC]: %s\n", SLERR_BMAP_IN_PTRUNC, slstrerror(SLERR_BMAP_IN_PTRUNC));
	printf("%4d [SLERR_BMAP_PTRUNC_STARTED]: %s\n", SLERR_BMAP_PTRUNC_STARTED, slstrerror(SLERR_BMAP_PTRUNC_STARTED));
	printf("%4d [SLERR_AIOWAIT]: %s\n", SLERR_AIOWAIT, slstrerror(SLERR_AIOWAIT));
	printf("%4d [SLERR_REIMPORT_OLD]: %s\n", SLERR_REIMPORT_OLD, slstrerror(SLERR_REIMPORT_OLD));
	printf("%4d [SLERR_IMPORT_XREPL_DIFF]: %s\n", SLERR_IMPORT_XREPL_DIFF, slstrerror(SLERR_IMPORT_XREPL_DIFF));
	printf("%4d [SLERR_RES_BADTYPE]: %s\n", SLERR_RES_BADTYPE, slstrerror(SLERR_RES_BADTYPE));
	printf("%4d [SLERR_CRCABSENT]: %s\n", SLERR_CRCABSENT, slstrerror(SLERR_CRCABSENT));
	/* end custom errnos */
	exit(0);
}
