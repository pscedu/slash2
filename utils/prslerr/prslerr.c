/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2012, Pittsburgh Supercomputing Center (PSC).
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
	printf("%4d [SLERR_REPL_ALREADY_ACT]: %s\n", SLERR_REPL_ALREADY_ACT, slstrerror(SLERR_REPL_ALREADY_ACT));
	printf("%4d [SLERR_REPL_NOT_ACT]: %s\n", SLERR_REPL_NOT_ACT, slstrerror(SLERR_REPL_NOT_ACT));
	printf("%4d [SLERR_REPL_ALREADY_INACT]: %s\n", SLERR_REPL_ALREADY_INACT, slstrerror(SLERR_REPL_ALREADY_INACT));
	printf("%4d [SLERR_REPLS_ALL_INACT]: %s\n", SLERR_REPLS_ALL_INACT, slstrerror(SLERR_REPLS_ALL_INACT));
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
	printf("%4d [SLERR_BADCRC]: %s\n", SLERR_BADCRC, slstrerror(SLERR_BADCRC));
	printf("%4d [SLERR_GEN_OLD]: %s\n", SLERR_GEN_OLD, slstrerror(SLERR_GEN_OLD));
	printf("%4d [SLERR_GEN_INVALID]: %s\n", SLERR_GEN_INVALID, slstrerror(SLERR_GEN_INVALID));
	printf("%4d [SLERR_NOTCONN]: %s\n", SLERR_NOTCONN, slstrerror(SLERR_NOTCONN));
	printf("%4d [SLERR_BMAP_IN_PTRUNC]: %s\n", SLERR_BMAP_IN_PTRUNC, slstrerror(SLERR_BMAP_IN_PTRUNC));
	printf("%4d [SLERR_BMAP_PTRUNC_STARTED]: %s\n", SLERR_BMAP_PTRUNC_STARTED, slstrerror(SLERR_BMAP_PTRUNC_STARTED));
	printf("%4d [SLERR_AIOWAIT]: %s\n", SLERR_AIOWAIT, slstrerror(SLERR_AIOWAIT));
	printf("%4d [SLERR_REIMPORT_OLD]: %s\n", SLERR_REIMPORT_OLD, slstrerror(SLERR_REIMPORT_OLD));
	printf("%4d [SLERR_IMPORT_XREPL_DIFF]: %s\n", SLERR_IMPORT_XREPL_DIFF, slstrerror(SLERR_IMPORT_XREPL_DIFF));
	printf("%4d [SLERR_BMAP_LEASEEXT_FAILED]: %s\n", SLERR_BMAP_LEASEEXT_FAILED, slstrerror(SLERR_BMAP_LEASEEXT_FAILED));
	printf("%4d [SLERR_RES_BADTYPE]: %s\n", SLERR_RES_BADTYPE, slstrerror(SLERR_RES_BADTYPE));
	printf("%4d [SLERR_ALREADY]: %s\n", SLERR_ALREADY, slstrerror(SLERR_ALREADY));
	printf("%4d [SLERR_INVAL]: %s\n", SLERR_INVAL, slstrerror(SLERR_INVAL));
	printf("%4d [SLERR_2BIG]: %s\n", SLERR_2BIG, slstrerror(SLERR_2BIG));
	printf("%4d [SLERR_CRCABSENT]: %s\n", SLERR_CRCABSENT, slstrerror(SLERR_CRCABSENT));
	printf("%4d [SLERR_BADMSG]: %s\n", SLERR_BADMSG, slstrerror(SLERR_BADMSG));
	/* end custom errnos */
	exit(0);
}
