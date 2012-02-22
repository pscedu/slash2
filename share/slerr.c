/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2012, Pittsburgh Supercomputing Center (PSC).
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

#include <string.h>
#include <stdlib.h>

#include "pfl/cdefs.h"

#include "slerr.h"

char *slash_errstrs[] = {
/*  0 */ "Replica is already active",
/*  1 */ "Replication is not active",
/*  2 */ "Replica is already inactive",
/*  3 */ "All replicas inactive",
/*  4 */ "Invalid bmap",
/*  5 */ "Bmap direct I/O must wait",
/*  6 */ "Uninitialized bmap",
/*  7 */ "Unknown resource",
/*  8 */ "Unknown I/O system",
/*  9 */ "Unknown I/O node",
/* 10 */ "I/O node connection could not be established",
/* 11 */ "I/O node does not have a replica",
/* 12 */ "Transaction could not be started",
/* 13 */ "Short I/O",
/* 14 */ "Authorization buffer has bad magic",
/* 15 */ "Authorization buffer has a bad src/dst peer",
/* 16 */ "Authorization buffer has a bad hash",
/* 17 */ "Authorization buffer not provided",
/* 18 */ "User account does not exist",
/* 19 */ "Bad checksum",
/* 20 */ "File generation old",
/* 21 */ "File generation invalid",
/* 22 */ "CONNECT protocol message has not been transmitted",
/* 23 */ "Bmap is awaiting partial truncation resolution",
/* 24 */ "Bmap has started partial truncation resolution",
/* 25 */ "Asynchronous I/O would block",
/* 26 */ "Reimport failed because target is newer",
/* 27 */ "Import additional replica registration failed because source and target differ",
/* 28 */ "Bmap lease extension failed",
/* 29 */ "Bmap lease reassignment failed",
/* 30 */ "Peer resource is of wrong type",
/* 31 */ "Activity already in progress",
/* 32 */ "Invalid argument",
/* 33 */ "Argument list too long",
/* 34 */ "CRC absent",
/* 35 */ "Bad message",
	NULL
};

char *
slstrerror(int error)
{
	error = abs(error);

	if (error >= _SLERR_START &&
	    error < _SLERR_START + nitems(slash_errstrs))
		/* XXX ensure strerror(error) == unknown) */
		return (slash_errstrs[error - _SLERR_START]);
	return (strerror(error));
}
