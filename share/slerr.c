/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
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
	"Replica is already active",
	"Replication is not active",
	"Replica is already inactive",
	"All replicas inactive",
	"Invalid bmap",
	"Uninitialized bmap",
	"Unknown resource",
	"Unknown I/O system",
	"Unknown I/O node",
	"I/O node connection could not be established",
	"I/O node is not a replica",
	"Transaction could not be started",
	"Short I/O",
	"Authorization buffer has bad magic",
	"Authorization buffer has a bad src/dst peer",
	"Authorization buffer has a bad hash",
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
