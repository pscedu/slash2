/* $Id$ */

#include <string.h>
#include <stdlib.h>

#include "pfl/cdefs.h"

#include "slerr.h"

char *slash_errstrs[] = {
	"Replica already active",
	"Replica already inactive",
	"All replicas inactive",
	"Invalid bmap",
	"Unknown I/O system",
	NULL
};

char *
slstrerror(int error)
{
	error = abs(error);

	if (error >= _SLERR_START &&
	    error < _SLERR_START + nitems(slash_errstrs))
		return (slash_errstrs[error - _SLERR_START - 1]);
	return (strerror(error));
}
