/* $Id$ */

#include <string.h>
#include <stdlib.h>

#include "psc_util/cdefs.h"

#include "slerr.h"

char *slash_errstrs[] = {
	"Replicas already active",
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
