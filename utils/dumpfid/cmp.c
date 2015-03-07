/* $Id$ */

#ifdef HAVE_FTS
#  undef _FILE_OFFSET_BITS	/* FTS is not 64-bit ready */
#endif

#include <sys/types.h>
#include <sys/stat.h>

#include <stdio.h>

#include "pfl/cdefs.h"
#include "pfl/fts.h"


int
f_inocmp(const FTSENT **a, const FTSENT **b)
{
	return (CMP((*a)->fts_ino, (*b)->fts_ino));
}

void *cmpf = f_inocmp;
