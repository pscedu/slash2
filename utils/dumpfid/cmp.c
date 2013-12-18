/* $Id$ */

#ifdef HAVE_FTS
#  undef _FILE_OFFSET_BITS	/* FTS is not 64-bit ready */
#endif

#include <sys/types.h>
#include <sys/stat.h>

#include <stdio.h>
#include <fts.h>

#include "pfl/cdefs.h"

int
f_inocmp(const FTSENT **a, const FTSENT **b)
{
	return (CMP((*a)->fts_statp->st_ino, (*b)->fts_statp->st_ino));
}

void *cmpf = f_inocmp;
