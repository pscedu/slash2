/* $Id$ */

#include <stdio.h>

#include "psc_ds/tree.h"

#include "rpc.h"
#include "slashrpc.h"

void
slashrpc_export_destroy(void *data)
{
	struct slashrpc_export *sexp = data;
	struct cfdent *c, *next;

	for (c = SPLAY_MIN(cfdtree, &sexp->cfdtree); c; c = next) {
		next = SPLAY_NEXT(cfdtree, &sexp->cfdtree, c);
		SPLAY_REMOVE(cfdtree, &sexp->cfdtree, c);
		free(c);
	}
	spinlock(&explock);
	SPLAY_REMOVE(exptree, &exptree, sexp);
	freelock(&explock);
	free(sexp);
}
