/* $Id$ */

#include <stdio.h>

void
slashrpc_export_destroy(void *data)
{
	struct slashrpc_export *sexp = data;
	struct cfdent *t;

	SPLAY_FOREACH(t, cfdtree, &sexp->cfdtree)
		free(t);
	free(sexp);
}
