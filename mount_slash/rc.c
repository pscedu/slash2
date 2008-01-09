/* $Id$ */

#include <err.h>
#include <stdio.h>

#include "psc_rpc/rpc.h"
#include "psc_ds/tree.h"
#include "psc_util/lock.h"
#include "mount_slash.h"

__static SPLAY_GENERATE(rctree, readdir_cache_ent, entry, rce_cmp);

int
rce_cmp(const void *a, const void *b)
{
	const struct readdir_cache_ent *r1 = a;
	const struct readdir_cache_ent *r2 = b;

	if (r1->offset < r2->offset)
		return (-1);
	else if (r1->offset > r2->offset)
		return (1);

	if (r1->cfd < r2->cfd)
		return (-1);
	else if (r1->cfd > r2->cfd)
		return (1);
	return (0);
}

struct slashrpc_export *
slashrpc_export_get(struct pscrpc_export *exp)
{
	if (exp->exp_private == NULL)
		exp->exp_private = PSCALLOC(sizeof(struct slashrpc_export));
	return (exp->exp_private);
}

void
rc_add(struct readdir_cache_ent *rce, struct pscrpc_export *exp)
{
	struct slashrpc_export *sexp;

	spinlock(&sexp->rclock);
	sexp = slashrpc_export_get(exp);
	if (SPLAY_INSERT(rctree, &sexp->rctree, rce))
		errx(1, "added duplicate readdir_cache_ent to tree");
	freelock(&sexp->rclock);
}

void
rc_remove(struct readdir_cache_ent *rce, struct pscrpc_export *exp)
{
	struct slashrpc_export *sexp;

	spinlock(&sexp->rclock);
	sexp = exp->exp_private;
	SPLAY_REMOVE(rctree, &sexp->rctree, rce);
	freelock(&sexp->rclock);
}
