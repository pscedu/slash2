/* $Id$ */

#include <stdio.h>

__static SPLAY_GENERATE(zeiltree, zeil, zeil_exp_entry, zeil_cmp);

int
rce_cmp(const void *a, const void *b)
{
	struct readdir_cache_ent *r1 = a;
	struct readdir_cache_ent *r2 = b;

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

void
rc_add(struct readdir_cache_ent *rce, struct pscrpc_export *exp)
{
	struct slashrpc_export *sexp;

	sexp = exp->exp_private;
	spinlock(sexp->&rclock);
	freelock(sexp->&rclock);
}

void
rc_remove(struct readdir_cache_ent *rce, struct pscrpc_export *exp)
{
	struct slashrpc_export *sexp;

	sexp = exp->exp_private;
	spinlock(sexp->&rclock);
	freelock(sexp->&rclock);
}
