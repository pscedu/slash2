/* $Id$ */

/*
 * Routines for manipulating the readdir cache.
 *
 * Since READDIR network I/O with slashd is done asynchronously,
 * mount_slash maintains a cache of entries associated with
 * expected READDIR-related network I/O.  It inserts entries when
 * a request is issued to slashd, accesses entries asynchronously
 * as needed during slashd network I/O, and removes entries when the
 * request has been satisfied, but via the file system and slashd.
 */

#include <err.h>
#include <stdio.h>

#include "psc_rpc/rpc.h"
#include "psc_ds/tree.h"
#include "psc_util/lock.h"
#include "mount_slash.h"

__static SPLAY_GENERATE(rctree, readdir_cache_ent, entry, rce_cmp);

/*
 * rce_cmp - compare to readdir cache entries.
 * @a: one entry.
 * @b: another.
 */
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

void
rc_add(struct readdir_cache_ent *rce, struct pscrpc_export *exp)
{
	struct slashrpc_export *sexp;

	spinlock(&sexp->rclock);
	sexp = slashrpc_export_get(exp);
	if (SPLAY_INSERT(rctree, &sexp->rctree, rce))
		errx(1, "added duplicate readdir cache entry to tree");
	freelock(&sexp->rclock);
}

void
rc_remove(struct readdir_cache_ent *rce, struct pscrpc_export *exp)
{
	struct slashrpc_export *sexp;

	spinlock(&sexp->rclock);
	sexp = exp->exp_private;
	if (SPLAY_REMOVE(rctree, &sexp->rctree, rce) == NULL)
		errx(1, "unable to find readdir cache entry");
	freelock(&sexp->rclock);

	free(rce);
}

struct readdir_cache_ent *
rc_lookup(struct pscrpc_export *exp, u64 cfd, u64 offset)
{
	struct slashrpc_export *sexp;
	struct readdir_cache_ent q, *rce;

	q.offset = offset;
	q.cfd = cfd;

	spinlock(&sexp->rclock);
	sexp = exp->exp_private;
	rce = SPLAY_FIND(rctree, &sexp->rctree, &q);
	freelock(&sexp->rclock);

	return (rce);
}
