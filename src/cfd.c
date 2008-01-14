/* $Id$ */

#include <errno.h>
#include <stdio.h>

#include "psc_ds/tree.h"
#include "psc_util/alloc.h"
#include "psc_util/lock.h"
#include "psc_rpc/rpc.h"

#include "cfd.h"
#include "rpc.h"
#include "slashrpc.h"

SPLAY_GENERATE(cfdtree, cfdent, entry, cfdcmp);

int
cfdcmp(const void *a, const void *b)
{
	const struct cfdent *ca = a;
	const struct cfdent *cb = b;

	if (ca->cfd < cb->cfd)
		return (-1);
	else if (ca->cfd > cb->cfd)
		return (1);
	return (0);
}

int
cfdnew(u64 *cfdp, struct pscrpc_export *exp, const char *fn)
{
	struct slashrpc_export *sexp;
	struct cfdent *c;

	c = PSCALLOC(sizeof(*c));
	if (fid_get(&c->fid, fn)) {
		errno = EINVAL;
		free(c);
		return (-1);
	}

	spinlock(&exp->exp_lock);
	sexp = slashrpc_export_get(exp);
	c->cfd = ++sexp->cfd;
	if (SPLAY_INSERT(cfdtree, &sexp->cfdtree, c))
		psc_fatalx("cfdtree already has entry");
	freelock(&exp->exp_lock);
	*cfdp = c->cfd;
	return (0);
}


/*
 * cfd2fid - look up a client file descriptor in the export cfdtree
 *	for the associated file ID.
 * @fidp: value-result file ID.
 * @rq: RPC request containing RPC export peer info.
 * @cfd: client file descriptor.
 */
int
cfd2fid(slash_fid_t *fidp, struct pscrpc_export *exp, u64 cfd)
{
	struct slashrpc_export *sexp;
	struct cfdent *c, q;
	int rc;

	rc = 0;
	q.cfd = cfd;
	spinlock(&exp->exp_lock);
	sexp = slashrpc_export_get(exp);
	c = SPLAY_FIND(cfdtree, &sexp->cfdtree, &q);
	if (c == NULL) {
		errno = ENOENT;
		rc = -1;
	} else
		*fidp = c->fid;
	freelock(&exp->exp_lock);
	return (rc);
}
