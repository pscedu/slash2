/* $Id$ */

/*
 * cfd - routines for manipulating the client file descriptor structures.
 * The application-specific RPC export structure to client peers contains
 * a tree comprised of associated file descriptors for open files the
 * client is doing operations on.  These routines maintain this tree.
 */

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

/*
 * cfdcmp - compare to client file descriptors, for tree lookups.
 * @a: one cfd.
 * @b: another cfd.
 */
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

struct cfdent *
cfdinsert(u64 cfd, struct pscrpc_export *exp, const slash_fid_t *fidp)
{
	struct slashrpc_export *sexp;
	struct cfdent *c;
	int locked;

	c = PSCALLOC(sizeof(*c));
	c->fid = *fidp;
	c->cfd = cfd;

	locked = reqlock(&exp->exp_lock);
	sexp = slashrpc_export_get(exp);
	if (SPLAY_INSERT(cfdtree, &sexp->cfdtree, c)) {
		free(c);
		c = NULL;
	}
	ureqlock(&exp->exp_lock, locked);
	return (c);
}

/*
 * cfdinsert - allocate a new file descriptor for a client.
 * @cfdp: value-result new client file descriptor.
 * @exp: RPC peer info.
 * @fn: server-translated filename to associate cfd with (i.e. the file specified
 *	by the client needs to be "translated" to the server's file system path).
 */
void
cfdnew(u64 *cfdp, struct pscrpc_export *exp, const slash_fid_t *fidp)
{
	struct slashrpc_export *sexp;

	spinlock(&exp->exp_lock);
	sexp = slashrpc_export_get(exp);
	*cfdp = ++sexp->nextcfd;
	if (cfdinsert(*cfdp, exp, fidp))
		psc_fatalx("cfdtree already has entry");
	freelock(&exp->exp_lock);
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

/*
 * cfdfree - release a client file descriptor.
 * @exp: RPC peer info.
 * @cfd: client fd to release.
 */
int
cfdfree(struct pscrpc_export *exp, u64 cfd)
{
	struct slashrpc_export *sexp;
	struct cfdent *c, q;
	int rc;

	q.cfd = cfd;

	rc = 0;
	spinlock(&exp->exp_lock);
	sexp = slashrpc_export_get(exp);
	c = SPLAY_FIND(cfdtree, &sexp->cfdtree, &q);
	if (c == NULL) {
		errno = ENOENT;
		rc = -1;
		goto done;
	}
	if (SPLAY_REMOVE(cfdtree, &sexp->cfdtree, c))
		free(c);
	else {
		errno = ENOENT;
		rc = -1;
	}
 done:
	freelock(&exp->exp_lock);
	return (rc);
}
