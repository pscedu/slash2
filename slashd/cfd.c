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
#include "psc_rpc/rpc.h"
#include "psc_util/alloc.h"
#include "psc_util/lock.h"

#include "cfd.h"
#include "slashexport.h"
#include "slashrpc.h"

__static SPLAY_GENERATE(cfdtree, cfdent, entry, cfdcmp);

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

int
cfdinsert(struct cfdent *c, struct pscrpc_export *exp, slfid_t fid)
{
	struct slashrpc_export *sexp;
	int rc=0;

	sexp = slashrpc_export_get(exp);
	spinlock(&exp->exp_lock);
	if (SPLAY_INSERT(cfdtree, &sexp->sexp_cfdtree, c))
		rc = EEXIST;
	else
		if (c->cfdops && c->cfdops->cfd_insert)
			rc = c->cfdops->cfd_insert(c, exp, fid);

	freelock(&exp->exp_lock);
	return (rc);
}

/*
 * cfdinsert - allocate a new file descriptor for a client.
 * @cfdp: value-result new client file descriptor.
 * @exp: RPC peer info.
 */
int
cfdnew(slfid_t fid, struct pscrpc_export *exp, void *pri, 
       struct cfdent **cfd, struct cfdops *cfdops)
{
	struct slashrpc_export *sexp;
	struct cfdent *c;
	int rc=0;

	if (cfd)
		*cfd = NULL;

	c = PSCALLOC(sizeof(*c));
	c->fid = fid;
	c->pri = pri;
	c->cfdops = cfdops;

	sexp = slashrpc_export_get(exp);
	spinlock(&exp->exp_lock);
	c->cfd = ++sexp->sexp_nextcfd;
	if (c->cfd == FID_ANY)
		c->cfd = ++sexp->sexp_nextcfd;
	freelock(&exp->exp_lock);

	if (c->cfdops && c->cfdops->cfd_init) {
		rc = (c->cfdops->cfd_init)(c, exp);
		if (rc) {
			psc_errorx("cfd_init() failed rc=%d", rc);
			PSCFREE(c);
			return (rc);
		}
	}

	psc_info("FID (%"_P_U64"d) CFD (%"_P_U64"d)", fid, c->cfd);

	if ((rc = cfdinsert(c, exp, fid))) {
		PSCFREE(c);
		c = NULL;
		if (rc == EEXIST) {
			/* Client requested a cfd that's already been opened.
			 */
			psc_errorx("cfdinsert() rc=%d", rc);
			rc = EADDRINUSE;
		} else
			psc_fatalx("cfdinsert() failed rc=%d", rc);
	}	
	if (cfd)
		*cfd = c;
	return (rc);
}

/*
 * cfd2fid - look up a client file descriptor in the export cfdtree
 *	for the associated file ID.
 * @fidp: value-result file ID.
 * @rq: RPC request containing RPC export peer info.
 * @cfd: client file descriptor.
 */
int
__cfd2fid(struct pscrpc_export *exp, u64 cfd, slfid_t *fidp, void **pri)
{
	struct slashrpc_export *sexp;
	struct cfdent *c, q;
	int rc=0;

	q.cfd = cfd;
	spinlock(&exp->exp_lock);
	sexp = slashrpc_export_get(exp);
	c = SPLAY_FIND(cfdtree, &sexp->sexp_cfdtree, &q);
	if (c == NULL) {
		errno = ENOENT;
		rc = -1;
	} else {
		*fidp = c->fid;
		if (pri) {
			if (c->cfdops->cfd_get_pri) {
				*pri = c->cfdops->cfd_get_pri(c, exp);
				psc_info("zfs pri data (%p)", *pri);
			}
			else
				*pri = c->pri;
		}
	}
	freelock(&exp->exp_lock);
	psc_trace("zfs pri data1 (%p)", *pri);
	return (rc);
}

struct cfdent *
cfdget(struct pscrpc_export *exp, u64 cfd)
{
	struct cfdent *c, q;
	struct slashrpc_export *sexp;

	q.cfd = cfd;
	spinlock(&exp->exp_lock);
	sexp = slashrpc_export_get(exp);
	c = SPLAY_FIND(cfdtree, &sexp->sexp_cfdtree, &q);
	freelock(&exp->exp_lock);
	return (c);
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
	int rc=0, l;

	q.cfd = cfd;

	rc = 0;
	l = reqlock(&exp->exp_lock);
	sexp = slashrpc_export_get(exp);
	c = SPLAY_FIND(cfdtree, &sexp->sexp_cfdtree, &q);
	if (c == NULL) {
		rc = -ENOENT;
		goto done;
	}
	if (SPLAY_REMOVE(cfdtree, &sexp->sexp_cfdtree, c)) {
		c->type |= CFD_CLOSING;
		if (c->cfdops && c->cfdops->cfd_free)
			rc = c->cfdops->cfd_free(c, exp);
		PSCFREE(c);
	} else
		rc = -ENOENT;

 done:
	ureqlock(&exp->exp_lock, l);
	return (rc);
}

void
cfdfreeall(struct pscrpc_export *exp)
{
	struct slashrpc_export *sexp=exp->exp_private;
	struct cfdent *c, *nxt;

	psc_warnx("exp=%p", exp);
	
	psc_assert(sexp);
	psc_assert(sexp->sexp_type & EXP_CLOSING);
	/* Don't bother locking if EXP_CLOSING is set.
	 */
	for (c = SPLAY_MIN(cfdtree, &sexp->sexp_cfdtree); 
	     c != NULL; c = nxt) {
		c->type |= (CFD_CLOSING|CFD_FORCE_CLOSE);
		nxt = SPLAY_NEXT(cfdtree, &sexp->sexp_cfdtree, c);

		SPLAY_REMOVE(cfdtree, &sexp->sexp_cfdtree, c);

		if (c->cfdops && c->cfdops->cfd_free)
			(int)c->cfdops->cfd_free(c, exp);
		PSCFREE(c);
	}
}
