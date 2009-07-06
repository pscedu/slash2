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
	const struct cfdent *ca = a, *cb = b;

	if (ca->fdb.sfdb_secret.sfs_cfd < cb->fdb.sfdb_secret.sfs_cfd)
		return (-1);
	else if (ca->fdb.sfdb_secret.sfs_cfd > cb->fdb.sfdb_secret.sfs_cfd)
		return (1);
	return (0);
}

__static int
cfdinsert(struct cfdent *c, struct pscrpc_export *exp)
{
	struct slashrpc_export *sexp;
	int rc=0;

	sexp = slashrpc_export_get(exp);
	spinlock(&exp->exp_lock);
	if (SPLAY_INSERT(cfdtree, &sexp->sexp_cfdtree, c))
		rc = EEXIST;
	else
		if (c->cfdops && c->cfdops->cfd_insert)
			rc = c->cfdops->cfd_insert(c, exp);
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
       struct cfdent **cfd, struct cfdops *cfdops, int type)
{
	struct slashrpc_export *sexp;
	struct cfdent *c;
	int rc=0;

	if (cfd)
		*cfd = NULL;
       	
	psc_assert(type == CFD_DIR || type == CFD_FILE);

	c = PSCALLOC(sizeof(*c));
	c->fdb.sfdb_secret.sfs_fg.fg_fid = fid;
	c->pri = pri;
	c->cfdops = cfdops;
	c->type = type;

	sexp = slashrpc_export_get(exp);
	spinlock(&exp->exp_lock);
	c->fdb.sfdb_secret.sfs_cfd = ++sexp->sexp_nextcfd;
	if (c->fdb.sfdb_secret.sfs_cfd == FID_ANY)
		c->fdb.sfdb_secret.sfs_cfd = ++sexp->sexp_nextcfd;
	freelock(&exp->exp_lock);

	if (c->cfdops && c->cfdops->cfd_init) {
		rc = (c->cfdops->cfd_init)(c, exp);
		if (rc) {
			psc_errorx("cfd_init() failed rc=%d", rc);
			PSCFREE(c);
			return (rc);
		}
	}

	psc_info("FID (%"_P_U64"d) CFD (%"_P_U64"d)", fid,
	    c->fdb.sfdb_secret.sfs_cfd);

	if ((rc = cfdinsert(c, exp))) {
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
 * cfdlookup - look up a client file descriptor in the export cfdtree
 *	for existence and the associated private data.
 * @exp: peer RPC info.
 * @cfd: client file descriptor/stream ID.
 * @datap: value-result private data attached to cfd entry.
 */
int
cfdlookup(struct pscrpc_export *exp, u64 cfd, void *datap)
{
	struct slashrpc_export *sexp;
	struct cfdent *c, q;
	int rc = 0;

	q.fdb.sfdb_secret.sfs_cfd = cfd;
	spinlock(&exp->exp_lock);
	sexp = slashrpc_export_get(exp);
	c = SPLAY_FIND(cfdtree, &sexp->sexp_cfdtree, &q);
	if (c == NULL)
		rc = ENOENT;
	else if (datap)
		*(void **)datap = c->pri;
	freelock(&exp->exp_lock);
	psc_trace("zfs pri data (%p)", c->pri);
	return (rc);
}

struct cfdent *
cfdget(struct pscrpc_export *exp, u64 cfd)
{
	struct cfdent *c, q;
	struct slashrpc_export *sexp;

	q.fdb.sfdb_secret.sfs_cfd = cfd;
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
	int rc=0, locked;

	q.fdb.sfdb_secret.sfs_cfd = cfd;

	rc = 0;
	locked = reqlock(&exp->exp_lock);
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
	ureqlock(&exp->exp_lock, locked);
	return (rc);
}

void
cfdfreeall(struct pscrpc_export *exp)
{
	struct slashrpc_export *sexp;
	struct cfdent *c, *nxt;

	psc_warnx("exp=%p", exp);

	sexp = slashrpc_export_get(exp);
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
