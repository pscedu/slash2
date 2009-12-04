/* $Id$ */

/*
 * cfd - routines for manipulating the client file descriptor structures.
 * The application-specific RPC export structure to client peers contains
 * a tree comprised of associated file descriptors for open files the
 * client is doing operations on.  These routines maintain this tree.
 */

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>

#include "psc_ds/tree.h"
#include "psc_rpc/rpc.h"
#include "psc_util/alloc.h"
#include "psc_util/lock.h"

#include "cfd.h"
#include "slashrpc.h"

__static SPLAY_GENERATE(cfdtree, cfdent, cfd_entry, cfdcmp);

/*
 * cfdcmp - compare to client file descriptors, for tree lookups.
 * @a: one cfd.
 * @b: another cfd.
 */
int
cfdcmp(const void *a, const void *b)
{
	const struct cfdent *ca = a, *cb = b;

	return (CMP(ca->cfd_fdb.sfdb_secret.sfs_cfd,
	    cb->cfd_fdb.sfdb_secret.sfs_cfd));
}

__static int
cfdinsert(struct cfdent *c, struct pscrpc_export *exp,
    enum slconn_type peertype)
{
	struct slashrpc_export *slexp;
	int rc=0;

	slexp = slashrpc_export_get(exp, peertype);
	spinlock(&exp->exp_lock);
	if (SPLAY_INSERT(cfdtree, slexp->slexp_cfdtree, c))
		rc = EEXIST;
	freelock(&exp->exp_lock);
	return (rc);
}

/*
 * cfdnew - allocate a new file descriptor for a client.
 * @cfdp: value-result new client file descriptor.
 * @exp: RPC peer info.
 */
int
cfdnew(slfid_t fid, struct pscrpc_export *exp,
    enum slconn_type peertype, void *finfo, struct cfdent **cfd,
    struct cfdops *cfdops, int type)
{
	struct slashrpc_export *slexp;
	struct cfdent *c;
	int rc=0;

	if (cfd)
		*cfd = NULL;

	psc_assert(type == CFD_DIR || type == CFD_FILE);

	c = PSCALLOC(sizeof(*c));
	c->cfd_fdb.sfdb_secret.sfs_fg.fg_fid = fid;
	c->cfd_ops = cfdops;
	c->cfd_type = type;

	spinlock(&exp->exp_lock);
	slexp = slashrpc_export_get(exp, peertype);
	c->cfd_fdb.sfdb_secret.sfs_cfd = ++slexp->slexp_nextcfd;
	if (c->cfd_fdb.sfdb_secret.sfs_cfd == FID_ANY)
		c->cfd_fdb.sfdb_secret.sfs_cfd = ++slexp->slexp_nextcfd;
	freelock(&exp->exp_lock);

	if (c->cfd_ops && c->cfd_ops->cfd_init) {
		rc = (c->cfd_ops->cfd_init)(c, finfo, exp);
		if (rc) {
			psc_errorx("cfd_init() failed rc=%d", rc);
			PSCFREE(c);
			return (rc);
		}
	}

	psc_info("FID (%"PRId64") CFD (%"PRId64") PRI(%p)", fid,
		 c->cfd_fdb.sfdb_secret.sfs_cfd, c->cfd_pri);

	rc = cfdinsert(c, exp, peertype);
	if (rc) {
		PSCFREE(c);
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
 *	for existence and access its associated private data.
 * @exp: peer RPC info.
 * @cfd: client file descriptor/stream ID.
 * @datap: value-result private data attached to cfd entry.
 */
int
cfdlookup(struct pscrpc_export *exp, enum slconn_type peertype,
    uint64_t cfd, void *datap)
{
	struct slashrpc_export *slexp;
	struct cfdent *c, q;
	int rc = 0;

	q.cfd_fdb.sfdb_secret.sfs_cfd = cfd;
	spinlock(&exp->exp_lock);
	slexp = slashrpc_export_get(exp, peertype);
	c = SPLAY_FIND(cfdtree, slexp->slexp_cfdtree, &q);
	if (c == NULL)
		rc = ENOENT;
	else if (datap)
		*(void **)datap = c->cfd_pri;
	freelock(&exp->exp_lock);
	psc_trace("cfd pri data (%p)", c->cfd_pri);
	return (rc);
}

struct cfdent *
cfdget(struct pscrpc_export *exp, enum slconn_type peertype, uint64_t cfd)
{
	struct slashrpc_export *slexp;
	struct cfdent *c, q;

	q.cfd_fdb.sfdb_secret.sfs_cfd = cfd;
	spinlock(&exp->exp_lock);
	slexp = slashrpc_export_get(exp, peertype);
	c = SPLAY_FIND(cfdtree, slexp->slexp_cfdtree, &q);
	freelock(&exp->exp_lock);
	return (c);
}

/*
 * cfdfree - release a client file descriptor.
 * @exp: RPC peer info.
 * @cfd: client fd to release.
 */
int
cfdfree(struct pscrpc_export *exp, enum slconn_type peertype, uint64_t cfd)
{
	struct slashrpc_export *slexp;
	struct cfdent *c, q;
	int rc=0, locked;

	q.cfd_fdb.sfdb_secret.sfs_cfd = cfd;

	rc = 0;
	locked = reqlock(&exp->exp_lock);
	slexp = slashrpc_export_get(exp, peertype);
	c = SPLAY_FIND(cfdtree, slexp->slexp_cfdtree, &q);
	if (c == NULL) {
		rc = -ENOENT;
		goto done;
	}
	if (SPLAY_REMOVE(cfdtree, slexp->slexp_cfdtree, c)) {
		c->cfd_type |= CFD_CLOSING;
		if (c->cfd_ops && c->cfd_ops->cfd_free)
			rc = c->cfd_ops->cfd_free(c, exp);
		PSCFREE(c);
	} else
		rc = -ENOENT;

 done:
	ureqlock(&exp->exp_lock, locked);
	return (rc);
}

void
cfdfreeall(struct pscrpc_export *exp, enum slconn_type peertype)
{
	struct slashrpc_export *slexp;
	struct cfdent *c, *nxt;

	psc_warnx("exp=%p", exp);

	slexp = slashrpc_export_get(exp, peertype);
	psc_assert(slexp->slexp_flags & EXP_CLOSING);

	/* Don't bother locking if EXP_CLOSING is set.
	 */
	for (c = SPLAY_MIN(cfdtree, slexp->slexp_cfdtree);
	     c != NULL; c = nxt) {
		c->cfd_type |= CFD_CLOSING | CFD_FORCE_CLOSE;
		nxt = SPLAY_NEXT(cfdtree, slexp->slexp_cfdtree, c);

		SPLAY_REMOVE(cfdtree, slexp->slexp_cfdtree, c);

		if (c->cfd_ops && c->cfd_ops->cfd_free)
			c->cfd_ops->cfd_free(c, exp);
		PSCFREE(c);
	}
}
