/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
 *
 * Permission to use, copy, and modify this software and its documentation
 * without fee for personal use or non-commercial use within your organization
 * is hereby granted, provided that the above copyright notice is preserved in
 * all copies and that the copyright and this permission notice appear in
 * supporting documentation.  Permission to redistribute this software to other
 * organizations or individuals is not permitted without the written permission
 * of the Pittsburgh Supercomputing Center.  PSC makes no representations about
 * the suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

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
#include "psc_ds/treeutil.h"
#include "psc_rpc/rpc.h"
#include "psc_util/alloc.h"
#include "psc_util/lock.h"

#include "cfd.h"
#include "mdsexpc.h"
#include "slashrpc.h"
#include "slconn.h"

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

	return (CMP(ca->cfd_cfd, cb->cfd_cfd));
}

__static int
cfdinsert(struct cfdent *c, struct pscrpc_export *exp)
{
	struct mexp_cli *mc;
	int rc = 0;

	spinlock(&exp->exp_lock);
	mc = mexpcli_get(exp);
	if (SPLAY_INSERT(cfdtree, &mc->mc_cfdtree, c))
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
cfdnew(slfid_t fid, struct pscrpc_export *exp, enum slconn_type peertype,
    struct cfdent **cfd, int flags)
{
	struct slashrpc_export *slexp;
	struct cfdent *c;
	int rc=0;

	if (cfd)
		*cfd = NULL;

	psc_assert(flags == CFD_DIR || flags == CFD_FILE);

	c = PSCALLOC(sizeof(*c));
	c->cfd_fg.fg_fid = fid;
	c->cfd_flags = flags;

	spinlock(&exp->exp_lock);
	slexp = slexp_get(exp, peertype);
	c->cfd_cfd = ++slexp->slexp_nextcfd;
	freelock(&exp->exp_lock);

	if (cfd_ops.cfd_init) {
		rc = cfd_ops.cfd_init(c, exp);
		if (rc) {
			psc_errorx("cfd_init() failed rc=%d", rc);
			PSCFREE(c);
			return (rc);
		}
	}

	psc_info("FID (%"PRId64") CFD (%"PRId64") PRI(%p)", fid,
	    c->cfd_cfd, c->cfd_pri);

	rc = cfdinsert(c, exp);
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
cfdlookup(struct pscrpc_export *exp, uint64_t cfd, void *datap)
{
	struct mexp_cli *mc;
	struct cfdent *c, q;
	int rc = 0;

	q.cfd_cfd = cfd;
	spinlock(&exp->exp_lock);
	mc = mexpcli_get(exp);
	c = SPLAY_FIND(cfdtree, &mc->mc_cfdtree, &q);
	if (c == NULL)
		rc = ENOENT;
	else
		*(void **)datap = c->cfd_pri;
	freelock(&exp->exp_lock);
	psc_trace("cfd pri data (%p)", c->cfd_pri);
	return (rc);
}

struct cfdent *
cfdget(struct pscrpc_export *exp, uint64_t cfd)
{
	struct mexp_cli *mc;
	struct cfdent *c, q;

	q.cfd_cfd = cfd;
	spinlock(&exp->exp_lock);
	mc = mexpcli_get(exp);
	c = SPLAY_FIND(cfdtree, &mc->mc_cfdtree, &q);
	freelock(&exp->exp_lock);
	return (c);
}

/*
 * cfdfree - release a client file descriptor.
 * @exp: RPC peer info.
 * @cfd: client fd to release.
 */
int
cfdfree(struct pscrpc_export *exp, uint64_t cfd)
{
	struct mexp_cli *mc;
	struct cfdent *c, q;
	int rc=0, locked;

	q.cfd_cfd = cfd;

	rc = 0;
	locked = reqlock(&exp->exp_lock);
	mc = mexpcli_get(exp);
	c = SPLAY_FIND(cfdtree, &mc->mc_cfdtree, &q);
	if (c == NULL) {
		rc = -ENOENT;
		goto done;
	}
	if (SPLAY_REMOVE(cfdtree, &mc->mc_cfdtree, c)) {
		c->cfd_flags |= CFD_CLOSING;
		if (cfd_ops.cfd_free)
			rc = cfd_ops.cfd_free(c, exp);
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
	struct mexp_cli *mc;
	struct cfdent *c, *nxt;

	psc_warnx("exp=%p", exp);

	slexp = slexp_get(exp, peertype);
	psc_assert(slexp->slexp_flags & SLEXPF_CLOSING);

	mc = mexpcli_get(exp);

	for (c = SPLAY_MIN(cfdtree, &mc->mc_cfdtree);
	     c != NULL; c = nxt) {
		c->cfd_flags |= CFD_CLOSING | CFD_FORCE_CLOSE;
		nxt = SPLAY_NEXT(cfdtree, &mc->mc_cfdtree, c);

		PSC_SPLAY_XREMOVE(cfdtree, &mc->mc_cfdtree, c);

		if (cfd_ops.cfd_free)
			cfd_ops.cfd_free(c, exp);
		PSCFREE(c);
	}
}
