/* $Id$ */

#include <stdio.h>

#include "psc_ds/tree.h"

#include "rpc.h"
#include "slashrpc.h"

struct sexptree sexptree;
psc_spinlock_t sexptreelock = LOCK_INITIALIZER;

SPLAY_GENERATE(sexptree, slashrpc_export, entry, sexpcmp);

/*
 * slashrpc_export_get - access our application-specific variables associated
 *	with an LNET connection.
 * @exp: RPC export of peer.
 */
struct slashrpc_export *
slashrpc_export_get(struct pscrpc_export *exp)
{
	spinlock(&exp->exp_lock);
	if (exp->exp_private == NULL) {
		exp->exp_private = PSCALLOC(sizeof(struct slashrpc_export));
		exp->exp_destroycb = slashrpc_export_destroy;

		spinlock(&sexptreelock);
		if (SPLAY_INSERT(sexptree, &sexptree, exp->exp_private))
			psc_fatalx("export already registered");
		freelock(&sexptreelock);
	}
	freelock(&exp->exp_lock);
	return (exp->exp_private);
}

void
slashrpc_export_destroy(void *data)
{
	struct slashrpc_export *sexp = data;
	struct cfdent *c, *next;

	for (c = SPLAY_MIN(cfdtree, &sexp->cfdtree); c; c = next) {
		next = SPLAY_NEXT(cfdtree, &sexp->cfdtree, c);
		SPLAY_REMOVE(cfdtree, &sexp->cfdtree, c);
		free(c);
	}
	spinlock(&sexptreelock);
	SPLAY_REMOVE(sexptree, &sexptree, sexp);
	freelock(&sexptreelock);
	free(sexp);
}

int
sexpcmp(const void *a, const void *b)
{
	const struct slashrpc_export *sa = a, *sb = b;

	if (sa->exp->exp_connection->c_peer.nid <
	    sb->exp->exp_connection->c_peer.nid)
		return (-1);
	else if (sa->exp->exp_connection->c_peer.nid >
	    sb->exp->exp_connection->c_peer.nid)
		return (1);

	if (sa->exp->exp_connection->c_peer.pid <
	    sb->exp->exp_connection->c_peer.pid)
		return (-1);
	else if (sa->exp->exp_connection->c_peer.pid >
	    sb->exp->exp_connection->c_peer.pid)
		return (1);

	return (0);
}
