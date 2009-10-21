/* $Id$ */

#include <stdio.h>

#include "psc_ds/tree.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/service.h"
#include "psc_util/strlcpy.h"

#include "cfd.h"
#include "slashexport.h"
#include "slashrpc.h"

#if SEXPTREE
struct slexptree slexptree;
psc_spinlock_t slexptreelock = LOCK_INITIALIZER;

SPLAY_GENERATE(slexptree, slashrpc_export, slexp_entry, slexpcmp);
#endif

/*
 * slashrpc_export_get - access our application-specific variables associated
 *	with an LNET connection.
 * @exp: RPC export of peer.
 */
struct slashrpc_export *
slashrpc_export_get(struct pscrpc_export *exp)
{
	struct slashrpc_export *slexp;
	int locked = reqlock(&exp->exp_lock);

	if (exp->exp_private == NULL) {
		slexp = exp->exp_private =
			PSCALLOC(sizeof(struct slashrpc_export));
		slexp->slexp_export = exp;
		exp->exp_hldropf = slashrpc_export_destroy;
#if SEXPTREE
		spinlock(&slexptreelock);
		if (SPLAY_INSERT(slexptree, &slexptree, exp->exp_private))
			psc_fatalx("export already registered");
		freelock(&slexptreelock);
#endif
	} else {
		slexp = exp->exp_private;
		psc_assert(slexp->slexp_export == exp);
	}

	ureqlock(&exp->exp_lock, locked);
	return (slexp);
}

void
slashrpc_export_destroy(void *data)
{
	struct slashrpc_export *slexp = data;
	struct pscrpc_export *exp = slexp->slexp_export;

	psc_assert(exp);
	/* There's no way to set this from the drop_callback()
	 */
	if (!(slexp->slexp_type & EXP_CLOSING))
		slexp->slexp_type |= EXP_CLOSING;
	/* Ok, no one else should be in here.
	 */
	cfdfreeall(exp);
#if SEXPTREE
	spinlock(&slexptreelock);
	SPLAY_REMOVE(slexptree, &slexptree, slexp);
	freelock(&slexptreelock);
#endif
	exp->exp_private = NULL;
	PSCFREE(slexp);
}

#if SEXPTREE
int
slexpcmp(const void *a, const void *b)
{
	const struct slashrpc_export *sa = a, *sb = b;
	const lnet_process_id_t *pa = &sa->exp->exp_connection->c_peer;
	const lnet_process_id_t *pb = &sb->exp->exp_connection->c_peer;

	if (pa->nid < pb->nid)
		return (-1);
	else if (pa->nid > pb->nid)
		return (1);

	if (pa->pid < pb->pid)
		return (-1);
	else if (pa->pid > pb->pid)
		return (1);

	return (0);
}
#endif
