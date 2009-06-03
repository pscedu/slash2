/* $Id$ */

#include <stdio.h>

#include "psc_ds/tree.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/service.h"
#include "psc_util/strlcpy.h"

#include "slashd/slashdthr.h"
#include "slashrpc.h"
#include "slashexport.h"
#include "slashd/cfd.h"

#if SEXPTREE
struct sexptree sexptree;
psc_spinlock_t sexptreelock = LOCK_INITIALIZER;

SPLAY_GENERATE(sexptree, slashrpc_export, sexp_entry, sexpcmp);
#endif

/*
 * slashrpc_export_get - access our application-specific variables associated
 *	with an LNET connection.
 * @exp: RPC export of peer.
 */
struct slashrpc_export *
slashrpc_export_get(struct pscrpc_export *exp)
{
	struct slashrpc_export *sexp;
	int l=reqlock(&exp->exp_lock);

	if (exp->exp_private == NULL) {
		sexp = exp->exp_private = 
			PSCALLOC(sizeof(struct slashrpc_export));
		sexp->sexp_export = exp;
		exp->exp_hldropf = slashrpc_export_destroy;		
#if SEXPTREE
		spinlock(&sexptreelock);
		if (SPLAY_INSERT(sexptree, &sexptree, exp->exp_private))
			psc_fatalx("export already registered");
		freelock(&sexptreelock);
#endif		
	} else {
		sexp = exp->exp_private;
		psc_assert(sexp->sexp_export == exp);
	}

	ureqlock(&exp->exp_lock, l);
	return (sexp);
}

void
slashrpc_export_destroy(void *data)
{
	struct slashrpc_export *sexp = data;
	struct pscrpc_export *exp = sexp->sexp_export;
	
	psc_assert(exp);
	/* There's no way to set this from the drop_callback()
	 */
	if (!(sexp->sexp_type & EXP_CLOSING))
		sexp->sexp_type |= EXP_CLOSING;
	/* Ok, no one else should be in here.
	 */
	cfdfreeall(exp);
#if SEXPTREE
	spinlock(&sexptreelock);
	SPLAY_REMOVE(sexptree, &sexptree, sexp);
	freelock(&sexptreelock);
#endif
	exp->exp_private = NULL;
	free(sexp);
	
}

#if SEXPTREE
int
sexpcmp(const void *a, const void *b)
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
