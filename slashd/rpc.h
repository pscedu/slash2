/* $Id$ */

#include <sys/types.h>

#include "psc_ds/tree.h"
#include "psc_util/lock.h"

#include "cfd.h"

struct slashrpc_export {
	struct pscrpc_export		*exp;
	uid_t				 uid;
	gid_t				 gid;
	u64				 nextcfd;
	struct cfdtree			 cfdtree;
	SPLAY_ENTRY(slashrpc_export)	 entry;
};

int sexpcmp(const void *, const void *);

SPLAY_HEAD(sexptree, slashrpc_export);
SPLAY_PROTOTYPE(sexptree, slashrpc_export, entry, sexpcmp);

extern struct sexptree	sexptree;
extern psc_spinlock_t	sexptreelock;
