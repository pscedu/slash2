/* $Id$ */

#include <sys/types.h>

#include "psc_ds/tree.h"

#include "cfd.h"

struct slashrpc_export {
	struct pscrpc_export		*exp;
	uid_t				 uid;
	gid_t				 gid;
	u64				 nextcfd;
	struct cfdtree			 cfdtree;
	SPLAY_ENTRY(slashrpc_export)	 entry;
};
