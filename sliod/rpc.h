/* $Id$ */

#include <sys/types.h>

#include "psc_types.h"
#include "../slashd/cfd.h"

#define RPCSVC_BE	0	/* backend: slashd <=> sliod */
#define NRPCSVCS	1

struct slashrpc_service {
	struct pscrpc_import	 *svc_import;
	psc_spinlock_t		  svc_lock;
	struct psclist_head	  svc_old_imports;
	int			  svc_failed;
	int			  svc_initialized;
};

struct slashrpc_export {
	uid_t	uid;
	gid_t	gid;
	struct cfdtree cfdtree;
};

extern struct slashrpc_service *rpcsvcs[];
