/* $Id$ */

#include <sys/types.h>

#include "psc_types.h"
#include "../slashd/cfd.h"

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

void rpc_svc_init(void);

extern struct slashrpc_service *ric_svc;
extern struct slashrpc_service *rim_svc;
extern struct slashrpc_service *rii_svc;

#define rim_imp (rim_svc->svc_import)
