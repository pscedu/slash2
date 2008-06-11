/* $Id$ */

#include <sys/types.h>

#include "psc_ds/tree.h"
#include "psc_util/lock.h"

#include "cfd.h"

struct pscrpc_export;

struct slashrpc_cservice {
	struct pscrpc_import	 *csvc_import;
	psc_spinlock_t		  csvc_lock;
	struct psclist_head	  csvc_old_imports;
	int			  csvc_failed;
	int			  csvc_initialized;
};

struct slashrpc_export {
	struct pscrpc_export		*exp;
	u64				 nextcfd;
	struct cfdtree			 cfdtree;
	SPLAY_ENTRY(slashrpc_export)	 entry;
};

struct slashrpc_export *
	slashrpc_export_get(struct pscrpc_export *);
int	sexpcmp(const void *, const void *);

struct slashrpc_cservice *
	rpc_csvc_create(u32, u32);
void	rpcsvc_init(void);
int	rpc_issue_connect(lnet_nid_t, struct pscrpc_import *, u64, u32);

int slrmc_handler(struct pscrpc_request *);
int slrmi_handler(struct pscrpc_request *);
int slrmm_handler(struct pscrpc_request *);

SPLAY_HEAD(sexptree, slashrpc_export);
SPLAY_PROTOTYPE(sexptree, slashrpc_export, entry, sexpcmp);

extern struct sexptree	sexptree;
extern psc_spinlock_t	sexptreelock;
