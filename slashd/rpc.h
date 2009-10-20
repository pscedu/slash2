/* $Id$ */

#ifndef _MDS_RPC_H_
#define _MDS_RPC_H_

#include <sys/types.h>

#include "psc_ds/tree.h"
#include "psc_util/lock.h"

#include "cfd.h"

struct pscrpc_export;

struct slashrpc_export {
	struct pscrpc_export		*exp;
	uint64_t			 nextcfd;
	struct cfdtree			 cfdtree;
	SPLAY_ENTRY(slashrpc_export)	 entry;
};

struct slashrpc_export *
	slashrpc_export_get(struct pscrpc_export *);
int	sexpcmp(const void *, const void *);

void	rpc_initsvc(void);

int	slrmc_handler(struct pscrpc_request *);
int	slrmi_handler(struct pscrpc_request *);
int	slrmm_handler(struct pscrpc_request *);

int	slrcm_issue_getreplst(struct pscrpc_import *,
	    slfid_t, int32_t, sl_ios_id_t, int, int, int);

SPLAY_HEAD(sexptree, slashrpc_export);
SPLAY_PROTOTYPE(sexptree, slashrpc_export, entry, sexpcmp);

extern struct sexptree	sexptree;
extern psc_spinlock_t	sexptreelock;

#endif /* _MDS_RPC_H_ */
