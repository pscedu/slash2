/* $Id$ */

#ifndef _SLC_RPC_H_
#define _SLC_RPC_H_

#include "psc_rpc/rpc.h"

struct pscrpc_request;

void	slc_rpc_initsvc(void);

int	msrmc_connect(const char *);
int	msrcm_handler(struct pscrpc_request *);

#define mds_import	(mds_csvc->csvc_import)

extern struct slashrpc_cservice	*mds_csvc;

#endif /* _SLC_RPC_H_ */
