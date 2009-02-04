/* $Id: rpc.h 4382 2008-10-17 17:56:07Z pauln $ */
#ifndef __MDS_RPC_H__
#define __MDS_RPC_H__ 1

#include <sys/types.h>

#define SRMM_NTHREADS   8
#define SRMM_NBUFS      1024
#define SRMM_BUFSZ      128
#define SRMM_REPSZ      128
#define SRMM_SVCNAME    "slrmmthr"

#define SRMI_NTHREADS   8
#define SRMI_NBUFS      1024
#define SRMI_BUFSZ      128
#define SRMI_REPSZ      128
#define SRMI_SVCNAME    "slrmithr"

#define SRMC_NTHREADS   8
#define SRMC_NBUFS      1024
#define SRMC_BUFSZ      384
#define SRMC_REPSZ      384
#define SRMC_SVCNAME    "slrmcthr"

extern void 
rpc_initsvc(void);

struct pscrpc_request;

int slrmc_handler(struct pscrpc_request *);
int slrmi_handler(struct pscrpc_request *);
int slrmm_handler(struct pscrpc_request *);


#endif
