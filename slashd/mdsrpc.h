/* $Id$ */

#ifndef _MDS_RPC_H_
#define _MDS_RPC_H_

#define SRMM_NTHREADS   8
#define SRMM_NBUFS      1024
#define SRMM_BUFSZ      128
#define SRMM_REPSZ      128
#define SRMM_SVCNAME    "slrmm"

#define SRMI_NTHREADS   8
#define SRMI_NBUFS      1024
#define SRMI_BUFSZ      256
#define SRMI_REPSZ      256
#define SRMI_SVCNAME    "slrmi"

#define SRMC_NTHREADS   8
#define SRMC_NBUFS      1024
#define SRMC_BUFSZ      384
#define SRMC_REPSZ      384
#define SRMC_SVCNAME    "slrmc"

struct pscrpc_request;

void rpc_initsvc(void);

int slrmc_handler(struct pscrpc_request *);
int slrmi_handler(struct pscrpc_request *);
int slrmm_handler(struct pscrpc_request *);

#endif /* _MDS_RPC_H_ */
