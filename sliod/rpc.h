/* $Id$ */

#ifndef _IO_RPC_H_
#define _IO_RPC_H_

#include <sys/types.h>

#include "psc_types.h"

#include "../slashd/cfd.h"

#define SRIM_NTHREADS	8
#define SRIM_NBUFS	1024
#define SRIM_BUFSZ	256
#define SRIM_REPSZ	256
#define SRIM_SVCNAME	"slrim"

#define SRIC_NTHREADS	8
#define SRIC_NBUFS	1024
#define SRIC_BUFSZ	(4096 + 256)
#define SRIC_REPSZ	128
#define SRIC_SVCNAME	"slric"

#define SRII_NTHREADS	8
#define SRII_NBUFS	1024
#define SRII_BUFSZ	(4096 + 256)
#define SRII_REPSZ	128
#define SRII_SVCNAME	"slrii"

void	rpc_initsvc(void);

int slrim_handler(struct pscrpc_request *);
int slric_handler(struct pscrpc_request *);
int slrii_handler(struct pscrpc_request *);

extern struct cfd_svrops *cfdOps;
extern struct slashrpc_cservice *rmi_csvc;

#endif /* _IO_RPC_H_ */
