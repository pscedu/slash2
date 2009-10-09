/* $Id$ */

#ifndef _IO_RPC_H_
#define _IO_RPC_H_

#include <sys/types.h>

#include "../slashd/cfd.h"

#define SRIM_NTHREADS	8
#define SRIM_NBUFS	1024
#define SRIM_BUFSZ	256
#define SRIM_REPSZ	256
#define SRIM_SVCNAME	"sliorim"

#define SRIC_NTHREADS	8
#define SRIC_NBUFS	1024
#define SRIC_BUFSZ	(4096 + 256)
#define SRIC_REPSZ	128
#define SRIC_SVCNAME	"slioric"

#define SRII_NTHREADS	8
#define SRII_NBUFS	1024
#define SRII_BUFSZ	(4096 + 256)
#define SRII_REPSZ	128
#define SRII_SVCNAME	"sliorii"

#define slric_handle_read(rq)	slric_handle_io((rq), SL_READ)
#define slric_handle_write(rq)	slric_handle_io((rq), SL_WRITE)

void	rpc_initsvc(void);

int slrim_handler(struct pscrpc_request *);
int slric_handler(struct pscrpc_request *);
int slrii_handler(struct pscrpc_request *);

int slrmi_issue_connect(const char *);

extern struct cfd_svrops *cfdOps;
extern struct slashrpc_cservice *rmi_csvc;

#endif /* _IO_RPC_H_ */
