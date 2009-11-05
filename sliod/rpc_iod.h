/* $Id$ */

#ifndef _IO_RPC_H_
#define _IO_RPC_H_

#include <sys/types.h>

#include "cfd.h"

struct sli_repl_workrq;

#define SRIM_NTHREADS	8
#define SRIM_NBUFS	1024
#define SRIM_BUFSZ	256
#define SRIM_REPSZ	256
#define SRIM_SVCNAME	"slirim"

#define SRIC_NTHREADS	8
#define SRIC_NBUFS	1024
#define SRIC_BUFSZ	(4096 + 256)
#define SRIC_REPSZ	128
#define SRIC_SVCNAME	"sliric"

#define SRII_NTHREADS	8
#define SRII_NBUFS	1024
#define SRII_BUFSZ	(4096 + 256)
#define SRII_REPSZ	128
#define SRII_SVCNAME	"slirii"

#define slric_handle_read(rq)	slric_handle_io((rq), SL_READ)
#define slric_handle_write(rq)	slric_handle_io((rq), SL_WRITE)

void	rpc_initsvc(void);

int sli_rim_handler(struct pscrpc_request *);
int slric_handler(struct pscrpc_request *);
int sli_rii_handler(struct pscrpc_request *);

int sli_rmi_issue_connect(const char *);
int sli_rmi_issue_schedwk(struct pscrpc_import *, struct sli_repl_workrq *);

extern struct cfd_svrops *cfdOps;
extern struct slashrpc_cservice *rmi_csvc;

#endif /* _IO_RPC_H_ */
