/* $Id$ */

#ifndef _IO_RPC_H_
#define _IO_RPC_H_

#include <sys/types.h>

#include "cfd.h"

struct sli_repl_workrq;

#define SLI_RIM_NTHREADS	8
#define SLI_RIM_NBUFS		1024
#define SLI_RIM_BUFSZ		256
#define SLI_RIM_REPSZ		256
#define SLI_RIM_SVCNAME		"slirim"

#define SLI_RIC_NTHREADS	8
#define SLI_RIC_NBUFS		1024
#define SLI_RIC_BUFSZ		(4096 + 256)
#define SLI_RIC_REPSZ		128
#define SLI_RIC_SVCNAME		"sliric"

#define SLI_RII_NTHREADS	8
#define SLI_RII_NBUFS		1024
#define SLI_RII_BUFSZ		(4096 + 256)
#define SLI_RII_REPSZ		128
#define SLI_RII_SVCNAME		"slirii"

#define resm2iri(resm)		((struct iod_resm_info *)(resm)->resm_pri)

/* aliases for connection management */
#define sli_geticonn(resm)						\
	slconn_get(&resm2iri(resm)->iri_csvc, NULL, (resm)->resm_nid,	\
	    SRII_REQ_PORTAL, SRII_REP_PORTAL, SRII_MAGIC, SRII_VERSION,	\
	    SLCONNT_IOD)

#define sli_ric_handle_read(rq)		sli_ric_handle_io((rq), SL_READ)
#define sli_ric_handle_write(rq)	sli_ric_handle_io((rq), SL_WRITE)

void rpc_initsvc(void);

int sli_rim_handler(struct pscrpc_request *);
int sli_ric_handler(struct pscrpc_request *);
int sli_rii_handler(struct pscrpc_request *);

int sli_rmi_connect(const char *);

int sli_rmi_issue_repl_schedwk(struct sli_repl_workrq *);

int sli_rii_issue_read(struct pscrpc_import *, struct sli_repl_workrq *);

extern struct cfd_svrops	*cfdOps;
extern struct slashrpc_cservice	*rmi_csvc;

#endif /* _IO_RPC_H_ */
