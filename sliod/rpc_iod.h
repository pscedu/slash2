/* $Id$ */

#ifndef _IO_RPC_H_
#define _IO_RPC_H_

#include <sys/types.h>

struct sli_repl_workrq;

#define SLI_RIM_NTHREADS	8
#define SLI_RIM_NBUFS		1024
#define SLI_RIM_BUFSZ		256
#define SLI_RIM_REPSZ		256
#define SLI_RIM_SVCNAME		"slirim"

#define SLI_RIC_NTHREADS	32
#define SLI_RIC_NBUFS		1024
#define SLI_RIC_BUFSZ		(4096 + 256)
#define SLI_RIC_REPSZ		128
#define SLI_RIC_SVCNAME		"sliric"

#define SLI_RII_NTHREADS	8
#define SLI_RII_NBUFS		1024
#define SLI_RII_BUFSZ		(4096 + 256)
#define SLI_RII_REPSZ		128
#define SLI_RII_SVCNAME		"slirii"

/* aliases for connection management */
#define sli_geticonn(resm)						\
	slconn_get(&resm2irmi(resm)->irmi_csvc, NULL, (resm)->resm_nid,	\
	    SRII_REQ_PORTAL, SRII_REP_PORTAL, SRII_MAGIC, SRII_VERSION,	\
	    &resm2irmi(resm)->irmi_lock, slconn_wake_waitq,		\
	    &resm2irmi(resm)->irmi_waitq, SLCONNT_IOD)

#define sli_getmconn(resm)						\
	slconn_get(&resm2irmi(resm)->irmi_csvc, NULL, (resm)->resm_nid,	\
	    SRMI_REQ_PORTAL, SRMI_REP_PORTAL, SRMI_MAGIC, SRMI_VERSION,	\
	    &resm2irmi(resm)->irmi_lock, slconn_wake_waitq,		\
	    &resm2irmi(resm)->irmi_waitq, SLCONNT_MDS)

#define sli_ric_handle_read(rq)		sli_ric_handle_io((rq), SL_READ)
#define sli_ric_handle_write(rq)	sli_ric_handle_io((rq), SL_WRITE)

void	sli_rpc_initsvc(void);

int	sli_rim_handler(struct pscrpc_request *);
int	sli_ric_handler(struct pscrpc_request *);
int	sli_rii_handler(struct pscrpc_request *);

struct pscrpc_import *
	sli_rmi_getimp(void);
int	sli_rmi_setmds(const char *);

int	sli_rmi_issue_repl_schedwk(struct sli_repl_workrq *);

int	sli_rii_issue_repl_read(struct pscrpc_import *, struct sli_repl_workrq *);

#endif /* _IO_RPC_H_ */
