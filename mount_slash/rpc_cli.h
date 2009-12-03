/* $Id$ */

#ifndef _SLC_RPC_H_
#define _SLC_RPC_H_

struct pscrpc_import;
struct pscrpc_request;

/* Slash RPC channel for client from MDS. */
#define SRCM_NTHREADS	8
#define SRCM_NBUFS	512
#define SRCM_BUFSZ	384
#define SRCM_REPSZ	384
#define SRCM_SVCNAME	"msrcmthr"

#define slc_geticonn(resm)						\
	slconn_get(&resm2crmi(resm)->crmi_csvc, NULL, (resm)->resm_nid,	\
	    SRIC_REQ_PORTAL, SRIC_REP_PORTAL, SRIC_MAGIC, SRIC_VERSION,	\
	    &resm2crmi(resm)->crmi_lock, slconn_wake_waitq,		\
	    &resm2crmi(resm)->crmi_waitq, SLCONNT_IOD)

#define slc_getmconn(resm)						\
	slconn_get(&resm2crmi(resm)->crmi_csvc, NULL, (resm)->resm_nid,	\
	    SRMC_REQ_PORTAL, SRMC_REP_PORTAL, SRMC_MAGIC, SRMC_VERSION,	\
	    &resm2crmi(resm)->crmi_lock, slconn_wake_waitq,		\
	    &resm2crmi(resm)->crmi_waitq, SLCONNT_MDS)

void	slc_rpc_initsvc(void);

struct pscrpc_import *
	slc_rmc_getimp(void);
int	slc_rmc_setmds(const char *);

int	slc_rcm_handler(struct pscrpc_request *);

#endif /* _SLC_RPC_H_ */
