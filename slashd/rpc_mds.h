/* $Id$ */

#ifndef _MDS_RPC_H_
#define _MDS_RPC_H_

#include "psc_util/multilock.h"

#include "mdsexpc.h"

struct pscrpc_request;
struct pscrpc_export;

#define SLM_RMM_NTHREADS   8
#define SLM_RMM_NBUFS      1024
#define SLM_RMM_BUFSZ      128
#define SLM_RMM_REPSZ      128
#define SLM_RMM_SVCNAME    "slmrmm"

#define SLM_RMI_NTHREADS   8
#define SLM_RMI_NBUFS      1024
#define SLM_RMI_BUFSZ      256
#define SLM_RMI_REPSZ      256
#define SLM_RMI_SVCNAME    "slmrmi"

#define SLM_RMC_NTHREADS   8
#define SLM_RMC_NBUFS      1024
#define SLM_RMC_BUFSZ      384
#define SLM_RMC_REPSZ      384
#define SLM_RMC_SVCNAME    "slmrmc"

struct slm_rmi_expdata {
	struct pscrpc_export *smie_exp;
};

void	slm_rpc_initsvc(void);

int	slm_rmc_handler(struct pscrpc_request *);
int	slm_rmi_handler(struct pscrpc_request *);
int	slm_rmm_handler(struct pscrpc_request *);

struct slm_rmi_expdata *
	slm_rmi_getexpdata(struct pscrpc_export *);

/* aliases for connection management */
#define slm_geticonn(resm)						\
	slconn_get(&resm2mrmi(resm)->mrmi_csvc, NULL, (resm)->resm_nid,	\
	    SRIM_REQ_PORTAL, SRIM_REP_PORTAL, SRIM_MAGIC, SRIM_VERSION,	\
	    &resm2mrmi(resm)->mrmi_lock, slconn_wake_mwcond,		\
	    &resm2mrmi(resm)->mrmi_mwcond, SLCONNT_IOD)

#define slm_geticonnx(resm, exp)					\
	slconn_get(&resm2mrmi(resm)->mrmi_csvc, (exp), 0,		\
	    SRIM_REQ_PORTAL, SRIM_REP_PORTAL, SRIM_MAGIC, SRIM_VERSION,	\
	    &resm2mrmi(resm)->mrmi_lock, slconn_wake_mwcond,		\
	    &resm2mrmi(resm)->mrmi_mwcond, SLCONNT_IOD)

#define slm_getmconn(resm)						\
	slconn_get(&resm2mrmi(resm)->mrmi_csvc, NULL, (resm)->resm_nid,	\
	    SRMM_REQ_PORTAL, SRMM_REP_PORTAL, SRMM_MAGIC, SRMM_VERSION,	\
	    &resm2mrmi(resm)->mrmi_lock, slconn_wake_mwcond,		\
	    &resm2mrmi(resm)->mrmi_mwcond, SLCONNT_MDS)

static __inline struct slashrpc_cservice *
slm_getclconn(struct pscrpc_export *exp)
{
	struct mexp_cli *mexpc;

	mexpc = mexpcli_get(exp);
	return (slconn_get(&mexpc->mc_csvc, exp, LNET_NID_ANY,
	    SRCM_REQ_PORTAL, SRCM_REP_PORTAL, SRCM_MAGIC, SRCM_VERSION,
	    &mexpc->mc_lock, NULL, NULL, SLCONNT_CLI));
}

#endif /* _MDS_RPC_H_ */
