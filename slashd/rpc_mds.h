/* $Id$ */

#ifndef _MDS_RPC_H_
#define _MDS_RPC_H_

struct pscrpc_request;
struct pscrpc_export;

#define SLM_RMM_NTHREADS   8
#define SLM_RMM_NBUFS      1024
#define SLM_RMM_BUFSZ      128
#define SLM_RMM_REPSZ      128
#define SLM_RMM_SVCNAME    "slrmm"

#define SLM_RMI_NTHREADS   8
#define SLM_RMI_NBUFS      1024
#define SLM_RMI_BUFSZ      256
#define SLM_RMI_REPSZ      256
#define SLM_RMI_SVCNAME    "slrmi"

#define SLM_RMC_NTHREADS   8
#define SLM_RMC_NBUFS      1024
#define SLM_RMC_BUFSZ      384
#define SLM_RMC_REPSZ      384
#define SLM_RMC_SVCNAME    "slrmc"

#define resm2mri(resm)	((struct mds_resm_info *)(resm)->resm_pri)

/* aliases for connection management */
#define slm_geticonn(resm)						\
	slconn_get(&resm2mri(resm)->mri_csvc, NULL, (resm)->resm_nid,	\
	    SRIM_REQ_PORTAL, SRIM_REP_PORTAL, SRIM_MAGIC, SRIM_VERSION)

#define slm_getmconn(resm)						\
	slconn_get(&resm2mri(resm)->mri_csvc, NULL, (resm)->resm_nid,	\
	    SRMM_REQ_PORTAL, SRMM_REP_PORTAL, SRMM_MAGIC, SRMM_VERSION)

#define slm_initclconn(csvc, exp)					\
	slconn_get(&(csvc), (exp), 0, SRCM_REQ_PORTAL, SRCM_REP_PORTAL,	\
	    SRCM_MAGIC, SRCM_VERSION)

#define slm_getclconn(csvc)		slm_rcm_initconn((csvc), NULL)

void	rpc_initsvc(void);

int	slrmc_handler(struct pscrpc_request *);
int	slrmi_handler(struct pscrpc_request *);
int	slrmm_handler(struct pscrpc_request *);

#endif /* _MDS_RPC_H_ */
