/* $Id$ */

#ifndef _MDS_RPC_H_
#define _MDS_RPC_H_

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

#define resm2mri(resm)	((struct mds_resm_info *)(resm)->resm_pri)

/* aliases for connection management */
#define slm_geticonn(resm)						\
	slconn_get(&resm2mri(resm)->mri_csvc, NULL, (resm)->resm_nid,	\
	    SRIM_REQ_PORTAL, SRIM_REP_PORTAL, SRIM_MAGIC, SRIM_VERSION,	\
	    &resm2mri(resm)->mri_lock, &resm2mri(resm)->mri_waitq, SLCONNT_IOD)

#define slm_getmconn(resm)						\
	slconn_get(&resm2mri(resm)->mri_csvc, NULL, (resm)->resm_nid,	\
	    SRMM_REQ_PORTAL, SRMM_REP_PORTAL, SRMM_MAGIC, SRMM_VERSION,	\
	    &resm2mri(resm)->mri_lock, &resm2mri(resm)->mri_waitq, SLCONNT_MDS)

#define slm_initclconn(mexpcli, exp)					\
	slconn_get(&(mexpcli)->mc_csvc, (exp), LNET_NID_ANY,		\
	    SRCM_REQ_PORTAL, SRCM_REP_PORTAL, SRCM_MAGIC, SRCM_VERSION,	\
	    &(mexpcli)->mc_lock, NULL, SLCONNT_CLI)

#define slm_getclconn(mexpcli)		slm_initclconn((mexpcli), NULL)

void	rpc_initsvc(void);

int	slm_rmc_handler(struct pscrpc_request *);
int	slm_rmi_handler(struct pscrpc_request *);
int	slm_rmm_handler(struct pscrpc_request *);

#endif /* _MDS_RPC_H_ */
