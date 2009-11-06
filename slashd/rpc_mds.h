/* $Id$ */

#ifndef _MDS_RPC_H_
#define _MDS_RPC_H_

struct pscrpc_request;
struct pscrpc_export;

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

/* aliases for connection management */
#define slm_mri_geticonn(mri)						\
	slconn_get(&(mri)->mri_csvc, NULL, (mri)->mri_resm->resm_nid,	\
	    SRIM_REQ_PORTAL, SRIM_REP_PORTAL, SRIM_MAGIC, SRIM_VERSION)

#define slm_rmm_getmconn(mri)						\
	slconn_get(&(mri)->mri_csvc, NULL, (mri)->mri_resm->resm_nid,	\
	    SRMM_REQ_PORTAL, SRMM_REP_PORTAL, SRMM_MAGIC, SRMM_VERSION)

#define slm_rcm_initconn(csvc, exp)					\
	slconn_get(&(csvc), (exp), 0, SRCM_REQ_PORTAL, SRCM_REP_PORTAL,	\
	    SRCM_MAGIC, SRCM_VERSION)

#define slm_rcm_getconn(csvc)		slm_rcm_initconn((csvc), NULL)

void	rpc_initsvc(void);

int	slrmc_handler(struct pscrpc_request *);
int	slrmi_handler(struct pscrpc_request *);
int	slrmm_handler(struct pscrpc_request *);

#endif /* _MDS_RPC_H_ */
