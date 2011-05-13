/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2011, Pittsburgh Supercomputing Center (PSC).
 *
 * Permission to use, copy, and modify this software and its documentation
 * without fee for personal use or non-commercial use within your organization
 * is hereby granted, provided that the above copyright notice is preserved in
 * all copies and that the copyright and this permission notice appear in
 * supporting documentation.  Permission to redistribute this software to other
 * organizations or individuals is not permitted without the written permission
 * of the Pittsburgh Supercomputing Center.  PSC makes no representations about
 * the suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

#ifndef _RPC_MDS_H_
#define _RPC_MDS_H_

#include "psc_util/multiwait.h"

#include "slconn.h"
#include "slashrpc.h"

struct pscrpc_request;
struct pscrpc_export;

#define SLM_RMM_NTHREADS		8
#define SLM_RMM_NBUFS			1024
#define SLM_RMM_BUFSZ			512
#define SLM_RMM_REPSZ			512
#define SLM_RMM_SVCNAME			"slmrmm"

#define SLM_RMI_NTHREADS		8
#define SLM_RMI_NBUFS			1024
#define SLM_RMI_BUFSZ			512
#define SLM_RMI_REPSZ			1320
#define SLM_RMI_SVCNAME			"slmrmi"

#define SLM_RMC_NTHREADS		32
#define SLM_RMC_NBUFS			1024
#define SLM_RMC_BUFSZ			512
#define SLM_RMC_REPSZ			588
#define SLM_RMC_SVCNAME			"slmrmc"

enum slm_fwd_op {
	SLM_FORWARD_MKDIR,
	SLM_FORWARD_RMDIR,
	SLM_FORWARD_CREATE,
	SLM_FORWARD_UNLINK
};

/*
 * The number of update or reclaim records saved in the same log file.
 * Each log record is identified by its transaction ID (xid), which is
 * always increasing, but not necessary contiguous.
 *
 * Increasing these values should help logging performance because we
 * can then sync less often.  However, the size of any individual log
 * file must be less than LNET_MTU so it can always be transmitted in
 * a single RPC bulk.
 */
#define SLM_UPDATE_BATCH		2048			/* namespace updates */
#define SLM_RECLAIM_BATCH		2048			/* garbage reclamation */

/* counterpart to csvc */
struct slm_cli_csvc_cpart {
	psc_spinlock_t			 mcccp_lock;
	struct psc_waitq		 mcccp_waitq;
};

struct slm_exp_cli {
	struct slashrpc_cservice	*mexpc_csvc;
	struct slm_cli_csvc_cpart	*mexpc_cccp;
	struct psclist_head		 mexpc_bmlhd;		/* bmap leases */
};

void	slm_rpc_initsvc(void);

int	slm_rmc_handler(struct pscrpc_request *);
int	slm_rmi_handler(struct pscrpc_request *);
int	slm_rmm_handler(struct pscrpc_request *);
int	slm_rmm_forward_namespace(sl_siteid_t, int, char *, uint32_t, struct slash_creds *);

/* aliases for connection management */
#define slm_getmcsvcx(resm, exp)					\
	sl_csvc_get(&(resm)->resm_csvc, CSVCF_USE_MULTIWAIT, (exp),	\
	    (resm)->resm_nid, SRMM_REQ_PORTAL, SRMM_REP_PORTAL,		\
	    SRMM_MAGIC, SRMM_VERSION, &resm2rmmi(resm)->rmmi_mutex,	\
	    &resm2rmmi(resm)->rmmi_mwcond, SLCONNT_MDS, NULL)

#define slm_geticsvcxf(resm, exp, fl, arg)				\
	sl_csvc_get(&(resm)->resm_csvc, CSVCF_USE_MULTIWAIT | (fl),	\
	    (exp), (resm)->resm_nid, SRIM_REQ_PORTAL, SRIM_REP_PORTAL,	\
	    SRIM_MAGIC,	SRIM_VERSION, &resm2rmmi(resm)->rmmi_mutex,	\
	    &resm2rmmi(resm)->rmmi_mwcond, SLCONNT_IOD, (arg))

#define slm_getmcsvc(resm)		slm_getmcsvcx((resm), NULL)
#define slm_geticsvcx(resm, exp)	slm_geticsvcxf((resm), (exp), 0, NULL)
#define slm_geticsvc_nb(resm, ml)	slm_geticsvcxf((resm), NULL, CSVCF_NONBLOCK, (ml))
#define slm_geticsvc(resm)		slm_geticsvcxf((resm), NULL, 0, NULL)

static __inline struct slashrpc_cservice *
slm_getclcsvc(struct pscrpc_export *exp)
{
	struct slm_exp_cli *mexpc;

	mexpc = sl_exp_getpri_cli(exp);
	return (sl_csvc_get(&mexpc->mexpc_csvc, 0, exp, LNET_NID_ANY,
	    SRCM_REQ_PORTAL, SRCM_REP_PORTAL, SRCM_MAGIC, SRCM_VERSION,
	    &mexpc->mexpc_cccp->mcccp_lock,
	    &mexpc->mexpc_cccp->mcccp_waitq, SLCONNT_CLI, NULL));
}

#endif /* _RPC_MDS_H_ */
