/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
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
#define SLM_RMM_BUFSZ			128
#define SLM_RMM_REPSZ			128
#define SLM_RMM_SVCNAME			"slmrmm"

#define SLM_RMI_NTHREADS		8
#define SLM_RMI_NBUFS			1024
#define SLM_RMI_BUFSZ			256
#define SLM_RMI_REPSZ			256
#define SLM_RMI_SVCNAME			"slmrmi"

#define SLM_RMC_NTHREADS		8
#define SLM_RMC_NBUFS			1024
#define SLM_RMC_BUFSZ			384
#define SLM_RMC_REPSZ			384
#define SLM_RMC_SVCNAME			"slmrmc"

/*
 * The number of namespace operations that are recorded in the same change log.
 * We make the size of each log file to be less than LNET_MTU, so that we can
 * send it out in one bulk RPC.
 */
#define SLM_NAMESPACE_BATCH	2048

struct slm_rmi_expdata {
	struct pscrpc_export		 *smie_exp;
};

struct mexp_cli {
	struct slashrpc_cservice	*mc_csvc;
	psc_spinlock_t			 mc_lock;
	struct psc_waitq		 mc_waitq;
};

struct mexp_cli	*mexpcli_get(struct pscrpc_export *);
void		 mexpcli_destroy(struct pscrpc_export *);

void	slm_rpc_initsvc(void);

int	slm_rmc_handler(struct pscrpc_request *);
int	slm_rmi_handler(struct pscrpc_request *);
int	slm_rmm_handler(struct pscrpc_request *);

struct slm_rmi_expdata *
	slm_rmi_getexpdata(struct pscrpc_export *);

/* aliases for connection management */
#define slm_getmcsvc(resm)							\
	sl_csvc_get(&resm2rmmi(resm)->rmmi_csvc, CSVCF_USE_MULTIWAIT, NULL,	\
	    (resm)->resm_nid, SRMM_REQ_PORTAL, SRMM_REP_PORTAL, SRMM_MAGIC,	\
	    SRMM_VERSION, &resm2rmmi(resm)->rmmi_lock,				\
	    &resm2rmmi(resm)->rmmi_mwcond, SLCONNT_MDS)

#define slm_geticsvc(resm)							\
	sl_csvc_get(&resm2rmmi(resm)->rmmi_csvc, CSVCF_USE_MULTIWAIT, NULL,	\
	    (resm)->resm_nid, SRIM_REQ_PORTAL, SRIM_REP_PORTAL, SRIM_MAGIC,	\
	    SRIM_VERSION, &resm2rmmi(resm)->rmmi_lock,				\
	    &resm2rmmi(resm)->rmmi_mwcond, SLCONNT_IOD)

#define slm_geticsvcx(resm, exp)						\
	sl_csvc_get(&resm2rmmi(resm)->rmmi_csvc, CSVCF_USE_MULTIWAIT, (exp),	\
	    0, SRIM_REQ_PORTAL, SRIM_REP_PORTAL, SRIM_MAGIC,			\
	    SRIM_VERSION, &resm2rmmi(resm)->rmmi_lock,				\
	    &resm2rmmi(resm)->rmmi_mwcond, SLCONNT_IOD)

static __inline struct slashrpc_cservice *
slm_getclcsvc(struct pscrpc_export *exp)
{
	struct mexp_cli *mexpc;

	mexpc = mexpcli_get(exp);
	return (sl_csvc_get(&mexpc->mc_csvc, 0, exp, LNET_NID_ANY,
	    SRCM_REQ_PORTAL, SRCM_REP_PORTAL, SRCM_MAGIC, SRCM_VERSION,
	    &mexpc->mc_lock, &mexpc->mc_waitq, SLCONNT_CLI));
}

#endif /* _RPC_MDS_H_ */
