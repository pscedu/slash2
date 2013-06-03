/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2012, Pittsburgh Supercomputing Center (PSC).
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

#ifndef _RPC_IOD_H_
#define _RPC_IOD_H_

#include <sys/types.h>

#include "slashrpc.h"
#include "slconfig.h"
#include "slconn.h"

struct sli_repl_workrq;

#define SLI_RIM_NTHREADS	8
#define SLI_RIM_NBUFS		1024
#define SLI_RIM_BUFSZ		384
#define SLI_RIM_REPSZ		384
#define SLI_RIM_SVCNAME		"slirim"

#define SLI_RIC_NTHREADS	32
#define SLI_RIC_NBUFS		4096
#define SLI_RIC_BUFSZ		648
#define SLI_RIC_REPSZ		256
#define SLI_RIC_SVCNAME		"sliric"

#define SLI_RII_NTHREADS	32
#define SLI_RII_NBUFS		8192
#define SLI_RII_BUFSZ		256
#define SLI_RII_REPSZ		256
#define SLI_RII_SVCNAME		"slirii"

/* counterpart to csvc */
struct sli_exp_cli {
	struct slashrpc_cservice	*iexpc_csvc;		/* must be first field */
	uint32_t			 iexpc_stkvers;		/* must be second field */
};

/* aliases for connection management */
#define sli_geticsvcx(resm, exp)					\
	sl_csvc_get(&(resm)->resm_csvc, 0, (exp), &(resm)->resm_nids,	\
	    SRII_REQ_PORTAL, SRII_REP_PORTAL, SRII_MAGIC, SRII_VERSION,	\
	    SLCONNT_IOD, NULL)

#define sli_getmcsvcx(resm, exp)					\
	sl_csvc_get(&(resm)->resm_csvc, 0, (exp), &(resm)->resm_nids,	\
	    SRMI_REQ_PORTAL, SRMI_REP_PORTAL, SRMI_MAGIC, SRMI_VERSION,	\
	    SLCONNT_MDS, NULL)

#define sli_geticsvc(resm)		sli_geticsvcx((resm), NULL)
#define sli_getmcsvc(resm)		sli_getmcsvcx((resm), NULL)

#define sli_ric_handle_read(rq)		sli_ric_handle_io((rq), SL_READ)
#define sli_ric_handle_write(rq)	sli_ric_handle_io((rq), SL_WRITE)

void	sli_rpc_initsvc(void);

void	sli_rpc_mds_unpack_bminseq(struct pscrpc_request *, int);

int	sli_rim_handler(struct pscrpc_request *);
int	sli_ric_handler(struct pscrpc_request *);
int	sli_rii_handler(struct pscrpc_request *);

int	sli_rmi_getcsvc(struct slashrpc_cservice **);
int	sli_rmi_setmds(const char *);

int	sli_rmi_issue_repl_schedwk(struct sli_repl_workrq *);

int	sli_rii_issue_repl_read(struct slashrpc_cservice *, int, int,
	    struct sli_repl_workrq *);

extern struct pscrpc_svc_handle sli_ric_svc;
extern struct pscrpc_svc_handle sli_rii_svc;
extern struct pscrpc_svc_handle sli_rim_svc;

static __inline struct slashrpc_cservice *
sli_getclcsvc(struct pscrpc_export *exp)
{
	struct sli_exp_cli *iexpc;

	iexpc = sl_exp_getpri_cli(exp);
	return (sl_csvc_get(&iexpc->iexpc_csvc, 0, exp, NULL,
	    SRCI_REQ_PORTAL, SRCI_REP_PORTAL, SRCI_MAGIC, SRCI_VERSION,
	    SLCONNT_CLI, NULL));
}

#endif /* _RPC_IOD_H_ */
