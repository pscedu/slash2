/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2012, Pittsburgh Supercomputing Center (PSC).
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

#ifndef _RPC_CLI_H_
#define _RPC_CLI_H_

#include "mount_slash.h"
#include "slconn.h"

struct pscfs_clientctx;
struct pscrpc_completion;
struct pscrpc_import;
struct pscrpc_request;

/* async RPC pointers */
#define MSL_CBARG_BMPCE			0
#define MSL_CBARG_CSVC			1
#define MSL_CBARG_BIORQ			3
#define MSL_CBARG_BIORQS		4
#define MSL_CBARG_BMPC			5
#define MSL_CBARG_BMAP			6

/* SLASH RPC channel for CLI from MDS. */
#define SRCM_NTHREADS			8
#define SRCM_NBUFS			256
#define SRCM_BUFSZ			512
#define SRCM_REPSZ			512
#define SRCM_SVCNAME			"msrcm"

/* SLASH RPC channel for CLI from ION. */
#define SRCI_NTHREADS			8
#define SRCI_NBUFS			256
#define SRCI_BUFSZ			512
#define SRCI_REPSZ			512
#define SRCI_SVCNAME			"msrci"

#define MSL_RMC_NEWREQ_PFCC(pfcc, f, csvc, op, rq, mq, mp, rc)		\
	do {								\
		struct sl_resm *_resm;					\
									\
		_resm = (f) ? fcmh_2_fci(f)->fci_resm : slc_rmc_resm;	\
		if (rq) {						\
			pscrpc_req_finished(rq);			\
			(rq) = NULL;					\
		}							\
		if (csvc) {						\
			sl_csvc_decref(csvc);				\
			(csvc) = NULL;					\
		}							\
		(rc) = slc_rmc_getcsvc((pfcc), _resm, &(csvc));		\
		if (rc)							\
			break;						\
		(rc) = SL_RSX_NEWREQ((csvc), (op), (rq), (mq),		\
		    (mp));						\
		if (rc) {						\
			sl_csvc_decref(csvc);				\
			(csvc) = NULL;					\
		}							\
	} while ((rc) && slc_rmc_retry_pfcc((pfcc), &(rc)))

#define MSL_RMC_NEWREQ(pfr, f, csvc, op, rq, mq, mp, rc)		\
	MSL_RMC_NEWREQ_PFCC(pscfs_getclientctx(pfr), (f), (csvc), (op),	\
	    (rq), (mq), (mp), (rc))

#define slc_geticsvcxf(resm, fl, exp)					\
	sl_csvc_get(&(resm)->resm_csvc, CSVCF_USE_MULTIWAIT | (fl),	\
	    (exp), &(resm)->resm_nids, SRIC_REQ_PORTAL,			\
	    SRIC_REP_PORTAL, SRIC_MAGIC, SRIC_VERSION,			\
	    &resm2rmci(resm)->rmci_mutex, &resm2rmci(resm)->rmci_mwc,	\
	    SLCONNT_IOD, msl_getmw())

#define slc_getmcsvcx(resm, fl, exp)					\
	sl_csvc_get(&(resm)->resm_csvc, CSVCF_USE_MULTIWAIT | (fl),	\
	    (exp), &(resm)->resm_nids, SRMC_REQ_PORTAL,			\
	    SRMC_REP_PORTAL, SRMC_MAGIC, SRMC_VERSION,			\
	    &resm2rmci(resm)->rmci_mutex, &resm2rmci(resm)->rmci_mwc,	\
	    SLCONNT_MDS, msl_getmw())

#define slc_geticsvc(resm)		slc_geticsvcxf((resm), 0, NULL)
#define slc_geticsvcx(resm, exp)	slc_geticsvcxf((resm), 0, (exp))
#define slc_geticsvc_nb(resm)		slc_geticsvcxf((resm), CSVCF_NONBLOCK, NULL)
#define slc_getmcsvc(resm)		slc_getmcsvcx((resm), 0, NULL)
#define slc_getmcsvc_nb(resm)		slc_getmcsvcx((resm), CSVC_NONBLOCK, NULL)

void	slc_rpc_initsvc(void);

int	slc_rmc_getcsvc(struct pscfs_clientctx *, struct sl_resm *, struct slashrpc_cservice **);
int	slc_rmc_getcsvc1(struct slashrpc_cservice **, struct sl_resm *);
int	slc_rmc_retry_pfcc(struct pscfs_clientctx *, int *);
int	slc_rmc_setmds(const char *);

#define slc_rmc_retry(pfr, rcp)	slc_rmc_retry_pfcc(pscfs_getclientctx(pfr), (rcp))

int	slc_rci_handler(struct pscrpc_request *);
int	slc_rcm_handler(struct pscrpc_request *);

extern struct pscrpc_completion rpcComp;

static __inline struct psc_multiwait *
msl_getmw(void)
{
	struct psc_thread *thr;

	thr = pscthr_get();

	switch (thr->pscthr_type) {
	case MSTHRT_FS:
		return (&msfsthr(thr)->mft_mw);
	case MSTHRT_ATTRFLSH:
		return (&msattrflthr(thr)->maft_mw);
	case MSTHRT_BMAPFLSHRLS:
		return (&msbmflrlsthr(thr)->mbfrlst_mw);
	case MSTHRT_BMAPFLSH:
		return (&msbmflthr(thr)->mbft_mw);
	case MSTHRT_BMAPREADAHEAD:
		return (&msbmfrathr(thr)->mbfra_mw);
	case MSTHRT_BMAPFLSHRPC:
		return (&msbmflrpc(thr)->mbflrpc_mw);
	case MSTHRT_BMAPLSWATCHER:
		return (&msbmflwthr(thr)->mbfwa_mw);
	case MSTHRT_RCI:
		return (&msrcithr(thr)->mrci_mw);
	case MSTHRT_CTL:
		return (NULL);
	}
	psc_fatalx("unknown thread type");
}

#endif /* _RPC_CLI_H_ */
