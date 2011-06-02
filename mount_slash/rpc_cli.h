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

#ifndef _SLC_RPC_H_
#define _SLC_RPC_H_

#include "mount_slash.h"
#include "slconn.h"

struct pscfs_req;
struct pscrpc_completion;
struct pscrpc_import;
struct pscrpc_request;

extern struct pscrpc_completion rpcComp;

/* async RPC pointers */
#define MSL_CBARG_BMPCE			0
#define MSL_CBARG_CSVC			1
#define MSL_CBARG_BIORQ			2
#define MSL_CBARG_BIORQS		3
#define MSL_CBARG_RA			4
#define MSL_CBARG_BMAP			4 /* don't mix with RA! */

/* SLASH RPC channel for client from MDS. */
#define SRCM_NTHREADS			8
#define SRCM_NBUFS			64
#define SRCM_BUFSZ			512
#define SRCM_REPSZ			512
#define SRCM_SVCNAME			"msrcm"

#define slc_geticsvcxf(resm, fl, exp)						\
	sl_csvc_get(&(resm)->resm_csvc, CSVCF_USE_MULTIWAIT | (fl), (exp),	\
	    (resm)->resm_nid, SRIC_REQ_PORTAL, SRIC_REP_PORTAL,			\
	    SRIC_MAGIC, SRIC_VERSION, &resm2rmci(resm)->rmci_mutex,		\
	    &resm2rmci(resm)->rmci_mwc,	SLCONNT_IOD, msl_getmw())

#define slc_getmcsvcx(resm, fl, exp)						\
	sl_csvc_get(&(resm)->resm_csvc, CSVCF_USE_MULTIWAIT | (fl), (exp),	\
	    (resm)->resm_nid, SRMC_REQ_PORTAL, SRMC_REP_PORTAL,			\
	    SRMC_MAGIC, SRMC_VERSION, &resm2rmci(resm)->rmci_mutex,		\
	    &resm2rmci(resm)->rmci_mwc, SLCONNT_MDS, msl_getmw())

#define slc_geticsvc(resm)		slc_geticsvcxf((resm), 0, NULL)
#define slc_geticsvcx(resm, exp)	slc_geticsvcxf((resm), 0, (exp))
#define slc_geticsvc_nb(resm)		slc_geticsvcxf((resm), CSVCF_NONBLOCK, NULL)
#define slc_getmcsvc(resm)		slc_getmcsvcx((resm), 0, NULL)
#define slc_getmcsvc_nb(resm)		slc_getmcsvcx((resm), CSVC_NONBLOCK, NULL)

void	slc_rpc_initsvc(void);

int	slc_rmc_getimp(struct pscfs_req *, struct slashrpc_cservice **);
int	slc_rmc_getimp1(struct slashrpc_cservice **, struct sl_resm *);
int	slc_rmc_retry(struct pscfs_req *, int *);
int	slc_rmc_setmds(const char *);

int	slc_rcm_handler(struct pscrpc_request *);

static __inline struct psc_multiwait *
msl_getmw(void)
{
	struct psc_thread *thr;

	thr = pscthr_get();

	switch (thr->pscthr_type) {
	case MSTHRT_FS:
		return (&msfsthr(thr)->mft_mw);
	case MSTHRT_BMAPFLSHRLS:
		return (&msbmflrlsthr(thr)->mbfrlst_mw);
	case MSTHRT_BMAPFLSH:
		return (&msbmflthr(thr)->mbft_mw);
	case MSTHRT_BMAPREADAHEAD:
		return (&msbmfrathr(thr)->mbfra_mw);
	case MSTHRT_CTL:
		return (NULL);
	}
	psc_fatalx("unknown thread type");
}

#define MSL_GET_RQ_STATUS(csvc, rq, mp, rc)				\
	do {								\
		(rc) = (rq)->rq_repmsg->status;				\
		if ((rc) == 0)						\
			(rc) = (rq)->rq_status;				\
		if ((rc) == 0)						\
			(rc) = authbuf_check((rq), PSCRPC_MSG_REPLY);	\
		if ((rc) == 0)						\
			(rc) = (mp) ? (mp)->rc : ENOMSG;		\
		if ((rc) == SLERR_NOTCONN)				\
			sl_csvc_disconnect(csvc);			\
	} while (0)

#define MSL_GET_RQ_STATUS_TYPE(csvc, rq, type, rc)			\
	do {								\
		struct type *_mp;					\
									\
		_mp = pscrpc_msg_buf((rq)->rq_repmsg, 0, sizeof(*_mp));	\
		GET_RQ_STATUS((csvc), (rq), _mp, (rc));			\
	} while (0)

#endif /* _SLC_RPC_H_ */
