/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright 2006-2016, Pittsburgh Supercomputing Center
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or (at
 * your option) any later version.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License contained in the file
 * `COPYING-GPL' at the top of this distribution or at
 * https://www.gnu.org/licenses/gpl-2.0.html for more details.
 * ---------------------------------------------------------------------
 * %END_LICENSE%
 */

#ifndef _RPC_CLI_H_
#define _RPC_CLI_H_

#include "mount_slash.h"
#include "slconn.h"

struct pscfs_clientctx;
struct pscrpc_import;
struct pscrpc_request;

struct slrpc_cservice;

/* async RPC pointers, must be less than PSCRPC_MAX_ASYNC_ARGS */
#define MSL_CBARG_BMPCE			0
#define MSL_CBARG_CSVC			1
#define MSL_CBARG_BIORQ			3
#define MSL_CBARG_BIORQS		4
#define MSL_CBARG_BMAP			5
#define MSL_CBARG_RESM			6
#define MSL_CBARG_IOVS			7

enum {
	MSL_BMLGET_CBARG_BMAP,
	MSL_BMLGET_CBARG_COMPL,
	MSL_BMLGET_CBARG_CSVC
};

#define MSL_READDIR_CBARG_CSVC		0
#define MSL_READDIR_CBARG_FCMH		1
#define MSL_READDIR_CBARG_PAGE		2
#define MSL_READDIR_CBARG_DENTBUF	3

/* RPC channel for CLI from MDS. */
#define SRCM_NTHREADS			8
#define SRCM_NBUFS			256
#define SRCM_BUFSZ			512
#define SRCM_REPSZ			512
#define SRCM_SVCNAME			"msrcm"

/* RPC channel for CLI from ION. */
#define SRCI_NTHREADS			8
#define SRCI_NBUFS			256
#define SRCI_BUFSZ			512
#define SRCI_REPSZ			512
#define SRCI_SVCNAME			"msrci"

#define RESM_MAX_IOS_OUTSTANDING_RPCS	480
#define RESM_MAX_MDS_OUTSTANDING_RPCS	2048

/*
 * Initialize a new RPC request for a pscfs clientctx.
 * Most arguments here are macro-value-result.
 */
#define MSL_RMC_NEWREQ(f, csvc, op, rq, mq, mp, rc)			\
	do {								\
		struct sl_resm *_resm;					\
									\
		(mq) = NULL;						\
		(mp) = NULL;						\
		_resm = (f) ? fcmh_2_fci(f)->fci_resm : msl_rmc_resm;	\
		pscrpc_req_finished(rq);				\
		(rq) = NULL;						\
		if (csvc) {						\
			sl_csvc_decref(csvc);				\
			(csvc) = NULL;					\
		}							\
		(rc) = slc_rmc_getcsvc(_resm, &(csvc));			\
		if (rc)							\
			break;						\
		(rc) = SL_RSX_NEWREQ((csvc), (op), (rq), (mq), (mp));	\
		if (rc) {						\
			sl_csvc_decref(csvc);				\
			(csvc) = NULL;					\
		}							\
	} while (0)

/* obtain csvc to an IOS */
#define slc_geticsvcxf(resm, fl, exp)					\
	sl_csvc_get(&(resm)->resm_csvc, (fl), (exp),			\
	    &(resm)->resm_nids, SRIC_REQ_PORTAL, SRIC_REP_PORTAL,	\
	    SRIC_MAGIC, SRIC_VERSION, SLCONNT_IOD, msl_getmw())

/* obtain csvc to an MDS */
#define slc_getmcsvcxf(resm, fl, exp)					\
	sl_csvc_get(&(resm)->resm_csvc, (fl), (exp),			\
	    &(resm)->resm_nids, SRMC_REQ_PORTAL, SRMC_REP_PORTAL,	\
	    SRMC_MAGIC, SRMC_VERSION, SLCONNT_MDS, msl_getmw())

#define slc_geticsvc(resm)		slc_geticsvcxf((resm), 0, NULL)
#define slc_geticsvcx(resm, exp)	slc_geticsvcxf((resm), 0, (exp))
#define slc_geticsvcf(resm, fl)		slc_geticsvcxf((resm), (fl), NULL)
#define slc_geticsvc_nb(resm)		slc_geticsvcxf((resm), CSVCF_NONBLOCK, NULL)

#define slc_getmcsvcx(resm, exp)	slc_getmcsvcxf((resm), 0, (exp))
#define slc_getmcsvc(resm)		slc_getmcsvcxf((resm), 0, NULL)
#define slc_getmcsvcf(resm, fl)		slc_getmcsvcxf((resm), (fl), NULL)
#define slc_getmcsvc_nb(resm)		slc_getmcsvcxf((resm), CSVCF_NONBLOCK, NULL)

void	slc_rpc_initsvc(void);
int	slc_rpc_retry(struct pscfs_req *, int *);

int	slc_rmc_getcsvc(struct sl_resm *, struct slrpc_cservice **);
int	slc_rmc_setmds(const char *);

int	slc_rci_handler(struct pscrpc_request *);
int	slc_rcm_handler(struct pscrpc_request *);

extern struct pscrpc_svc_handle	*msl_rci_svh;
extern struct pscrpc_svc_handle	*msl_rcm_svh;

/* Grab calling thread's multiwait structure. */
static __inline struct pfl_multiwait *
msl_getmw(void)
{
	struct psc_thread *thr;

	thr = pscthr_get();
	switch (thr->pscthr_type) {
	case MSTHRT_ATTR_FLUSH:
		return (&msattrflushthr(thr)->maft_mw);
	case MSTHRT_BRELEASE:
		return (&msbreleasethr(thr)->mbrt_mw);
	case MSTHRT_BWATCH:
		return (&msbwatchthr(thr)->mbwt_mw);
	case MSTHRT_FLUSH:
		return (&msflushthr(thr)->mflt_mw);
	case PFL_THRT_FS:
		return (&msfsthr(thr)->mft_mw);
	case MSTHRT_RCI:
		return (&msrcithr(thr)->mrci_mw);
	case MSTHRT_RCM:
		return (&msrcmthr(thr)->mrcm_mw);
	case MSTHRT_READAHEAD:
		return (&msreadaheadthr(thr)->mrat_mw);
	case MSTHRT_CTL:
	case MSTHRT_WORKER:
	case PFL_THRT_CTL:
		return (NULL);
	}
	psc_fatalx("unknown thread type");
}

#endif /* _RPC_CLI_H_ */
