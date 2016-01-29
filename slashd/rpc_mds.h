/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015, Google, Inc.
 * Copyright (c) 2006-2015, Pittsburgh Supercomputing Center (PSC).
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

#ifndef _RPC_MDS_H_
#define _RPC_MDS_H_

#include "slconn.h"
#include "slashrpc.h"

struct pscrpc_request;
struct pscrpc_export;

#define SLM_RMM_NTHREADS		1
#define SLM_RMM_NBUFS			1024
#define SLM_RMM_BUFSZ			640
#define SLM_RMM_REPSZ			512
#define SLM_RMM_SVCNAME			"slmrmm"

#define SLM_RMI_NTHREADS		8
#define SLM_RMI_NBUFS			1024
#define SLM_RMI_BUFSZ			768
#define SLM_RMI_REPSZ			1320
#define SLM_RMI_SVCNAME			"slmrmi"

#define SLM_RMC_NTHREADS		32
#define SLM_RMC_NBUFS			1024
#define SLM_RMC_BUFSZ			664
#define SLM_RMC_REPSZ			1024
#define SLM_RMC_SVCNAME			"slmrmc"

enum slm_fwd_op {
	SLM_FORWARD_CREATE,
	SLM_FORWARD_MKDIR,
	SLM_FORWARD_RMDIR,
	SLM_FORWARD_UNLINK,
	SLM_FORWARD_RENAME,
	SLM_FORWARD_SYMLINK,
	SLM_FORWARD_SETATTR
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
 *
 * These two numbers were determined when the size of each entry was
 * 512 bytes.
 */
#define SLM_UPDATE_BATCH_NENTS		2048			/* namespace updates */
#define SLM_RECLAIM_BATCH_NENTS		2048			/* garbage reclamation */

struct slm_exp_cli {
	struct slashrpc_cservice	 *mexpc_csvc;		/* must be first field */
	uint32_t			  mexpc_stkvers;	/* must be second field */
};

struct batchrq {
	uint64_t			  br_bid;		/* batch RPC ID */
	struct psc_listentry		  br_lentry;
	struct psc_listentry		  br_lentry_ml;
	struct timeval			  br_expire;
	struct sl_resource		 *br_res;

	struct pscrpc_request		 *br_rq;
	struct slashrpc_cservice	 *br_csvc;
	int				  br_snd_ptl:16;	/* bulk RPC portal */
	int				  br_rcv_ptl:16;	/* bulk RPC portal */
	int				  br_flags;
	int				  br_refcnt;
	int				  br_rc;

	void				 *br_buf;
	size_t				  br_len;

	void				 *br_reply;
	size_t				  br_replen;

	struct psc_dynarray		  br_scratch;
	void				(*br_cbf)(struct batchrq *, int);
};

#define BATCHF_RQINFL			(1 << 0)	/* request RPC inflight */
#define BATCHF_WAITREPLY		(1 << 1)	/* awaiting RPC reply */

#define PFLOG_BATCHRPC(level, br, fmt, ...)				\
	psclogs((level), PSS_RPC,					\
	    "batch@%p bid=%"PRIu64" refs=%d buf=%p "			\
	    "flags=%#x len=%zd rc=%d "fmt,				\
	    (br), (br)->br_bid, (br)->br_refcnt, (br)->br_buf,		\
	    (br)->br_flags, (br)->br_len, (br)->br_rc, ##__VA_ARGS__)

#define batchrq_2_lc(br)		(&res2rpmi((br)->br_res)->rpmi_batchrqs)

void	slm_rpc_initsvc(void);

int	slm_rmc_handle_lookup(struct pscrpc_request *);

int	slm_rmc_handler(struct pscrpc_request *);
int	slm_rmi_handler(struct pscrpc_request *);
int	slm_rmm_handler(struct pscrpc_request *);
int	slm_rmm_forward_namespace(int, struct sl_fidgen *,
	    struct sl_fidgen *, char *, char *, uint32_t,
	    const struct slash_creds *, struct srt_stat *, int32_t);

int	slm_mkdir(int, struct srm_mkdir_req *, struct srm_mkdir_rep *,
	    int, struct fidc_membh **);
int	slm_symlink(struct pscrpc_request *, struct srm_symlink_req *,
	    struct srm_symlink_rep *, int);

int	batchrq_add(struct sl_resource *, struct slashrpc_cservice *,
	    uint32_t, int, int, void *, size_t, void *,
	    void (*)(struct batchrq *, int), int);
int	sl_handle_batchrp(struct pscrpc_request *);

void	slmbchrqthr_spawn(void);

/* aliases for connection management */
#define slm_getmcsvc(resm, exp, fl, mw)					\
	sl_csvc_get(&(resm)->resm_csvc, (fl), (exp),			\
	    &(resm)->resm_nids, SRMM_REQ_PORTAL, SRMM_REP_PORTAL,	\
	    SRMM_MAGIC, SRMM_VERSION, SLCONNT_MDS, (mw))

#define slm_geticsvc(resm, exp, fl, mw)					\
	sl_csvc_get(&(resm)->resm_csvc, (fl), (exp),			\
	    &(resm)->resm_nids, SRIM_REQ_PORTAL, SRIM_REP_PORTAL,	\
	    SRIM_MAGIC,	SRIM_VERSION, SLCONNT_IOD, (mw))

#define slm_getclcsvc(x)	_slm_getclcsvc(PFL_CALLERINFO(), (x))

#define slm_getmcsvcx(m, x)	slm_getmcsvc((m), (x), 0, NULL)
#define slm_getmcsvcf(m, fl)	slm_getmcsvc((m), NULL, (fl), NULL)
#define slm_getmcsvc_wait(m)	slm_getmcsvc((m), NULL, 0, NULL)

#define slm_geticsvcx(m, x)	slm_geticsvc((m), (x), 0, NULL)
#define slm_geticsvcf(m, fl)	slm_geticsvc((m), NULL, (fl), NULL)
#define slm_geticsvc_nb(m, mw)	slm_geticsvc((m), NULL, CSVCF_NONBLOCK, (mw))

#define _pfl_callerinfo pci
static __inline struct slashrpc_cservice *
_slm_getclcsvc(const struct pfl_callerinfo *pci,
    struct pscrpc_export *exp)
{
	struct slm_exp_cli *mexpc;

	mexpc = sl_exp_getpri_cli(exp, 0);
	if (mexpc == NULL)
		return (NULL);
	return (sl_csvc_get(&mexpc->mexpc_csvc, 0, exp, NULL,
	    SRCM_REQ_PORTAL, SRCM_REP_PORTAL, SRCM_MAGIC, SRCM_VERSION,
	    SLCONNT_CLI, NULL));
}
#undef _pfl_callerinfo

#endif /* _RPC_MDS_H_ */
