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

#ifndef _SLC_RPC_H_
#define _SLC_RPC_H_

struct pscrpc_import;
struct pscrpc_request;

#include "slconn.h"

/* Slash RPC channel for client from MDS. */
#define SRCM_NTHREADS	8
#define SRCM_NBUFS	512
#define SRCM_BUFSZ	384
#define SRCM_REPSZ	384
#define SRCM_SVCNAME	"msrcmthr"

#define slc_geticsvc(resm)							\
	sl_csvc_get(&resm2rmci(resm)->rmci_csvc, 0, NULL, (resm)->resm_nid,	\
	    SRIC_REQ_PORTAL, SRIC_REP_PORTAL, SRIC_MAGIC, SRIC_VERSION,		\
	    &resm2rmci(resm)->rmci_lock, &resm2rmci(resm)->rmci_waitq,		\
	    SLCONNT_IOD)

#define slc_getmcsvc(resm)							\
	sl_csvc_get(&resm2rmci(resm)->rmci_csvc, 0, NULL, (resm)->resm_nid,	\
	    SRMC_REQ_PORTAL, SRMC_REP_PORTAL, SRMC_MAGIC, SRMC_VERSION,		\
	    &resm2rmci(resm)->rmci_lock, &resm2rmci(resm)->rmci_waitq,		\
	    SLCONNT_MDS)

void	slc_rpc_initsvc(void);

struct pscrpc_import *
	slc_rmc_getimp(void);
int	slc_rmc_setmds(const char *);

int	slc_rcm_handler(struct pscrpc_request *);

#endif /* _SLC_RPC_H_ */
