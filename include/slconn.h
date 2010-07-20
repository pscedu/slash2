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

/*
 * This interface provides connections to hosts (servers and clients) in
 * a SLASH network.
 */

#ifndef _SLCONN_H_
#define _SLCONN_H_

#include <sys/types.h>
#include <sys/time.h>

#include "psc_util/lock.h"
#include "psc_util/atomic.h"

#include <stdint.h>

struct pscrpc_import;
struct pscrpc_export;

struct sl_resm;

enum slconn_type {
	SLCONNT_CLI,
	SLCONNT_IOD,
	SLCONNT_MDS,
	SLNCONNT
};

struct slconn_thread {
	struct sl_resm		*sct_resm;
	uint32_t		 sct_rqptl;
	uint32_t		 sct_rpptl;
	uint64_t		 sct_magic;
	uint32_t		 sct_version;
	void			*sct_lockp;
	void			*sct_waitinfo;
	enum slconn_type	 sct_conntype;
	int			 sct_flags;
};

struct slashrpc_cservice {
	struct pscrpc_import	*csvc_import;
	void			*csvc_lockp;
	void			*csvc_waitinfo;
	psc_atomic32_t		 csvc_flags;
	int			 csvc_lasterrno;
	psc_atomic32_t		 csvc_refcnt;
	time_t			 csvc_mtime;		/* last connection try */
};

/* csvc_flags */
#define CSVCF_CONNECTING	(1 << 0)
#define CSVCF_CONNECTED		(1 << 1)
#define CSVCF_USE_MULTIWAIT	(1 << 2)
#define CSVCF_ABANDON		(1 << 3)

#define CSVC_RECONNECT_INTV	30			/* seconds */

#define CSVC_GETSPINLOCK(csvc)	((psc_spinlock_t *)(csvc)->csvc_lockp)

struct slashrpc_export {
	uint64_t		 slexp_nextcfd;
	enum slconn_type	 slexp_peertype;
	void			*slexp_data;
	int			 slexp_flags;
	struct pscrpc_export	*slexp_export;
	struct psclist_head      slexp_bmlhd;		/* bmap leases */
};

/* slashrpc_export flags */
#define SLEXPF_CLOSING		(1 << 0)		/* XXX why do we need this? */

#define sl_csvc_waitrel_s(csvc, s)	_sl_csvc_waitrelv((csvc), (s), 0L)

struct slashrpc_cservice *
	sl_csvc_get(struct slashrpc_cservice **, int, struct pscrpc_export *,
	    lnet_nid_t, uint32_t, uint32_t, uint64_t, uint32_t,
	    void *, void *, enum slconn_type);
void	sl_csvc_decref(struct slashrpc_cservice *);
void	sl_csvc_free(struct slashrpc_cservice *);
void	sl_csvc_incref(struct slashrpc_cservice *);
void	sl_csvc_lock_ensure(struct slashrpc_cservice *);
int	sl_csvc_useable(struct slashrpc_cservice *);
int	sl_csvc_usemultiwait(struct slashrpc_cservice *);
void   _sl_csvc_waitrelv(struct slashrpc_cservice *, long, long);
void	sl_csvc_wake(struct slashrpc_cservice *);

struct slashrpc_export *
	slexp_get(struct pscrpc_export *, enum slconn_type);
void    slexp_put(struct pscrpc_export *);
void	slexp_destroy(void *);

void	slconnthr_spawn(int, struct sl_resm *, const char *);

extern struct psc_dynarray lnet_nids;

extern void (*slexp_freef[SLNCONNT])(struct pscrpc_export *);

#endif /* _SLCONN_H_ */
