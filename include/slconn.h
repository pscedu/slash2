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

enum slconn_type {
	SLCONNT_CLI,
	SLCONNT_IOD,
	SLCONNT_MDS,
	SLNCONNT
};

struct slashrpc_cservice {
	struct pscrpc_import	*csvc_import;
	psc_spinlock_t		*csvc_lockp;
	void			*csvc_waitinfo;
	int			 csvc_flags;
	int			 csvc_lasterrno;
	psc_atomic32_t		 csvc_refcnt;
	time_t			 csvc_mtime;		/* last connection try */
};

/* csvc_flags */
#define CSVCF_CONNECTING	(1 << 0)
#define CSVCF_USE_MULTIWAIT	(1 << 1)

#define CSVC_RECONNECT_INTV	30			/* seconds */

#define CSVC_LOCK_ENSURE(c)	LOCK_ENSURE((c)->csvc_lockp)
#define CSVC_LOCK(c)		spinlock((c)->csvc_lockp)
#define CSVC_ULOCK(c)		freelock((c)->csvc_lockp)
#define CSVC_RLOCK(c)		reqlock((c)->csvc_lockp)
#define CSVC_URLOCK(c, lk)	ureqlock((c)->csvc_lockp, (lk))

struct slashrpc_export {
	uint64_t		 slexp_nextcfd;
	enum slconn_type	 slexp_peertype;
	void			*slexp_data;
	int			 slexp_flags;
	struct pscrpc_export	*slexp_export;
	struct psclist_head      slexp_list;
};

/* slashrpc_export flags */
#define SLEXPF_CLOSING		(1 << 0)		/* XXX why do we need this? */

struct slashrpc_cservice *
	sl_csvc_get(struct slashrpc_cservice **, int, struct pscrpc_export *,
	    lnet_nid_t, uint32_t, uint32_t, uint64_t, uint32_t,
	    psc_spinlock_t *, void *, enum slconn_type);
void	sl_csvc_decref(struct slashrpc_cservice *);
void	sl_csvc_incref(struct slashrpc_cservice *);
void	sl_csvc_free(struct slashrpc_cservice *);

struct slashrpc_export *
	slexp_get(struct pscrpc_export *, enum slconn_type);
void    slexp_put(struct pscrpc_export *);
void	slexp_destroy(void *);

extern struct psc_dynarray lnet_nids;

extern void (*slexp_freef[SLNCONNT])(struct pscrpc_export *);

#endif /* _SLCONN_H_ */
