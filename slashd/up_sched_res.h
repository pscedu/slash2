/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2010-2011, Pittsburgh Supercomputing Center (PSC).
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
 * Update scheduler: this component manages updates to I/O systems such
 * as file chunks to replicate and garbage reclamation.  One thread is
 * spawned per site to watch over activity destined for any I/O system
 * contained therein.
 */

#ifndef _UP_SCHED_RES_H_
#define _UP_SCHED_RES_H_

#include "subsys_mds.h"

struct up_sched_work_item {
	struct fidc_membh		*uswi_fcmh;	/* key is fg_fid; see uswi_cmp() */
	psc_atomic32_t			 uswi_refcnt;
	int				 uswi_gen;
	int				 uswi_flags;
	struct pfl_mutex		 uswi_mutex;
	struct psc_multiwaitcond	 uswi_mwcond;
	struct psclist_head		 uswi_lentry;
	SPLAY_ENTRY(up_sched_work_item)	 uswi_tentry;
};

/* work item flags */
#define USWIF_BUSY		(1 << 0)	/* work item is being modified */
#define USWIF_DIE		(1 << 1)	/* work item is going away */

#define USWI_INOH(wk)		fcmh_2_inoh((wk)->uswi_fcmh)
#define USWI_INO(wk)		(&USWI_INOH(wk)->inoh_ino)
#define USWI_INOX(wk)		USWI_INOH(wk)->inoh_extras
#define USWI_NREPLS(wk)		USWI_INO(wk)->ino_nrepls
#define USWI_FG(wk)		(&(wk)->uswi_fcmh->fcmh_fg)
#define USWI_FID(wk)		USWI_FG(wk)->fg_fid

#define USWI_GETREPL(wk, n)	((n) < SL_DEF_REPLICAS ?		\
				    USWI_INO(wk)->ino_repls[n] :	\
				    USWI_INOX(wk)->inox_repls[(n) - SL_DEF_REPLICAS])

#define USWI_LOCK(wk)		psc_mutex_lock(&(wk)->uswi_mutex)
#define USWI_ULOCK(wk)		psc_mutex_unlock(&(wk)->uswi_mutex)
#define USWI_RLOCK(wk)		psc_mutex_reqlock(&(wk)->uswi_mutex)
#define USWI_URLOCK(wk, lk)	psc_mutex_ureqlock(&(wk)->uswi_mutex, (lk))

enum {
/* 0 */	USWI_REFT_LOOKUP,	/* uswi_find() temporary */
/* 1 */	USWI_REFT_SITEUPQ,	/* in scheduler queue for a site */
/* 2 */	USWI_REFT_TREE		/* in tree/list in memory */
};

#define DEBUG_USWI(lvl, wk, fmt, ...)					\
	psclogs((lvl), SLMSS_UPSCH, "uswi@%p f+g:"SLPRI_FG" "		\
	    "fl:%#x:%s%s ref:%d gen:%d : " fmt,				\
	    (wk), SLPRI_FG_ARGS(USWI_FG(wk)), (wk)->uswi_flags,		\
	    (wk)->uswi_flags & USWIF_BUSY	? "b" : "",		\
	    (wk)->uswi_flags & USWIF_DIE	? "d" : "",		\
	    psc_atomic32_read(&(wk)->uswi_refcnt), (wk)->uswi_gen, ## __VA_ARGS__)

#define USWI_INCREF(wk, reftype)					\
	do {								\
		psc_atomic32_inc(&(wk)->uswi_refcnt);			\
		DEBUG_USWI(PLL_DEBUG, (wk),				\
		    "grabbed reference [type=%d]", (reftype));		\
	} while (0)

#define USWI_DECREF(wk, reftype)					\
	do {								\
		psc_assert(psc_atomic32_read(&(wk)->uswi_refcnt) > 0);	\
		psc_atomic32_dec(&(wk)->uswi_refcnt);			\
		DEBUG_USWI(PLL_DEBUG, (wk),				\
		    "dropped reference [type=%d]", (reftype));		\
	} while (0)

#define uswi_access(wk)		_uswi_access((wk), 0)
#define uswi_access_lock(wk)	_uswi_access((wk), 1)

#define uswi_unref(wk)		_uswi_unref(PFL_CALLERINFO(), (wk), 1)
#define uswi_unref_nowake(wk)	_uswi_unref(PFL_CALLERINFO(), (wk), 0)

struct up_sched_work_item *
	 uswi_find(const struct slash_fidgen *);
int	_uswi_access(struct up_sched_work_item *, int);
int	 uswi_cmp(const void *, const void *);
void	 uswi_enqueue_sites(struct up_sched_work_item *, const sl_replica_t *, int);
int	 uswi_findoradd(const struct slash_fidgen *, struct up_sched_work_item **);
void	 uswi_init(struct up_sched_work_item *, slfid_t);
int	_uswi_unref(const struct pfl_callerinfo *, struct up_sched_work_item *, int);

void	 upsched_scandir(void);

SPLAY_HEAD(upschedtree, up_sched_work_item);
SPLAY_PROTOTYPE(upschedtree, up_sched_work_item, uswi_tentry, uswi_cmp);

#define UPSCHED_MGR_LOCK()		PLL_LOCK(&upsched_listhd)
#define UPSCHED_MGR_ULOCK()		PLL_ULOCK(&upsched_listhd)
#define UPSCHED_MGR_HASLOCK()		PLL_HASLOCK(&upsched_listhd)
#define UPSCHED_MGR_RLOCK()		PLL_RLOCK(&upsched_listhd)
#define UPSCHED_MGR_URLOCK(lk)		PLL_URLOCK(&upsched_listhd, (lk))
#define UPSCHED_MGR_ENSURE_LOCKED()	PLL_ENSURE_LOCKED(&upsched_listhd)

extern struct psc_poolmgr	*upsched_pool;
extern struct upschedtree	 upsched_tree;
extern struct psc_lockedlist	 upsched_listhd;

#endif /* _UP_SCHED_RES_H_ */
