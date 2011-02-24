/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2011, Pittsburgh Supercomputing Center (PSC).
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
 * The slab interface provides a backing for storing regions of file
 * space in CLI memory.  The slab API provides hooks into the RPC
 * layer for managing transportation over the network.
 */

#ifndef _SL_BUFFER_H_
#define _SL_BUFFER_H_

#include "pfl/cdefs.h"
#include "psc_ds/list.h"
#include "psc_ds/listcache.h"
#include "psc_util/atomic.h"
#include "psc_util/lock.h"
#include "psc_util/pool.h"

#include "cache_params.h"

struct psc_vbitmap;

#define SLB_BLKSZ	SLASH_SLVR_BLKSZ
#define SLB_SIZE	(SLB_BLKSZ * SLB_NBLK)

enum {
	SLB_DIRTY	= 0x01, /* have dirty data		*/
	SLB_INFLIGHT	= 0x02, /* data faulting in or out	*/
	SLB_FREEING	= 0x04,
	SLB_PINNED	= 0x08, /* not freeable			*/
	SLB_LRU		= 0x10, /* on the lru, nothing pinned or dirty */
	SLB_FREE	= 0x20,
	SLB_INIT	= 0x40,
	SLB_FRESH	= 0x80,
};

#define SLB_FULL(slb) (!psc_vbitmap_nfree((slb)->slb_inuse))

#define SLB_IOV2EBASE(iov, slb)						\
	(((iov)->oftiov_base + ((iov)->oftiov_nblks * (slb)->slb_blksz)) - 1)

#define SLB_REF2EBASE(ref, slb)						\
	(((ref)->slbir_base + ((ref)->slbir_nblks * (slb)->slb_blksz)) - 1)

#define SLB_SLB2EBASE(slb)						\
	(((slb)->slb_base + ((slb)->slb_nblks * (slb)->slb_blksz)) - 1)

/*
 * sl_buffer - used for both read caching and write
 *   aggregation.  The buffer is split into N subsections where N is
 *   the size of the vbitmap structure.
 * @slb_ref is maintained for every offtree entry which accesses this
 *     buffer.
 * @slb_iov_list is used to hold a sorted list of offtree_memb pointers
 *    (sorted by floff).  This is used when the LRU tells us to free
 *    our segments.
 * @slb_mgmt_lentry is used for the global free list, global LRU, and
 *    the dirty list.
 */
struct sl_buffer {
	struct psc_vbitmap	*slb_inuse;		/* track which segments are busy	*/
	int			 slb_nblks;		/* num blocks				*/
	uint32_t		 slb_blksz;		/* blocksize				*/
	void			*slb_base;		/* point to the data buffer		*/
	atomic_t		 slb_ref;
	atomic_t		 slb_unmapd_ref;
	atomic_t		 slb_inflight;
	atomic_t		 slb_inflpndg;
	psc_spinlock_t		 slb_lock;
	uint32_t		 slb_flags;
	struct psc_listcache	*slb_lc_owner;
	struct psc_lockedlist	*slb_lc_fcmh;
	struct psclist_head	 slb_iov_list;		/* list iovref backpointers */
	struct psclist_head	 slb_mgmt_lentry;	/* chain lru or outgoing q  */
	struct psclist_head	 slb_fcmh_lentry;	/* chain in fidc_membh      */
};

#define DEBUG_SLB_FLAGS(slb)						\
	ATTR_TEST((slb)->slb_flags, SLB_DIRTY)		? "d" : "",	\
	ATTR_TEST((slb)->slb_flags, SLB_INFLIGHT)	? "I" : "",	\
	ATTR_TEST((slb)->slb_flags, SLB_FREEING)	? "F" : "",	\
	ATTR_TEST((slb)->slb_flags, SLB_PINNED)		? "P" : "",	\
	ATTR_TEST((slb)->slb_flags, SLB_LRU)		? "L" : "",	\
	ATTR_TEST((slb)->slb_flags, SLB_FREE)		? "f" : "",	\
	ATTR_TEST((slb)->slb_flags, SLB_INIT)		? "i" : "",	\
	ATTR_TEST((slb)->slb_flags, SLB_FRESH)		? "r" : ""

#define SLB_FLAGS_FMT "%s%s%s%s%s%s%s%s"

#define DEBUG_SLB(level, slb, fmt, ...)					\
	psc_logs((level), PSS_DEF,					\
		"slb@%p b:%p sz(%d/%d) bsz:%u"				\
		" ref:%d umref:%d inf:%d infp:%d fl:"SLB_FLAGS_FMT	\
		" fcmh:%p lco:%p "fmt,					\
		(slb), (slb)->slb_base, (slb)->slb_nblks,		\
		psc_vbitmap_nfree((slb)->slb_inuse),			\
		(slb)->slb_blksz,					\
		atomic_read(&(slb)->slb_ref),				\
		atomic_read(&(slb)->slb_unmapd_ref),			\
		atomic_read(&(slb)->slb_inflight),			\
		atomic_read(&(slb)->slb_inflpndg),			\
		DEBUG_SLB_FLAGS(slb),					\
		(slb)->slb_lc_fcmh, (slb)->slb_lc_owner,		\
		## __VA_ARGS__)

#define DUMP_SLB(level, slb, fmt, ...)					\
	do {								\
		struct sl_buffer_iovref *__r;				\
		int __l;						\
									\
		DEBUG_SLB((level), (slb), fmt, ## __VA_ARGS__);		\
		__l = reqlock(&(slb)->slb_lock);			\
		psclist_for_each_entry(__r, &(slb)->slb_iov_list,	\
		    slbir_lentry) {					\
			if (__r->slbir_pri) {				\
				struct offtree_memb *__m;		\
									\
				__m = __r->slbir_pri;			\
				DEBUG_OFT((level), __m,			\
				    "SLB ref %p memb", __r);		\
				DEBUG_OFFTIOV((level),			\
				    __m->oft_norl.oft_iov,		\
				    "iov of memb %p", __m);		\
			} else						\
				psc_logs((level), PSS_DEF,		\
				    "--> Unmapped SLB ref %p memb "	\
				    fmt, __r, ## __VA_ARGS__);		\
		}							\
		ureqlock(&(slb)->slb_lock, __l);			\
	} while (0)

struct sl_buffer_iovref {
	void			*slbir_base;		/* base pointer val (within slb)	*/
	size_t			 slbir_nblks;		/* allocation size			*/
	void			*slbir_pri;		/* backpointer to oftmemb		*/
	void			*slbir_pri_bmap;
	int			 slbir_flags;
	struct psclist_head	 slbir_lentry;		/* chain to slb				*/
};

enum {
	SLBREF_MAPPED	= (1 << 0),	/* Backpointer to oftm in place	*/
	SLBREF_REAP	= (1 << 1)	/* Freeing			*/
};

/* Should have been done earlier
 * have to add ref's before adding to pin list
 */
#define slb_fresh_2_pinned(slb) do {					\
		ATTR_UNSET((slb)->slb_flags, SLB_FRESH);		\
		ATTR_SET((slb)->slb_flags, SLB_PINNED);			\
		(slb)->slb_lc_owner = NULL;				\
	} while (0)

#define slb_lru_2_pinned(slb) do {					\
		sl_buffer_lru_assertions(slb);				\
		ATTR_UNSET((slb)->slb_flags, SLB_LRU);			\
		ATTR_SET((slb)->slb_flags, SLB_PINNED);			\
	} while (0)

#define slb_pinned_2_lru(slb) do {					\
		sl_buffer_pin_2_lru_assertions(slb);			\
		ATTR_UNSET((slb)->slb_flags, SLB_PINNED);		\
		ATTR_SET((slb)->slb_flags, SLB_LRU);			\
		(slb)->slb_lc_owner = NULL;				\
	} while (0)

#define SLB_TIMEOUT_SECS	5
#define SLB_TIMEOUT_NSECS	0

#define slb_set_alloctimer(t) do {					\
		PFL_GETTIMESPEC(t);					\
		(t)->tv_sec  += SLB_TIMEOUT_SECS;			\
		(t)->tv_nsec += SLB_TIMEOUT_NSECS;			\
	} while (0)

#define SLB_RP_TIMEOUT_SECS	0
#define SLB_RP_TIMEOUT_NSECS	200000

#define slb_inflight_cb(iov, op)					\
	do {								\
		if (slInflightCb)					\
			(*slInflightCb)((iov), (op));			\
	} while (0)

#define SL_INFLIGHT_INC		0
#define SL_INFLIGHT_DEC		1

int  sl_buffer_init(struct psc_poolmgr *, void *);
void sl_buffer_destroy(void *);
void sl_buffer_cache_init(void);
void sl_buffer_fresh_assertions(struct sl_buffer *);
void sl_buffer_clear(struct sl_buffer *, size_t);

typedef int (*sl_iov_try_memrls_t)(void *);
typedef void (*sl_iov_memrls_ulock_t)(void *);

extern sl_iov_try_memrls_t	 slMemRlsTrylock;
extern sl_iov_memrls_ulock_t	 slMemRlsUlock;

extern struct psc_poolmgr	*slBufsPool;

#endif /* _SL_BUFFER_H_ */
