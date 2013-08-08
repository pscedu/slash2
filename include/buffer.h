/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2013, Pittsburgh Supercomputing Center (PSC).
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
 *
 * Pittsburgh Supercomputing Center	phone: 412.268.4960  fax: 412.268.5832
 * 300 S. Craig Street			e-mail: remarks@psc.edu
 * Pittsburgh, PA 15213			web: http://www.psc.edu/
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
#include "pfl/list.h"
#include "pfl/listcache.h"
#include "psc_util/atomic.h"
#include "psc_util/lock.h"
#include "psc_util/pool.h"

#include "cache_params.h"

struct psc_vbitmap;

#define SLB_BLKSZ	SLASH_SLVR_BLKSZ
#define SLB_SIZE	(SLB_BLKSZ * SLB_NBLK)

enum {
	SLB_FRESH	= 0x01
};

/**
 * sl_buffer - Used for both read caching and write
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
	int			 slb_nblks;		/* num blocks, XXX: always 32		*/
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
	struct psclist_head	 slb_mgmt_lentry;	/* chain lru or outgoing q  */
};

#define DEBUG_SLB(level, slb, fmt, ...)					\
	psclogs((level), PSS_DEF,					\
	    "slb@%p b:%p sz:%d bsz:%u"					\
	    " ref:%d umref:%d inf:%d infp:%d fl:%s"			\
	    " fcmh:%p lco:%p "fmt,					\
	    (slb), (slb)->slb_base, (slb)->slb_nblks,			\
	    (slb)->slb_blksz,						\
	    atomic_read(&(slb)->slb_ref),				\
	    atomic_read(&(slb)->slb_unmapd_ref),			\
	    atomic_read(&(slb)->slb_inflight),				\
	    atomic_read(&(slb)->slb_inflpndg),				\
	    ATTR_TEST((slb)->slb_flags, SLB_FRESH)	? "r" : "",	\
	    (slb)->slb_lc_fcmh, (slb)->slb_lc_owner,			\
	    ## __VA_ARGS__)

void sl_buffer_cache_init(void);
void sl_buffer_fresh_assertions(struct sl_buffer *);
void sl_buffer_clear(struct sl_buffer *, size_t);

extern struct psc_poolmgr	*sl_bufs_pool;

#endif /* _SL_BUFFER_H_ */
