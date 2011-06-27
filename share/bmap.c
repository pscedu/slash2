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

#define PSC_SUBSYS SLSS_BMAP
#include "slsubsys.h"

#include <limits.h>

#include "pfl/cdefs.h"
#include "psc_ds/tree.h"
#include "psc_ds/treeutil.h"
#include "psc_util/alloc.h"
#include "psc_util/lock.h"

#include "lnet/types.h"

#include "bmap.h"
#include "fidcache.h"
#include "slashrpc.h"
#include "slerr.h"

__static SPLAY_GENERATE(bmap_cache, bmapc_memb, bcm_tentry, bmap_cmp);

struct psc_poolmaster	 bmap_poolmaster;
struct psc_poolmgr	*bmap_pool;

/**
 * bmap_cmp - comparator for bmapc_membs in the splay cache.
 * @a: a bmapc_memb
 * @b: another bmapc_memb
 */
int
bmap_cmp(const void *x, const void *y)
{
	const struct bmapc_memb *a = x, *b = y;

	return (CMP(a->bcm_bmapno, b->bcm_bmapno));
}

void
bmap_orphan(struct bmapc_memb *b)
{
	struct fidc_membh *f = b->bcm_fcmh;
	int locked;

	locked = BMAP_RLOCK(b);

	DEBUG_BMAP(PLL_INFO, b, "orphan");
	psc_assert(!(b->bcm_flags & (BMAP_ORPHAN|BMAP_CLOSING)));
	b->bcm_flags |= BMAP_ORPHAN;
	BMAP_URLOCK(b, locked);

	FCMH_LOCK(f);
	psc_assert(f->fcmh_refcnt > 0);
	PSC_SPLAY_XREMOVE(bmap_cache, &f->fcmh_bmaptree, b);

	FCMH_ULOCK(f);
}

void
bmap_remove(struct bmapc_memb *b)
{
	struct fidc_membh *f = b->bcm_fcmh;

	DEBUG_BMAP(PLL_INFO, b, "removing");

	psc_assert(b->bcm_flags & BMAP_CLOSING);
	psc_assert(!(b->bcm_flags & BMAP_DIRTY));
	psc_assert(!atomic_read(&b->bcm_opcnt));

	FCMH_RLOCK(f);

	if (!(b->bcm_flags & BMAP_ORPHAN))
		PSC_SPLAY_XREMOVE(bmap_cache, &f->fcmh_bmaptree, b);

	psc_pool_return(bmap_pool, b);
	fcmh_op_done_type(f, FCMH_OPCNT_BMAP);
}

void
_bmap_op_done(const struct pfl_callerinfo *pci, struct bmapc_memb *b,
    const char *fmt, ...)
{
	va_list ap;

	PFL_START_TRACE(pci);

	BMAP_LOCK_ENSURE(b);
	(b)->bcm_flags &= ~BMAP_BUSY;

	va_start(ap, fmt);
	psclogsv(PLL_DEBUG, SLSS_BMAP, fmt, ap);
	va_end(ap);

	if (!psc_atomic32_read(&b->bcm_opcnt)) {
		b->bcm_flags |= BMAP_CLOSING;
		BMAP_ULOCK(b);

		if (bmap_ops.bmo_final_cleanupf)
			bmap_ops.bmo_final_cleanupf(b);

		bmap_remove(b);
	} else {
		bcm_wake_locked(b);
		BMAP_ULOCK(b);
	}
	PFL_END_TRACE();
}

__static struct bmapc_memb *
bmap_lookup_cache_locked(struct fidc_membh *f, sl_bmapno_t n)
{
	struct bmapc_memb lb, *b;

 restart:
	lb.bcm_bmapno = n;
	b = SPLAY_FIND(bmap_cache, &f->fcmh_bmaptree, &lb);
	if (b) {
		BMAP_LOCK(b);
		if (b->bcm_flags & BMAP_CLOSING) {
			/*
			 * This bmap is going away; wait for
			 * it so we can reload it back.
			 */
			BMAP_ULOCK(b);
			fcmh_wait_nocond_locked(f);
			goto restart;
		}
		bmap_op_start_type(b, BMAP_OPCNT_LOOKUP);
	}
	return (b);
}

/**
 * _bmap_get - Get the specified bmap.
 * @f: fcmh.
 * @n: bmap number.
 * @rw: access mode.
 * @flags: retrieval parameters.
 * @bp: value-result bmap pointer.
 * Notes: returns the bmap referenced via bcm_opcnt.
 */
int
_bmap_get(const struct pfl_callerinfo *pci, struct fidc_membh *f,
    sl_bmapno_t n, enum rw rw, int flags, struct bmapc_memb **bp)
{
	int rc = 0, do_load = 0, locked, bmaprw = 0;
	struct bmapc_memb *b;

	PFL_START_TRACE(pci);

	*bp = NULL;

	if (rw)
		bmaprw = ((rw == SL_WRITE) ? BMAP_WR : BMAP_RD);

	locked = FCMH_RLOCK(f);
	b = bmap_lookup_cache_locked(f, n);
	if (b == NULL) {
		if ((flags & BMAPGETF_LOAD) == 0) {
			ureqlock(&f->fcmh_lock, locked);
			rc = ENOENT;
			goto out;
		}
		b = psc_pool_get(bmap_pool);
		memset(b, 0, bmap_pool->ppm_master->pms_entsize);
		INIT_PSC_LISTENTRY(&b->bcm_lentry);
		INIT_SPINLOCK(&b->bcm_lock);

		atomic_set(&b->bcm_opcnt, 0);
		b->bcm_fcmh = f;
		b->bcm_bmapno = n;

		/*
		 * Signify that the bmap is newly initialized and therefore
		 *  may not contain certain structures.
		 */
		b->bcm_flags = BMAP_INIT | bmaprw;

		bmap_op_start_type(b, BMAP_OPCNT_LOOKUP);
		/* Perform app-specific substructure initialization. */
		bmap_ops.bmo_init_privatef(b);

		/* Add to the fcmh's bmap cache */
		SPLAY_INSERT(bmap_cache, &f->fcmh_bmaptree, b);
		fcmh_op_start_type(f, FCMH_OPCNT_BMAP);
		do_load = 1;
	}
	FCMH_URLOCK(f, locked);

	if (do_load) {
		if ((flags & BMAPGETF_NORETRIEVE) == 0)
			rc = bmap_ops.bmo_retrievef(b, rw, flags);

		BMAP_LOCK(b);
		b->bcm_flags &= ~BMAP_INIT;
		bcm_wake_locked(b);
		if (rc)
			goto out;

	} else {
		/* Wait while BMAP_INIT is set.
		 */
		bcm_wait_locked(b, (b->bcm_flags & BMAP_INIT));

 retry:
		/* Not all lookups are done with the intent of
		 *   changing the bmap mode.  bmap_lookup() does not
		 *   specify a rw value.
		 */
		if (bmaprw && !(bmaprw & b->bcm_flags) &&
		    bmap_ops.bmo_mode_chngf) {
			/* Others wishing to access this bmap in the
			 *   same mode must wait until MDCHNG ops have
			 *   completed.  If the desired mode is present
			 *   then a thread may proceed without blocking
			 *   here so long as it only accesses structures
			 *   which pertain to its mode.
			 */
			if (b->bcm_flags & BMAP_MDCHNG) {
				bcm_wait_locked(b,
				    b->bcm_flags & BMAP_MDCHNG);
				goto retry;

			} else {
				b->bcm_flags |= BMAP_MDCHNG;
				BMAP_ULOCK(b);

				DEBUG_BMAP(PLL_INFO, b,
				   "about to mode change (rw=%d)", rw);

				rc = bmap_ops.bmo_mode_chngf(b, rw, 0);
				BMAP_LOCK(b);
				b->bcm_flags &= ~BMAP_MDCHNG;
				if (!rc)
					b->bcm_flags |= bmaprw;
				bcm_wake_locked(b);
				if (rc)
					goto out;
			}
		}
	}
 out:
	if (b) {
		DEBUG_BMAP(rc && (rc != SLERR_BMAP_INVALID ||
		    (flags & BMAPGETF_NOAUTOINST) == 0) ?
		    PLL_ERROR : PLL_INFO, b, "grabbed rc=%d", rc);
		if (rc)
			bmap_op_done_type(b, BMAP_OPCNT_LOOKUP);
		else {
			BMAP_ULOCK(b);
			*bp = b;
		}
	}
	PFL_END_TRACE();
	return (rc);
}

void
bmap_cache_init(size_t priv_size)
{
	_psc_poolmaster_init(&bmap_poolmaster, sizeof(struct bmapc_memb) +
	    priv_size, offsetof(struct bmapc_memb, bcm_lentry), PPMF_AUTO,
	    64, 64, 0, NULL, NULL, NULL, NULL, "bmap");
	bmap_pool = psc_poolmaster_getmgr(&bmap_poolmaster);
}

void
_dump_bmapod(struct bmapc_memb *bmap, const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	DEBUG_BMAPODV(PLL_MAX, bmap, fmt, ap);
	va_end(ap);
}

int
bmapdesc_access_check(struct srt_bmapdesc *sbd, enum rw rw,
    sl_ios_id_t ios_id, uint64_t ion_nid)
{
	if (rw == SL_READ) {
		/* Read requests can get by with looser authentication. */
		if (sbd->sbd_ion_nid != ion_nid &&
		    sbd->sbd_ion_nid != LNET_NID_ANY)
			return (EBADF);
		if (sbd->sbd_ios_id != ios_id &&
		    sbd->sbd_ios_id != IOS_ID_ANY)
			return (EBADF);
	} else if (rw == SL_WRITE) {
		if (sbd->sbd_ion_nid != ion_nid)
			return (EBADF);
		if (sbd->sbd_ios_id != ios_id)
			return (EBADF);
	} else {
		psclog_errorx("invalid rw mode: %d", rw);
		return (EBADF);
	}
	return (0);
}

void
_log_dump_bmapodv(const struct pfl_callerinfo *pci, int level,
    struct bmapc_memb *bmap, const char *fmt, va_list ap)
{
	char mbuf[LINE_MAX], rbuf[SL_MAX_REPLICAS + 1],
	     cbuf[SLASH_CRCS_PER_BMAP + 1];
	unsigned char *b = bmap->bcm_repls;
	int off, k, ch[NBREPLST];

	vsnprintf(mbuf, sizeof(mbuf), fmt, ap);

	ch[BREPLST_INVALID] = '-';
	ch[BREPLST_REPL_SCHED] = 's';
	ch[BREPLST_REPL_QUEUED] = 'q';
	ch[BREPLST_VALID] = '+';
	ch[BREPLST_TRUNCPNDG] = 't';
	ch[BREPLST_TRUNCPNDG_SCHED] = 'p';
	ch[BREPLST_GARBAGE] = 'g';
	ch[BREPLST_GARBAGE_SCHED] = 'x';

	for (k = 0, off = 0; k < SL_MAX_REPLICAS;
	    k++, off += SL_BITS_PER_REPLICA)
		rbuf[k] = ch[SL_REPL_GET_BMAP_IOS_STAT(b, off)];
	while (k > 1 && rbuf[k - 1] == '-')
		k--;
	rbuf[k] = '\0';

	for (k = 0; k < SLASH_CRCS_PER_BMAP; k++)
		if (bmap->bcm_crcstates[k] > 9)
			cbuf[k] = 'a' + bmap->bcm_crcstates[k] - 10;
		else
			cbuf[k] = '0' + bmap->bcm_crcstates[k];
	while (k > 1 && cbuf[k - 1] == '0')
		k--;
	cbuf[k] = '\0';

	_DEBUG_BMAP(pci, level, bmap, "repls={%s} crcstates=[0x%s] %s",
	    rbuf, cbuf, mbuf);
}

void
_log_dump_bmapod(const struct pfl_callerinfo *pci, int level,
    struct bmapc_memb *bmap, const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	_log_dump_bmapodv(pci, level, bmap, fmt, ap);
	va_end(ap);
}

#if PFL_DEBUG > 0
void
_dump_bmap_flags_common(uint32_t *flags, int *seq)
{
	PFL_PRFLAG(BMAP_RD, flags, seq);
	PFL_PRFLAG(BMAP_WR, flags, seq);
	PFL_PRFLAG(BMAP_INIT, flags, seq);
	PFL_PRFLAG(BMAP_DIO, flags, seq);
	PFL_PRFLAG(BMAP_DIORQ, flags, seq);
	PFL_PRFLAG(BMAP_CLOSING, flags, seq);
	PFL_PRFLAG(BMAP_DIRTY, flags, seq);
	PFL_PRFLAG(BMAP_MEMRLS, flags, seq);
	PFL_PRFLAG(BMAP_DIRTY2LRU, flags, seq);
	PFL_PRFLAG(BMAP_TIMEOQ, flags, seq);
	PFL_PRFLAG(BMAP_IONASSIGN, flags, seq);
	PFL_PRFLAG(BMAP_MDCHNG, flags, seq);
	PFL_PRFLAG(BMAP_WAITERS, flags, seq);
	PFL_PRFLAG(BMAP_ORPHAN, flags, seq);
	PFL_PRFLAG(BMAP_BUSY, flags, seq);
}

__weak void
dump_bmap_flags(uint32_t flags)
{
	int seq = 0;

	_dump_bmap_flags_common(&flags, &seq);
	if (flags)
		printf(" unknown: %x", flags);
	printf("\n");
}

void
_dump_bmap_common(struct bmapc_memb *b)
{
	DEBUG_BMAP(PLL_MAX, b, "");
}

__weak void
dump_bmap(struct bmapc_memb *b)
{
	_dump_bmap_common(b);
}
#endif
