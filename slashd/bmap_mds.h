/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2013, Pittsburgh Supercomputing Center (PSC).
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

#ifndef _SLASHD_BMAP_MDS_H_
#define _SLASHD_BMAP_MDS_H_

#include <sys/time.h>

#include "psc_ds/lockedlist.h"
#include "psc_rpc/rpc.h"
#include "psc_util/odtable.h"
#include "psc_util/pthrutil.h"

#include "bmap.h"
#include "inode.h"
#include "mdslog.h"
#include "slashd.h"
#include "up_sched_res.h"

struct srm_bmap_crcwrt_req;
struct srt_bmapdesc;

/*
 * bmap_mds_info - the bmap_get_pri() data structure for the SLASH2 MDS.
 *   bmap_mds_info holds all bmap specific context for the mds which
 *   includes the journal handle, ref counts for client readers and writers
 *   a point to our ION, a tree of our client's exports, a pointer to the
 *   on-disk structure, a receipt for the odtable, and a reqset for issuing
 *   callbacks (XXX is that really needed?).
 * Notes: both read and write clients are stored to bmi_exports, the ref
 *   counts are used to determine the number of both and hence the caching
 *   mode used at the clients.   bmi_wr_ion is a shortcut pointer used
 *   only when the bmap has client writers - all writers (and readers) are
 *   directed to this ion once a client has invoked write mode on the bmap.
 */
struct bmap_mds_info {
	/*
	 * This structure must start with the continuation of
	 * bmap_ondisk from where bmapc_memb left off so an entire
	 * bmap_ondisk will be laid contiguously in memory for I/O over
	 * the network and with ZFS.
	 */
	struct bmap_extra_state	 bmi_extrastate;

	struct resm_mds_info	*bmi_wr_ion;		/* pointer to write ION */
	struct psc_lockedlist	 bmi_leases;		/* tracked bmap leases */
	struct odtable_receipt	*bmi_assign;
	uint64_t		 bmi_seq;		/* Largest write bml seq # */
	uint32_t		 bmi_xid;		/* last op recv'd from ION */
	/*
	 * The following track the number of clients that have a
	 * write or read lease.  Each client can have more than
	 * one outstanding lease.
	 */
	int32_t			 bmi_writers;
	int32_t			 bmi_readers;
	struct psc_rwlock	 bmi_rwlock;
	struct slm_update_data	 bmi_upd;
	uint8_t			 bmi_orepls[SL_REPLICA_NBYTES];
};

/* MDS-specific bcm_flags */
#define BMAP_MDS_CRC_UP		(_BMAP_FLSHFT << 0)	/* CRC update in progress */
#define BMAP_MDS_CRCWRT		(_BMAP_FLSHFT << 1)
#define BMAP_MDS_NOION		(_BMAP_FLSHFT << 2)
#define BMAP_MDS_DIO		(_BMAP_FLSHFT << 3)	/* direct I/O enabled */
#define BMAP_MDS_SEQWRAP	(_BMAP_FLSHFT << 4)	/* sequence number wrapped */
#define BMAP_MDS_REPLMODWR	(_BMAP_FLSHFT << 5)	/* res state changes have been written */

#define bmi_2_fcmh(bmi)		bmi_2_bmap(bmi)->bcm_fcmh

#define bmap_2_xstate(b)	(&bmap_2_bmi(b)->bmi_extrastate)
#define bmap_2_bgen(b)		bmap_2_xstate(b)->bes_gen
#define bmap_2_replpol(b)	bmap_2_xstate(b)->bes_replpol
#define bmap_2_repl(b, i)	fcmh_2_repl((b)->bcm_fcmh, (i))
#define bmap_2_crcs(b, n)	bmap_2_xstate(b)->bes_crcs[n]
#define bmap_2_ondisk(b)	((struct bmap_ondisk *)&(b)->bcm_corestate)
#define bmap_2_upd(b)		(&bmap_2_bmi(b)->bmi_upd)
#define bmap_2_ino(b)		fcmh_2_ino((b)->bcm_fcmh)
#define bmap_2_inoh(b)		fcmh_2_inoh((b)->bcm_fcmh)

#define BMAPOD_CALLERINFO	PFL_CALLERINFOSS(SLSS_BMAP)
#define BMAPOD_RDLOCK(bmi)	_psc_rwlock_rdlock(BMAPOD_CALLERINFO, &(bmi)->bmi_rwlock)
#define BMAPOD_REQRDLOCK(bmi)	_psc_rwlock_reqrdlock(BMAPOD_CALLERINFO, &(bmi)->bmi_rwlock)
#define BMAPOD_REQWRLOCK(bmi)	_psc_rwlock_reqwrlock(BMAPOD_CALLERINFO, &(bmi)->bmi_rwlock)
#define BMAPOD_HASRDLOCK(bmi)	 psc_rwlock_hasrdlock(&(bmi)->bmi_rwlock)
#define BMAPOD_HASWRLOCK(bmi)	 psc_rwlock_haswrlock(&(bmi)->bmi_rwlock)
#define BMAPOD_ULOCK(bmi)	_psc_rwlock_unlock(BMAPOD_CALLERINFO, &(bmi)->bmi_rwlock)
#define BMAPOD_UREQLOCK(bmi, l)	_psc_rwlock_ureqlock(BMAPOD_CALLERINFO, &(bmi)->bmi_rwlock, (l))
#define BMAPOD_WRLOCK(bmi)	_psc_rwlock_wrlock(BMAPOD_CALLERINFO, &(bmi)->bmi_rwlock)

static __inline struct bmap_mds_info *
bmap_2_bmi(struct bmapc_memb *b)
{
	return (bmap_get_pri(b));
}

#define BMAPOD_MODIFY_START(b)	BMAPOD_REQWRLOCK(bmap_2_bmi(b))
#define BMAPOD_MODIFY_DONE(b,w)	BMAPOD_UREQLOCK(bmap_2_bmi(b), (w))

#define BMAPOD_READ_START(b)	BMAPOD_REQRDLOCK(bmap_2_bmi(b))
#define BMAPOD_READ_DONE(b, lk)	BMAPOD_UREQLOCK(bmap_2_bmi(b), (lk))

#define BHREPL_POLICY_SET(b, pol)					\
	do {								\
		int _lk;						\
									\
		_lk = BMAPOD_MODIFY_START(b);				\
		bmap_2_replpol(b) = (pol);				\
		BMAPOD_MODIFY_DONE((b), _lk);				\
	} while (0)

#define BHREPL_POLICY_GET(b, pol)					\
	do {								\
		int _lk;						\
									\
		_lk = BMAPOD_READ_START(b);				\
		*(pol) = bmap_2_replpol(b);				\
		BMAPOD_READ_DONE((b), _lk);				\
	} while (0)

#define BHGEN_INCREMENT(b)						\
	do {								\
		int _lk;						\
									\
		_lk = BMAPOD_MODIFY_START(b);				\
		bmap_2_bgen(b)++;					\
		BMAPOD_MODIFY_DONE((b), _lk);				\
	} while (0)

#define BHGEN_GET(b, bgen)						\
	do {								\
		int _lk;						\
									\
		_lk = BMAPOD_READ_START(b);				\
		*(bgen) = bmap_2_bgen(b);				\
		BMAPOD_READ_DONE((b), _lk);				\
	} while (0)

struct bmap_timeo_table {
	psc_spinlock_t		 btt_lock;
	/*
	 * High and low water marks of the bmap sequence number.  The
	 * MDS communicates the low water mark to an I/O server so that
	 * the latter can reject timed out bmaps.
	 */
	uint64_t		 btt_maxseq;
	uint64_t		 btt_minseq;
	struct psc_lockedlist	 btt_leases;
};

/* mds_bmap_timeotbl_mdsi (bmap timeout event) ops */
#define BTE_ADD			(1 << 0)
#define BTE_DEL			(1 << 1)
#define BTE_REATTACH		(1 << 2)

#define BMAP_TIMEO_MAX		240	/* Max bmap lease timeout */
#define BMAP_SEQLOG_FACTOR	100
#define BMAP_RECOVERY_TIMEO_EXT BMAP_TIMEO_MAX /* Extend recovered leases
						* after an MDS failure.
						*/

struct bmap_mds_lease {
	 int32_t		  bml_refcnt;
	uint64_t		  bml_seq;
	sl_ios_id_t		  bml_ios;
	lnet_process_id_t	  bml_cli_nidpid;
	uint32_t		  bml_flags;
	time_t			  bml_start;
	time_t			  bml_expire;
	psc_spinlock_t		  bml_lock;
	struct bmap_mds_info	 *bml_bmi;
	struct pscrpc_export	 *bml_exp;
	struct psclist_head	  bml_bmi_lentry;
	struct psclist_head	  bml_timeo_lentry;
	struct bmap_mds_lease	 *bml_chain;		/* chain of duplicate leases */
};

/* bml_flags */
#define BML_READ		(1 <<  0)		/* lease is for read activity */
#define BML_WRITE		(1 <<  1)		/* lease is for write activity */
#define BML_DIO			(1 <<  2)
#define BML_DIOCB		(1 <<  3)
#define BML_TIMEOQ		(1 <<  4)
#define BML_BMI			(1 <<  5)		/* linked in bmap_mds_info */
#define BML_RECOVER		(1 <<  6)
#define BML_CHAIN		(1 <<  7)
#define BML_FREEING		(1 <<  8)		/* being freed, don't reuse */
#define BML_ASSFAIL		(1 <<  9)		/* IOS assignment failed */
#define BML_RECOVERFAIL		(1 << 10)

#define bml_2_bmap(bml)		bmi_2_bmap((bml)->bml_bmi)

#define BML_LOCK_ENSURE(bml)	LOCK_ENSURE(&(bml)->bml_lock)
#define BML_LOCK(bml)		spinlock(&(bml)->bml_lock)
#define BML_ULOCK(bml)		freelock(&(bml)->bml_lock)
#define BML_REQLOCK(bml)	reqlock(&(bml)->bml_lock)
#define BML_TRYLOCK(bml)	trylock(&(bml)->bml_lock)

#define BMAP_FOREACH_LEASE(b, bml)					\
	PLL_FOREACH((bml), &bmap_2_bmi(b)->bmi_leases)

/**
 * bmap_ios_assign - The structure used for tracking the MDS's bmap/ion
 *   assignments.  These structures are stored in a odtable.
 * Note: default odtable entry size is 128 bytes.
 * XXX is the generation number needed here? - pauln
 */
struct bmap_ios_assign {
	sl_ios_id_t		bia_ios;
	lnet_process_id_t	bia_lastcli;
	slfid_t			bia_fid;
	uint64_t		bia_seq;
	sl_bmapno_t		bia_bmapno;
	time_t			bia_start;
	int			bia_flags;
};

/* bia_flags */
#define BIAF_DIO		(1 << 0)

#define mds_bmap_load(f, n, bp)	bmap_get((f), (n), SL_WRITE, (bp))

int	 mds_bmap_read(struct bmapc_memb *, enum rw, int);
int	 mds_bmap_write(struct bmapc_memb *, int, void *, void *);
int	_mds_bmap_write_rel(const struct pfl_callerinfo *, struct bmapc_memb *, void *);

#define mds_bmap_write_rel(b, logf)	_mds_bmap_write_rel(PFL_CALLERINFOSS(SLSS_BMAP), (b), (logf))

#define mds_bmap_write_repls_rel(b)	mds_bmap_write_rel((b), mdslog_bmap_repls)

#define mds_bmap_write_logrepls(b)	mds_bmap_write((b), 0, mdslog_bmap_repls, (b))

int	 mds_bmap_crc_write(struct srm_bmap_crcup *, sl_ios_id_t,
	    const struct srm_bmap_crcwrt_req *);
int	 mds_bmap_exists(struct fidc_membh *, sl_bmapno_t);
int	 mds_bmap_load_cli(struct fidc_membh *, sl_bmapno_t, int, enum rw,
	    sl_ios_id_t, struct srt_bmapdesc *, struct pscrpc_export *,
	    struct bmap_core_state *);
int	 mds_bmap_load_fg(const struct slash_fidgen *, sl_bmapno_t,
	    struct bmapc_memb **);
int	 mds_bmap_loadvalid(struct fidc_membh *, sl_bmapno_t,
	    struct bmapc_memb **);
int	 mds_bmap_bml_chwrmode(struct bmap_mds_lease *, sl_ios_id_t);
int	 mds_bmap_bml_release(struct bmap_mds_lease *);
void	 mds_bmap_ensure_valid(struct bmapc_memb *);
struct bmap_mds_lease *
	 mds_bmap_getbml_locked(struct bmapc_memb *, uint64_t, uint64_t, uint32_t);

void	 mds_bmap_setcurseq(uint64_t, uint64_t);
int	 mds_bmap_getcurseq(uint64_t *, uint64_t *);

void	 mds_bmap_timeotbl_init(void);
uint64_t mds_bmap_timeotbl_getnextseq(void);
uint64_t mds_bmap_timeotbl_mdsi(struct bmap_mds_lease *, int);

int64_t	 slm_bmap_calc_repltraffic(struct bmapc_memb *);

int	 mds_bia_odtable_startup_cb(void *, struct odtable_receipt *, void *);

extern struct psc_poolmaster	 bmapMdsLeasePoolMaster;
extern struct psc_poolmgr	*bmapMdsLeasePool;
extern struct bmap_timeo_table	 mdsBmapTimeoTbl;

static __inline struct bmapc_memb *
bmi_2_bmap(struct bmap_mds_info *bmi)
{
	struct bmapc_memb *b;

	psc_assert(bmi);
	b = (void *)bmi;
	return (b - 1);
}

#endif /* _SLASHD_BMAP_MDS_H_ */
