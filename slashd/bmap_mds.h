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

#ifndef _SLASHD_MDS_BMAP_H_
#define _SLASHD_MDS_BMAP_H_

#include <sys/time.h>

#include "psc_ds/lockedlist.h"
#include "psc_rpc/rpc.h"
#include "psc_util/odtable.h"

#include "bmap.h"
#include "mdslog.h"
#include "slashd.h"
#include "inode.h"

/*
 * bmap_mds_info - the bcm_pri data structure for the slash2 mds.
 *   bmap_mds_info holds all bmap specific context for the mds which
 *   includes the journal handle, ref counts for client readers and writers
 *   a point to our ION, a tree of our client's exports, a pointer to the
 *   on-disk structure, a receipt for the odtable, and a reqset for issuing
 *   callbacks (XXX is that really needed?).
 * Notes: both read and write clients are stored to bmdsi_exports, the ref
 *   counts are used to determine the number of both and hence the caching
 *   mode used at the clients.   bmdsi_wr_ion is a shortcut pointer used
 *   only when the bmap has client writers - all writers (and readers) are
 *   directed to this ion once a client has invoked write mode on the bmap.
 */
struct bmap_mds_info {
	struct bmapc_memb		*bmdsi_bmap;    /* back pointer            */
	struct resm_mds_info		*bmdsi_wr_ion;	/* pointer to write ION    */
	struct psc_lockedlist		 bmdsi_leases;  /* tracked bmap leases     */
	struct odtable_receipt		*bmdsi_assign;
	uint64_t			 bmdsi_seq;     /* Largest write bml seq # */
	uint32_t			 bmdsi_xid;	/* last op recv'd from ION */
	int32_t				 bmdsi_writers;
	int32_t				 bmdsi_readers;
	int				 bmdsi_flags;
	pthread_rwlock_t		 bmdsi_rwlock;
};

#define BMAPOD_RDLOCK(bmdsi)						\
	psc_assert(!pthread_rwlock_rdlock(&(bmdsi)->bmdsi_rwlock))

#define BMAPOD_WRLOCK(bmdsi)						\
	psc_assert(!pthread_rwlock_wrlock(&(bmdsi)->bmdsi_rwlock))

#define BMAPOD_ULOCK(bmdsi)						\
	psc_assert(!pthread_rwlock_unlock(&(bmdsi)->bmdsi_rwlock))

#define BMDSI_LOGCHG_SET(b)						\
	do {								\
		int _bmdsi_set_locked;					\
									\
		_bmdsi_set_locked = BMAP_RLOCK(b);			\
		bmap_2_bmdsi(b)->bmdsi_flags |= BMIM_LOGCHG;		\
		BMAP_URLOCK((b), _bmdsi_set_locked);			\
	} while (0)

#define BMDSI_LOGCHG_CLEAR(b)						\
	do {								\
		int _bmdsi_clear_locked;				\
									\
		_bmdsi_clear_locked = BMAP_RLOCK(b);			\
		bmap_2_bmdsi(b)->bmdsi_flags &= ~BMIM_LOGCHG;		\
		BMAP_URLOCK((b), _bmdsi_clear_locked);			\
	} while (0)

#define BMDSI_LOGCHG_CHECK(b, set)					\
	do {								\
		int _bmdsi_logchg_locked;				\
									\
		_bmdsi_logchg_locked = BMAP_RLOCK(b);			\
		set = (bmap_2_bmdsi(b)->bmdsi_flags & BMIM_LOGCHG);	\
		BMAP_URLOCK((b), _bmdsi_logchg_locked);			\
	} while (0)

#define BMAPOD_MODIFY_START(b)	BMAPOD_WRLOCK(bmap_2_bmdsi(b))
#define BMAPOD_MODIFY_DONE(b)	BMAPOD_ULOCK(bmap_2_bmdsi(b))
#define BMAPOD_READ_START(b)	BMAPOD_RDLOCK(bmap_2_bmdsi(b))
#define BMAPOD_READ_DONE(b)	BMAPOD_ULOCK(bmap_2_bmdsi(b))

#define BHREPL_POLICY_SET(b, p)						\
	do {								\
		BMAPOD_MODIFY_START(b);					\
		(b)->bcm_od->bh_repl_policy = (p);			\
		BMDSI_LOGCHG_SET(b);					\
		BMAPOD_MODIFY_DONE(b);					\
	} while (0)

#define BHREPL_POLICY_GET(b, p)						\
	do {								\
		BMAPOD_READ_START(b);					\
		(p) = (b)->bcm_od->bh_repl_policy;			\
		BMAPOD_READ_DONE(b);					\
	} while (0)

#define BHGEN_INCREMENT(b)						\
	do {								\
		BMAPOD_MODIFY_START(b);					\
		(b)->bcm_od->bh_gen++;					\
		BMDSI_LOGCHG_SET(b);					\
		BMAPOD_MODIFY_DONE(b);					\
	} while (0)

#define BHGEN_GET(b, gen)						\
	do {								\
		BMAPOD_READ_START(b);					\
		(gen) = (b)->bcm_od->bh_gen;				\
		BMAPOD_READ_DONE(b);					\
	} while (0)


/* bmap MDS modes */
#define BMAP_MDS_CRC_UP		(_BMAP_FLSHFT << 0)	/* CRC update in progress */
#define BMAP_MDS_CRCWRT		(_BMAP_FLSHFT << 1)
#define BMAP_MDS_NOION		(_BMAP_FLSHFT << 2)

/* bmap_mds_info modes */
#define BMIM_LOGCHG		(1 << 0)	/* in-mem change made, needs logged */
#define BMIM_DIO		(1 << 1)	/* directio enabled                 */

struct bmap_timeo_entry {
	uint64_t		 bte_maxseq;
	struct psclist_head	 bte_bmaps;
};

struct bmap_timeo_table {
	psc_spinlock_t		 btt_lock;
	/*
	 * High and low water marks of the bmap sequence number. The MDS communicates
	 * the low water mark to an I/O server so that the latter can reject timed
	 * out bmaps.
	 */
	uint64_t		 btt_maxseq;
	uint64_t		 btt_minseq;
	struct bmap_timeo_entry	*btt_entries;
	int			 btt_nentries;
	int			 btt_ready;
};

#define BTE_ADD			(1 << 0)
#define BTE_DEL			(1 << 1)
#define BTE_REATTACH		(1 << 2)

#define BMAP_TIMEO_MAX		120 /* Max bmap lease timeout */
#define BMAP_TIMEO_TBL_QUANT	5
#define BMAP_TIMEO_TBL_SZ	(BMAP_TIMEO_MAX / BMAP_TIMEO_TBL_QUANT)
#define BMAP_SEQLOG_FACTOR	100

struct bmap_mds_lease {
	uint64_t		  bml_seq;
	uint64_t		  bml_key;
	lnet_nid_t		  bml_ion_nid;
	lnet_process_id_t	  bml_cli_nidpid;
	uint32_t		  bml_flags;
	psc_spinlock_t		  bml_lock;
	time_t			  bml_start;
	struct bmap_mds_info	 *bml_bmdsi;
	struct pscrpc_export	 *bml_exp;
	struct psclist_head	  bml_bmdsi_lentry;
	struct psclist_head	  bml_timeo_lentry;
	struct psclist_head	  bml_exp_lentry;
	struct psclist_head	  bml_coh_lentry;
	struct bmap_mds_lease	 *bml_chain;
};

#define bml_2_bmap(b)		(b)->bml_bmdsi->bmdsi_bmap

#define BML_LOCK_ENSURE(b)	LOCK_ENSURE(&(b)->bml_lock)
#define BML_LOCK(b)		spinlock(&(b)->bml_lock)
#define BML_ULOCK(b)		freelock(&(b)->bml_lock)

enum {
	BML_READ    = (1 << 0),
	BML_WRITE   = (1 << 1),
	BML_CDIO    = (1 << 2),
	BML_COHRLS  = (1 << 3),
	BML_COHDIO  = (1 << 4),
	BML_EXP     = (1 << 5),
	BML_TIMEOQ  = (1 << 6),
	BML_BMDSI   = (1 << 7),
	BML_COH     = (1 << 8),
	BML_RECOVER = (1 << 9),
	BML_CHAIN   = (1 << 10),
	BML_UPGRADE = (1 << 11),
	BML_EXPFAIL = (1 << 12)
};

/*
 * bmi_assign - the structure used for tracking the mds's bmap/ion
 *   assignments.  These structures are stored in a odtable.
 * Note: default odtable entry size is 128 bytes.
 */
struct bmi_assign {
	lnet_nid_t		bmi_ion_nid;
	lnet_process_id_t	bmi_lastcli;
	sl_ios_id_t		bmi_ios;
	slfid_t			bmi_fid;
	uint64_t		bmi_seq;
	sl_bmapno_t		bmi_bmapno;
	time_t			bmi_start;
	int			bmi_flags;
};

#define BMI_DIO (1 << 0)

#define bmap_2_bmdsi(b)		((struct bmap_mds_info *)(b)->bcm_pri)
#define bmap_2_bmdsassign(b)	bmap_2_bmdsi(b)->bmdsi_assign
#define bmap_2_bgen(b)		(b)->bcm_od->bh_gen
#define bmap_2_repl(b, i)	fcmh_2_repl((b)->bcm_fcmh, (i))

#define mds_bmap_load(f, n, bp)	bmap_get((f), (n), 0, (bp))

#define mds_bml_free(bml)	psc_pool_return(bmapMdsLeasePool, (bml))

int	 mds_bmap_crc_write(struct srm_bmap_crcup *, lnet_nid_t);
int	 mds_bmap_exists(struct fidc_membh *, sl_bmapno_t);
int	 mds_bmap_load_cli(struct fidc_membh *, sl_bmapno_t, int, enum rw,
	    sl_ios_id_t, struct srt_bmapdesc *, struct pscrpc_export *,
	    struct bmapc_memb **);
int	 mds_bmap_load_ion(const struct slash_fidgen *, sl_bmapno_t,
	    struct bmapc_memb **);
int	 mds_bmap_loadvalid(struct fidc_membh *, sl_bmapno_t,
	    struct bmapc_memb **);
int	 mds_bmap_bml_chwrmode(struct bmap_mds_lease *, sl_ios_id_t);
int	 mds_bmap_bml_release(struct bmap_mds_lease *);
struct bmap_mds_lease *
	mds_bmap_getbml(struct bmapc_memb *, lnet_nid_t, lnet_pid_t, uint64_t);

void	 mds_bmap_setcurseq(uint64_t, uint64_t);
int	 mds_bmap_getcurseq(uint64_t *, uint64_t *);

void	 mds_bmap_timeotbl_init(void);
uint64_t mds_bmap_timeotbl_getnextseq(void);
uint64_t mds_bmap_timeotbl_mdsi(struct bmap_mds_lease *, int);

void	 mds_bmi_odtable_startup_cb(void *, struct odtable_receipt *);
extern struct psc_poolmaster	 bmapMdsLeasePoolMaster;
extern struct psc_poolmgr	*bmapMdsLeasePool;

#endif /* _SLASHD_MDS_BMAP_H_ */
