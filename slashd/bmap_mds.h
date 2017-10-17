/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright 2006-2016, Pittsburgh Supercomputing Center
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

#ifndef _SLASHD_BMAP_MDS_H_
#define _SLASHD_BMAP_MDS_H_

#include <sys/time.h>

#include "pfl/lockedlist.h"
#include "pfl/odtable.h"
#include "pfl/pthrutil.h"
#include "pfl/rpc.h"

#include "bmap.h"
#include "inode.h"
#include "journal_mds.h"
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
 *
 * Notes: both read and write clients are stored to bmi_exports, the ref
 *   counts are used to determine the number of both and hence the caching
 *   mode used at the clients.   bmi_wr_ion is a shortcut pointer used
 *   only when the bmap has client writers - all writers (and readers) are
 *   directed to this ION once a client has invoked write mode on the bmap.
 */
struct bmap_mds_info {
	struct bmap_core_state   bmi_corestate;
#define bmi_crcstates		bmi_corestate.bcs_crcstates
#define bmi_repls		bmi_corestate.bcs_repls

	struct bmap_extra_state	 bmi_extrastate;
#define bmi_crcs		bmi_extrastate.bes_crcs

	struct resm_mds_info	*bmi_wr_ion;		/* pointer to write ION */
	struct psc_lockedlist	 bmi_leases;		/* tracked bmap leases */
	int64_t			 bmi_assign;		/* bmap <-> ION binding */
	uint64_t		 bmi_seq;		/* Largest write bml seq # */

	/*
	 * The following track the number of clients that have a
	 * write or read lease.  Each client can have more than
	 * one outstanding lease.
	 */
	int32_t			 bmi_writers;
	int32_t			 bmi_readers;
	int32_t			 bmi_diocb;		/* # of DIO downgrade RPCs inflight */
	struct slm_update_data	 bmi_upd;		/* data for upsch (replication engine) */

	/*
	 * These fields are used when writing changes to bmap in-memory
	 * before they hit persistent store.
	 */
	uint8_t			 bmi_orepls[SL_REPLICA_NBYTES];
	int			 bmi_sys_prio;		/* upsch admin priority */
	int			 bmi_usr_prio;		/* upsch user priority */
};

#define bmi_2_fcmh(bmi)		bmi_2_bmap(bmi)->bcm_fcmh
#define bmi_2_ondisk(bmi)	((struct bmap_ondisk *)&(bmi)->bmi_corestate)

/* MDS-specific bcm_flags, _BMAPF_SHIFT	 = (1 <<  9) */

#define BMAPF_CRC_UP		(_BMAPF_SHIFT << 0)	/* CRC update in progress */
#define BMAPF_REPLMODWR		(_BMAPF_SHIFT << 1)	/* res state changes have been written */
#define BMAPF_IOSASSIGNED	(_BMAPF_SHIFT << 2)	/* write request bound an IOS to this bmap */

#define bmap_2_xstate(b)	(&bmap_2_bmi(b)->bmi_extrastate)
#define bmap_2_bgen(b)		bmap_2_xstate(b)->bes_gen
#define bmap_2_replpol(b)	bmap_2_xstate(b)->bes_replpol
#define bmap_2_repl(b, i)	fcmh_2_repl((b)->bcm_fcmh, (i))
#define bmap_2_crcs(b, n)	bmap_2_xstate(b)->bes_crcs[n]
#define bmap_2_upd(b)		(&bmap_2_bmi(b)->bmi_upd)

#define BMAPOD_CALLERINFO	PFL_CALLERINFOSS(SLSS_BMAP)

static __inline struct bmap_mds_info *
bmap_2_bmi(struct bmap *b)
{
	return (bmap_get_pri(b));
}

#define BHGEN_INCREMENT(b)						\
	do {								\
		bmap_2_bgen(b)++;					\
	} while (0)

#define BHGEN_GET(b, bgen)						\
	do {								\
		*(bgen) = bmap_2_bgen(b);				\
	} while (0)

#define BHGEN_SET(b, bgen)						\
	do {								\
		bmap_2_bgen(b) = *(bgen);				\
	} while (0)

struct bmap_timeo_table {
	psc_spinlock_t		 btt_lock;
	/*
	 * High and low water marks of the bmap sequence number.  The
	 * MDS communicates the low water mark to an I/O server so that
	 * the latter can reject timed out bmaps.
	 *
	 * They are initialized during startup by mds_bmap_setcurseq().
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

struct bmap_mds_lease {
	uint64_t		  bml_seq;
	int32_t		  	  bml_refcnt;
	sl_ios_id_t		  bml_ios;
	lnet_process_id_t	  bml_cli_nidpid;
	uint32_t		  bml_flags;
	time_t			  bml_start;
	time_t			  bml_expire;
	struct bmap_mds_info	 *bml_bmi;
	struct pscrpc_export	 *bml_exp;
	struct psc_listentry	  bml_bmi_lentry;
	struct psc_listentry	  bml_timeo_lentry;
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
#define BML_RECOVERFAIL		(1 <<  9)

#define bml_2_bmap(bml)		bmi_2_bmap((bml)->bml_bmi)

#define PFLOG_BML(level, bml, fmt, ...)					\
	psclogs((level), SLSS_BMAP, "bml@%p " fmt, (bml), ##__VA_ARGS__)

#define BMAP_FOREACH_LEASE(b, bml)					\
	PLL_FOREACH((bml), &bmap_2_bmi(b)->bmi_leases)

/**
 * bmap_ios_assign - The structure used for tracking the MDS's bmap/ion
 *   assignments.  These structures are stored in a odtable.
 * XXX is the generation number needed here?
 * XXX reorder these fields to pack 32-bit fields together.
 */
struct bmap_ios_assign {
	lnet_process_id_t	bia_lastcli;
	sl_ios_id_t		bia_ios;
	slfid_t			bia_fid;
	uint64_t		bia_seq;
	sl_bmapno_t		bia_bmapno;
	int			bia_flags;
	time_t			bia_start;
};

/* bia_flags */
#define BIAF_DIO		(1 << 0)

int	 mds_bmap_read(struct bmap *, int);
int	 mds_bmap_write(struct bmap *, void *, void *);

#define mds_bmap_write_logrepls(b)	mds_bmap_write((b), mdslog_bmap_repls, (b))

int	 mds_bmap_exists(struct fidc_membh *, sl_bmapno_t);
int	 mds_bmap_load_cli(struct fidc_membh *, sl_bmapno_t, int, enum rw,
	    sl_ios_id_t, struct srt_bmapdesc *, struct pscrpc_export *,
	    uint8_t *, int);
int	 mds_bmap_bml_chwrmode(struct bmap_mds_lease *, sl_ios_id_t);
int	 mds_bmap_bml_release(struct bmap_mds_lease *);
void	 mds_bmap_ensure_valid(struct bmap *);

struct bmap_mds_lease *
	 mds_bmap_getbml(struct bmap *, uint64_t, uint64_t, uint32_t);

void	 mds_bmap_setcurseq(uint64_t, uint64_t);
void	 mds_bmap_getcurseq(uint64_t *, uint64_t *);

void	 mds_bmap_timeotbl_init(void);
uint64_t mds_bmap_timeotbl_getnextseq(void);
uint64_t mds_bmap_timeotbl_mdsi(struct bmap_mds_lease *, int);

int64_t	 slm_bmap_calc_repltraffic(struct bmap *);

void	 mds_bia_odtable_startup_cb(void *, int64_t, void *);

extern struct psc_poolmaster	 slm_bml_poolmaster;
extern struct psc_poolmgr	*slm_bml_pool;
extern struct bmap_timeo_table	 slm_bmap_leases;

static __inline struct bmap *
bmi_2_bmap(struct bmap_mds_info *bmi)
{
	struct bmap *b;

	psc_assert(bmi);
	b = (void *)bmi;
	return (b - 1);
}

#endif /* _SLASHD_BMAP_MDS_H_ */
