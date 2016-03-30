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

#ifndef _SLASHD_H_
#define _SLASHD_H_

#include <sqlite3.h>

#include "pfl/ctlsvr.h"
#include "pfl/dynarray.h"
#include "pfl/meter.h"
#include "pfl/multiwait.h"
#include "pfl/odtable.h"
#include "pfl/rpc.h"
#include "pfl/service.h"
#include "pfl/vbitmap.h"
#include "pfl/workthr.h"

#include "inode.h"
#include "namespace.h"
#include "slashrpc.h"
#include "slconfig.h"
#include "slconn.h"
#include "sltypes.h"

struct fidc_membh;
struct srt_stat;

struct slm_sth;
struct bmap_mds_lease;

/* MDS thread types. */
enum {
	SLMTHRT_BATCHRPC = _PFL_NTHRT,	/* batch RPC reaper */
	SLMTHRT_FREAP,			/* file reaper */
	SLMTHRT_BKDB,			/* upsch database backup */
	SLMTHRT_BMAPTIMEO,		/* bmap timeout thread */
	SLMTHRT_CONN,			/* peer resource connection monitor */
	SLMTHRT_CTL,			/* control processor */
	SLMTHRT_CTLAC,			/* control acceptor */
	SLMTHRT_CURSOR,			/* cursor update thread */
	SLMTHRT_DBWORKER,		/* database worker */
	SLMTHRT_JNAMESPACE,		/* namespace propagating thread */
	SLMTHRT_JRECLAIM,		/* garbage reclamation thread */
	SLMTHRT_JRNL,			/* journal distill thread */
	SLMTHRT_LNETAC,			/* lustre net accept thr */
	SLMTHRT_NBRQ,			/* non-blocking RPC reply handler */
	SLMTHRT_RCM,			/* CLI <- MDS msg issuer */
	SLMTHRT_RMC,			/* MDS <- CLI msg svc handler */
	SLMTHRT_RMI,			/* MDS <- I/O msg svc handler */
	SLMTHRT_RMM,			/* MDS <- MDS msg svc handler */
	SLMTHRT_OPSTIMER,		/* opstats updater */
	SLMTHRT_UPSCHED,		/* update scheduler for site resources */
	SLMTHRT_USKLNDPL,		/* userland socket lustre net dev poll thr */
	SLMTHRT_WORKER,			/* miscellaneous work */
	SLMTHRT_ZFS_KSTAT		/* ZFS stats */
};

struct slmthr_dbh {
	sqlite3			 *dbh;
	struct psc_hashtbl	  dbh_sth_hashtbl;
};

struct slmrmc_thread {
	struct pscrpc_thread	  smrct_prt;
	struct slmthr_dbh	  smrct_dbh;
};

struct slmrcm_thread {
	struct slmthr_dbh	  srcm_dbh;
	char			 *srcm_page;
	int			  srcm_page_bitpos;
};

struct slmrmi_thread {
	struct pscrpc_thread	  smrit_prt;
	struct slmthr_dbh	  smrit_dbh;
};

struct slmrmm_thread {
	struct pscrpc_thread	  smrmt_prt;
};

struct slmdbwk_thread {
	struct pfl_wk_thread	  smdw_wkthr;
	struct slmthr_dbh	  smdw_dbh;
};

struct slmctl_thread {
	struct slmthr_dbh	  smct_dbh;
};

struct slmupsch_thread {
	struct slmthr_dbh	  sus_dbh;
};

PSCTHR_MKCAST(slmctlthr, psc_ctlthr, SLMTHRT_CTL)
PSCTHR_MKCAST(slmdbwkthr, slmdbwk_thread, SLMTHRT_DBWORKER)
PSCTHR_MKCAST(slmrcmthr, slmrcm_thread, SLMTHRT_RCM)
PSCTHR_MKCAST(slmrmcthr, slmrmc_thread, SLMTHRT_RMC)
PSCTHR_MKCAST(slmrmithr, slmrmi_thread, SLMTHRT_RMI)
PSCTHR_MKCAST(slmrmmthr, slmrmm_thread, SLMTHRT_RMM)
PSCTHR_MKCAST(slmupschthr, slmupsch_thread, SLMTHRT_UPSCHED)

static __inline struct slmctl_thread *
slmctlthr_getpri(struct psc_thread *thr)
{
	return ((void *)(slmctlthr(thr) + 1));
}

static __inline struct slmthr_dbh *
slmthr_getdbh(void)
{
	struct psc_thread *thr;

	thr = pscthr_get();
	switch (thr->pscthr_type) {
	case SLMTHRT_CTL:
		return (&slmctlthr_getpri(thr)->smct_dbh);
	case SLMTHRT_RCM:
		return (&slmrcmthr(thr)->srcm_dbh);
	case SLMTHRT_RMC:
		return (&slmrmcthr(thr)->smrct_dbh);
	case SLMTHRT_RMI:
		return (&slmrmithr(thr)->smrit_dbh);
	case SLMTHRT_UPSCHED:
		return (&slmupschthr(thr)->sus_dbh);
	case SLMTHRT_DBWORKER:
		return (&slmdbwkthr(thr)->smdw_dbh);
	}
	psc_fatalx("unknown thread type");
}

struct site_mds_info {
};

static __inline struct site_mds_info *
site2smi(struct sl_site *site)
{
	return (site_get_pri(site));
}

/* per-MDS eventually consistent namespace stats */
struct slm_nsstats {
	psc_atomic32_t		  ns_stats[NS_NDIRS][NS_NOPS + 1][NS_NSUMS];
};

#define _SLM_NSSTATS_ADJ(adj, peerinfo, dir, op, sum)			\
	do {								\
		psc_atomic32_##adj(&(peerinfo)->sp_stats.		\
		    ns_stats[dir][op][sum]);				\
		psc_atomic32_##adj(&(peerinfo)->sp_stats.		\
		    ns_stats[dir][NS_NOPS][sum]);			\
									\
		psc_atomic32_##adj(&slm_nsstats_aggr.			\
		    ns_stats[dir][op][sum]);				\
		psc_atomic32_##adj(&slm_nsstats_aggr.			\
		    ns_stats[dir][NS_NOPS][sum]);			\
	} while (0)

#define SLM_NSSTATS_INCR(peerinfo, dir, op, sum)			\
	_SLM_NSSTATS_ADJ(inc, (peerinfo), (dir), (op), (sum))
#define SLM_NSSTATS_DECR(peerinfo, dir, op, sum)			\
	_SLM_NSSTATS_ADJ(dec, (peerinfo), (dir), (op), (sum))

/*
 * This structure is attached to the sl_resource for MDS peers.  It tracks the
 * progress of namespace log application on an MDS.  We allow one pending
 * request per MDS until it responds or timeouts.
 */
struct rpmi_mds {
	struct pfl_meter	  sp_batchmeter;
#define sp_batchno sp_batchmeter.pm_cur
	uint64_t		  sp_xid;
	int			  sp_flags;

	int			  sp_fails;		/* the number of successive RPC failures */
	int			  sp_skips;		/* the number of times to skip */

	int			  sp_send_count;	/* # of updates in the batch */
	uint64_t		  sp_send_seqno;	/* next log sequence number to send */
	uint64_t		  sp_recv_seqno;	/* last received log sequence number */

	struct slm_nsstats	  sp_stats;
};
#define sl_mds_peerinfo rpmi_mds

#define SPF_NEED_JRNL_INIT	(1 << 0)		/* journal fields need initialized */

#define res2rpmi_mds(res)	((struct rpmi_mds *)res2rpmi(res)->rpmi_info)
#define res2mdsinfo(res)	res2rpmi_mds(res)

/* bandwidth and reserved in one direction */
struct bw_dir {
	int32_t			 bwd_queued;		/* bandwidth in reserve queue */
	int32_t			 bwd_inflight;		/* scratchpad until update is recv'd */
	int32_t			 bwd_assigned;		/* amount MDS has assigned */
};

/*
 * This structure is attached to the sl_resource for IOS peers.  It
 * tracks the progress of garbage collection on each IOS.
 */
struct rpmi_ios {
	struct timespec		  si_lastcomm;		/* PING timeout to trigger conn reset */
	uint64_t		  si_xid;		/* garbage reclaim transaction group identifier */
	struct pfl_meter	  si_batchmeter;
#define si_batchno si_batchmeter.pm_cur
	int			  si_index;		/* index into the reclaim progress file */
	int			  si_flags;
	struct srt_statfs	  si_ssfb;
	struct timespec		  si_ssfb_send;

	struct bw_dir		  si_bw_ingress;	/* incoming bandwidth */
	struct bw_dir		  si_bw_egress;		/* outgoing bandwidth */
	struct bw_dir		  si_bw_aggr;		/* aggregate (incoming + outgoing) */
};
#define sl_mds_iosinfo rpmi_ios

#define SIF_NEED_JRNL_INIT	(1 << 0)		/* journal fields need initialized */
#define SIF_DISABLE_LEASE	(1 << 1)		/* disable bmap lease assignments */
#define SIF_DISABLE_ADVLEASE	(1 << 2)		/* advisory (from sliod) control */
#define SIF_DISABLE_GC		(1 << 3)		/* disable garbage collection temporarily */
#define SIF_UPSCH_PAGING	(1 << 4)		/* upsch will page more work in destined for this IOS */
#define SIF_NEW_PROG_ENTRY	(1 << 5)		/* new entry in the reclaim prog file */
#define SIF_PRECLAIM_NOTSUP	(1 << 6)		/* can punch holes for replica ejection */

#define res2rpmi_ios(r)		((struct rpmi_ios *)res2rpmi(r)->rpmi_info)
#define res2iosinfo(res)	res2rpmi_ios(res)

/* MDS-specific data for struct sl_resource */
struct resprof_mds_info {
	struct pfl_mutex	  rpmi_mutex;
	struct psc_dynarray	  rpmi_upschq;		/* updates queue */
	struct psc_waitq	  rpmi_waitq;
	struct psc_listcache	  rpmi_batchrqs;

	/* rpmi_mds for peer MDS or rpmi_ios for IOS */
	void			 *rpmi_info;
};

#define RPMI_LOCK(rpmi)		psc_mutex_lock(&(rpmi)->rpmi_mutex)
#define RPMI_RLOCK(rpmi)	psc_mutex_reqlock(&(rpmi)->rpmi_mutex)
#define RPMI_ULOCK(rpmi)	psc_mutex_unlock(&(rpmi)->rpmi_mutex)
#define RPMI_URLOCK(rpmi, lkd)	psc_mutex_ureqlock(&(rpmi)->rpmi_mutex, (lkd))

static __inline struct resprof_mds_info *
res2rpmi(struct sl_resource *res)
{
	return (resprof_get_pri(res));
}

/* MDS-specific data for struct sl_resm */
struct resm_mds_info {
	psc_atomic32_t		 rmmi_refcnt;		/* #CLIs using this ion */
};

static __inline struct resm_mds_info *
resm2rmmi(struct sl_resm *resm)
{
	return (resm_get_pri(resm));
}

static __inline struct sl_resm *
rmmi2resm(struct resm_mds_info *rmmi)
{
	struct sl_resm *m;

	psc_assert(rmmi);
	m = (void *)rmmi;
	return (m - 1);
}

#define resm2rpmi(resm)		res2rpmi((resm)->resm_res)

struct slm_wkdata_wr_brepl {
	struct bmapc_memb	*b;

	/* only used during REPLAY */
	struct sl_fidgen	 fg;
	sl_bmapno_t		 bno;
};

struct slm_wkdata_ptrunc {
	struct fidc_membh	*f;
};

struct slm_wkdata_upsch_purge {
	slfid_t			 fid;
	sl_bmapno_t		 bno;
};

struct slm_wkdata_upsch_cb {
	struct slrpc_cservice	*csvc;
	struct sl_resm		*src_resm;
	struct sl_resm		*dst_resm;
	struct bmap		*b;
	int			 rc;
	int			 off;
	int64_t			 amt;
};

struct slm_wkdata_upschq {
	struct sl_fidgen	 fg;
	sl_bmapno_t		 bno;
};

struct slm_wkdata_rmdir_ino {
	slfid_t			 fid;
};

struct slm_batchscratch_repl {
	int64_t			 bsr_amt;
	int			 bsr_off;
	struct sl_resource	*bsr_res;
};

struct slm_batchscratch_preclaim {
	struct sl_resource	*bsp_res;
};

struct mio_rootnames {
	char			 rn_name[PATH_MAX];
	int			 rn_vfsid;
	struct pfl_hashentry	 rn_hentry;
};

#define SLM_NWORKER_THREADS	4

enum {
	SLM_OPSTATE_INIT = 0,
	SLM_OPSTATE_REPLAY,
	SLM_OPSTATE_NORMAL
//	SLM_OPSTATE_EXITING
};

int	 mds_handle_rls_bmap(struct pscrpc_request *, int);
int	 mds_lease_renew(struct fidc_membh *, struct srt_bmapdesc *,
	    struct srt_bmapdesc *, struct pscrpc_export *);
int	 mds_lease_reassign(struct fidc_membh *, struct srt_bmapdesc *,
	    sl_ios_id_t, sl_ios_id_t *, int, struct srt_bmapdesc *,
	    struct pscrpc_export *);

int	 mds_sliod_alive(void *);

void	 slmbkdbthr_main(struct psc_thread *);
void	 slmbmaptimeothr_spawn(void);
void	 slmctlthr_main(const char *);
void	 slmrcmthr_main(struct psc_thread *);

slfid_t	 slm_get_curr_slashfid(void);
void	 slm_set_curr_slashfid(slfid_t);
int	 slm_get_next_slashfid(slfid_t *);

void	 slm_ptrunc_odt_startup_cb(void *, struct pfl_odt_receipt *, void *);
int	 slm_setattr_core(struct fidc_membh *, struct srt_stat *, int);

int	 mdscoh_req(struct bmap_mds_lease *);

void	 psc_scan_filesystems(void);
void	 mds_note_update(int);

#define dbdo(cb, arg, fmt, ...)	_dbdo(PFL_CALLERINFO(), (cb), (arg), (fmt), ## __VA_ARGS__)
void	 _dbdo(const struct pfl_callerinfo *,
	    int (*)(struct slm_sth *, void *), void *, const char *,
	    ...);

extern struct slash_creds	 rootcreds;
extern struct pfl_odt		*slm_bia_odt;
extern struct pfl_odt		*slm_ptrunc_odt;
extern struct slm_nsstats	 slm_nsstats_aggr;	/* aggregate namespace stats */
extern struct psc_listcache	 slm_db_lopri_workq;
extern struct psc_listcache	 slm_db_hipri_workq;

extern struct psc_thread	*slmconnthr;

extern int			 slm_opstate;

extern struct pfl_odt_ops	 slm_odtops;

extern int			 slm_global_mount;
extern int			 slm_ptrunc_enabled;
extern int			 slm_preclaim_enabled;

extern struct psc_hashtbl	 slm_roots;

#endif /* _SLASHD_H_ */
