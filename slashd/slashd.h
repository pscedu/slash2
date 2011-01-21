/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2011, Pittsburgh Supercomputing Center (PSC).
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

#ifndef _SLASHD_H_
#define _SLASHD_H_

#include "psc_ds/dynarray.h"
#include "psc_ds/vbitmap.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/service.h"
#include "psc_util/multiwait.h"

#include "inode.h"
#include "namespace.h"
#include "slconfig.h"
#include "sltypes.h"

struct odtable;
struct odtable_receipt;

struct bmapc_memb;
struct fidc_membh;
struct slash_inode_handle;

/* MDS thread types. */
enum {
	SLMTHRT_BMAPTIMEO,	/* bmap timeout thread */
	SLMTHRT_COH,		/* coherency thread */
	SLMTHRT_CTL,		/* control processor */
	SLMTHRT_CTLAC,		/* control acceptor */
	SLMTHRT_CURSOR,		/* cursor update thread */
	SLMTHRT_FSSYNC,		/* file system syncer */
	SLMTHRT_JNAMESPACE,	/* namespace propagating thread */
	SLMTHRT_JRECLAIM,	/* garbage reclamation thread */
	SLMTHRT_JRNL,		/* journal distill thread */
	SLMTHRT_LNETAC,		/* lustre net accept thr */
	SLMTHRT_RCM,		/* CLI <- MDS msg issuer */
	SLMTHRT_RMC,		/* MDS <- CLI msg svc handler */
	SLMTHRT_RMI,		/* MDS <- I/O msg svc handler */
	SLMTHRT_RMM,		/* MDS <- MDS msg svc handler */
	SLMTHRT_TIOS,		/* I/O stats updater */
	SLMTHRT_UPSCHED,	/* update scheduler for site resources */
	SLMTHRT_USKLNDPL	/* userland socket lustre net dev poll thr */
};

struct slmrmc_thread {
	struct pscrpc_thread	  smrct_prt;
};

struct slmrcm_thread {
	char			 *srcm_page;
	int			  srcm_page_bitpos;
};

struct slmrmi_thread {
	struct pscrpc_thread	  smrit_prt;
};

struct slmrmm_thread {
	struct pscrpc_thread	  smrmt_prt;
};

struct slmupsched_thread {
	struct sl_site		 *smut_site;
};

struct slmjns_thread {
};

PSCTHR_MKCAST(slmjnsthr, slmjns_thread, SLMTHRT_JNAMESPACE)
PSCTHR_MKCAST(slmrcmthr, slmrcm_thread, SLMTHRT_RCM)
PSCTHR_MKCAST(slmrmcthr, slmrmc_thread, SLMTHRT_RMC)
PSCTHR_MKCAST(slmrmithr, slmrmi_thread, SLMTHRT_RMI)
PSCTHR_MKCAST(slmrmmthr, slmrmm_thread, SLMTHRT_RMM)
PSCTHR_MKCAST(slmupschedthr, slmupsched_thread, SLMTHRT_UPSCHED)

struct site_mds_info {
	struct psc_dynarray	  smi_upq;		/* update queue */
	psc_spinlock_t		  smi_lock;
	struct psc_multiwait	  smi_mw;
	struct psc_multiwaitcond  smi_mwcond;
	int			  smi_flags;
};

#define SMIF_DIRTYQ		  (1 << 0)		/* queue has changed */

#define site2smi(site)		 ((struct site_mds_info *)(site)->site_pri)

/* per-MDS eventually consistent namespace stats */
struct sl_mds_nsstats {
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
 * This structure tracks the progress of namespace log application on a MDS.
 * We allow one pending request per MDS until it responds or timeouts.
 */
struct sl_mds_peerinfo {
	uint64_t		  sp_xid;
	uint64_t		  sp_batchno;

	int			  sp_fails;		/* the number of failures */
	int			  sp_skips;		/* the number of times to skip */

	int			  sp_send_count;	/* # of updates in the batch */
	uint64_t		  sp_send_seqno;	/* next log sequence number to send */

	uint64_t		  sp_recv_seqno;	/* last received log sequence number */

	struct sl_mds_nsstats	  sp_stats;
};

/*
 * This structure tracks the progress of garbage collection on each I/O server.
 */
struct sl_mds_iosinfo {
	int			  si_flags;
	uint64_t		  si_xid;
	uint64_t		  si_batchno;
};

/* IOS round-robin counter for assigning IONs.  Attaches at res_pri. */
struct resprof_mds_info {
	int			  rpmi_cnt;
	psc_spinlock_t		  rpmi_lock;

	/* sl_mds_peerinfo for peer MDS or sl_mds_iosinfo for IOS */
	void			 *rpmi_info;
};

#define RPMI_LOCK(rpmi)		spinlock(&(rpmi)->rpmi_lock)
#define RPMI_ULOCK(rpmi)	freelock(&(rpmi)->rpmi_lock)

/* attaches at resm_pri via slcfg_init_resm() */
struct resm_mds_info {
	pthread_mutex_t		  rmmi_mutex;
	struct psc_multiwaitcond  rmmi_mwcond;
	int			  rmmi_busyid;
	struct sl_resm		 *rmmi_resm;
	atomic_t		  rmmi_refcnt;		/* #CLIs using this ion */
};

#define RMMI_TRYLOCK(rmmi)	psc_pthread_mutex_trylock(&(rmmi)->rmmi_mutex)
#define RMMI_RLOCK(rmmi)	psc_pthread_mutex_reqlock(&(rmmi)->rmmi_mutex)
#define RMMI_TRYRLOCK(rmmi, lk)	psc_pthread_mutex_tryreqlock(&(rmmi)->rmmi_mutex, (lk))
#define RMMI_URLOCK(rmmi, lk)	psc_pthread_mutex_ureqlock(&(rmmi)->rmmi_mutex, (lk))
#define RMMI_HASLOCK(rmmi)	psc_pthread_mutex_haslock(&(rmmi)->rmmi_mutex)

#define resm2rmmi(resm)		((struct resm_mds_info *)(resm)->resm_pri)
#define res2rpmi(res)		((struct resprof_mds_info *)(res)->res_pri)
#define resm2rpmi(resm)		res2rpmi((resm)->resm_res)

int		 mds_inode_read(struct slash_inode_handle *);
int		 mds_inox_load_locked(struct slash_inode_handle *);
int		 mds_inox_ensure_loaded(struct slash_inode_handle *);
int		 mds_handle_rls_bmap(struct pscrpc_request *, int);

__dead void	 slmctlthr_main(const char *);
void		 slmbmaptimeothr_spawn(void);
void		 slmfssyncthr_spawn(void);
void		 slmrcmthr_main(struct psc_thread *);
void		 slmupschedthr_spawnall(void);
void		 slmtimerthr_spawn(void);

uint64_t	slm_get_curr_slashid(void);
uint64_t	slm_get_next_slashid(void);
void		slm_set_curr_slashid(uint64_t);

extern struct slash_creds			 rootcreds;
extern struct psc_listcache			 dirtyMdsData;
extern struct odtable				*mdsBmapAssignTable;
extern const struct slash_inode_extras_od	 null_inox_od;
extern const struct slash_inode_od		 null_inode_od;
extern struct sl_mds_nsstats			 slm_nsstats_aggr;	/* aggregate namespace stats */
extern struct sl_mds_peerinfo			*localinfo;

static __inline int
slm_get_rpmi_idx(struct sl_resource *res)
{
	struct resprof_mds_info *rpmi;
	int locked, n;

	rpmi = res->res_pri;
	locked = reqlock(&rpmi->rpmi_lock);
	if (rpmi->rpmi_cnt >= psc_dynarray_len(&res->res_members))
		rpmi->rpmi_cnt = 0;
	n = rpmi->rpmi_cnt++;
	ureqlock(&rpmi->rpmi_lock, locked);
	return (n);
}

#endif /* _SLASHD_H_ */
