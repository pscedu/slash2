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
#define SLMTHRT_CTL		0	/* control processor */
#define SLMTHRT_CTLAC		1	/* control acceptor */
#define SLMTHRT_RMC		2	/* MDS <- CLI msg svc handler */
#define SLMTHRT_RMI		3	/* MDS <- I/O msg svc handler */
#define SLMTHRT_RMM		4	/* MDS <- MDS msg svc handler */
#define SLMTHRT_RCM		5	/* CLI <- MDS msg issuer */
#define SLMTHRT_LNETAC		6	/* lustre net accept thr */
#define SLMTHRT_USKLNDPL	7	/* userland socket lustre net dev poll thr */
#define SLMTHRT_TINTV		8	/* timer interval */
#define SLMTHRT_TIOS		9	/* I/O stats updater */
#define SLMTHRT_COH		10	/* coherency thread */
#define SLMTHRT_FSSYNC		11	/* file system syncer */
#define SLMTHRT_UPSCHED		12	/* update scheduler for site resources */
#define SLMTHRT_BMAPTIMEO	13	/* bmap timeout thread */
#define SLMTHRT_JRNL		14	/* journal distill thread */
#define SLMTHRT_CURSOR		15	/* cursor update thread */
#define SLMTHRT_NAMESPACE	16	/* namespace propagating thread */

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

PSCTHR_MKCAST(slmrcmthr, slmrcm_thread, SLMTHRT_RCM)
PSCTHR_MKCAST(slmrmcthr, slmrmc_thread, SLMTHRT_RMC)
PSCTHR_MKCAST(slmrmithr, slmrmi_thread, SLMTHRT_RMI)
PSCTHR_MKCAST(slmrmmthr, slmrmm_thread, SLMTHRT_RMM)
PSCTHR_MKCAST(slmupschedthr, slmupsched_thread, SLMTHRT_UPSCHED)

struct site_mds_info {
	struct psc_dynarray	  smi_upq;
	psc_spinlock_t		  smi_lock;
	struct psc_multiwait	  smi_mw;
	struct psc_multiwaitcond  smi_mwcond;
	int			  smi_flags;
};

#define SMIF_DIRTYQ		  (1 << 0)		/* queue has changed */

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
	psc_spinlock_t		  sp_lock;
	sl_siteid_t		  sp_siteid;
	struct psclist_head	  sp_lentry;
	struct sl_resm		 *sp_resm;
	int			  sp_flags;		/* see SP_FLAG_* below */
	struct sl_mds_logbuf	 *sp_logbuf;		/* the log buffer being used */

	uint64_t		  sp_send_seqno;	/* next log sequence number to send */
	int			  sp_send_count;	/* # of updates in the batch */

	uint64_t		  sp_recv_seqno;	/* last received log sequence number */

	struct sl_mds_nsstats	  sp_stats;
};

/* sml_flags values */
#define	SP_FLAG_NONE		   0
#define	SP_FLAG_MIA		  (1 << 0)
#define	SP_FLAG_INFLIGHT	  (1 << 1)

/* allocated by slcfg_init_resm(), which is tied into the lex/yacc code */
struct resm_mds_info {
	pthread_mutex_t		  rmmi_mutex;
	struct psc_multiwaitcond  rmmi_mwcond;
	int			  rmmi_busyid;
	struct sl_resm		 *rmmi_resm;
	atomic_t		  rmmi_refcnt;		/* #CLIs using this ion */
};

#define resm2rmmi(resm)		((struct resm_mds_info *)(resm)->resm_pri)
#define res2rpmi(res)		((struct resprof_mds_info *)(res)->res_pri)

#define RMMI_RLOCK(rmmi)	psc_pthread_mutex_reqlock(&(rmmi)->rmmi_mutex)
#define RMMI_URLOCK(rmmi, lk)	psc_pthread_mutex_ureqlock(&(rmmi)->rmmi_mutex, (lk))

/* IOS round-robin counter for assigning IONs.  Attaches at res_pri.
 */
struct resprof_mds_info {
	int			  rpmi_cnt;
	psc_spinlock_t		  rpmi_lock;
	struct sl_mds_peerinfo	 *rpmi_peerinfo;
};

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

extern struct slash_creds			 rootcreds;
extern struct psc_listcache			 dirtyMdsData;
extern struct odtable				*mdsBmapAssignTable;
extern const struct slash_inode_extras_od	 null_inox_od;
extern struct sl_mds_nsstats			 slm_nsstats_aggr;	/* aggregate stats */

uint64_t	slm_get_curr_slashid(void);
uint64_t	slm_get_next_slashid(void);
void		slm_set_curr_slashid(uint64_t);

#endif /* _SLASHD_H_ */
