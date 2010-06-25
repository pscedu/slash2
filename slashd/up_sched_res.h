/* $Id$ */

#ifndef _UP_SCHED_RES_H_
#define _UP_SCHED_RES_H_

struct up_sched_work_item {
	struct fidc_membh		*uswi_fcmh;
	psc_atomic32_t			 uswi_refcnt;
	int				 uswi_gen;
	int				 uswi_flags;
	pthread_mutex_t			 uswi_mutex;
	struct psc_multiwaitcond	 uswi_mwcond;
	union {
		struct psclist_head	 uswiu_lentry;
		SPLAY_ENTRY(sl_replrq)	 uswiu_tentry;
	} uswi_u;
#define uswi_tentry uswi_u.uswiu_tentry
#define uswi_lentry uswi_u.uswiu_lentry
};

/* work item flags */
#define USWIF_BUSY		(1 << 0)	/* work item is being modified */
#define USWIF_DIE		(1 << 1)	/* work item is going away */
#define USWIF_REPLRQ		(1 << 2)	/* replication work needs done */
#define USWIF_GARBAGE		(1 << 3)	/* garbage relinquishment needs done */

#define USWI_INO(wk)		(&fcmh_2_inoh((wk)->uswi_fcmh)->inoh_ino)
#define USWI_INOX(wk)		fcmh_2_inoh((wk)->uswi_fcmh)->inoh_extras
#define USWI_NREPLS(wk)		USWI_INO(wk)->ino_nrepls
#define USWI_FG(wk)		(&USWI_INO(wk)->ino_fg)
#define USWI_FID(wk)		USWI_FG(wk)->fg_fid
#define USWI_NBMAPS(wk)		fcmh_2_nbmaps((wk)->uswi_fcmh)

#define USWI_GETREPL(wk, n)	((n) < SL_DEF_REPLICAS ?		\
				    USWI_INO(wk)->ino_repls[n] :	\
				    USWI_INOX(wk)->inox_repls[(n) - 1])

#endif /* _UP_SCHED_RES_H_ */
