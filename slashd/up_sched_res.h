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
		SPLAY_ENTRY(up_sched_work_item)	 uswiu_tentry;
	} uswi_u;
#define uswi_tentry uswi_u.uswiu_tentry
#define uswi_lentry uswi_u.uswiu_lentry
};

/* work item flags */
#define USWIF_BUSY		(1 << 0)	/* work item is being modified */
#define USWIF_DIE		(1 << 1)	/* work item is going away */

#define USWI_INOH(wk)		fcmh_2_inoh((wk)->uswi_fcmh)
#define USWI_INO(wk)		(&USWI_INOH(wk)->inoh_ino)
#define USWI_INOX(wk)		USWI_INOH(wk)->inoh_extras
#define USWI_NREPLS(wk)		USWI_INO(wk)->ino_nrepls
#define USWI_FG(wk)		(&(wk)->uswi_fcmh->fcmh_fg)
#define USWI_FID(wk)		USWI_FG(wk)->fg_fid
#define USWI_NBMAPS(wk)		fcmh_2_nbmaps((wk)->uswi_fcmh)

#define USWI_GETREPL(wk, n)	((n) < SL_DEF_REPLICAS ?		\
				    USWI_INO(wk)->ino_repls[n] :	\
				    USWI_INOX(wk)->inox_repls[(n) - 1])

struct up_sched_work_item *
	 uswi_find(const struct slash_fidgen *, int *);
int	 uswi_access(struct up_sched_work_item *);
int	 uswi_cmp(const void *, const void *);
void	 uswi_init(struct up_sched_work_item *, struct fidc_membh *);
void	 uswi_unref(struct up_sched_work_item *);

SPLAY_HEAD(upschedtree, up_sched_work_item);
SPLAY_PROTOTYPE(upschedtree, up_sched_work_item, uswi_tentry, uswi_cmp);

extern struct psc_poolmgr	*upsched_pool;

extern struct upschedtree	 upsched_tree;
extern psc_spinlock_t		 upsched_tree_lock;

#endif /* _UP_SCHED_RES_H_ */
