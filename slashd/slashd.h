/* $Id$ */

#ifndef _SLASHD_H_
#define _SLASHD_H_

#include "psc_ds/tree.h"

#define SL_ROOT_INUM 1

struct sl_replrq {
	SPLAY_ENTRY(sl_replrq)		 rrq_tentry;
	struct slash_inode_handle	*rrq_inoh;
	psc_spinlock_t			 rrq_lock;
	struct psc_waitq		 rrq_waitq;
	int				 rrq_flags;
	int				 rrq_refcnt;
};

#define REPLRQF_BUSY	(1 << 0)
#define REPLRQF_DIE	(1 << 1)

int replrq_cmp(const void *, const void *);

SPLAY_HEAD(replrqtree, sl_replrq);
SPLAY_PROTOTYPE(replrqtree, sl_replrq, rrq_tentry, replrq_cmp);

uint64_t sl_get_repls_inum(void);
struct sl_replrq *
	mds_replrq_find(struct slash_fidgen *, int *);
int	mds_replrq_del(struct slash_fidgen *, sl_blkno_t);
int	mds_replrq_add(struct slash_fidgen *, sl_blkno_t);

extern struct cfdops		mdsCfdOps;
extern struct replrqtree	replrq_tree;
extern psc_spinlock_t		replrq_tree_lock;
extern struct slash_creds	rootcreds;
extern int			allow_internal_fsaccess;

#endif /* _SLASHD_H_ */
