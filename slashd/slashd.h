/* $Id$ */

#ifndef _SLASHD_H_
#define _SLASHD_H_

#include "psc_ds/tree.h"

struct sl_replrq {
	SPLAY_ENTRY(sl_replrq)		 rrq_tentry;
	struct slash_inode_handle	*rrq_inoh;
};

int replrq_cmp(const void *, const void *);

SPLAY_HEAD(replrqtree, sl_replrq);
SPLAY_PROTOTYPE(replrqtree, sl_replrq, rrq_tentry, replrq_cmp);

uint64_t sl_get_repls_inum(void);
int	 mds_repl_delrq(struct slash_fidgen *, sl_blkno_t);
int	 mds_repl_addrq(struct slash_fidgen *, sl_blkno_t);

extern struct cfdops mdsCfdOps;
extern struct replrqtree replrq_tree;
extern struct slash_creds rootcreds;

#endif /* _SLASHD_H_ */
