/* $Id$ */

#ifndef _SLASH_H_
#define _SLASH_H_

#include "inode.h"

struct slash_sb_mem;

/* Slash server thread types. */
#define SLTHRT_CTL	0	/* control */
#define SLTHRT_RPCMDS	1	/* client metadata messages */
#define SLTHRT_RPCBE	2	/* MDS <-> I/O server backend messages */
#define SLTHRT_LND	3	/* lustre networking device helper */

struct slash_ctlthr {
	int sc_st_nclients;
	int sc_st_nsent;
	int sc_st_nrecv;
};

#define slctlthr(thr) ((struct slash_ctlthr *)(thr)->pscthr_private)

void		slmds_init(void);
void		slbe_init(void);
void		slash_journal_init(void);
sl_inum_t	slash_get_inum(void);

extern struct slash_sb_mem sbm;

#endif /* _SLASH_H_ */
