/* $Id$ */

#ifndef _SLASH_H_
#define _SLASH_H_

#include "psc_rpc/service.h"
#include "psc_util/waitq.h"

#include "inode.h"

struct slash_sb_mem;

/* Slash server thread types. */
#define SLTHRT_CTL	0	/* control */
#define SLTHRT_RPCMDS	1	/* client metadata messages */
#define SLTHRT_RPCBE	2	/* MDS <-> I/O server backend messages */
#define SLTHRT_LND	3	/* lustre networking device helper */
#define SLTHRT_TINTV	4	/* timer interval */
#define SLTHRT_TIOS	5	/* iostats updater */

struct slash_rpcmdsthr {
	struct pscrpc_thread	 srm_prt;
};

struct slash_rpcbethr {
	struct pscrpc_thread	 srb_prt;
};

#define slrpcmdsthr(thr) ((struct slash_rpcmdsthr *)(thr)->pscthr_private)
#define slrpcbethr(thr)	 ((struct slash_rpcbethr *)(thr)->pscthr_private)

void		slmds_init(void);
void		slbe_init(void);
void		slash_journal_init(void);
sl_inum_t	slash_get_inum(void);

void	sltimerthr_spawn(void);

extern struct slash_sb_mem	sbm;

#endif /* _SLASH_H_ */
