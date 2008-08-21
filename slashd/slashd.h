/* $Id$ */

#ifndef _SLASH_H_
#define _SLASH_H_

#include "psc_rpc/service.h"
#include "psc_util/waitq.h"

#include "inode.h"

struct slash_sb_mem;

/* Slash server thread types. */
#define SLTHRT_CTL	0	/* control */
#define SLTHRT_ACSVC	1	/* access service */
#define SLTHRT_RMC	2	/* Client <-> MDS messages */
#define SLTHRT_RMI	3	/* MDS <-> I/O messages */
#define SLTHRT_RMM	4	/* MDS <-> MDS messages */
#define SLTHRT_LND	5	/* lustre networking device helper */
#define SLTHRT_TINTV	6	/* timer interval */
#define SLTHRT_TIOS	7	/* iostats updater */
#define SLTHRT_MDSCOH	8	/* mds coherency thread */
#define SLTHRT_MDSFSSYNC 9      /* mds fs syncer */


struct slash_rmcthr {
	struct pscrpc_thread	 srm_prt;
};

struct slash_rmithr {
	struct pscrpc_thread	 srb_prt;
};

struct slash_rmmthr {
	struct pscrpc_thread	 srb_prt;
};

#define slrmcthr(thr)	((struct slash_rmcthr *)(thr)->pscthr_private)
#define slrmithr(thr)	((struct slash_rmithr *)(thr)->pscthr_private)
#define slrmmthr(thr)	((struct slash_rmmthr *)(thr)->pscthr_private)

void		slash_journal_init(void);
sl_inum_t	slash_get_inum(void);

void	sltimerthr_spawn(void);

void fidcache_init(void);
int  fid_get(const char *, struct slash_fidgen *,
	struct slash_creds *, int, mode_t);

extern struct slash_sb_mem	sbm;

#endif /* _SLASH_H_ */
