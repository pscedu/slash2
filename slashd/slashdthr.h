/* $Id$ */

#ifndef _SLASH_H_
#define _SLASH_H_

#include "psc_rpc/service.h"
#include "psc_util/waitq.h"

#include "inode.h"

struct slash_sb_mem;

/* Slash server thread types. */
#define SLTHRT_CTL		0	/* control */
#define SLTHRT_ACSVC		1	/* access service */
#define SLTHRT_RMC		2	/* Client <-> MDS messages */
#define SLTHRT_RMI		3	/* MDS <-> I/O messages */
#define SLTHRT_RMM		4	/* MDS <-> MDS messages */
#define SLTHRT_LNETAC		5	/* lustre net accept thr */
#define SLTHRT_USKLNDPL		6	/* userland socket lustre net dev poll thr */
#define SLTHRT_TINTV		7	/* timer interval */
#define SLTHRT_TIOS		8	/* iostats updater */
#define SLTHRT_MDSCOH		9	/* mds coherency thread */
#define SLTHRT_MDSFSSYNC	10      /* mds fs syncer */


struct slash_rmcthr {
	struct pscrpc_thread	 srmc_prt;
};

struct slash_rmithr {
	struct pscrpc_thread	 srmi_prt;
};

struct slash_rmmthr {
	struct pscrpc_thread	 srmm_prt;
};

PSCTHR_MKCAST(slrmcthr, slash_rmcthr, SLTHRT_RMC)
PSCTHR_MKCAST(slrmithr, slash_rmithr, SLTHRT_RMI)
PSCTHR_MKCAST(slrmmthr, slash_rmmthr, SLTHRT_RMM)

void		sl_journal_init(void);
sl_inum_t	slmds_get_inum(void);

void	sltimerthr_spawn(void);
void	slctlthr_main(const char *);

int  fid_get(const char *, struct slash_fidgen *,
	struct slash_creds *, int, mode_t);

extern struct slash_sb_mem	slSuperBlk;
extern int                      slSuperFd;

#endif /* _SLASH_H_ */
