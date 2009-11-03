/* $Id$ */

#ifndef _SLASH_H_
#define _SLASH_H_

#include "psc_rpc/service.h"
#include "psc_util/waitq.h"

#include "inode.h"

struct bmapc_memb;

/* Slash server thread types. */
#define SLTHRT_CTL		0	/* control */
#define SLTHRT_RMC		1	/* MDS <- CLI message handler */
#define SLTHRT_RMI		2	/* MDS <- I/O message handler */
#define SLTHRT_RMM		3	/* MDS <- MDS message handler */
#define SLTHRT_RCM		4	/* CLI <- MDS message issuer */
#define SLTHRT_LNETAC		5	/* lustre net accept thr */
#define SLTHRT_USKLNDPL		6	/* userland socket lustre net dev poll thr */
#define SLTHRT_TINTV		7	/* timer interval */
#define SLTHRT_TIOS		8	/* I/O stats updater */
#define SLTHRT_COH		9	/* coherency thread */
#define SLTHRT_FSSYNC		10      /* file system syncer */
#define SLTHRT_IONMON		10      /* I/O system monitor for replication, etc. */

struct slash_rmcthr {
	struct pscrpc_thread	  srmc_prt;
};

struct slash_rcmthr {
	struct slashrpc_cservice *srcm_csvc;
	int			  srcm_uniqid;	/* thread ID */
	struct slash_fidgen	  srcm_fg;
	int			  srcm_id;	/* private client ID */
	char			 *srcm_page;
	int			  srcm_pagelen;
};

struct slash_rmithr {
	struct pscrpc_thread	  srmi_prt;
};

struct slash_rmmthr {
	struct pscrpc_thread	  srmm_prt;
};

PSCTHR_MKCAST(slrcmthr, slash_rcmthr, SLTHRT_RCM)
PSCTHR_MKCAST(slrmcthr, slash_rmcthr, SLTHRT_RMC)
PSCTHR_MKCAST(slrmithr, slash_rmithr, SLTHRT_RMI)
PSCTHR_MKCAST(slrmmthr, slash_rmmthr, SLTHRT_RMM)

void	sl_journal_init(void);

void	sltimerthr_spawn(void);
void	slctlthr_main(const char *);

int	fid_get(const char *, struct slash_fidgen *,
	    struct slash_creds *, int, mode_t);

int	mdsio_zfs_bmap_read(struct bmapc_memb *);
int	mdsio_zfs_bmap_write(struct bmapc_memb *);

void	mds_init(void);
void	mdsfssync_init(void);

void	*slrcmthr_main(void *);

extern struct vbitmap	 slrcmthr_uniqidmap;
extern psc_spinlock_t	 slrcmthr_uniqidmap_lock;

#endif /* _SLASH_H_ */
