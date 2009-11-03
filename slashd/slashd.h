/* $Id$ */

#ifndef _SLASHD_H_
#define _SLASHD_H_

#include "psc_rpc/service.h"

#include "inode.h"

struct bmapc_memb;
struct fidc_membh;
struct mexpfcm;

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

/* cfd private accessors */
#define cfd_2_mexpfcm(cfd)	((struct mexpfcm *)(cfd)->cfd_pri)
#define cfd_2_fcmh(cfd)		cfd_2_mexpfcm(cfd)->mexpfcm_fcmh
#define cfd_2_fmdsi(cfd)	fcmh_2_fmdsi(cfd_2_fcmh(cfd))
#define cfd_2_zfsdata(cfd)	fcmh_2_zfsdata(cfd_2_fcmh(cfd))

int	fid_get(const char *, struct slash_fidgen *,
	    struct slash_creds *, int, mode_t);

void	mds_init(void);
int	mds_inode_release(struct fidc_membh *);

void	sltimerthr_spawn(void);
void	slctlthr_main(const char *);
void	mdsfssyncthr_init(void);
void	*slrcmthr_main(void *);

extern struct vbitmap		 slrcmthr_uniqidmap;
extern psc_spinlock_t		 slrcmthr_uniqidmap_lock;

extern struct cfdops		 mdsCfdOps;
extern struct slash_creds	 rootcreds;

extern struct psc_listcache	 dirtyMdsData;

#endif /* _SLASHD_H_ */
