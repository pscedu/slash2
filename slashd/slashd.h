/* $Id$ */

#ifndef _SLASHD_H_
#define _SLASHD_H_

#include "psc_ds/dynarray.h"
#include "psc_rpc/service.h"

#include "inode.h"

struct bmapc_memb;
struct fidc_membh;
struct mexpfcm;

/* Slash server thread types. */
#define SLMTHRT_CTL		0	/* control */
#define SLMTHRT_RMC		1	/* MDS <- CLI msg svc handler */
#define SLMTHRT_RMI		2	/* MDS <- I/O msg svc handler */
#define SLMTHRT_RMM		3	/* MDS <- MDS msg svc handler */
#define SLMTHRT_RCM		4	/* CLI <- MDS msg issuer */
#define SLMTHRT_LNETAC		5	/* lustre net accept thr */
#define SLMTHRT_USKLNDPL	6	/* userland socket lustre net dev poll thr */
#define SLMTHRT_TINTV		7	/* timer interval */
#define SLMTHRT_TIOS		8	/* I/O stats updater */
#define SLMTHRT_COH		9	/* coherency thread */
#define SLMTHRT_FSSYNC		10	/* file system syncer */
#define SLMTHRT_SITEMON		11	/* site monitor for replication, etc. */

struct slmrmc_thread {
	struct pscrpc_thread	  smrct_prt;
};

struct slmrcm_thread {
	struct slashrpc_cservice *srcm_csvc;
	int			  srcm_uniqid;	/* thread ID */
	struct slash_fidgen	  srcm_fg;
	int			  srcm_id;	/* private client ID */
	char			 *srcm_page;
	int			  srcm_pagelen;
};

struct slmrmi_thread {
	struct pscrpc_thread	  smrit_prt;
};

struct slmrmm_thread {
	struct pscrpc_thread	  smrmt_prt;
};

struct slmsm_thread {
	struct psc_dynarray	  smsmt_replq;
	struct sl_site		 *smsmt_site;
};

PSCTHR_MKCAST(slmrcmthr, slmrcm_thread, SLMTHRT_RCM)
PSCTHR_MKCAST(slmrmcthr, slmrmc_thread, SLMTHRT_RMC)
PSCTHR_MKCAST(slmrmithr, slmrmi_thread, SLMTHRT_RMI)
PSCTHR_MKCAST(slmrmmthr, slmrmm_thread, SLMTHRT_RMM)
PSCTHR_MKCAST(slmsmthr, slmsm_thread, SLMTHRT_SITEMON)

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
void	sitemons_spawn(void);
void	*slrcmthr_main(void *);

extern struct vbitmap		 slrcmthr_uniqidmap;
extern psc_spinlock_t		 slrcmthr_uniqidmap_lock;

extern struct cfdops		 mdsCfdOps;
extern struct slash_creds	 rootcreds;

extern struct psc_listcache	 dirtyMdsData;

#endif /* _SLASHD_H_ */
