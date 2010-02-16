/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
 *
 * Permission to use, copy, and modify this software and its documentation
 * without fee for personal use or non-commercial use within your organization
 * is hereby granted, provided that the above copyright notice is preserved in
 * all copies and that the copyright and this permission notice appear in
 * supporting documentation.  Permission to redistribute this software to other
 * organizations or individuals is not permitted without the written permission
 * of the Pittsburgh Supercomputing Center.  PSC makes no representations about
 * the suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

#ifndef _SLASHD_H_
#define _SLASHD_H_

#include "psc_ds/dynarray.h"
#include "psc_ds/vbitmap.h"
#include "psc_rpc/service.h"
#include "psc_util/multiwait.h"

#include "inode.h"
#include "slconfig.h"

struct odtable;
struct odtable_receipt;

struct bmapc_memb;
struct fidc_membh;
struct mexpfcm;
struct slash_inode_handle;

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
#define SLMTHRT_REPLQ		11	/* per-site replication queuer */

struct slmrmc_thread {
	struct pscrpc_thread	  smrct_prt;
};

struct slmrcm_thread {
	char			 *srcm_page;
	int			  srcm_page_bitpos;
};

struct slmrmi_thread {
	struct pscrpc_thread	  smrit_prt;
};

struct slmrmm_thread {
	struct pscrpc_thread	  smrmt_prt;
};

struct slmreplq_thread {
	struct sl_site		 *smrt_site;
};

PSCTHR_MKCAST(slmrcmthr, slmrcm_thread, SLMTHRT_RCM)
PSCTHR_MKCAST(slmrmcthr, slmrmc_thread, SLMTHRT_RMC)
PSCTHR_MKCAST(slmrmithr, slmrmi_thread, SLMTHRT_RMI)
PSCTHR_MKCAST(slmrmmthr, slmrmm_thread, SLMTHRT_RMM)
PSCTHR_MKCAST(slmreplqthr, slmreplq_thread, SLMTHRT_REPLQ)

struct site_mds_info {
	struct psc_dynarray	  smi_replq;
	psc_spinlock_t		  smi_lock;
	struct psc_multiwait	  smi_mw;
	struct psc_multiwaitcond  smi_mwcond;
	int			  smi_flags;
};

#define SMIF_DIRTYQ		(1 << 0)		/* queue has changed */

struct resm_mds_info {
	struct slashrpc_cservice *rmmi_csvc;
	psc_spinlock_t		  rmmi_lock;
	struct psc_multiwaitcond  rmmi_mwcond;

	int			  rmmi_busyid;
	struct sl_resm		 *rmmi_resm;
	struct psc_dynarray	  rmmi_bmaps;		/* array of struct mexpbcm */
	struct psc_dynarray	  rmmi_bmaps_deref;	/* dereferencing bmaps */
	atomic_t		  rmmi_refcnt;		/* #CLIs using this ion */
};

#define resm2rmmi(resm)		((struct resm_mds_info *)(resm)->resm_pri)

/* IOS round-robin counter for assigning IONs.  Attaches at res_pri.
 */
struct resprof_mds_info {
	int			  rpmi_cnt;
	psc_spinlock_t		  rpmi_lock;
};

/* cfd private accessors */
#define cfd_2_mexpfcm(cfd)	((struct mexpfcm *)(cfd)->cfd_pri)
#define cfd_2_fcmh(cfd)		cfd_2_mexpfcm(cfd)->mexpfcm_fcmh
#define cfd_2_fmi(cfd)		fcmh_2_fmi(cfd_2_fcmh(cfd))
#define cfd_2_mdsio_data(cfd)	fcmh_2_mdsio_data(cfd_2_fcmh(cfd))

int		 fid_get(const char *, struct slash_fidgen *,
		    struct slash_creds *, int, mode_t);

int		 mds_inode_release(struct fidc_membh *);
int		 mds_inox_load_locked(struct slash_inode_handle *);
int		 mds_inox_ensure_loaded(struct slash_inode_handle *);

void		 mds_bmi_cb(void *, struct odtable_receipt *);

void		 slmtimerthr_spawn(void);
__dead void	 slmctlthr_main(const char *);
void		 slmfssyncthr_spawn(void);
void		 slmreplqthr_spawnall(void);
void		*slmrcmthr_main(void *);

extern struct slash_creds			 rootcreds;

extern struct psc_listcache			 dirtyMdsData;
extern struct odtable				*mdsBmapAssignTable;
extern const struct slash_inode_extras_od	 null_inox_od;

#endif /* _SLASHD_H_ */
