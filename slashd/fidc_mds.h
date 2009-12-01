/* $Id$ */

#ifndef _FIDC_MDS_H_
#define _FIDC_MDS_H_

#include "fid.h"
#include "fidcache.h"
#include "mdsexpc.h"
#include "inodeh.h"
#include "mdslog.h"

SPLAY_HEAD(fcm_exports, mexpfcm);
SPLAY_PROTOTYPE(fcm_exports, mexpfcm, mexpfcm_fcm_tentry, mexpfcm_cache_cmp);

struct fidc_mds_info {
	struct fcm_exports	  fmdsi_exports;	/* tree of mexpfcm */
	struct slash_inode_handle fmdsi_inodeh;		/* MDS sl_inodeh_t goes here */
	atomic_t		  fmdsi_ref;
	uint32_t		  fmdsi_xid;
	void			 *fmdsi_data;
};

#define fcmh_2_fmdsi(f)		((struct fidc_mds_info *)(f)->fcmh_fcoo->fcoo_pri)
#define fcmh_2_inoh(f)		(&fcmh_2_fmdsi(f)->fmdsi_inodeh)
#define fcmh_2_zfsdata(f)	fcmh_2_fmdsi(f)->fmdsi_data

#define inoh_2_zfsdata(ih)	fcmh_2_zfsdata((ih)->inoh_fcmh)
#define inoh_2_fsz(ih)		fcmh_2_fsz((ih)->inoh_fcmh)
#define inoh_2_fid(ih)		fcmh_2_fid((ih)->inoh_fcmh)

static inline void
fmdsi_init(struct fidc_mds_info *mdsi, struct fidc_membh *fcmh, void *pri)
{
	SPLAY_INIT(&mdsi->fmdsi_exports);
	atomic_set(&mdsi->fmdsi_ref, 0);
	mdsi->fmdsi_xid = 0;
	mdsi->fmdsi_data = pri;

	slash_inode_handle_init(&mdsi->fmdsi_inodeh, fcmh, mds_inode_sync);
}

struct fidc_mds_info *fidc_fid2fmdsi(slfid_t, struct fidc_membh **);
struct fidc_mds_info *fidc_fcmh2fmdsi(struct fidc_membh *);

int mds_fcmh_load_fmdsi(struct fidc_membh *, void *, int);
int mds_fcmh_tryref_fmdsi(struct fidc_membh *);
int mds_fcmh_apply_fsize(struct fidc_membh *, off64_t);

#endif /* _FIDC_MDS_H_ */
