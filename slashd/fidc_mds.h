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

#ifndef _FIDC_MDS_H_
#define _FIDC_MDS_H_

#include "fid.h"
#include "fidcache.h"
#include "inodeh.h"
#include "mdsexpc.h"
#include "mdsio.h"
#include "mdslog.h"

SPLAY_HEAD(fcm_exports, mexpfcm);
SPLAY_PROTOTYPE(fcm_exports, mexpfcm, mexpfcm_fcmh_tentry, mexpfcm_cache_cmp);

struct fcmh_mds_info {
	mdsio_fid_t		  fcmi_mdsio_fid;	/* underlying mdsio file ID */
};

struct fcoo_mds_info {
	struct fcm_exports	  fmi_exports;		/* tree of mexpfcm */
	struct slash_inode_handle fmi_inodeh;		/* MDS sl_inodeh_t goes here */
	atomic_t		  fmi_refcnt;		/* # active references */
	void			 *fmi_mdsio_data;	/* mdsio descriptor */
};

#define fcmh_2_fmi(f)		((struct fcoo_mds_info *)		\
				    fcoo_get_pri((f)->fcmh_fcoo))
#define fcmh_2_fcmi(f)		((struct fcmh_mds_info *)fcmh_get_pri(f))
#define fcmh_2_inoh(f)		(&fcmh_2_fmi(f)->fmi_inodeh)
#define fcmh_2_mdsio_data(f)	fcmh_2_fmi(f)->fmi_mdsio_data
#define fcmh_2_mdsio_fid(f)	fcmh_2_fcmi(f)->fcmi_mdsio_fid

#define inoh_2_mdsio_data(ih)	fcmh_2_mdsio_data((ih)->inoh_fcmh)
#define inoh_2_fsz(ih)		fcmh_2_fsz((ih)->inoh_fcmh)
#define inoh_2_fid(ih)		fcmh_2_fid((ih)->inoh_fcmh)

struct fcoo_mds_info *fidc_fid2fmi(slfid_t, struct fidc_membh **);
struct fcoo_mds_info *fidc_fcmh2fmi(struct fidc_membh *);

int	mds_fcmh_load_fmi(struct fidc_membh *);
int	mds_fcmh_tryref_fmi(struct fidc_membh *);

int	slm_fcmh_get(const struct slash_fidgen *, struct slash_creds *,
	    struct fidc_membh **);
void	slm_fcmh_release(struct fidc_membh *);

#endif /* _FIDC_MDS_H_ */
