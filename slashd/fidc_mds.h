/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2009, Pittsburgh Supercomputing Center (PSC).
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
#include "mdsexpc.h"
#include "inodeh.h"
#include "mdslog.h"

SPLAY_HEAD(fcm_exports, mexpfcm);
SPLAY_PROTOTYPE(fcm_exports, mexpfcm, mexpfcm_fcm_tentry, mexpfcm_cache_cmp);

struct fidc_mds_info {
	struct fcm_exports	  fmdsi_exports;	/* tree of mexpfcm */
	struct slash_inode_handle fmdsi_inodeh;		/* MDS sl_inodeh_t goes here */
	atomic_t		  fmdsi_ref;
	void			 *fmdsi_data;		/* mdsio descriptor data */
};

#define fcmh_2_fmdsi(f)		((struct fidc_mds_info *)(f)->fcmh_fcoo->fcoo_pri)
#define fcmh_2_inoh(f)		(&fcmh_2_fmdsi(f)->fmdsi_inodeh)
#define fcmh_2_mdsio_data(f)	fcmh_2_fmdsi(f)->fmdsi_data

#define inoh_2_mdsio_data(ih)	fcmh_2_mdsio_data((ih)->inoh_fcmh)
#define inoh_2_fsz(ih)		fcmh_2_fsz((ih)->inoh_fcmh)
#define inoh_2_fid(ih)		fcmh_2_fid((ih)->inoh_fcmh)

static inline void
fmdsi_init(struct fidc_mds_info *mdsi, struct fidc_membh *fcmh, void *finfo)
{
	SPLAY_INIT(&mdsi->fmdsi_exports);
	atomic_set(&mdsi->fmdsi_ref, 0);
	mdsi->fmdsi_data = finfo;

	slash_inode_handle_init(&mdsi->fmdsi_inodeh, fcmh, mds_inode_sync);
}

struct fidc_mds_info *fidc_fid2fmdsi(slfid_t, struct fidc_membh **);
struct fidc_mds_info *fidc_fcmh2fmdsi(struct fidc_membh *);

int mds_fcmh_load_fmdsi(struct fidc_membh *, void *, int);
int mds_fcmh_tryref_fmdsi(struct fidc_membh *);

#endif /* _FIDC_MDS_H_ */
