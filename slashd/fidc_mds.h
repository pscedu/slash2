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
#include "mdsio.h"
#include "mdslog.h"
#include "slashd.h"

struct fcmh_mds_info {
	struct slash_inode_handle fmi_inodeh;		/* MDS sl_inodeh_t goes here */
	mdsio_fid_t		  fmi_mdsio_fid;	/* underlying mdsio file ID */
	void			 *fmi_mdsio_data;	/* mdsio descriptor */
	int			  fmi_ctor_rc;		/* constructor return code */
};

#define fcmh_2_fmi(f)		((struct fcmh_mds_info *)fcmh_get_pri(f))
#define fcmh_2_inoh(f)		(&fcmh_2_fmi(f)->fmi_inodeh)
#define fcmh_2_ino(f)		(&fcmh_2_inoh(f)->inoh_ino)
#define fcmh_2_mdsio_data(f)	fcmh_2_fmi(f)->fmi_mdsio_data
#define fcmh_2_mdsio_fid(f)	fcmh_2_fmi(f)->fmi_mdsio_fid
#define fcmh_2_nrepls(f)	fcmh_2_ino(f)->ino_nrepls
#define fcmh_2_repl(f, i)	fcmh_2_ino(f)->ino_repls[i].bs_id

#define inoh_2_mdsio_data(ih)	fcmh_2_mdsio_data((ih)->inoh_fcmh)
#define inoh_2_fsz(ih)		fcmh_2_fsz((ih)->inoh_fcmh)
#define inoh_2_fid(ih)		fcmh_2_fid((ih)->inoh_fcmh)

#define slm_fcmh_get(fgp, fp)	fidc_lookup((fgp), FIDC_LOOKUP_CREATE, NULL, 0, (fp))

void	mds_fcmh_increase_fsz(struct fidc_membh *, off_t);

#endif /* _FIDC_MDS_H_ */
