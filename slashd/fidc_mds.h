/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2011, Pittsburgh Supercomputing Center (PSC).
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

#include "psc_ds/dynarray.h"

#include "fid.h"
#include "fidcache.h"
#include "inode.h"
#include "mdsio.h"
#include "mdslog.h"
#include "slashd.h"

struct fcmh_mds_info {
	struct slash_inode_handle fmi_inodeh;		/* MDS sl_inodeh_t goes here */
	mdsio_fid_t		  fmi_mdsio_fid;	/* underlying mdsio file ID */
	void			 *fmi_mdsio_data;	/* mdsio descriptor */
	int			  fmi_ctor_rc;		/* constructor return code */
	uint64_t		  fmi_ptrunc_size;	/* new truncate(2) size */
	struct psc_dynarray	  fmi_ptrunc_clients;	/* clients awaiting CRC recalc */
};

#define FCMH_IN_PTRUNC		(_FCMH_FLGSHFT << 0)
#define FCMH_IN_SETATTR		(_FCMH_FLGSHFT << 1)

#define fcmh_2_inoh(f)		(&fcmh_2_fmi(f)->fmi_inodeh)
#define fcmh_2_ino(f)		(&fcmh_2_inoh(f)->inoh_ino)
#define fcmh_2_inox(f)		fcmh_2_inoh(f)->inoh_extras
#define fcmh_2_mdsio_data(f)	fcmh_2_fmi(f)->fmi_mdsio_data
#define fcmh_2_mdsio_fid(f)	fcmh_2_fmi(f)->fmi_mdsio_fid
#define fcmh_2_nrepls(f)	fcmh_2_ino(f)->ino_nrepls
#define fcmh_2_metafsize(f)	(f)->fcmh_sstb.sst_blksize
#define fcmh_nallbmaps(f)	howmany(fcmh_2_metafsize(f) - SL_BMAP_START_OFF, BMAP_OD_SZ)
#define fcmh_nvalidbmaps(f)	howmany(fcmh_2_fsz(f), SLASH_BMAP_SIZE)

#define FCMH_HAS_GARBAGE(f)	(fcmh_nallbmaps(f) > fcmh_nvalidbmaps(f))

#define slm_fcmh_get(fgp, fp)	fidc_lookup((fgp), FIDC_LOOKUP_CREATE, NULL, 0, (fp))
#define slm_fcmh_peek(fgp, fp)	fidc_lookup((fgp), FIDC_LOOKUP_NONE, NULL, 0, (fp))

int	mds_fcmh_setattr(struct fidc_membh *, int);

static __inline struct fcmh_mds_info *
fcmh_2_fmi(struct fidc_membh *f)
{
	return (fcmh_get_pri(f));
}

static __inline sl_ios_id_t
fcmh_2_repl(struct fidc_membh *f, int idx)
{
	if (idx < SL_DEF_REPLICAS)
		return (fcmh_2_ino(f)->ino_repls[idx].bs_id);
	mds_inox_ensure_loaded(fcmh_2_inoh(f));
	return (fcmh_2_inox(f)->inox_repls[idx - SL_DEF_REPLICAS].bs_id);
}

static __inline uint64_t
fcmh_2_repl_nblks(struct fidc_membh *f, int idx)
{
	if (idx < SL_DEF_REPLICAS)
		return (fcmh_2_ino(f)->ino_repl_nblks[idx]);
	mds_inox_ensure_loaded(fcmh_2_inoh(f));
	return (fcmh_2_inox(f)->inox_repl_nblks[idx - SL_DEF_REPLICAS]);
}

static __inline void
fcmh_set_repl_nblks(struct fidc_membh *f, int idx, uint64_t nblks)
{
	if (idx < SL_DEF_REPLICAS)
		fcmh_2_ino(f)->ino_repl_nblks[idx] = nblks;
	else {
		mds_inox_ensure_loaded(fcmh_2_inoh(f));
		fcmh_2_inox(f)->inox_repl_nblks[idx - SL_DEF_REPLICAS] = nblks;
	}
}

#endif /* _FIDC_MDS_H_ */
