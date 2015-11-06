/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2014, Pittsburgh Supercomputing Center (PSC).
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or (at
 * your option) any later version.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License contained in the file
 * `COPYING-GPL' at the top of this distribution or at
 * https://www.gnu.org/licenses/gpl-2.0.html for more details.
 *
 * Pittsburgh Supercomputing Center	phone: 412.268.4960  fax: 412.268.5832
 * 300 S. Craig Street			e-mail: remarks@psc.edu
 * Pittsburgh, PA 15213			web: http://www.psc.edu/
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

#ifndef _FIDC_MDS_H_
#define _FIDC_MDS_H_

#include "pfl/dynarray.h"

#include "fid.h"
#include "fidcache.h"
#include "inode.h"
#include "journal_mds.h"
#include "mdsio.h"
#include "slashd.h"
#include "up_sched_res.h"

/**
 * fcmh_mds_info - MDS-specific fcmh data.
 * @fmi_mfid - backing object MIO FID.  This is used to access the
 *	backing object (for files: the SLASH2 inode; for directories:
 *	the directory itself).  For the directory SLASH2 inode, see
 *	@fmi_dino_mf*.
 * @fmi_mfh - file descriptor for backing object.
 */
struct fcmh_mds_info {
	struct slash_inode_handle fmi_inodeh;
	mio_fid_t		  fmi_mfid;		/* backing object inum */
	struct mio_fh		  fmi_mfh;		/* file descriptor */
	int			  fmi_ctor_rc;		/* constructor return code */
	union {
		struct {
			mio_fid_t fmid_dino_mfid;	/* for inode */
			struct mio_fh
				  fmid_dino_mfh;
		} d;
		struct {
			uint64_t  fmif_ptrunc_size;	/* new truncate(2) size */
			struct psc_dynarray
				  fmif_ptrunc_clients;	/* clients awaiting CRC recalc */
		} f;
	} u;
#define fmi_dino_mfid		u.d.fmid_dino_mfid
#define fmi_dino_mfh		u.d.fmid_dino_mfh

#define fmi_ptrunc_size		u.f.fmif_ptrunc_size
#define fmi_ptrunc_clients	u.f.fmif_ptrunc_clients
};

/* mds-specific fcmh_flags */
#define FCMH_MDS_IN_PTRUNC	(_FCMH_FLGSHFT << 0)

#define fcmh_2_inoh(f)		(&fcmh_2_fmi(f)->fmi_inodeh)
#define fcmh_2_ino(f)		(&fcmh_2_inoh(f)->inoh_ino)
#define fcmh_2_inox(f)		fcmh_2_inoh(f)->inoh_extras

#define fcmh_2_mfhp(f)		(&fcmh_2_fmi(f)->fmi_mfh)
#define fcmh_2_mfh(f)		fcmh_2_mfhp(f)->fh
#define fcmh_2_mfid(f)		fcmh_2_fmi(f)->fmi_mfid

#define fcmh_2_dino_mfhp(f)	(&fcmh_2_fmi(f)->fmi_dino_mfh)
#define fcmh_2_dino_mfh(f)	fcmh_2_dino_mfhp(f)->fh
#define fcmh_2_dino_mfid(f)	fcmh_2_fmi(f)->fmi_dino_mfid

#undef fcmh_2_nrepls
#define fcmh_2_nrepls(f)	fcmh_2_ino(f)->ino_nrepls
#define fcmh_2_replpol(f)	fcmh_2_ino(f)->ino_replpol
#define fcmh_2_metafsize(f)	(f)->fcmh_sstb.sst_blksize
#define fcmh_nallbmaps(f)	howmany(fcmh_2_metafsize(f) - SL_BMAP_START_OFF, BMAP_OD_SZ)
#define fcmh_nvalidbmaps(f)	howmany(fcmh_2_fsz(f), SLASH_BMAP_SIZE)

#define fcmh_getrepl(f, n)	((n) < SL_DEF_REPLICAS ?		\
				    fcmh_2_ino(f)->ino_repls[n] :	\
				    fcmh_2_inox(f)->inox_repls[(n) - SL_DEF_REPLICAS])

#define FCMH_HAS_GARBAGE(f)	(fcmh_nallbmaps(f) > fcmh_nvalidbmaps(f))

#define IS_REMOTE_FID(fid)						\
	((fid) != SLFID_ROOT && nodeSite->site_id != FID_GET_SITEID(fid))

#define slm_fcmh_get(fgp, fp)	fidc_lookup((fgp), FIDC_LOOKUP_CREATE, (fp))
#define slm_fcmh_peek(fgp, fp)	fidc_lookup((fgp), FIDC_LOOKUP_NONE, (fp))

#define mds_fcmh_setattr(vfsid, f, to_set, sstb)	_mds_fcmh_setattr((vfsid), (f), (to_set), (sstb), 1)
#define mds_fcmh_setattr_nolog(vfsid, f, to_set, sstb)	_mds_fcmh_setattr((vfsid), (f), (to_set), (sstb), 0)

int	_mds_fcmh_setattr(int, struct fidc_membh *, int, const struct srt_stat *, int);

#define slm_fcmh_endow(vfsid, p, c)			_slm_fcmh_endow((vfsid), (p), (c), 1)
#define slm_fcmh_endow_nolog(vfsid, p, c)		_slm_fcmh_endow((vfsid), (p), (c), 0)

int	_slm_fcmh_endow(int, struct fidc_membh *, struct fidc_membh *, int);

int	slfid_to_vfsid(slfid_t, int *);

extern uint64_t		slm_next_fid;
extern psc_spinlock_t	slm_fid_lock;

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

static __inline void
fcmh_set_repl(struct fidc_membh *f, int idx, sl_ios_id_t iosid)
{
	if (idx < SL_DEF_REPLICAS)
		fcmh_2_ino(f)->ino_repls[idx].bs_id = iosid;
	else {
		mds_inox_ensure_loaded(fcmh_2_inoh(f));
		fcmh_2_inox(f)->inox_repls[idx - SL_DEF_REPLICAS].bs_id = iosid;
	}
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

static __inline struct fidc_membh *
fmi_2_fcmh(struct fcmh_mds_info *fmi)
{
	return (fcmh_from_pri(fmi));
}

static __inline const struct fidc_membh *
fmi_2_fcmh_const(const struct fcmh_mds_info *fmi)
{
	return (fcmh_from_pri_const(fmi));
}

static __inline struct fcmh_mds_info *
inoh_2_fmi(struct slash_inode_handle *ih)
{
	psc_assert(ih);
	return (PSC_AGP(ih, -offsetof(struct fcmh_mds_info, fmi_inodeh)));
}

static __inline const struct fcmh_mds_info *
inoh_2_fmi_const(const struct slash_inode_handle *ih)
{
	psc_assert(ih);
	return (PSC_AGP(ih, -offsetof(struct fcmh_mds_info, fmi_inodeh)));
}

#endif /* _FIDC_MDS_H_ */
