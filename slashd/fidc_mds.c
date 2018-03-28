/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2007-2018, Pittsburgh Supercomputing Center
 * All rights reserved.
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
 * ---------------------------------------------------------------------
 * %END_LICENSE%
 */

#include <sys/types.h>
#include <sys/stat.h>

#include <fcntl.h>
#include <stdio.h>

#include "pfl/cdefs.h"
#include "pfl/fs.h"
#include "pfl/atomic.h"
#include "pfl/lock.h"
#include "pfl/log.h"

#include "cache_params.h"
#include "fid.h"
#include "fidc_mds.h"
#include "fidcache.h"
#include "mdsio.h"
#include "slashd.h"

#include "zfs-fuse/zfs_slashlib.h"

int
slfid_to_vfsid(slfid_t fid, int *vfsid)
{
	int i, siteid;

	/*
	 * Our client uses this special fid to contact us during mount,
	 * at which time it does not know the site ID yet.
	 *
	 * XXX The client should be able to retrieve the site id from
	 * the slash2 config file.
	 */
	if (fid == SLFID_ROOT) {
		*vfsid = current_vfsid;
		return (0);
	}

	/* only have default file system in the root */
	if (zfs_nmounts == 1) {
		*vfsid = current_vfsid;
		return (0);
	}

	siteid = FID_GET_SITEID(fid);
	for (i = 0; i < zfs_nmounts; i++) {
		if (zfs_mounts[i].zm_siteid == (uint64_t)siteid) {
			*vfsid = i;
			return (0);
		}
	}
	return (-EINVAL);
}

int
_mds_fcmh_setattr(int vfsid, struct fidc_membh *f, int to_set,
    const struct srt_stat *sstb, int log)
{
	struct srt_stat sstb_out;
	int rc;

	FCMH_LOCK_ENSURE(f);
	FCMH_ULOCK(f);

	if (log)
		mds_reserve_slot(1);
	rc = mdsio_setattr(vfsid, fcmh_2_mfid(f), sstb, to_set,
	    &rootcreds, &sstb_out, fcmh_2_mfh(f),
	    log ? mdslog_namespace : NULL);
	if (log)
		mds_unreserve_slot(1);

	if (!rc) {
		FCMH_LOCK(f);
		if (sstb_out.sst_fid != fcmh_2_fid(f)) {
			psclog_errorx("FIDs: "SLPRI_FID" versus "SLPRI_FID,
			    sstb_out.sst_fid, fcmh_2_fid(f));
			psc_fatal("setattr: fid mismatch");
		}
		f->fcmh_sstb = sstb_out;
		FCMH_ULOCK(f);
	}

	return (rc);
}

int
slm_fcmh_ctor(struct fidc_membh *f, __unusedx int flags)
{
	struct fcmh_mds_info *fmi;
	struct mio_fh *ino_mfh;
	struct slm_inoh *ih;
	mio_fid_t ino_mfid;
	int rc, vfsid;

	DEBUG_FCMH(PLL_DIAG, f, "ctor");

	rc = slfid_to_vfsid(fcmh_2_fid(f), &vfsid);
	if (rc) {
		DEBUG_FCMH(PLL_WARN, f, "invalid file system ID; "
		    "rc=%d", rc);
		return (rc);
	}
	fmi = fcmh_2_fmi(f);
	memset(fmi, 0, sizeof(*fmi));
	INIT_PSCLIST_HEAD(&fmi->fmi_callbacks);

	rc = mdsio_lookup_slfid(vfsid, fcmh_2_fid(f), &rootcreds,
	    &f->fcmh_sstb, &fcmh_2_mfid(f));
	if (rc) {
		/* system administrator needed in this case */
		DEBUG_FCMH(PLL_WARN, f, "mdsio_lookup_slfid failed; "
		    "fid="SLPRI_FID" rc=%d",
		    fcmh_2_fid(f), rc);
		OPSTAT_INCR("lookup-slfid-err");
		return (rc);
	}

	ih = &fmi->fmi_inodeh;
	ih->inoh_flags = INOH_INO_NOTLOADED;

	ino_mfid = fcmh_2_mfid(f);
	ino_mfh = fcmh_2_mfhp(f);

	if (fcmh_isdir(f)) {
		mio_fid_t pmfid;
		char fn[24];

		rc = mdsio_opendir(vfsid, fcmh_2_mfid(f), &rootcreds,
		    NULL, &fcmh_2_mfh(f));
		if (rc) {
			DEBUG_FCMH(PLL_WARN, f, "mdsio_opendir failed; "
			    "mio_fid=%"PRIx64" rc=%d", fcmh_2_mfid(f),
			    rc);
			return (rc);
		}

		/* 
		 * Introduced by d56424e5f35de84cef5ba3b61afb8583efbd0a7b.
		 *
		 * XXX if we ever support this, we should at least do it 
		 * on-demand.
		 */
		snprintf(fn, sizeof(fn), "%016"PRIx64".ino",
		    fcmh_2_fid(f));

		pmfid = mdsio_getfidlinkdir(fcmh_2_fid(f));
		rc = mdsio_lookup(vfsid, pmfid, fn,
		    &fcmh_2_dino_mfid(f), &rootcreds, NULL);
		if (rc == ENOENT) {
			struct slm_inox_od inox;

			rc = mdsio_opencreatef(vfsid, pmfid, &rootcreds,
			    O_CREAT | O_EXCL | O_RDWR,
			    MDSIO_OPENCRF_NOLINK, 0644, fn,
			    &fcmh_2_dino_mfid(f), NULL,
			    &fcmh_2_dino_mfh(f), NULL, NULL, 0);
			psc_assert(rc == 0);

			INOH_LOCK(ih);

			rc = mds_inode_write(vfsid, ih, NULL, NULL);
			psc_assert(rc == 0);

			memset(&inox, 0, sizeof(inox));
			ih->inoh_extras = &inox;
			rc = mds_inox_write(vfsid, ih, NULL, NULL);
			ih->inoh_extras = NULL;

			INOH_ULOCK(ih);

			psc_assert(rc == 0);

			/*
 			 * We are going to open this file again shortly by
 			 * setting ino_mfid.
 			 *
 			 * We should just use the file handle without close
 			 * and open it again.
 			 */
			mdsio_release(vfsid, &rootcreds, fcmh_2_dino_mfh(f));
		} else if (rc) {
			DEBUG_FCMH(PLL_WARN, f,
			    "mdsio_lookup failed; rc=%d", rc);
			return (rc);
		}

		ino_mfid = fcmh_2_dino_mfid(f);
		ino_mfh = fcmh_2_dino_mfhp(f);
	}

	if (fcmh_isdir(f) || fcmh_isreg(f)) {
		/*
		 * We shouldn't need O_LARGEFILE because SLASH2 metafiles 
		 * are small.
		 *
		 * I created a file with size of 8070450532247928832
		 * using dd by seeking to a large offset and writing one
		 * byte.  Somehow, the ZFS size becomes 5119601018368.
		 * Without O_LARGEFILE, I got EOVERFLOW (75) here.  The
		 * SLASH2 size is correct though.
		 */
		rc = mdsio_opencreate(vfsid, ino_mfid, &rootcreds,
		    O_RDWR, 0, NULL, NULL, NULL, &ino_mfh->fh, NULL,
		    NULL, 0);
		if (rc == 0) {
			rc = mds_inode_read(&fmi->fmi_inodeh);
			if (rc)
				DEBUG_FCMH(PLL_WARN, f,
				    "could not load inode; "
				    "mfid=%"PRIx64" rc=%d",
				    ino_mfid, rc);
		} else {
			DEBUG_FCMH(PLL_WARN, f,
			    "mdsio_opencreate failed; "
			    "mfid=%"PRIx64" rc=%d",
			    ino_mfid, rc);
		}
	} else
		DEBUG_FCMH(PLL_DIAG, f, "special file, no zfs obj");

	return (rc);
}

void
slm_fcmh_dtor(struct fidc_membh *f)
{
	struct fcmh_mds_info *fmi;
	int rc, vfsid;

	fmi = fcmh_2_fmi(f);
	if (fcmh_isreg(f) || fcmh_isdir(f)) {
		/* XXX Need to worry about other modes here */
		if (fcmh_2_mfh(f)) {
			psc_assert(fcmh_2_mfh(f));
			slfid_to_vfsid(fcmh_2_fid(f), &vfsid);
			rc = mdsio_release(vfsid, &rootcreds,
			    fcmh_2_mfh(f));
			psc_assert(rc == 0);
		}
	}

	if (fcmh_isdir(f)) {
		if (fcmh_2_dino_mfh(f)) {
			slfid_to_vfsid(fcmh_2_fid(f), &vfsid);
			psc_assert(fcmh_2_dino_mfh(f));
			rc = mdsio_release(vfsid, &rootcreds,
			    fcmh_2_dino_mfh(f));
			psc_assert(rc == 0);
		}
	}

	PSCFREE(fmi->fmi_inodeh.inoh_extras);
}

/*
 * "Endow" or apply inheritance to a new directory entry from its parent
 * directory replica layout.
 *
 * Note: the bulk of this is empty until we have a place to store such
 * info in the SLASH2 metafile.
 */
int
slm_fcmh_endow(int vfsid, struct fidc_membh *p, struct fidc_membh *c)
{
	sl_replica_t repls[SL_MAX_REPLICAS];
	int nr, rc = 0;
	uint32_t pol;

	FCMH_LOCK(p);
	nr = fcmh_2_nrepls(p);
	if (!nr) {
		FCMH_ULOCK(p);
		return (0);
	}
	pol = fcmh_2_ino(p)->ino_replpol;
	memcpy(repls, fcmh_2_ino(p)->ino_repls, sizeof(repls[0]) *
	    SL_DEF_REPLICAS);
	if (nr > SL_DEF_REPLICAS) {
		mds_inox_ensure_loaded(fcmh_2_inoh(p));
		memcpy(&repls[SL_DEF_REPLICAS],
		    fcmh_2_inox(p)->inox_repls, sizeof(repls[0]) *
		    SL_INOX_NREPLICAS);
	}
	FCMH_ULOCK(p);

	/*
	 * XXX If you don't set BREPLST_VALID, this logic is not really used.
 	 * The only information that might be useful is the policy perhaps.
 	 */
	FCMH_LOCK(c);
	fcmh_2_replpol(c) = pol;
	fcmh_2_ino(c)->ino_nrepls = nr;
	memcpy(fcmh_2_ino(c)->ino_repls, repls, sizeof(repls[0]) *
	    SL_DEF_REPLICAS);
	if (nr > SL_DEF_REPLICAS) {
		mds_inox_ensure_loaded(fcmh_2_inoh(c));
		memcpy(fcmh_2_inox(c)->inox_repls,
		    &repls[SL_DEF_REPLICAS], sizeof(repls[0]) *
		    SL_INOX_NREPLICAS);
	}
	rc = mds_inodes_odsync(vfsid, c, mdslog_ino_repls);
	FCMH_ULOCK(c);
	return (rc);
}

#if PFL_DEBUG > 0
void
dump_fcmh_flags(int flags)
{
	int seq = 0;

	_dump_fcmh_flags_common(&flags, &seq);
	PFL_PRFLAG(FCMH_MDS_IN_PTRUNC, &flags, &seq);
	if (flags)
		printf(" unknown: %#x", flags);
	printf("\n");
}
#endif

struct sl_fcmh_ops sl_fcmh_ops = {
	slm_fcmh_ctor,		/* sfop_ctor */
	slm_fcmh_dtor,		/* sfop_dtor */
	NULL,			/* sfop_getattr */
	NULL			/* sfop_reopen */
};
