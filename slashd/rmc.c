/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2007-2013, Pittsburgh Supercomputing Center (PSC).
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

/*
 * Routines for handling RPC requests for MDS from CLIENT.
 */

#define PSC_SUBSYS PSS_RPC

#include <sys/param.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/statvfs.h>

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <unistd.h>

#include "pfl/ctlsvr.h"
#include "pfl/export.h"
#include "pfl/fs.h"
#include "pfl/lock.h"
#include "pfl/rpc.h"
#include "pfl/rpclog.h"
#include "pfl/rsx.h"
#include "pfl/service.h"
#include "pfl/str.h"
#include "pfl/time.h"

#include "authbuf.h"
#include "bmap_mds.h"
#include "fidc_mds.h"
#include "fidcache.h"
#include "journal_mds.h"
#include "mdsio.h"
#include "mkfn.h"
#include "pathnames.h"
#include "repl_mds.h"
#include "rpc_mds.h"
#include "slashd.h"
#include "slashrpc.h"
#include "slerr.h"
#include "slutil.h"
#include "up_sched_res.h"

#include "lib/libsolkerncompat/include/errno_compat.h"
#include "zfs-fuse/zfs_slashlib.h"

uint64_t		slm_next_fid = UINT64_MAX;
psc_spinlock_t		slm_fid_lock = SPINLOCK_INIT;

extern struct psc_hashtbl rootHtable;

void *
slm_rmc_search_roots(char *name)
{
	void *p;

	p = psc_hashtbl_search(&rootHtable, NULL, NULL, name);
	return (p);
}

slfid_t
slm_get_curr_slashfid(void)
{
	slfid_t fid;

	spinlock(&slm_fid_lock);
	fid = slm_next_fid;
	freelock(&slm_fid_lock);
	return (fid);
}

void
slm_set_curr_slashfid(slfid_t slfid)
{
	spinlock(&slm_fid_lock);
	slm_next_fid = slfid;
	freelock(&slm_fid_lock);
}

/**
 * slm_get_next_slashfid - Return the next SLASH FID to use.  Note that
 *	from ZFS point of view, it is perfectly okay that we use the
 *	same SLASH FID to refer to different files/directories.
 *	However, doing so can confuse our clients (think identity
 *	theft).  So we must make sure that we never reuse a SLASH FID,
 *	even after a crash.
 */
int
slm_get_next_slashfid(slfid_t *fidp)
{
	uint64_t fid;

	spinlock(&slm_fid_lock);
	/*
	 * This should never happen.  If it does, we crash to let the
	 * sys admin know.  He could fix this if there are still room in
	 * the cycle bits.  We have to let sys admin know, otherwise,
	 * he/she does not know how to bump the cycle bits.
	 */
	if (FID_GET_INUM(slm_next_fid) >= FID_MAX_INUM) {
		psclog_warnx("Max FID "SLPRI_FID" reached, manual "
		    "intervention needed", slm_next_fid);
		freelock(&slm_fid_lock);
		return (ENOSPC);
	}
	fid = slm_next_fid++;
	freelock(&slm_fid_lock);

	psclog_info("next FID "SLPRI_FID, fid);
	* fidp = fid;
	return (0);
}

int
slm_rmc_handle_ping(struct pscrpc_request *rq)
{
	const struct srm_ping_req *mq;
	struct srm_ping_rep *mp;

	SL_RSX_ALLOCREP(rq, mq, mp);
	return (0);
}

int
slm_rmc_handle_getattr(struct pscrpc_request *rq)
{
	const struct srm_getattr_req *mq;
	struct srm_getattr_rep *mp;
	struct fidc_membh *f;
	size_t xlen;
	int vfsid;

	OPSTAT_INCR(SLM_OPST_GETATTR);
	SL_RSX_ALLOCREP(rq, mq, mp);
	mp->rc = -slm_fcmh_get(&mq->fg, &f);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);
	mp->rc = slfid_to_vfsid(mq->fg.fg_fid, &vfsid);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	/*
	 * XXX we can cut this out and just lie and return
	 * xattrsize with a nonzero value.
	 */
	zfsslash2_listxattr(vfsid, &rootcreds, NULL, 0, &xlen,
	    fcmh_2_mfid(f));

	FCMH_LOCK(f);
	mp->attr = f->fcmh_sstb;
	mp->xattrsize = xlen;

 out:
	if (f)
		fcmh_op_done(f);
	return (0);
}

/**
 * slm_rmc_handle_bmap_chwrmode - Handle a BMAPCHWRMODE request to
 *	upgrade a client bmap lease from READ-only to READ+WRITE.
 * @rq: RPC request.
 */
int
slm_rmc_handle_bmap_chwrmode(struct pscrpc_request *rq)
{
	struct bmap_mds_lease *bml = NULL;
	struct srm_bmap_chwrmode_req *mq;
	struct srm_bmap_chwrmode_rep *mp;
	struct fidc_membh *f = NULL;
	struct bmapc_memb *b = NULL;
	struct bmap_mds_info *bmi;

	OPSTAT_INCR(SLM_OPST_BMAP_CHWRMODE);
	SL_RSX_ALLOCREP(rq, mq, mp);
	mp->rc = -slm_fcmh_get(&mq->sbd.sbd_fg, &f);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);
	mp->rc = bmap_lookup(f, mq->sbd.sbd_bmapno, &b);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	bmi = bmap_2_bmi(b);

	BMAP_LOCK(b);
	bml = mds_bmap_getbml_locked(b, mq->sbd.sbd_seq,
	    mq->sbd.sbd_nid, mq->sbd.sbd_pid);

	if (bml == NULL)
		PFL_GOTOERR(out, mp->rc = -EINVAL);

	mp->rc = mds_bmap_bml_chwrmode(bml, mq->prefios[0]);
	if (mp->rc == -PFLERR_ALREADY)
		mp->rc = 0;
	else if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	mp->sbd = mq->sbd;
	mp->sbd.sbd_seq = bml->bml_seq;
	mp->sbd.sbd_key = bmi->bmi_assign->odtr_key;

	psc_assert(bmi->bmi_wr_ion);
	mp->sbd.sbd_ios = rmmi2resm(bmi->bmi_wr_ion)->resm_res_id;

 out:
	if (bml)
		mds_bmap_bml_release(bml);
	if (b)
		bmap_op_done(b);
	if (f)
		fcmh_op_done(f);
	return (0);
}

int
slm_rmc_handle_extendbmapls(struct pscrpc_request *rq)
{
	struct srm_leasebmapext_req *mq;
	struct srm_leasebmapext_rep *mp;
	struct fidc_membh *f;

	OPSTAT_INCR(SLM_OPST_LEASE_RENEW);
	SL_RSX_ALLOCREP(rq, mq, mp);

	mp->rc = -slm_fcmh_get(&mq->sbd.sbd_fg, &f);
	if (mp->rc)
		return (0);

	mp->rc = mds_lease_renew(f, &mq->sbd, &mp->sbd, rq->rq_export);
	fcmh_op_done(f);
	return (0);
}

int
slm_rmc_handle_reassignbmapls(struct pscrpc_request *rq)
{
	struct srm_reassignbmap_req *mq;
	struct srm_reassignbmap_rep *mp;
	struct fidc_membh *f;

	SL_RSX_ALLOCREP(rq, mq, mp);

	mp->rc = -slm_fcmh_get(&mq->sbd.sbd_fg, &f);
	if (mp->rc)
		return (0);

	mp->rc = mds_lease_reassign(f, &mq->sbd, mq->pios,
	    mq->prev_sliods, mq->nreassigns, &mp->sbd, rq->rq_export);

	fcmh_op_done(f);
	return (0);
}

void
slm_pack_inode(struct fidc_membh *f, struct srt_inode *in)
{
	struct slash_inode_handle *ih;
	int rc;

	ih = fcmh_2_inoh(f);
	in->newreplpol = ih->inoh_ino.ino_replpol;
	in->nrepls = ih->inoh_ino.ino_nrepls;
	memcpy(in->reptbl, &ih->inoh_ino.ino_repls,
	    sizeof(ih->inoh_ino.ino_repls));

	if (in->nrepls > SL_DEF_REPLICAS) {
		rc = mds_inox_ensure_loaded(ih);
		if (!rc)
			memcpy(&in->reptbl[SL_DEF_REPLICAS],
			    &ih->inoh_extras->inox_repls,
			    sizeof(ih->inoh_extras->inox_repls));
	}
}

int
slm_rmc_handle_getinode(struct pscrpc_request *rq)
{
	const struct srm_get_inode_req *mq;
	struct srm_get_inode_rep *mp;
	struct fidc_membh *f;

	SL_RSX_ALLOCREP(rq, mq, mp);
	mp->rc = -slm_fcmh_get(&mq->fg, &f);
	if (mp->rc)
		return (0);
	slm_pack_inode(f, &mp->ino);
	fcmh_op_done(f);
	return (0);
}

int
slm_rmc_handle_getbmap(struct pscrpc_request *rq)
{
	const struct srm_leasebmap_req *mq;
	struct srm_leasebmap_rep *mp;
	struct fidc_membh *f;
	int rc = 0;

	SL_RSX_ALLOCREP(rq, mq, mp);

	if (mq->rw != SL_WRITE)
		OPSTAT_INCR(SLM_OPST_GET_BMAP_LEASE_READ);
	else
		OPSTAT_INCR(SLM_OPST_GET_BMAP_LEASE_WRITE);
	if (mq->rw != SL_READ && mq->rw != SL_WRITE) {
		mp->rc = -EINVAL;
		return (0);
	}

	mp->rc = -slm_fcmh_get(&mq->fg, &f);
	if (mp->rc)
		return (0);
	mp->flags = mq->flags;

	mp->rc = mds_bmap_load_cli(f, mq->bmapno, mq->flags, mq->rw,
	    mq->prefios[0], &mp->sbd, rq->rq_export, mp->repls, 0);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	if (mp->flags & SRM_LEASEBMAPF_GETINODE)
		slm_pack_inode(f, &mp->ino);

 out:
	fcmh_op_done(f);
	return (rc ? rc : mp->rc);
}

int
slm_rmc_handle_link(struct pscrpc_request *rq)
{
	struct fidc_membh *p = NULL, *c = NULL;
	struct srm_link_req *mq;
	struct srm_link_rep *mp;
	int vfsid;

	OPSTAT_INCR(SLM_OPST_LINK);
	SL_RSX_ALLOCREP(rq, mq, mp);
	mp->rc = slfid_to_vfsid(mq->fg.fg_fid, &vfsid);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	mp->rc = -slm_fcmh_get(&mq->fg, &c);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);
	mp->rc = -slm_fcmh_get(&mq->pfg, &p);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	mq->name[sizeof(mq->name) - 1] = '\0';
	mds_reserve_slot(1);
	mp->rc = mdsio_link(vfsid, fcmh_2_mfid(c),
	    fcmh_2_mfid(p), mq->name, &rootcreds, &mp->cattr,
	    mdslog_namespace);
	mds_unreserve_slot(1);

	mdsio_fcmh_refreshattr(p, &mp->pattr);

 out:
	if (c)
		fcmh_op_done(c);
	if (p)
		fcmh_op_done(p);
	return (0);
}

int
slm_rmc_handle_lookup(struct pscrpc_request *rq)
{
	struct srm_lookup_req *mq;
	struct srm_lookup_rep *mp;
	struct fidc_membh *p = NULL;
	int vfsid;

	OPSTAT_INCR(SLM_OPST_LOOKUP);
	SL_RSX_ALLOCREP(rq, mq, mp);
	mp->rc = slfid_to_vfsid(mq->pfg.fg_fid, &vfsid);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);
	mp->rc = -slm_fcmh_get(&mq->pfg, &p);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	mq->name[sizeof(mq->name) - 1] = '\0';
	if (fcmh_2_mfid(p) == SLFID_ROOT &&
	    strcmp(mq->name, SL_RPATH_META_DIR) == 0)
		PFL_GOTOERR(out, mp->rc = -EINVAL);
	mp->rc = mdsio_lookup(vfsid, fcmh_2_mfid(p), mq->name,
	    NULL, &rootcreds, &mp->attr);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	if (mq->pfg.fg_fid == SLFID_ROOT) {
		mount_info_t *mountinfo;
		struct srt_stat tmpattr;
		struct mio_rootnames *rn;
		uint64_t fid;
		int error;

		rn = slm_rmc_search_roots(mq->name);
		if (rn) {
			mountinfo = &zfsMount[rn->rn_vfsid];
			fid = SLFID_ROOT;
			FID_SET_SITEID(fid, mountinfo->siteid);

			error = mdsio_getattr(rn->rn_vfsid,
			    mountinfo->rootid, mountinfo->rootinfo,
			    &rootcreds, &tmpattr);
			if (!error) {
				tmpattr.sst_fg.fg_fid = fid;
				mp->attr = tmpattr;
			} else
				/* better than nothing */
				mp->attr.sst_fg.fg_fid = fid;
		}
	}

 out:
	if (p)
		fcmh_op_done(p);
	return (0);
}

int
slm_mkdir(int vfsid, struct srm_mkdir_req *mq, struct srm_mkdir_rep *mp,
    int opflags, struct fidc_membh **dp)
{
	struct fidc_membh *p = NULL, *c = NULL;
	slfid_t fid = 0;

	mq->name[sizeof(mq->name) - 1] = '\0';
	if (IS_REMOTE_FID(mq->pfg.fg_fid)) {
		struct slash_creds cr;

		cr.scr_uid = mq->sstb.sst_uid;
		cr.scr_gid = mq->sstb.sst_gid;
		/* XXX pass opflags */
		mp->rc = slm_rmm_forward_namespace(SLM_FORWARD_MKDIR,
		    &mq->pfg, NULL, mq->name, NULL, mq->sstb.sst_mode,
		    &cr, &mp->cattr, 0);
		if (mp->rc)
			PFL_GOTOERR(out, mp->rc);
		fid = mp->cattr.sst_fg.fg_fid;
	}

	mp->rc = -slm_fcmh_get(&mq->pfg, &p);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	mds_reserve_slot(1);
	mp->rc = -mdsio_mkdir(vfsid, fcmh_2_mfid(p), mq->name,
	    &mq->sstb, 0, opflags, &mp->cattr, NULL, fid ? NULL :
	    mdslog_namespace, fid ? 0 : slm_get_next_slashfid, fid);
	mds_unreserve_slot(1);

 out:
	if (p)
		mdsio_fcmh_refreshattr(p, &mp->pattr);

	/*
	 * Set new subdir's new files' default replication policy from
	 * parent dir.
	 */
	if (mp->rc == 0 && slm_fcmh_get(&mp->cattr.sst_fg, &c) == 0)
		slm_fcmh_endow(vfsid, p, c);

	if (dp) {
		if (mp->rc == -EEXIST &&
		    mdsio_lookup(vfsid, fcmh_2_mfid(p), mq->name, NULL,
		    &rootcreds, &mp->cattr) == 0)
			slm_fcmh_get(&mp->cattr.sst_fg, &c);
		*dp = c;
		c = NULL;
	}
	if (p)
		fcmh_op_done(p);
	if (c)
		fcmh_op_done(c);
	return (0);
}

int
slm_rmc_handle_mkdir(struct pscrpc_request *rq)
{
	struct srm_mkdir_req *mq;
	struct srm_mkdir_rep *mp;
	int vfsid;

	OPSTAT_INCR(SLM_OPST_MKDIR);
	SL_RSX_ALLOCREP(rq, mq, mp);
	mp->rc = slfid_to_vfsid(mq->pfg.fg_fid, &vfsid);
	if (mp->rc)
		return (0);
	return (slm_mkdir(vfsid, mq, mp, 0, NULL));
}

int
slm_rmc_handle_mknod(struct pscrpc_request *rq)
{
	struct fidc_membh *p = NULL;
	struct srm_mknod_req *mq;
	struct srm_mknod_rep *mp;
	struct slash_creds cr;
	int vfsid;

	OPSTAT_INCR(SLM_OPST_MKNOD);
	SL_RSX_ALLOCREP(rq, mq, mp);
	mp->rc = slfid_to_vfsid(mq->pfg.fg_fid, &vfsid);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	mp->rc = -slm_fcmh_get(&mq->pfg, &p);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	mq->name[sizeof(mq->name) - 1] = '\0';
	mds_reserve_slot(1);
	cr.scr_uid = mq->creds.scr_uid;
	cr.scr_gid = mq->creds.scr_gid;
	mp->rc = mdsio_mknod(vfsid, fcmh_2_mfid(p), mq->name,
	    mq->mode, &cr, &mp->cattr, NULL, mdslog_namespace,
	    slm_get_next_slashfid);
	mds_unreserve_slot(1);

	mdsio_fcmh_refreshattr(p, &mp->pattr);
 out:
	if (p)
		fcmh_op_done(p);
	return (0);
}

/**
 * slm_rmc_handle_create - Handle a CREATE from CLI.  As an
 *	optimization, we bundle a write lease for bmap 0 in the reply.
 */
int
slm_rmc_handle_create(struct pscrpc_request *rq)
{
	struct fidc_membh *p = NULL, *c;
	struct srm_create_rep *mp;
	struct srm_create_req *mq;
	struct slash_creds cr;
	slfid_t fid = 0;
	void *mfh;
	int vfsid;

	OPSTAT_INCR(SLM_OPST_CREATE);
	SL_RSX_ALLOCREP(rq, mq, mp);
	mp->rc = slfid_to_vfsid(mq->pfg.fg_fid, &vfsid);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);
	if (mq->flags & SRM_LEASEBMAPF_GETINODE)
		PFL_GOTOERR(out, mp->rc = -EINVAL);

	mq->name[sizeof(mq->name) - 1] = '\0';

	cr.scr_uid = mq->creds.scr_uid;
	cr.scr_gid = mq->creds.scr_gid;

	if (IS_REMOTE_FID(mq->pfg.fg_fid)) {
		mp->rc = slm_rmm_forward_namespace(SLM_FORWARD_CREATE,
		    &mq->pfg, NULL, mq->name, NULL, mq->mode, &cr,
		    &mp->cattr, 0);
		if (mp->rc)
			PFL_GOTOERR(out, mp->rc);
		fid = mp->cattr.sst_fg.fg_fid;
	}

	/*
	 * Lookup the parent directory in the cache so that the SLASH2
	 * ino can be translated into the inode for the underlying fs.
	 */
	mp->rc = -slm_fcmh_get(&mq->pfg, &p);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	DEBUG_FCMH(PLL_DEBUG, p, "create op start for %s", mq->name);

	mds_reserve_slot(1);
	mp->rc = mdsio_opencreate(vfsid, fcmh_2_mfid(p), &cr,
	    O_CREAT | O_EXCL | O_RDWR, mq->mode, mq->name, NULL,
	    &mp->cattr, &mfh, fid ? NULL : mdslog_namespace,
	    fid ? 0 : slm_get_next_slashfid, fid);
	mds_unreserve_slot(1);

	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	/*
	 * Refresh the cached attributes of our parent and pack them in
	 * the reply.
	 */
	mdsio_fcmh_refreshattr(p, &mp->pattr);

	DEBUG_FCMH(PLL_DEBUG, p, "create op done for %s", mq->name);
	/*
	 * XXX enter this into the fcmh cache instead of doing it again
	 * This release may be the sanest thing actually, unless EXCL is
	 * used.
	 */
	mdsio_release(vfsid, &rootcreds, mfh);

	DEBUG_FCMH(PLL_DEBUG, p, "mdsio_release() done for %s",
	    mq->name);

	if (fid)
		PFL_GOTOERR(out, mp->rc2 = ENOENT);

	mp->rc = -slm_fcmh_get(&mp->cattr.sst_fg, &c);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	slm_fcmh_endow_nolog(vfsid, p, c);

	/* obtain lease for first bmap as optimization */
	mp->flags = mq->flags;

	mp->rc2 = mds_bmap_load_cli(c, 0, mp->flags, SL_WRITE,
	    mq->prefios[0], &mp->sbd, rq->rq_export, NULL, 1);

	fcmh_op_done(c);

	if (mp->rc2)
		PFL_GOTOERR(out, mp->rc2);

 out:
	if (p)
		fcmh_op_done(p);
	return (0);
}

void
slm_rmc_handle_readdir_roots(struct iovec *iov0, struct iovec *iov1,
    size_t nents)
{
	struct srt_stat tmpattr, *attr;
	struct pscfs_dirent *dirent;
	struct mio_rootnames *rn;
	mount_info_t *mountinfo;
	size_t i, entsize;
	uint64_t fid;
	int error;

	attr = iov1->iov_base;
	dirent = iov0->iov_base;
	for (i = 0; i < nents; i++) {

		rn = slm_rmc_search_roots(dirent->pfd_name);
		if (rn) {
			mountinfo = &zfsMount[rn->rn_vfsid];
			fid = SLFID_ROOT;
			FID_SET_SITEID(fid, mountinfo->siteid);
			dirent->pfd_ino = fid;

			error = mdsio_getattr(rn->rn_vfsid,
			    mountinfo->rootid, mountinfo->rootinfo,
			    &rootcreds, &tmpattr);
			if (!error) {
				tmpattr.sst_fg.fg_fid = fid;
				*attr = tmpattr;
			} else
				/* better than nothing */
				attr->sst_fg.fg_fid = fid;
		}
		attr++;
		entsize = PFL_DIRENT_SIZE(dirent->pfd_namelen);
		dirent = PSC_AGP(dirent, entsize);
	}
}

#define RCM_READDIR_CBARGP_CSVC		0
#define RCM_READDIR_CBARGP_EXP		1
#define RCM_READDIR_CBARGP_BASE_DENTS	2
#define RCM_READDIR_CBARGP_BASE_ATTR	3

#define RCM_READDIR_CBARGI_NEXTOFF	0
#define RCM_READDIR_CBARGI_DECR		1

int  slm_rcmc_readdir_cb(struct pscrpc_request *, struct pscrpc_async_args *);
void slm_rcm_try_readdir_ra(struct pscrpc_export *, struct sl_fidgen *, int, off_t, size_t);
int  slm_readdir_issue(struct pscrpc_export *, struct sl_fidgen *,
	size_t, off_t, size_t *, int *, int *, unsigned char *, size_t,
	int);

/**
 * Special case routine for processing READDIR request (non readahead).
 */
int
slm_rcm_issue_readdir_wk(void *p)
{
	struct slm_wkdata_readdir *wk = p;
	struct srm_readdir_ra_req *mq;
	struct srm_readdir_ra_rep *mp;
	struct pscrpc_request *rq;
	int rc;

	rc = SL_RSX_NEWREQ(wk->csvc, SRMT_READDIR, rq, mq, mp);
	if (rc) {
		sl_csvc_decref(wk->csvc);
		goto out;
	}
	mq->fg = wk->fg;
	mq->offset = wk->off;
	mq->size = wk->size;
	mq->eof = wk->eof;
	mq->num = wk->num;

	rq->rq_interpret_reply = slm_rcmc_readdir_cb;
	rq->rq_async_args.pointer_arg[RCM_READDIR_CBARGP_CSVC] = wk->csvc;
	rq->rq_async_args.pointer_arg[RCM_READDIR_CBARGP_BASE_DENTS] = wk->iov[0].iov_base;
	rq->rq_async_args.pointer_arg[RCM_READDIR_CBARGP_BASE_ATTR] = wk->iov[1].iov_base;
	rq->rq_async_args.pointer_arg[RCM_READDIR_CBARGP_EXP] =
	    pscrpc_export_get(wk->exp);
	rq->rq_async_args.space[RCM_READDIR_CBARGI_NEXTOFF] = wk->nextoff;
	rq->rq_async_args.space[RCM_READDIR_CBARGI_DECR] = wk->ra;
	if (wk->iov[0].iov_len)
		rc = slrpc_bulkclient(rq, BULK_GET_SOURCE,
		    SRCM_BULK_PORTAL, wk->iov, nitems(wk->iov));
	else
		psc_assert(wk->eof);
	if (rc == 0)
		rc = SL_NBRQSET_ADD(wk->csvc, rq);
	if (rc) {
		pscrpc_req_finished(rq);
		sl_csvc_decref(wk->csvc);
		pscrpc_export_put(wk->exp);
	} else {
		slm_rcm_try_readdir_ra(wk->exp, &wk->fg, wk->eof,
		    wk->nextoff, wk->size);
	}

 out:
	if (rc) {
		PSCFREE(wk->iov[0].iov_base);
		PSCFREE(wk->iov[1].iov_base);
	}
	pscrpc_export_put(wk->exp);
	return (0);
}

/**
 * Special interface routine for issuing a READDIR bulk reply from work
 * context.
 */
int
slm_readdir_ra_issue(void *p)
{
	struct slm_wkdata_readdir *wk = p;
	size_t outsize;
	int nents, eof;

	slm_readdir_issue(wk->exp, &wk->fg, wk->size, wk->off, &outsize,
	    &nents, &eof, NULL, 0, 1);
	pscrpc_export_put(wk->exp);
	return (0);
}

/**
 * Determine if another READDIR (for readahead) should be done and setup
 * an async RPC to get it moving.
 *
 * This routine is called in a number of places:
 *  (o) after a READDIR completes, immediately schedule a readahead if
 *	within limits;
 *  (o) when client acknowledges READDIR received;
 */
void
slm_rcm_try_readdir_ra(struct pscrpc_export *exp, struct sl_fidgen *fgp,
    int eof, off_t off, size_t size)
{
#define READDIR_RA_PAST_TIMEO 3
	struct timespec now, expire = { READDIR_RA_PAST_TIMEO, 0 };
	struct slm_readdir_ra_past *inact, *act = NULL, *cv;
	struct slm_wkdata_readdir *wk;
	struct slm_exp_cli *mexpc;
	int i;

	if (eof)
		return;

	EXPORT_LOCK(exp);
	PFL_GETTIMESPEC(&now);
	mexpc = sl_exp_getpri_cli(exp);
	for (i = 0, cv = mexpc->mexpc_readdir_past;
	    i < nitems(mexpc->mexpc_readdir_past) || (cv = NULL);
	    i++, cv++) {
		/* expire old entries */
		if (timespeccmp(&cv->crap_exp, &now, <))
			cv->crap_fid = FID_ANY;

		/*
		 * Found an inactive slot; save in case we can't find an
		 * empty slot.
		 */
		if (!CRAP_GET_ACTIVE(cv))
			inact = cv;

		/* found an unused entry; use it */
		if (cv->crap_fid == FID_ANY)
			act = cv;

		/* found our request; cancel */
		if (cv->crap_fid == fgp->fg_fid &&
		    cv->crap_off == off)
			break;
	}
	/* use an inactive slot if we must */
	if (!act && inact)
		act = inact;
	if (cv || !act) {
		EXPORT_ULOCK(exp);
		return;
	}
	act->crap_fid = fgp->fg_fid;
	act->crap_off = off;
	timespecadd(&now, &expire, &act->crap_exp);
	CRAP_SET_ACTIVE(act);
	EXPORT_ULOCK(exp);

	wk = pfl_workq_getitem(slm_readdir_ra_issue,
	    struct slm_wkdata_readdir);
	wk->exp = pscrpc_export_get(exp);
	wk->fg = *fgp;
	wk->off = off;
	wk->size = size;
	pfl_workq_putitem(wk);
}

/**
 * Callback run signifying client completion of receiving a bulk READDIR.
 */
int
slm_rcmc_readdir_cb(struct pscrpc_request *rq,
    struct pscrpc_async_args *av)
{
	int decr = av->space[RCM_READDIR_CBARGI_DECR];
	off_t nextoff = av->space[RCM_READDIR_CBARGI_NEXTOFF];
	void *base_attr = av->pointer_arg[RCM_READDIR_CBARGP_BASE_ATTR];
	void *base_dents = av->pointer_arg[RCM_READDIR_CBARGP_BASE_DENTS];
	struct slashrpc_cservice *csvc = av->pointer_arg[RCM_READDIR_CBARGP_CSVC];
	struct pscrpc_export *exp = av->pointer_arg[RCM_READDIR_CBARGP_EXP];
	struct slm_readdir_ra_past *crap;
	struct srm_readdir_ra_req *mq;
	struct slm_exp_cli *mexpc;
	int i, rc;

	SL_GET_RQ_STATUS_TYPE(csvc, rq, struct srm_readdir_ra_rep, rc);
	if (rc == 0) {
		mq = pscrpc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq));
		slrpc_rep_in(csvc, rq);
	}

	if (decr) {
		EXPORT_LOCK(exp);
		mexpc = sl_exp_getpri_cli(exp);
		for (i = 0, crap = mexpc->mexpc_readdir_past;
		    i < nitems(mexpc->mexpc_readdir_past); i++, crap++)
			if (crap->crap_fid == mq->fg.fg_fid &&
			    crap->crap_off == mq->offset) {
				CRAP_CLR_ACTIVE(crap);
				break;
			}
		EXPORT_ULOCK(exp);
	}

	if (rc == 0)
		slm_rcm_try_readdir_ra(exp, &mq->fg, mq->eof,
		    nextoff, mq->size);

	PSCFREE(base_dents);
	PSCFREE(base_attr);
	sl_csvc_decref(csvc);
	return (0);
}

/**
 * Perform a READDIR and setup an async RPC to send it to a client.
 */
int
slm_readdir_issue(struct pscrpc_export *exp, struct sl_fidgen *fgp,
    size_t size, off_t off, size_t *outsize, int *nents, int *eof,
    unsigned char *piggybuf, size_t piggysize, int ra)
{
	struct fidc_membh *f = NULL;
	struct iovec iov[2];
	off_t nextoff;
	int rc, vfsid;

	memset(iov, 0, sizeof(iov));

	rc = slfid_to_vfsid(fgp->fg_fid, &vfsid);
	if (rc)
		PFL_GOTOERR(out, rc);

	rc = -slm_fcmh_get(fgp, &f);
	if (rc)
		PFL_GOTOERR(out, rc);

	iov[0].iov_base = PSCALLOC(size);

	/*
	 * Ensure things are populated under the root before handling
	 * subdirs.
	 */
	if (fgp->fg_fid == SLFID_ROOT)
		psc_scan_filesystems();

	rc = mdsio_readdir(vfsid, &rootcreds, size, off,
	    iov[0].iov_base, outsize, nents, &iov[1], eof, &nextoff,
	    fcmh_2_mfh(f));
	if (rc)
		PFL_GOTOERR(out, rc);

	iov[0].iov_len = *outsize;

	/*
	 * If this is a request for the root, we fake part of the
	 * readdir contents by returning the file system names here.
	 */
	if (fgp->fg_fid == SLFID_ROOT)
		slm_rmc_handle_readdir_roots(&iov[0], &iov[1], *nents);

	if (piggybuf &&
	    SRM_READDIR_BUFSZ(*outsize, *nents) <= piggysize) {
		memcpy(piggybuf, iov[0].iov_base, *outsize);
		memcpy(piggybuf + *outsize, iov[1].iov_base,
		    *nents * sizeof(struct srt_readdir_ent));
	} else {
		struct slm_wkdata_readdir *wk;

		wk = pfl_workq_getitem(slm_rcm_issue_readdir_wk,
		    struct slm_wkdata_readdir);
		wk->exp = pscrpc_export_get(exp);
		wk->csvc = slm_getclcsvc(exp);
		wk->fg = *fgp;
		wk->off = off;
		wk->size = *outsize;
		wk->nextoff = nextoff;
		wk->num = *nents;
		wk->ra = ra;
		memcpy(wk->iov, iov, sizeof(iov));
		wk->eof = *eof;
		iov[0].iov_base = NULL;
		iov[1].iov_base = NULL;
		pfl_workq_putitem(wk);

		slm_rcm_try_readdir_ra(exp, fgp, *eof, nextoff,
		    size);
	}

 out:
	PSCFREE(iov[0].iov_base);
	PSCFREE(iov[1].iov_base);
	if (f)
		fcmh_op_done(f);
	return (rc);
}

int
slm_rmc_handle_readdir(struct pscrpc_request *rq)
{
	struct srm_readdir_req *mq;
	struct srm_readdir_rep *mp;
	int eof, num;

	OPSTAT_INCR(SLM_OPST_READDIR);

	SL_RSX_ALLOCREP(rq, mq, mp);
	if (mq->size > LNET_MTU)
		PFL_GOTOERR(out, mp->rc = -EINVAL);

	/*
	 * XXX Check if currently being processed (on workq) and wait.
	 */

	mp->rc = slm_readdir_issue(rq->rq_export, &mq->fg, mq->size,
	    mq->offset, &mp->size, &num, &eof, mp->ents,
	    sizeof(mp->ents), 0);
	mp->num = num;
	mp->eof = eof;

 out:
	return (mp->rc);
}

int
slm_rmc_handle_readlink(struct pscrpc_request *rq)
{
	struct srm_readlink_req *mq;
	struct srm_readlink_rep *mp;
	struct fidc_membh *f = NULL;
	struct iovec iov;
	char buf[SL_PATH_MAX];
	int vfsid;

	OPSTAT_INCR(SLM_OPST_READLINK);
	SL_RSX_ALLOCREP(rq, mq, mp);
	mp->rc = slfid_to_vfsid(mq->fg.fg_fid, &vfsid);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	mp->rc = -slm_fcmh_get(&mq->fg, &f);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	mp->rc = mdsio_readlink(vfsid, fcmh_2_mfid(f), buf, &rootcreds);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	iov.iov_base = buf;
	iov.iov_len = sizeof(buf);
	mp->rc = slrpc_bulkserver(rq, BULK_PUT_SOURCE, SRMC_BULK_PORTAL,
	    &iov, 1);

 out:
	if (f)
		fcmh_op_done(f);
	return (mp->rc);
}

int
slm_rmc_handle_rls_bmap(struct pscrpc_request *rq)
{
	OPSTAT_INCR(SLM_OPST_BMAP_RELEASE);
	return (mds_handle_rls_bmap(rq, 0));
}

int
slm_rmc_handle_rename(struct pscrpc_request *rq)
{
	char from[SL_NAME_MAX + 1], to[SL_NAME_MAX + 1];
	struct fidc_membh *c, *op = NULL, *np = NULL;
	struct srm_rename_req *mq;
	struct srm_rename_rep *mp;
	struct slash_fidgen chfg[2];
	struct iovec iov[2];
	int vfsid;

	chfg[0].fg_fid = FID_ANY;
	chfg[1].fg_fid = FID_ANY;

	OPSTAT_INCR(SLM_OPST_RENAME);
	SL_RSX_ALLOCREP(rq, mq, mp);
	mp->rc = slfid_to_vfsid(mq->opfg.fg_fid, &vfsid);
	if (mp->rc)
		return (mp->rc);

	if (mq->fromlen == 0 || mq->tolen == 0 ||
	    mq->fromlen > SL_NAME_MAX ||
	    mq->tolen   > SL_NAME_MAX)
		return (mp->rc = -EINVAL);

	if (FID_GET_SITEID(mq->opfg.fg_fid) !=
	    FID_GET_SITEID(mq->npfg.fg_fid))
		return (mp->rc = -EXDEV);

	if (mq->fromlen + mq->tolen > SRM_RENAME_NAMEMAX) {
		iov[0].iov_base = from;
		iov[0].iov_len = mq->fromlen;
		iov[1].iov_base = to;
		iov[1].iov_len = mq->tolen;
		mp->rc = slrpc_bulkserver(rq, BULK_GET_SINK,
		    SRMC_BULK_PORTAL, iov, 2);
		if (mp->rc)
			return (mp->rc);
	} else {
		memcpy(from, mq->buf, mq->fromlen);
		memcpy(to, mq->buf + mq->fromlen, mq->tolen);
	}

	from[mq->fromlen] = '\0';
	to[mq->tolen]     = '\0';

	if (IS_REMOTE_FID(mq->opfg.fg_fid)) {
		mp->rc = slm_rmm_forward_namespace(SLM_FORWARD_RENAME,
		    &mq->opfg, &mq->npfg, from, to, 0, &rootcreds,
		    &mp->srr_npattr, 0);
		if (mp->rc)
			PFL_GOTOERR(out, mp->rc);
	}

	mp->rc = -slm_fcmh_get(&mq->opfg, &op);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	if (SAMEFG(&mq->opfg, &mq->npfg)) {
		np = op;
	} else {
		mp->rc = -slm_fcmh_get(&mq->npfg, &np);
		if (mp->rc)
			PFL_GOTOERR(out, mp->rc);
	}

	/* if we get here, op and np must be owned by the current MDS */
	mds_reserve_slot(2);
	mp->rc = mdsio_rename(vfsid, fcmh_2_mfid(op), from,
	    fcmh_2_mfid(np), to, &rootcreds, mdslog_namespace,
	    chfg);
	mds_unreserve_slot(2);

 out:
	if (mp->rc == 0) {
		mdsio_fcmh_refreshattr(op, &mp->srr_opattr);
		if (op != np)
			mdsio_fcmh_refreshattr(np, &mp->srr_npattr);

		if (chfg[0].fg_fid != FID_ANY &&
		    slm_fcmh_get(&chfg[0], &c) == 0) {
			mdsio_fcmh_refreshattr(c,
			    &mp->srr_cattr);
			fcmh_op_done(c);
		} else
			mp->srr_cattr.sst_fid = FID_ANY;

		if (chfg[1].fg_fid != FID_ANY &&
		    slm_fcmh_get(&chfg[1], &c) == 0) {
			mdsio_fcmh_refreshattr(c,
			    &mp->srr_clattr);
			fcmh_op_done(c);
		} else
			mp->srr_clattr.sst_fid = FID_ANY;
	}

	if (np)
		fcmh_op_done(np);
	if (op && op != np)
		fcmh_op_done(op);
	return (0);
}

int
slm_rmc_handle_setattr(struct pscrpc_request *rq)
{
	int to_set, flush, tadj = 0, unbump = 0;
	struct slashrpc_cservice *csvc;
	struct fidc_membh *f = NULL;
	struct srm_setattr_req *mq;
	struct srm_setattr_rep *mp;
	uint32_t i;
	int vfsid;

	OPSTAT_INCR(SLM_OPST_SETATTR);
	SL_RSX_ALLOCREP(rq, mq, mp);
	mp->rc = slfid_to_vfsid(mq->attr.sst_fg.fg_fid, &vfsid);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	mp->rc = -slm_fcmh_get(&mq->attr.sst_fg, &f);
	if (mp->rc)
		return (0);

	FCMH_WAIT_BUSY(f);

	flush = mq->to_set & PSCFS_SETATTRF_FLUSH;
	to_set = mq->to_set & SL_SETATTRF_CLI_ALL;

	if (to_set & PSCFS_SETATTRF_DATASIZE) {
#if 0
		if (IS_REMOTE_FID(mq->attr.sst_fg.fg_fid)) {
			mp->rc = -PFLERR_NOSYS;
			goto out;
		}
#endif
		/* our client should really do this on its own */
		if (!(to_set & PSCFS_SETATTRF_MTIME)) {
			psclog_warnx("missing MTIME flag in RPC request");
			to_set |= PSCFS_SETATTRF_MTIME;
			PFL_GETPTIMESPEC(&mq->attr.sst_mtim);
		}
		if (mq->attr.sst_size == 0 || !fcmh_2_fsz(f)) {
			/*
			 * Full truncate.  If file size is already zero,
			 * we must still bump the generation since size
			 * updates from the sliod may be pending for
			 * this generation.
			 */
			mq->attr.sst_fg.fg_gen = fcmh_2_gen(f) + 1;
			mq->attr.sst_blocks = 0;
			for (i = 0; i < fcmh_2_nrepls(f); i++)
				fcmh_set_repl_nblks(f, i, 0);
			to_set |= SL_SETATTRF_GEN | SL_SETATTRF_NBLKS;
			unbump = 1;
		} else if (!flush) {
PFL_GOTOERR(out, mp->rc = -PFLERR_NOTSUP);

			/* partial truncate */
			if (f->fcmh_flags & FCMH_IN_PTRUNC)
				PFL_GOTOERR(out, mp->rc =
				    -SLERR_BMAP_IN_PTRUNC);
			to_set &= ~PSCFS_SETATTRF_DATASIZE;
			tadj |= PSCFS_SETATTRF_DATASIZE;
		}
	}

	if (to_set) {
		if (IS_REMOTE_FID(mq->attr.sst_fg.fg_fid)) {
			mp->rc = slm_rmm_forward_namespace(
			    SLM_FORWARD_SETATTR, &mq->attr.sst_fg, NULL,
			    NULL, NULL, 0, NULL, &mq->attr, to_set);
			if (mp->rc)
				PFL_GOTOERR(out, mp->rc);
		}
		/*
		 * If the file is open, mfh will be valid and used.
		 * Otherwise, it will be NULL, and we'll use the
		 * mio_fid.
		 */
		mp->rc = mds_fcmh_setattr(vfsid, f, to_set, &mq->attr);
	}

	if (mp->rc) {
		if (unbump)
			fcmh_2_gen(f)--;
	} else if (!flush) {
		if (tadj & PSCFS_SETATTRF_DATASIZE) {
			f->fcmh_flags |= FCMH_IN_PTRUNC;

			csvc = slm_getclcsvc(rq->rq_export);
			psc_dynarray_add(&fcmh_2_fmi(f)->
			    fmi_ptrunc_clients, csvc);

			mp->rc = -SLERR_BMAP_PTRUNC_STARTED;
		}

		slm_setattr_core(f, &mq->attr, to_set | tadj);
	}

 out:
	if (f) {
		(void)FCMH_RLOCK(f);
		if (mp->rc == 0 || mp->rc == SLERR_BMAP_PTRUNC_STARTED)
			mp->attr = f->fcmh_sstb;
		FCMH_UNBUSY(f);
		fcmh_op_done(f);
	}
	return (0);
}

int
slm_rmc_handle_set_fattr(struct pscrpc_request *rq)
{
	struct srm_set_fattr_req *mq;
	struct srm_set_fattr_rep *mp;
	struct fidc_membh *f = NULL;
	int vfsid;

	SL_RSX_ALLOCREP(rq, mq, mp);

	mp->rc = slfid_to_vfsid(mq->fg.fg_fid, &vfsid);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);
	if (vfsid != current_vfsid)
		PFL_GOTOERR(out, mp->rc = -EINVAL);

	mp->rc = -slm_fcmh_get(&mq->fg, &f);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	FCMH_LOCK(f);
	switch (mq->attrid) {
	case SL_FATTR_IOS_AFFINITY:
		if (mq->val)
			fcmh_2_ino(f)->ino_flags |= INOF_IOS_AFFINITY;
		else
			fcmh_2_ino(f)->ino_flags &= ~INOF_IOS_AFFINITY;
		break;
	case SL_FATTR_REPLPOL:
		if (mq->val < 0 || mq->val >= NBRPOL)
			mp->rc = -EINVAL;
		else
			fcmh_2_replpol(f) = mq->val;
		break;
	default:
		mp->rc = -EINVAL;
		break;
	}
	if (mp->rc == 0)
		mp->rc = mds_inode_write(vfsid, fcmh_2_inoh(f),
		    mdslog_ino_repls, f);

 out:
	if (f)
		fcmh_op_done(f);
	return (0);
}

int
slm_rmc_handle_set_bmapreplpol(struct pscrpc_request *rq)
{
	struct srm_set_bmapreplpol_req *mq;
	struct srm_set_bmapreplpol_rep *mp;
	struct fidc_membh *f;
	struct bmapc_memb *b;

	SL_RSX_ALLOCREP(rq, mq, mp);

	if (mq->pol < 0 || mq->pol >= NBRPOL) {
		mp->rc = -EINVAL;
		return (0);
	}

	mp->rc = -slm_fcmh_get(&mq->fg, &f);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	if (!mds_bmap_exists(f, mq->bmapno))
		PFL_GOTOERR(out, mp->rc = -SLERR_BMAP_INVALID);
	mp->rc = bmap_get(f, mq->bmapno, SL_WRITE, &b);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	BHREPL_POLICY_SET(b, mq->pol);

	mds_bmap_write_repls_rel(b);
	/* XXX upd_enqueue */

 out:
	if (f)
		fcmh_op_done(f);
	return (0);
}

int
slm_rmc_handle_statfs(struct pscrpc_request *rq)
{
	int j = 0, single = 0, vfsid;
	struct resprof_mds_info *rpmi;
	struct sl_resource *r, *ri;
	struct srm_statfs_req *mq;
	struct srm_statfs_rep *mp;
	struct sl_mds_iosinfo *si;
	struct statvfs sfb;
	double adj;

	OPSTAT_INCR(SLM_OPST_STATFS);
	SL_RSX_ALLOCREP(rq, mq, mp);
	mp->rc = slfid_to_vfsid(mq->fid, &vfsid);
	if (mp->rc)
		return (0);
	mp->rc = mdsio_statfs(vfsid, &sfb);
	sl_externalize_statfs(&sfb, &mp->ssfb);
	r = libsl_id2res(mq->iosid);
	if (r == NULL) {
		mp->rc = -SLERR_RES_UNKNOWN;
		return (0);
	}
	mp->ssfb.sf_bsize = 0;
	mp->ssfb.sf_blocks = 0;
	mp->ssfb.sf_bfree = 0;
	mp->ssfb.sf_bavail = 0;
	if (!RES_ISCLUSTER(r)) {
		ri = r;
		single = 1;
		goto single;
	}
	DYNARRAY_FOREACH(ri, j, &r->res_peers) {
 single:
		rpmi = res2rpmi(r);
		si = res2iosinfo(ri);
		RPMI_LOCK(rpmi);
		if (si->si_ssfb.sf_bsize == 0) {
			RPMI_ULOCK(rpmi);
			continue;
		}
		if (mp->ssfb.sf_bsize == 0)
			mp->ssfb.sf_bsize = si->si_ssfb.sf_bsize;
		adj = mp->ssfb.sf_bsize * 1. / si->si_ssfb.sf_bsize;
		mp->ssfb.sf_blocks	+= adj * si->si_ssfb.sf_blocks;
		mp->ssfb.sf_bfree	+= adj * si->si_ssfb.sf_bfree;
		mp->ssfb.sf_bavail	+= adj * si->si_ssfb.sf_bavail;
		RPMI_ULOCK(rpmi);

		if (single)
			break;
	}
	return (0);
}

int
slm_symlink(struct pscrpc_request *rq, struct srm_symlink_req *mq,
    struct srm_symlink_rep *mp, int ptl)
{
	char linkname[SL_PATH_MAX];
	struct fidc_membh *p = NULL;
	struct slash_creds cr;
	struct iovec iov;
	slfid_t fid = 0;
	int vfsid;

	mp->rc = slfid_to_vfsid(mq->pfg.fg_fid, &vfsid);
	if (mp->rc)
		return (mp->rc);

	mq->name[sizeof(mq->name) - 1] = '\0';
	if (mq->linklen == 0 || mq->linklen >= SL_PATH_MAX) {
		mp->rc = -EINVAL;
		return (mp->rc);
	}

	iov.iov_base = linkname;
	iov.iov_len = mq->linklen;
	mp->rc = slrpc_bulkserver(rq, BULK_GET_SINK, ptl, &iov, 1);
	if (mp->rc)
		return (mp->rc);

	linkname[mq->linklen] = '\0';

	cr.scr_uid = mq->sstb.sst_uid;
	cr.scr_gid = mq->sstb.sst_gid;

	if (IS_REMOTE_FID(mq->pfg.fg_fid)) {
		mp->rc = slm_rmm_forward_namespace(SLM_FORWARD_SYMLINK,
		    &mq->pfg, NULL, mq->name, linkname, 0, &cr,
		    &mp->cattr, 0);
		if (mp->rc)
			PFL_GOTOERR(out, mp->rc);
		fid = mp->cattr.sst_fg.fg_fid;
	}

	mp->rc = -slm_fcmh_get(&mq->pfg, &p);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	mds_reserve_slot(1);
	mp->rc = mdsio_symlink(vfsid, linkname, fcmh_2_mfid(p),
	    mq->name, &cr, &mp->cattr, NULL, fid ? NULL :
	    mdslog_namespace, fid ? 0 : slm_get_next_slashfid, fid);
	mds_unreserve_slot(1);

	mdsio_fcmh_refreshattr(p, &mp->pattr);

 out:
	if (p)
		fcmh_op_done(p);
	return (0);
}

int
slm_rmc_handle_symlink(struct pscrpc_request *rq)
{
	struct srm_symlink_req *mq;
	struct srm_symlink_rep *mp;

	OPSTAT_INCR(SLM_OPST_SYMLINK);
	SL_RSX_ALLOCREP(rq, mq, mp);
	return (slm_symlink(rq, mq, mp, SRMC_BULK_PORTAL));
}

int
slm_rmc_handle_unlink(struct pscrpc_request *rq, int isfile)
{
	struct slash_fidgen fg, chfg;
	struct fidc_membh *p = NULL;
	struct srm_unlink_req *mq;
	struct srm_unlink_rep *mp;
	int vfsid;

	chfg.fg_fid = FID_ANY;

	OPSTAT_INCR(SLM_OPST_UNLINK);
	SL_RSX_ALLOCREP(rq, mq, mp);
	mp->cattr.sst_fid = FID_ANY;
	mp->rc = slfid_to_vfsid(mq->pfid, &vfsid);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	fg.fg_fid = mq->pfid;
	fg.fg_gen = FGEN_ANY;
	mq->name[sizeof(mq->name) - 1] = '\0';

	if (IS_REMOTE_FID(mq->pfid)) {
		mp->rc = slm_rmm_forward_namespace(isfile ?
		    SLM_FORWARD_UNLINK : SLM_FORWARD_RMDIR, &fg, NULL,
		    mq->name, NULL, 0, NULL, NULL, 0);
		if (mp->rc)
			PFL_GOTOERR(out, mp->rc);
	}

	mp->rc = -slm_fcmh_get(&fg, &p);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	mds_reserve_slot(1);
	if (isfile)
		mp->rc = mdsio_unlink(vfsid, fcmh_2_mfid(p), NULL,
		    mq->name, &rootcreds, mdslog_namespace, &chfg);
	else
		mp->rc = mdsio_rmdir(vfsid, fcmh_2_mfid(p), NULL,
		    mq->name, &rootcreds, mdslog_namespace);
	mds_unreserve_slot(1);

 out:
	if (mp->rc == 0)
		mdsio_fcmh_refreshattr(p, &mp->pattr);
	if (p)
		fcmh_op_done(p);

	if (chfg.fg_fid != FID_ANY) {
		struct fidc_membh *c;

		if (slm_fcmh_get(&chfg, &c) == 0) {
			mdsio_fcmh_refreshattr(c, &mp->cattr);
			fcmh_op_done(c);
		}
	}

	psclog_info("%s parent="SLPRI_FID" name=%s rc=%d",
	    isfile ? "unlink" : "rmdir", mq->pfid, mq->name, mp->rc);
	return (0);
}

int
slm_rmc_handle_listxattr(struct pscrpc_request *rq)
{
	struct fidc_membh *f = NULL;
	struct srm_listxattr_req *mq;
	struct srm_listxattr_rep *mp;
	struct iovec iov;
	size_t outsize;
	int vfsid;

	iov.iov_base = NULL;
	SL_RSX_ALLOCREP(rq, mq, mp);
	mp->rc = slfid_to_vfsid(mq->fg.fg_fid, &vfsid);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);
	mp->rc = -slm_fcmh_get(&mq->fg, &f);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	if (mq->size) {
		iov.iov_base = PSCALLOC(mq->size);
		iov.iov_len = mq->size;
	}
	mp->size = 0;

	/* even a list can create the xaddr directory */
	mds_reserve_slot(1);
	mp->rc = mdsio_listxattr(vfsid, &rootcreds,
	    iov.iov_base, mq->size, &outsize, fcmh_2_mfid(f));
	mds_unreserve_slot(1);
	if (mp->rc) {
		if (mq->size)
			pscrpc_msg_add_flags(rq->rq_repmsg,
			    MSG_ABORT_BULK);
		PFL_GOTOERR(out, mp->rc);
	}

	mp->size = outsize;
	if (mq->size)
		mp->rc = slrpc_bulkserver(rq, BULK_PUT_SOURCE,
		    SRMC_BULK_PORTAL, &iov, 1);

 out:
	if (mq->size)
		PSCFREE(iov.iov_base);
	if (f)
		fcmh_op_done(f);
	return (0);
}

int
slm_rmc_handle_setxattr(struct pscrpc_request *rq)
{
	char name[SL_NAME_MAX + 1], value[SL_NAME_MAX + 1];
	struct fidc_membh *f = NULL;
	struct srm_setxattr_req *mq;
	struct srm_setxattr_rep *mp;
	struct iovec iov;
	int vfsid;

	OPSTAT_INCR(SLM_OPST_SETXATTR);
	SL_RSX_ALLOCREP(rq, mq, mp);
	mp->rc = slfid_to_vfsid(mq->fg.fg_fid, &vfsid);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);
	if (mq->namelen  > SL_NAME_MAX ||
	    mq->valuelen > SL_NAME_MAX)
		PFL_GOTOERR(out, mp->rc = -EINVAL);

	mp->rc = -slm_fcmh_get(&mq->fg, &f);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	memcpy(name, mq->name, mq->namelen);
	name[mq->namelen] = '\0';

	iov.iov_base = value;
	iov.iov_len = mq->valuelen;
	mp->rc = slrpc_bulkserver(rq, BULK_GET_SINK, SRMC_BULK_PORTAL,
	    &iov, 1);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	mds_reserve_slot(1);
	mp->rc = mdsio_setxattr(vfsid, &rootcreds, name, value,
	    mq->valuelen,  fcmh_2_mfid(f));
	mds_unreserve_slot(1);

 out:
	if (f)
		fcmh_op_done(f);
	return (0);
}

int
slm_rmc_handle_getxattr(struct pscrpc_request *rq)
{
	char value[SL_NAME_MAX + 1];
	int vfsid, abort_bulk = 0;
	struct fidc_membh *f = NULL;
	struct srm_getxattr_req *mq;
	struct srm_getxattr_rep *mp;
	struct iovec iov;
	size_t outsize;

	OPSTAT_INCR(SLM_OPST_GETXATTR);
	SL_RSX_ALLOCREP(rq, mq, mp);
	mp->rc = slfid_to_vfsid(mq->fg.fg_fid, &vfsid);
	if (mp->rc) {
		if (mq->size)
			abort_bulk = 1;
		PFL_GOTOERR(out, mp->rc);
	}
	mp->rc = -slm_fcmh_get(&mq->fg, &f);
	if (mp->rc) {
		if (mq->size)
			abort_bulk = 1;
		PFL_GOTOERR(out, mp->rc);
	}

	mp->valuelen = 0;
	mds_reserve_slot(1);
	mp->rc = mdsio_getxattr(vfsid, &rootcreds, mq->name, value,
	    mq->size, &outsize, fcmh_2_mfid(f));
	mds_unreserve_slot(1);
	if (mp->rc) {
		if (mp->rc == ENOATTR)
			mp->rc = 0;
		if (mq->size)
			abort_bulk = 1;
		PFL_GOTOERR(out, mp->rc);
	}
	mp->valuelen = outsize;

	iov.iov_base = value;
	iov.iov_len = outsize;
	if (mq->size)
		mp->rc = slrpc_bulkserver(rq, BULK_PUT_SOURCE,
		    SRMC_BULK_PORTAL, &iov, 1);

 out:
	if (abort_bulk)
		pscrpc_msg_add_flags(rq->rq_repmsg, MSG_ABORT_BULK);
	if (f)
		fcmh_op_done(f);
	return (0);
}

int
slm_rmc_handle_removexattr(struct pscrpc_request *rq)
{
	struct fidc_membh *f = NULL;
	struct srm_removexattr_req *mq;
	struct srm_removexattr_rep *mp;
	int vfsid;

	SL_RSX_ALLOCREP(rq, mq, mp);
	mp->rc = slfid_to_vfsid(mq->fg.fg_fid, &vfsid);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);
	mp->rc = -slm_fcmh_get(&mq->fg, &f);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	mq->name[sizeof(mq->name) - 1] = '\0';
	mds_reserve_slot(1);
	mp->rc = mdsio_removexattr(vfsid, &rootcreds, mq->name,
	    fcmh_2_mfid(f));
	mds_unreserve_slot(1);

 out:
	if (f)
		fcmh_op_done(f);
	return (0);
}

int
slm_rmc_handle_addreplrq(struct pscrpc_request *rq)
{
	struct srm_replrq_req *mq;
	struct srm_replrq_rep *mp;

	SL_RSX_ALLOCREP(rq, mq, mp);
	mp->rc = mds_repl_addrq(&mq->fg, mq->bmapno, mq->repls,
	    mq->nrepls, mq->sys_prio, mq->usr_prio);
	return (0);
}

int
slm_rmc_handle_delreplrq(struct pscrpc_request *rq)
{
	struct srm_replrq_req *mq;
	struct srm_replrq_rep *mp;

	SL_RSX_ALLOCREP(rq, mq, mp);
	mp->rc = mds_repl_delrq(&mq->fg, mq->bmapno, mq->repls,
	    mq->nrepls);
	return (0);
}

int
slm_rmc_handle_getreplst(struct pscrpc_request *rq)
{
	const struct srm_replst_master_req *mq;
	struct srm_replst_master_rep *mp;
	struct slm_replst_workreq *rsw;

	SL_RSX_ALLOCREP(rq, mq, mp);
	rsw = PSCALLOC(sizeof(*rsw));
	INIT_PSC_LISTENTRY(&rsw->rsw_lentry);
	rsw->rsw_fg = mq->fg;
	rsw->rsw_cid = mq->id;
	rsw->rsw_csvc = slm_getclcsvc(rq->rq_export);
	lc_add(&slm_replst_workq, rsw);
	return (0);
}

int
slm_rmc_handler(struct pscrpc_request *rq)
{
	int rc = 0;

	if (rq->rq_reqmsg->opc != SRMT_CONNECT) {
		EXPORT_LOCK(rq->rq_export);
		if (rq->rq_export->exp_private == NULL)
			rc = -PFLERR_NOTCONN;
		EXPORT_ULOCK(rq->rq_export);
		if (rc)
			PFL_GOTOERR(out, rc);
	}

	mds_note_update(1);

	switch (rq->rq_reqmsg->opc) {
	/* bmap messages */
	case SRMT_BMAPCHWRMODE:
		rc = slm_rmc_handle_bmap_chwrmode(rq);
		break;
	case SRMT_EXTENDBMAPLS:
		rc = slm_rmc_handle_extendbmapls(rq);
		break;
	case SRMT_REASSIGNBMAPLS:
		rc = slm_rmc_handle_reassignbmapls(rq);
		break;
	case SRMT_GETBMAP:
		rc = slm_rmc_handle_getbmap(rq);
		break;
	case SRMT_RELEASEBMAP:
		rc = slm_rmc_handle_rls_bmap(rq);
		break;
	case SRMT_GET_INODE:
		rc = slm_rmc_handle_getinode(rq);
		break;

	/* replication messages */
	case SRMT_SET_FATTR:
		rc = slm_rmc_handle_set_fattr(rq);
		break;
	case SRMT_SET_BMAPREPLPOL:
		rc = slm_rmc_handle_set_bmapreplpol(rq);
		break;
	case SRMT_REPL_ADDRQ:
		rc = slm_rmc_handle_addreplrq(rq);
		break;
	case SRMT_REPL_DELRQ:
		rc = slm_rmc_handle_delreplrq(rq);
		break;
	case SRMT_REPL_GETST:
		rc = slm_rmc_handle_getreplst(rq);
		break;

	/* control messages */
	case SRMT_CONNECT:
		rc = slrpc_handle_connect(rq, SRMC_MAGIC, SRMC_VERSION,
		    SLCONNT_CLI);
		break;
	case SRMT_PING:
		rc = slm_rmc_handle_ping(rq);
		break;

	/* file system messages */
	case SRMT_CREATE:
		rc = slm_rmc_handle_create(rq);
		break;
	case SRMT_GETATTR:
		rc = slm_rmc_handle_getattr(rq);
		break;
	case SRMT_LINK:
		rc = slm_rmc_handle_link(rq);
		break;
	case SRMT_MKDIR:
		rc = slm_rmc_handle_mkdir(rq);
		break;
	case SRMT_MKNOD:
		rc = slm_rmc_handle_mknod(rq);
		break;
	case SRMT_LOOKUP:
		rc = slm_rmc_handle_lookup(rq);
		break;
	case SRMT_READDIR:
		rc = slm_rmc_handle_readdir(rq);
		break;
	case SRMT_READLINK:
		rc = slm_rmc_handle_readlink(rq);
		break;
	case SRMT_RENAME:
		rc = slm_rmc_handle_rename(rq);
		break;
	case SRMT_RMDIR:
		rc = slm_rmc_handle_unlink(rq, 0);
		break;
	case SRMT_SETATTR:
		rc = slm_rmc_handle_setattr(rq);
		break;
	case SRMT_STATFS:
		rc = slm_rmc_handle_statfs(rq);
		break;
	case SRMT_SYMLINK:
		rc = slm_rmc_handle_symlink(rq);
		break;
	case SRMT_UNLINK:
		rc = slm_rmc_handle_unlink(rq, 1);
		break;
	case SRMT_LISTXATTR:
		rc = slm_rmc_handle_listxattr(rq);
		break;
	case SRMT_SETXATTR:
		rc = slm_rmc_handle_setxattr(rq);
		break;
	case SRMT_GETXATTR:
		rc = slm_rmc_handle_getxattr(rq);
		break;
	case SRMT_REMOVEXATTR:
		rc = slm_rmc_handle_removexattr(rq);
		break;
	default:
		psclog_errorx("unexpected opcode %d",
		    rq->rq_reqmsg->opc);
		rq->rq_status = -PFLERR_NOSYS;
		mds_note_update(-1);
		return (pscrpc_error(rq));
	}
 out:
	mds_note_update(-1);
	authbuf_sign(rq, PSCRPC_MSG_REPLY);
	pscrpc_target_send_reply_msg(rq, -(abs(rc)), 0);
	return (rc);
}

void
mexpc_allocpri(struct pscrpc_export *exp)
{
	struct slm_exp_cli *mexpc;
	int i;

	mexpc = exp->exp_private = PSCALLOC(sizeof(*mexpc));
	slm_getclcsvc(exp);
	for (i = 0; i < nitems(mexpc->mexpc_readdir_past); i++)
		mexpc->mexpc_readdir_past[i].crap_fid = FID_ANY;
}

struct sl_expcli_ops sl_expcli_ops = {
	mexpc_allocpri,
	NULL
};
