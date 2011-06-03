/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2007-2011, Pittsburgh Supercomputing Center (PSC).
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

#include "pfl/fs.h"
#include "pfl/str.h"
#include "psc_rpc/export.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/service.h"
#include "psc_util/lock.h"

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

#define IS_REMOTE_FID(fid)						\
	((fid) != SLFID_ROOT && nodeSite->site_id != FID_GET_SITEID(fid))

uint64_t		next_slash_id;
static psc_spinlock_t	slash_id_lock = SPINLOCK_INIT;

uint64_t
slm_get_curr_slashid(void)
{
	uint64_t slid;

	spinlock(&slash_id_lock);
	slid = next_slash_id;
	freelock(&slash_id_lock);
	return (slid);
}

void
slm_set_curr_slashid(uint64_t slfid)
{
	spinlock(&slash_id_lock);
	next_slash_id = slfid;
	freelock(&slash_id_lock);
}

/*
 * slm_get_next_slashid - Return the next SLASH FID to use.  Note that from ZFS
 *     point of view, it is perfectly okay that we use the same SLASH FID to
 *     refer to different files/directories.  However, doing so can confuse
 *     our clients (think identity theft).  So we must make sure that we never
 *     reuse a SLASH FID, even after a crash.
 */
uint64_t
slm_get_next_slashid(void)
{
	uint64_t slid;

	spinlock(&slash_id_lock);
	if (next_slash_id >= (UINT64_C(1) << SLASH_ID_FID_BITS))
		next_slash_id = SLFID_MIN;
	slid = next_slash_id++;
	freelock(&slash_id_lock);

	slid |= ((uint64_t)nodeResm->resm_site->site_id << SLASH_ID_FID_BITS);
	psclog_info("next slash ID "SLPRI_FID, slid);
	return slid;
}

int
slm_rmc_handle_connect(struct pscrpc_request *rq)
{
	struct pscrpc_export *e = rq->rq_export;
	const struct srm_connect_req *mq;
	struct srm_connect_rep *mp;

	SL_RSX_ALLOCREP(rq, mq, mp);
	if (mq->magic != SRMC_MAGIC || mq->version != SRMC_VERSION)
		mp->rc = EINVAL;
	//psc_assert(e->exp_private == NULL);
	sl_exp_getpri_cli(e);
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
	struct fidc_membh *fcmh;

	SL_RSX_ALLOCREP(rq, mq, mp);
	mp->rc = slm_fcmh_get(&mq->fg, &fcmh);
	if (mp->rc)
		goto out;

	FCMH_LOCK(fcmh);
	mp->attr = fcmh->fcmh_sstb;
	FCMH_ULOCK(fcmh);

 out:
	if (fcmh)
		fcmh_op_done_type(fcmh, FCMH_OPCNT_LOOKUP_FIDC);
	return (0);
}

static void
slm_rmc_bmapdesc_setup(struct bmapc_memb *bmap,
       struct srt_bmapdesc *sbd, enum rw rw)
{
	sbd->sbd_fg = bmap->bcm_fcmh->fcmh_fg;
	sbd->sbd_bmapno = bmap->bcm_bmapno;
	if (bmap->bcm_flags & BMAP_DIO)
		sbd->sbd_flags |= SRM_LEASEBMAPF_DIRECTIO;

	if (rw == SL_WRITE) {
		struct bmap_mds_info *bmi = bmap_2_bmi(bmap);

		psc_assert(bmi->bmdsi_wr_ion);
		sbd->sbd_ion_nid = bmi->bmdsi_wr_ion->rmmi_resm->resm_nid;
		sbd->sbd_ios_id = bmi->bmdsi_wr_ion->rmmi_resm->resm_res->res_id;
	} else {
		sbd->sbd_ion_nid = LNET_NID_ANY;
		sbd->sbd_ios_id = IOS_ID_ANY;
	}
}

/**
 * slm_rmc_handle_bmap_chwrmode - Handle a BMAPCHWRMODE request to
 *	upgrade a client bmap lease from READ-only to READ+WRITE.
 * @rq: RPC request.
 */
int
slm_rmc_handle_bmap_chwrmode(struct pscrpc_request *rq)
{
	const struct srm_bmap_chwrmode_req *mq;
	struct srm_bmap_chwrmode_rep *mp;
	struct fidc_membh *f = NULL;
	struct bmapc_memb *b = NULL;
	struct bmap_mds_lease *bml;
	struct bmap_mds_info *bmi;

	SL_RSX_ALLOCREP(rq, mq, mp);
	mp->rc = slm_fcmh_get(&mq->sbd.sbd_fg, &f);
	if (mp->rc)
		goto out;
	mp->rc = bmap_lookup(f, mq->sbd.sbd_bmapno, &b);
	if (mp->rc)
		goto out;

	bmi = bmap_2_bmi(b);

	BMAP_LOCK(b);
	bml = mds_bmap_getbml(b, rq->rq_conn->c_peer.nid,
	    rq->rq_conn->c_peer.pid, mq->sbd.sbd_seq);
	if (bml == NULL) {
		mp->rc = EINVAL;
		goto out;
	}

	mp->rc = mds_bmap_bml_chwrmode(bml, mq->prefios);
	if (mp->rc == EALREADY)
		mp->rc = 0;
	else if (mp->rc)
		goto out;

	mp->sbd = mq->sbd;
	mp->sbd.sbd_seq = bml->bml_seq;
	mp->sbd.sbd_key = bmi->bmdsi_assign->odtr_key;

	psc_assert(bmi->bmdsi_wr_ion);
	mp->sbd.sbd_ion_nid = bmi->bmdsi_wr_ion->rmmi_resm->resm_nid;
	mp->sbd.sbd_ios_id = bmi->bmdsi_wr_ion->rmmi_resm->resm_res->res_id;

 out:
	if (b)
		bmap_op_done_type(b, BMAP_OPCNT_LOOKUP);
	if (f)
		fcmh_op_done_type(f, FCMH_OPCNT_LOOKUP_FIDC);
	return (0);
}

int
slm_rmc_handle_extendbmapls(struct pscrpc_request *rq)
{
	struct srm_leasebmapext_req *mq;
	struct srm_leasebmapext_rep *mp;
	struct fidc_membh *f;

	SL_RSX_ALLOCREP(rq, mq, mp);

	mp->rc = slm_fcmh_get(&mq->sbd.sbd_fg, &f);
	if (mp->rc)
		return (0);

	mp->rc = mds_lease_renew(f, &mq->sbd, &mp->sbd, rq->rq_export);
	fcmh_op_done_type(f, FCMH_OPCNT_LOOKUP_FIDC);
	return (0);
}

int
slm_rmc_handle_getbmap(struct pscrpc_request *rq)
{
	const struct srm_leasebmap_req *mq;
	struct bmapc_memb *bmap = NULL;
	struct srm_leasebmap_rep *mp;
	struct fidc_membh *fcmh;
	int rc = 0;

	SL_RSX_ALLOCREP(rq, mq, mp);
	if (mq->rw != SL_READ && mq->rw != SL_WRITE) {
		mp->rc = EINVAL;
		return (0);
	}

	mp->rc = slm_fcmh_get(&mq->fg, &fcmh);
	if (mp->rc)
		return (0);
	mp->flags = mq->flags;

	mp->rc = mds_bmap_load_cli(fcmh, mq->bmapno, mq->flags, mq->rw,
	    mq->prefios, &mp->sbd, rq->rq_export, &bmap);
	if (mp->rc)
		goto out;

	if (mq->flags & SRM_LEASEBMAPF_DIRECTIO)
		mp->sbd.sbd_flags |= SRM_LEASEBMAPF_DIRECTIO;

	slm_rmc_bmapdesc_setup(bmap, &mp->sbd, mq->rw);

	memcpy(&mp->bcs, &bmap->bcm_corestate, sizeof(mp->bcs));

	if (mp->flags & SRM_LEASEBMAPF_GETREPLTBL) {
		struct slash_inode_handle *ih;

		ih = fcmh_2_inoh(fcmh);
		mp->nrepls = ih->inoh_ino.ino_nrepls;
		memcpy(&mp->reptbl[0], &ih->inoh_ino.ino_repls,
		    sizeof(ih->inoh_ino.ino_repls));

		if (mp->nrepls > SL_DEF_REPLICAS) {
			rc = mds_inox_ensure_loaded(ih);
			if (!rc)
				memcpy(&mp->reptbl[SL_DEF_REPLICAS],
				    &ih->inoh_extras->inox_repls,
				    sizeof(ih->inoh_extras->inox_repls));
		}
	}

 out:
	fcmh_op_done_type(fcmh, FCMH_OPCNT_LOOKUP_FIDC);
	return (rc ? rc : mp->rc);
}

int
slm_rmc_handle_link(struct pscrpc_request *rq)
{
	struct fidc_membh *p = NULL, *c = NULL;
	struct srm_link_req *mq;
	struct srm_link_rep *mp;

	SL_RSX_ALLOCREP(rq, mq, mp);
	mp->rc = slm_fcmh_get(&mq->fg, &c);
	if (mp->rc)
		goto out;
	mp->rc = slm_fcmh_get(&mq->pfg, &p);
	if (mp->rc)
		goto out;

	mq->name[sizeof(mq->name) - 1] = '\0';
	mds_reserve_slot();
	mp->rc = mdsio_link(fcmh_2_mdsio_fid(c), fcmh_2_mdsio_fid(p),
	    mq->name, &rootcreds, &mp->cattr, mdslog_namespace);
	mds_unreserve_slot();

	mdsio_fcmh_refreshattr(p, &mp->pattr);

 out:
	if (c)
		fcmh_op_done_type(c, FCMH_OPCNT_LOOKUP_FIDC);
	if (p)
		fcmh_op_done_type(p, FCMH_OPCNT_LOOKUP_FIDC);
	return (0);
}

int
slm_rmc_handle_lookup(struct pscrpc_request *rq)
{
	struct srm_lookup_req *mq;
	struct srm_lookup_rep *mp;
	struct fidc_membh *p;

	SL_RSX_ALLOCREP(rq, mq, mp);
	mp->rc = slm_fcmh_get(&mq->pfg, &p);
	if (mp->rc)
		goto out;

	mq->name[sizeof(mq->name) - 1] = '\0';
	if (fcmh_2_mdsio_fid(p) == SLFID_ROOT &&
	    strcmp(mq->name, SL_RPATH_META_DIR) == 0) {
		mp->rc = EINVAL;
		goto out;
	}
	mp->rc = mdsio_lookup(fcmh_2_mdsio_fid(p),
	    mq->name, NULL, &rootcreds, &mp->attr);

 out:
	if (p)
		fcmh_op_done_type(p, FCMH_OPCNT_LOOKUP_FIDC);
	return (0);
}

int
slm_rmc_handle_mkdir(struct pscrpc_request *rq)
{
	struct fidc_membh *p = NULL, *c = NULL;
	struct srm_mkdir_req *mq;
	struct srm_mkdir_rep *mp;
	uint32_t pol;

	SL_RSX_ALLOCREP(rq, mq, mp);
	mq->name[sizeof(mq->name) - 1] = '\0';

	mp->rc = slm_fcmh_get(&mq->pfg, &p);
	if (mp->rc)
		goto out;

	if (IS_REMOTE_FID(mq->pfg.fg_fid)) {
		mp->rc = slm_rmm_forward_namespace(SLM_FORWARD_MKDIR,
		    &mq->pfg, NULL, mq->name, NULL, mq->mode, &mq->creds,
		    &mp->cattr, 0);
		mdsio_fcmh_refreshattr(p, &mp->pattr);
		goto out;
	}
	mds_reserve_slot();
	mp->rc = mdsio_mkdir(fcmh_2_mdsio_fid(p), mq->name, mq->mode,
	    &mq->creds, &mp->cattr, NULL, mdslog_namespace,
	    slm_get_next_slashid, 0);
	mds_unreserve_slot();

	mdsio_fcmh_refreshattr(p, &mp->pattr);

	/*
	 * Set new subdir's new files' default replication policy from
	 * parent dir.
	 */
	if (slm_fcmh_get(&mp->cattr.sst_fg, &c) == 0) {
		FCMH_LOCK(p);
		pol = p->fcmh_sstb.sstd_freplpol;
		FCMH_ULOCK(p);

		FCMH_LOCK(c);
		fcmh_wait_locked(c, c->fcmh_flags & FCMH_IN_SETATTR);
		c->fcmh_sstb.sstd_freplpol = pol;
		mds_fcmh_setattr(c, SL_SETATTRF_FREPLPOL);
		FCMH_ULOCK(c);
	}

 out:
	if (p)
		fcmh_op_done_type(p, FCMH_OPCNT_LOOKUP_FIDC);
	if (c)
		fcmh_op_done_type(c, FCMH_OPCNT_LOOKUP_FIDC);
	return (0);
}

int
slm_rmc_handle_mknod(struct pscrpc_request *rq)
{
	struct srm_mknod_req *mq;
	struct srm_mknod_rep *mp;
	struct fidc_membh *p;

	SL_RSX_ALLOCREP(rq, mq, mp);
	mp->rc = slm_fcmh_get(&mq->pfg, &p);
	if (mp->rc)
		goto out;

	mq->name[sizeof(mq->name) - 1] = '\0';
	mds_reserve_slot();
	mp->rc = mdsio_mknod(fcmh_2_mdsio_fid(p), mq->name, mq->mode,
	    &mq->creds, &mp->cattr, NULL, mdslog_namespace,
	    slm_get_next_slashid);
	mds_unreserve_slot();

	mdsio_fcmh_refreshattr(p, &mp->pattr);
 out:
	if (p)
		fcmh_op_done_type(p, FCMH_OPCNT_LOOKUP_FIDC);
	return (0);
}

/**
 * slm_rmc_handle_create - Handle a CREATE from CLI.  As an
 *	optimization, we bundle a write bmap lease in the reply.
 */
int
slm_rmc_handle_create(struct pscrpc_request *rq)
{
	struct fidc_membh *p = NULL, *c;
	struct srm_create_rep *mp;
	struct srm_create_req *mq;
	struct bmapc_memb *bmap;
	void *mdsio_data;
	uint32_t pol;

	SL_RSX_ALLOCREP(rq, mq, mp);
	if (mq->flags & SRM_LEASEBMAPF_GETREPLTBL) {
		mp->rc = EINVAL;
		goto out;
	}

	/* Lookup the parent directory in the cache so that the
	 *   slash2 ino can be translated into the inode for the
	 *   underlying fs.
	 */
	mp->rc = slm_fcmh_get(&mq->pfg, &p);
	if (mp->rc)
		goto out;

	mq->name[sizeof(mq->name) - 1] = '\0';

	if (IS_REMOTE_FID(mq->pfg.fg_fid)) {
		mp->rc = slm_rmm_forward_namespace(SLM_FORWARD_CREATE,
		    &mq->pfg, NULL, mq->name, NULL, mq->mode, &mq->creds,
		    &mp->cattr, 0);
		if (!mp->rc) {
			mp->rc2 = ENOENT;
			mdsio_fcmh_refreshattr(p, &mp->pattr);
		}
		goto out;
	}

	DEBUG_FCMH(PLL_DEBUG, p, "create op start for %s", mq->name);

	mds_reserve_slot();
	mp->rc = mdsio_opencreate(fcmh_2_mdsio_fid(p), &mq->creds,
	    O_CREAT | O_EXCL | O_RDWR, mq->mode, mq->name, NULL,
	    &mp->cattr, &mdsio_data, mdslog_namespace,
	    slm_get_next_slashid, 0);
	mds_unreserve_slot();

	if (mp->rc)
		goto out;

	/* Refresh the cached attributes of our parent and pack them
	 *   in the reply.
	 */
	mdsio_fcmh_refreshattr(p, &mp->pattr);

	DEBUG_FCMH(PLL_DEBUG, p, "create op done for %s", mq->name);
	/* XXX enter this into the fcmh cache instead of doing it again
	 *   This release may be the sanest thing actually, unless EXCL is
	 *   used.
	 */
	mdsio_release(&rootcreds, mdsio_data);

	DEBUG_FCMH(PLL_DEBUG, p, "mdsio_release() done for %s", mq->name);

	mp->rc2 = slm_fcmh_get(&mp->cattr.sst_fg, &c);
	if (mp->rc2)
		goto out;

	FCMH_LOCK(p);
	pol = p->fcmh_sstb.sstd_freplpol;
	FCMH_ULOCK(p);

	INOH_LOCK(fcmh_2_inoh(c));
	fcmh_2_ino(c)->ino_replpol = pol;
	INOH_ULOCK(fcmh_2_inoh(c));

	/* obtain lease for first bmap as optimization */
	mp->flags = mq->flags;

	bmap = NULL;
	mp->rc2 = mds_bmap_load_cli(c, 0, mp->flags, SL_WRITE,
	    mq->prefios, &mp->sbd, rq->rq_export, &bmap);

	fcmh_op_done_type(c, FCMH_OPCNT_LOOKUP_FIDC);

	if (mp->rc2)
		goto out;

	slm_rmc_bmapdesc_setup(bmap, &mp->sbd, SL_WRITE);
 out:
	if (p)
		fcmh_op_done_type(p, FCMH_OPCNT_LOOKUP_FIDC);
	return (0);
}

int
slm_rmc_handle_readdir(struct pscrpc_request *rq)
{
	struct fidc_membh *fcmh = NULL;
	struct srm_readdir_req *mq;
	struct srm_readdir_rep *mp;
	struct iovec iov[2];
	size_t outsize, nents;
	int niov;

	iov[0].iov_base = NULL;
	iov[1].iov_base = NULL;

	SL_RSX_ALLOCREP(rq, mq, mp);

	mp->rc = slm_fcmh_get(&mq->fg, &fcmh);
	if (mp->rc)
		goto out;

	if (mq->size > MAX_READDIR_BUFSIZ ||
	    mq->nstbpref > MAX_READDIR_NENTS) {
		mp->rc = EINVAL;
		goto out;
	}

	iov[0].iov_base = PSCALLOC(mq->size);
	iov[0].iov_len = mq->size;

	niov = 1;
	if (mq->nstbpref) {
		niov++;
		iov[1].iov_len = mq->nstbpref * sizeof(struct srt_stat);
		iov[1].iov_base = PSCALLOC(iov[1].iov_len);
	} else {
		iov[1].iov_len = 0;
		iov[1].iov_base = NULL;
	}

	mp->rc = mdsio_readdir(&rootcreds, mq->size, mq->offset,
	    iov[0].iov_base, &outsize, &nents, iov[1].iov_base,
	    mq->nstbpref, fcmh_2_mdsio_data(fcmh));
	mp->size = outsize;
	mp->num = nents;

	psclog_info("mdsio_readdir: rc=%d, data=%p", mp->rc,
	    fcmh_2_mdsio_data(fcmh));

	if (mp->rc)
		goto out;

#if 0
	{
		/* debugging only */
		unsigned int i;
		struct srt_stat *attr;
		attr = iov[1].iov_base;
		for (i = 0; i < mq->nstbpref; i++, attr++) {
			if (!attr->sst_fg.fg_fid)
				break;
			psclog_info("reply: f+g:"SLPRI_FG", mode=%#o",
				SLPRI_FG_ARGS(&attr->sst_fg),
				attr->sst_mode);
		}
	}
#endif

	if (SRM_READDIR_BUFSZ(mp->size, mp->num, mq->nstbpref) <=
	    sizeof(mp->ents)) {
		size_t sz;

		sz = MIN(mp->num, mq->nstbpref) *
		    sizeof(struct srt_stat);
		memcpy(mp->ents, iov[1].iov_base, sz);
		memcpy(mp->ents + sz, iov[0].iov_base, mp->size);
		pscrpc_msg_add_flags(rq->rq_repmsg, MSG_ABORT_BULK);
	} else {
		mp->rc = rsx_bulkserver(rq, BULK_PUT_SOURCE, SRMC_BULK_PORTAL,
		    iov, niov);
	}

 out:
	PSCFREE(iov[0].iov_base);
	PSCFREE(iov[1].iov_base);
	if (fcmh)
		fcmh_op_done_type(fcmh, FCMH_OPCNT_LOOKUP_FIDC);
	return (mp->rc);
}

int
slm_rmc_handle_readlink(struct pscrpc_request *rq)
{
	struct srm_readlink_req *mq;
	struct srm_readlink_rep *mp;
	struct fidc_membh *fcmh;
	struct iovec iov;
	char buf[SL_PATH_MAX];

	SL_RSX_ALLOCREP(rq, mq, mp);
	mp->rc = slm_fcmh_get(&mq->fg, &fcmh);
	if (mp->rc)
		goto out;

	mp->rc = mdsio_readlink(fcmh_2_mdsio_fid(fcmh), buf, &rootcreds);
	if (mp->rc)
		goto out;

	iov.iov_base = buf;
	iov.iov_len = sizeof(buf);
	mp->rc = rsx_bulkserver(rq, BULK_PUT_SOURCE, SRMC_BULK_PORTAL,
	    &iov, 1);

 out:
	if (fcmh)
		fcmh_op_done_type(fcmh, FCMH_OPCNT_LOOKUP_FIDC);

	return (mp->rc);
}

int
slm_rmc_handle_rls_bmap(struct pscrpc_request *rq)
{
	return (mds_handle_rls_bmap(rq, 0));
}

int
slm_rmc_handle_rename(struct pscrpc_request *rq)
{
	char from[SL_NAME_MAX + 1], to[SL_NAME_MAX + 1];
	struct fidc_membh *op, *np;
	struct srm_rename_req *mq;
	struct srm_rename_rep *mp;
	struct iovec iov[2];

	op = np = NULL;
	SL_RSX_ALLOCREP(rq, mq, mp);
	if (mq->fromlen == 0 || mq->tolen == 0 ||
	    mq->fromlen > SL_NAME_MAX || mq->tolen > SL_NAME_MAX)
		return (EINVAL);

	iov[0].iov_base = from;
	iov[0].iov_len = mq->fromlen;
	iov[1].iov_base = to;
	iov[1].iov_len = mq->tolen;

	mp->rc = rsx_bulkserver(rq, BULK_GET_SINK, SRMC_BULK_PORTAL,
	    iov, 2);
	if (mp->rc)
		return (mp->rc);

	from[mq->fromlen] = '\0';
	to[mq->tolen] = '\0';

	mp->rc = slm_fcmh_get(&mq->opfg, &op);
	if (mp->rc)
		goto out;

	if (SAMEFG(&mq->opfg, &mq->npfg)) {
		np = op;
	} else {
		mp->rc = slm_fcmh_get(&mq->npfg, &np);
		if (mp->rc)
			goto out;
	}

	if (FID_GET_SITEID(mq->opfg.fg_fid) !=
	    FID_GET_SITEID(mq->npfg.fg_fid)) {
		mp->rc = EXDEV;
		goto out;
	}

	if (IS_REMOTE_FID(mq->opfg.fg_fid)) {
		mp->rc = slm_rmm_forward_namespace(SLM_FORWARD_RENAME,
		    &mq->opfg, &mq->npfg, from, to, 0, &rootcreds,
		    &mp->cattr, 0);
		goto out;
	}


	/* if we get here, op and np must be owned by the current MDS */
	mp->rc = mdsio_rename(fcmh_2_mdsio_fid(op), from,
	    fcmh_2_mdsio_fid(np), to, &rootcreds, mdslog_namespace);

	/* update target ctime */
	if (mp->rc == 0) {
		struct srt_stat c_sstb;
		struct fidc_membh *c;

		/* XXX race between RENAME just before and LOOKUP here!! */
		if (mdsio_lookup(fcmh_2_mdsio_fid(np), to, NULL,
		    &rootcreds, &c_sstb) == 0 &&
		    slm_fcmh_get(&c_sstb.sst_fg, &c) == 0) {
			FCMH_LOCK(c);
			fcmh_wait_locked(c, c->fcmh_flags & FCMH_IN_SETATTR);
			SL_GETTIMESPEC(&c->fcmh_sstb.sst_ctim);
			mds_fcmh_setattr(c, PSCFS_SETATTRF_CTIME);
			fcmh_op_done_type(c, FCMH_OPCNT_LOOKUP_FIDC);
		}
	}

 out:
	if (mp->rc == 0) {
		mdsio_fcmh_refreshattr(op, &mp->srr_opattr);
		if (op != np)
			mdsio_fcmh_refreshattr(np, &mp->srr_npattr);
	}

	if (np)
		fcmh_op_done_type(np, FCMH_OPCNT_LOOKUP_FIDC);
	if (op != np)
		fcmh_op_done_type(op, FCMH_OPCNT_LOOKUP_FIDC);
	return (0);
}

int
slm_rmc_handle_setattr(struct pscrpc_request *rq)
{
	int to_set, tadj = 0, unbump = 0;
	struct slashrpc_cservice *csvc;
	struct fidc_membh *fcmh = NULL;
	struct srm_setattr_req *mq;
	struct srm_setattr_rep *mp;

	SL_RSX_ALLOCREP(rq, mq, mp);

	mp->rc = slm_fcmh_get(&mq->attr.sst_fg, &fcmh);
	if (mp->rc)
		goto out;

	FCMH_LOCK(fcmh);

	to_set = mq->to_set & SL_SETATTRF_CLI_ALL;
	if (to_set & PSCFS_SETATTRF_DATASIZE) {
		if (IS_REMOTE_FID(mq->attr.sst_fg.fg_fid)) {
			mp->rc = ENOSYS;
			goto out;
		}
		to_set |= PSCFS_SETATTRF_MTIME;
		SL_GETTIMESPEC(&mq->attr.sst_mtim);
		if (mq->attr.sst_size == 0) {
			/*
			 * Full truncate.  If file size is already zero,
			 * we must still bump the generation since size
			 * updates from the sliod may be pending for
			 * this generation.
			 */
			fcmh_2_gen(fcmh)++;
			to_set |= SL_SETATTRF_GEN;
			unbump = 1;
		} else {
			/* partial truncate */
			if (fcmh->fcmh_flags & FCMH_IN_PTRUNC) {
				mp->rc = SLERR_BMAP_IN_PTRUNC;
				goto out;
			}
			to_set &= ~PSCFS_SETATTRF_DATASIZE;
			tadj |= PSCFS_SETATTRF_DATASIZE;
		}
	}

	if (to_set) {
		if (IS_REMOTE_FID(mq->attr.sst_fg.fg_fid)) {
			mp->rc = slm_rmm_forward_namespace(SLM_FORWARD_SETATTR,
			    &mq->attr.sst_fg, NULL, NULL, NULL, 0, NULL, &mq->attr,
			    to_set);
			mp->attr = mq->attr;
		} else {
			/*
			 * If the file is open, mdsio_data will be valid and
			 * used.  Otherwise, it will be NULL, and we'll use the
			 * mdsio_fid.
			 */
			mds_reserve_slot();
			mp->rc = mdsio_setattr(fcmh_2_mdsio_fid(fcmh),
			    &mq->attr, to_set, &rootcreds, &mp->attr,
			    fcmh_2_mdsio_data(fcmh), mdslog_namespace);
			mds_unreserve_slot();
		}
	}

	if (mp->rc) {
		if (unbump)
			fcmh_2_gen(fcmh)--;
	} else {
		if (tadj & PSCFS_SETATTRF_DATASIZE) {
			fcmh->fcmh_flags |= FCMH_IN_PTRUNC;

			csvc = slm_getclcsvc(rq->rq_export);
			psc_dynarray_add(&fcmh_2_fmi(fcmh)->
			    fmi_ptrunc_clients, csvc);

			mp->rc = SLERR_BMAP_PTRUNC_STARTED;
		}

		slm_setattr_core(fcmh, &mq->attr, to_set | tadj);

		/* refresh to latest from mdsio layer */
		fcmh->fcmh_sstb = mp->attr;
	}

 out:
	if (fcmh) {
		FCMH_RLOCK(fcmh);
		if (mp->rc == 0 || mp->rc == SLERR_BMAP_PTRUNC_STARTED)
			mp->attr = fcmh->fcmh_sstb;
		fcmh_op_done_type(fcmh, FCMH_OPCNT_LOOKUP_FIDC);
	}
	return (0);
}

int
slm_rmc_handle_set_newreplpol(struct pscrpc_request *rq)
{
	struct srm_set_newreplpol_req *mq;
	struct srm_set_newreplpol_rep *mp;
	struct slash_inode_handle *ih;
	struct fidc_membh *fcmh;

	SL_RSX_ALLOCREP(rq, mq, mp);

	if (mq->pol < 0 || mq->pol >= NBRPOL) {
		mp->rc = EINVAL;
		return (0);
	}

	mp->rc = slm_fcmh_get(&mq->fg, &fcmh);
	if (mp->rc)
		goto out;

	ih = fcmh_2_inoh(fcmh);
	mp->rc = mds_inox_ensure_loaded(ih);
	if (mp->rc == 0) {
		struct slmds_jent_ino_repls sjir;

		INOH_LOCK(ih);
		ih->inoh_ino.ino_replpol = mq->pol;
		INOH_ULOCK(ih);

		sjir.sjir_fid = fcmh_2_fid(ih->inoh_fcmh);
		sjir.sjir_replpol = ih->inoh_ino.ino_replpol;

		mds_inode_write(ih, mdslog_ino_repls, &sjir);
	}

 out:
	if (fcmh)
		fcmh_op_done_type(fcmh, FCMH_OPCNT_LOOKUP_FIDC);

	return (0);
}

int
slm_rmc_handle_set_bmapreplpol(struct pscrpc_request *rq)
{
	struct srm_set_bmapreplpol_req *mq;
	struct srm_set_bmapreplpol_rep *mp;
	struct fcmh_mds_info *fmi;
	struct fidc_membh *fcmh;
	struct bmapc_memb *bcm;

	fmi = NULL;

	SL_RSX_ALLOCREP(rq, mq, mp);

	if (mq->pol < 0 || mq->pol >= NBRPOL) {
		mp->rc = EINVAL;
		return (0);
	}

	mp->rc = slm_fcmh_get(&mq->fg, &fcmh);
	if (mp->rc)
		goto out;

	fmi = fcmh_2_fmi(fcmh);

	if (!mds_bmap_exists(fcmh, mq->bmapno)) {
		mp->rc = SLERR_BMAP_INVALID;
		goto out;
	}
	mp->rc = mds_bmap_load(fcmh, mq->bmapno, &bcm);
	if (mp->rc)
		goto out;

	BHREPL_POLICY_SET(bcm, mq->pol);
	mds_bmap_write_repls_rel(bcm);

 out:
	if (fcmh)
		fcmh_op_done_type(fcmh, FCMH_OPCNT_LOOKUP_FIDC);
	return (0);
}

int
slm_rmc_handle_statfs(struct pscrpc_request *rq)
{
	struct srm_statfs_req *mq;
	struct srm_statfs_rep *mp;
	struct sl_mds_iosinfo *si;
	struct sl_resource *res;
	struct statvfs sfb;

	SL_RSX_ALLOCREP(rq, mq, mp);
	mp->rc = mdsio_statfs(&sfb);
	res = libsl_id2res(mq->iosid);
	if (res == NULL) {
		mp->rc = SLERR_RES_UNKNOWN;
		return (0);
	}
	si = res2iosinfo(res);
	sl_externalize_statfs(&sfb, &mp->ssfb);
	mp->ssfb.sf_bsize	= si->si_ssfb.sf_bsize;
	mp->ssfb.sf_frsize	= si->si_ssfb.sf_frsize;
	mp->ssfb.sf_blocks	= si->si_ssfb.sf_blocks;
	mp->ssfb.sf_bfree	= si->si_ssfb.sf_bfree;
	mp->ssfb.sf_bavail	= si->si_ssfb.sf_bavail;
	return (0);
}

int
slm_rmc_handle_symlink(struct pscrpc_request *rq)
{
	char linkname[SL_PATH_MAX];
	struct fidc_membh *p = NULL;
	struct srm_symlink_req *mq;
	struct srm_symlink_rep *mp;
	struct iovec iov;

	SL_RSX_ALLOCREP(rq, mq, mp);
	mq->name[sizeof(mq->name) - 1] = '\0';
	if (mq->linklen == 0 || mq->linklen >= SL_PATH_MAX)
		return (EINVAL);

	iov.iov_base = linkname;
	iov.iov_len = mq->linklen;
	mp->rc = rsx_bulkserver(rq, BULK_GET_SINK, SRMC_BULK_PORTAL,
	    &iov, 1);
	if (mp->rc)
		return (mp->rc);

	mp->rc = slm_fcmh_get(&mq->pfg, &p);
	if (mp->rc)
		goto out;

	linkname[mq->linklen] = '\0';

	mds_reserve_slot();
	mp->rc = mdsio_symlink(linkname, fcmh_2_mdsio_fid(p), mq->name,
	    &mq->creds, &mp->cattr, NULL, slm_get_next_slashid,
	    mdslog_namespace);
	mds_unreserve_slot();

	mdsio_fcmh_refreshattr(p, &mp->pattr);
 out:
	if (p)
		fcmh_op_done_type(p, FCMH_OPCNT_LOOKUP_FIDC);
	return (0);
}

int
slm_rmc_handle_unlink(struct pscrpc_request *rq, int isfile)
{
	struct fidc_membh *p = NULL;
	struct srm_unlink_req *mq;
	struct srm_unlink_rep *mp;
	struct slash_fidgen fg;

	SL_RSX_ALLOCREP(rq, mq, mp);

	fg.fg_fid = mq->pfid;
	fg.fg_gen = FGEN_ANY;
	mq->name[sizeof(mq->name) - 1] = '\0';
	mp->rc = slm_fcmh_get(&fg, &p);
	if (mp->rc)
		goto out;

	if (IS_REMOTE_FID(mq->pfid)) {
		mp->rc = slm_rmm_forward_namespace(isfile ?
		    SLM_FORWARD_UNLINK : SLM_FORWARD_RMDIR, &fg,
		    NULL, mq->name, NULL, 0, NULL, NULL, 0);
		goto out;
	}

	mds_reserve_slot();
	if (isfile)
		mp->rc = mdsio_unlink(fcmh_2_mdsio_fid(p), NULL,
		    mq->name, &rootcreds, mdslog_namespace);
	else
		mp->rc = mdsio_rmdir(fcmh_2_mdsio_fid(p), NULL,
		    mq->name, &rootcreds, mdslog_namespace);
	mds_unreserve_slot();

 out:
	if (mp->rc == 0) {
		FCMH_LOCK(p);
		fcmh_wait_locked(p, p->fcmh_flags & FCMH_IN_SETATTR);
		SL_GETTIMESPEC(&p->fcmh_sstb.sst_ctim);
		mds_fcmh_setattr(p, PSCFS_SETATTRF_CTIME);
		memcpy(&mp->attr, &p->fcmh_sstb, sizeof(mp->attr));
	}
	psclog_info("unlink parent="SLPRI_FID" name=%s rc=%d",
	    mq->pfid, mq->name, mp->rc);
	if (p)
		fcmh_op_done_type(p, FCMH_OPCNT_LOOKUP_FIDC);
	return (0);
}

int
slm_rmc_handle_addreplrq(struct pscrpc_request *rq)
{
	const struct srm_replrq_req *mq;
	struct srm_replrq_rep *mp;

	SL_RSX_ALLOCREP(rq, mq, mp);
	mp->rc = mds_repl_addrq(&mq->fg,
	    mq->bmapno, mq->repls, mq->nrepls);
	return (0);
}

int
slm_rmc_handle_delreplrq(struct pscrpc_request *rq)
{
	const struct srm_replrq_req *mq;
	struct srm_replrq_rep *mp;

	SL_RSX_ALLOCREP(rq, mq, mp);
	mp->rc = mds_repl_delrq(&mq->fg,
	    mq->bmapno, mq->repls, mq->nrepls);
	return (0);
}

int
slm_rmc_handle_getreplst(struct pscrpc_request *rq)
{
	struct srm_replst_master_req *mq;
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
			rc = SLERR_NOTCONN;
		EXPORT_ULOCK(rq->rq_export);
		if (rc)
			goto out;
	}

	switch (rq->rq_reqmsg->opc) {
	/* bmap messages */
	case SRMT_BMAPCHWRMODE:
		rc = slm_rmc_handle_bmap_chwrmode(rq);
		break;
	case SRMT_EXTENDBMAPLS:
		rc = slm_rmc_handle_extendbmapls(rq);
		break;
	case SRMT_GETBMAP:
		rc = slm_rmc_handle_getbmap(rq);
		break;
	case SRMT_RELEASEBMAP:
		rc = slm_rmc_handle_rls_bmap(rq);
		break;

	/* replication messages */
	case SRMT_SET_NEWREPLPOL:
		rc = slm_rmc_handle_set_newreplpol(rq);
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
		rc = slm_rmc_handle_connect(rq);
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
	default:
		psclog_errorx("Unexpected opcode %d", rq->rq_reqmsg->opc);
		rq->rq_status = -ENOSYS;
		return (pscrpc_error(rq));
	}
 out:
	authbuf_sign(rq, PSCRPC_MSG_REPLY);
	pscrpc_target_send_reply_msg(rq, -(abs(rc)), 0);
	return (rc);
}

void
mexpc_destroy(void *arg)
{
	struct bmap_mds_lease *bml, *tmp;
	struct slm_exp_cli *mexpc = arg;

	psclist_for_each_entry_safe(bml, tmp, &mexpc->mexpc_bmlhd,
	    bml_exp_lentry) {
		BML_LOCK(bml);
		psc_assert(bml->bml_flags & BML_EXP);
		bml->bml_flags &= ~BML_EXP;
		bml->bml_flags |= BML_EXPFAIL;
		BML_ULOCK(bml);
		psclist_del(&bml->bml_exp_lentry, &mexpc->mexpc_bmlhd);
	}
}

void
mexpc_allocpri(struct pscrpc_export *exp)
{
	struct slm_cli_csvc_cpart *mcccp;
	struct slm_exp_cli *mexpc;

	mexpc = exp->exp_private = PSCALLOC(sizeof(*mexpc));
	mcccp = mexpc->mexpc_cccp = PSCALLOC(sizeof(*mcccp));
	INIT_PSCLIST_HEAD(&mexpc->mexpc_bmlhd);
	INIT_SPINLOCK(&mcccp->mcccp_lock);
	psc_waitq_init(&mcccp->mcccp_waitq);
	slm_getclcsvc(exp);
}

struct sl_expcli_ops sl_expcli_ops = {
	mexpc_allocpri,
	mexpc_destroy
};
