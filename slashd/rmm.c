/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2011, Pittsburgh Supercomputing Center (PSC).
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
 * Routines for handling RPC requests for MDS from MDS.
 */

#define PSC_SUBSYS PSS_RPC

#include <fcntl.h>
#include <stdio.h>

#include "pfl/str.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/service.h"
#include "psc_util/lock.h"

#include "fid.h"
#include "fidc_mds.h"
#include "rpc_mds.h"
#include "slashd.h"
#include "slashrpc.h"
#include "slconn.h"
#include "slerr.h"
#include "sljournal.h"

#include "zfs-fuse/zfs_slashlib.h"

static int forward_not_ready = 1;

int
slm_rmm_apply_update(struct srt_update_entry *entryp)
{
	struct sl_mds_peerinfo *localinfo;
	struct slmds_jent_namespace sjnm;
	int rc;

	memset(&sjnm, 0, sizeof(sjnm));
	sjnm.sjnm_op = entryp->op;
	sjnm.sjnm_uid = entryp->uid;
	sjnm.sjnm_gid = entryp->gid;
	sjnm.sjnm_mask = entryp->mask;
	sjnm.sjnm_mode = entryp->mode;
	sjnm.sjnm_size = entryp->size;

	sjnm.sjnm_atime = entryp->atime;
	sjnm.sjnm_mtime = entryp->mtime;
	sjnm.sjnm_ctime = entryp->ctime;
	sjnm.sjnm_parent_fid = entryp->parent_fid;
	sjnm.sjnm_target_fid = entryp->target_fid;

	sjnm.sjnm_namelen = entryp->namelen;
	sjnm.sjnm_namelen2 = entryp->namelen2;
	memcpy(sjnm.sjnm_name, entryp->name,
	    entryp->namelen + entryp->namelen2);

	localinfo = res2rpmi(nodeResProf)->rpmi_info;
	rc = mds_redo_namespace(&sjnm, 0);
	if (rc)
		psc_atomic32_inc(&localinfo->sp_stats.ns_stats[NS_DIR_RECV]
		    [sjnm.sjnm_op][NS_SUM_FAIL]);
	else
		psc_atomic32_inc(&localinfo->sp_stats.ns_stats[NS_DIR_RECV]
		    [sjnm.sjnm_op][NS_SUM_SUCC]);
	return (rc);
}

/**
 * slm_rmm_handle_connect - Handle a CONNECT request from another MDS.
 */
int
slm_rmm_handle_connect(struct pscrpc_request *rq)
{
	struct srm_connect_req *mq;
	struct srm_connect_rep *mp;

	SL_RSX_ALLOCREP(rq, mq, mp);
	if (mq->magic != SRMM_MAGIC || mq->version != SRMM_VERSION)
		mp->rc = EINVAL;
	return (0);
}

/**
 * slm_rmm_handle_namespace_update - Handle a NAMESPACE_UPDATE request from
 *	another MDS.
 */
int
slm_rmm_handle_namespace_update(struct pscrpc_request *rq)
{
	struct srt_update_entry *entryp;
	struct srm_update_req *mq;
	struct srm_update_rep *mp;
	struct sl_mds_peerinfo *p;
	struct sl_resource *res;
	struct sl_site *site;
	struct iovec iov;
	uint64_t crc, seqno;
	int i, len, count;

	SL_RSX_ALLOCREP(rq, mq, mp);

	count = mq->count;
	seqno = mq->seqno;
	if (count <= 0 || mq->size > LNET_MTU)
		return (EINVAL);

	iov.iov_len = mq->size;
	iov.iov_base = PSCALLOC(mq->size);

	mp->rc = rsx_bulkserver(rq, BULK_GET_SINK, SRMM_BULK_PORTAL,
	    &iov, 1);
	if (mp->rc)
		goto out;

	psc_crc64_calc(&crc, iov.iov_base, iov.iov_len);
	if (crc != mq->crc) {
		mp->rc = EINVAL;
		goto out;
	}

	/* Search for the peer information by the given site ID. */
	site = libsl_siteid2site(mq->siteid);
	p = NULL;
	if (site)
		SITE_FOREACH_RES(site, res, i)
			if (res->res_type == SLREST_MDS) {
				p = res2rpmi(res)->rpmi_info;
				break;
			}
	if (p == NULL) {
		psclog_info("fail to find site ID %d", mq->siteid);
		mp->rc = EINVAL;
		goto out;
	}

	/*
	 * Iterate through the namespace update buffer and apply updates.
	 * If we fail to apply an update, we still report success to our
	 * peer because reporting an error does not help our cause.
	 */
	entryp = iov.iov_base;
	for (i = 0; i < count; i++) {
		slm_rmm_apply_update(entryp);
		len = UPDATE_ENTRY_LEN(entryp);
		entryp = PSC_AGP(entryp, len);
	}
	zfsslash2_wait_synced(0);

 out:
	PSCFREE(iov.iov_base);
	return (mp->rc);
}

/**
 * slm_rmm_handle_namespace_forward - Handle a NAMESPACE_FORWARD request from
 *	another MDS.
 */
int
slm_rmm_handle_namespace_forward(struct pscrpc_request *rq)
{
	struct srm_forward_req *mq;
	struct srm_forward_rep *mp;
	struct fidc_membh *p = NULL;
	void *mdsio_data;

	SL_RSX_ALLOCREP(rq, mq, mp);

	if (mq->op != SLM_FORWARD_MKDIR && mq->op != SLM_FORWARD_RMDIR &&
	    mq->op != SLM_FORWARD_CREATE && mq->op != SLM_FORWARD_UNLINK &&
	    mq->op != SLM_FORWARD_SETATTR) {
		mp->rc = EINVAL;
		return (0);
	}

	psclog_info("op=%d, fid="SLPRI_FID", name=%s", mq->op, mq->fid, mq->req.name);

	mds_reserve_slot();
	switch (mq->op) {
	    case SLM_FORWARD_MKDIR:
		mp->rc = slm_fcmh_get(&mq->pfg, &p);
		if (mp->rc)
			break;
		mp->rc = mdsio_mkdir(fcmh_2_mdsio_fid(p), mq->req.name, mq->mode,
		    &mq->creds, &mp->cattr, NULL, mds_namespace_log,
		    NULL, mq->fid);
		break;
	    case SLM_FORWARD_CREATE:
		mp->rc = slm_fcmh_get(&mq->pfg, &p);
		if (mp->rc)
			break;
		mp->rc = mdsio_opencreate(fcmh_2_mdsio_fid(p), &mq->creds,
		    O_CREAT | O_EXCL | O_RDWR, mq->mode, mq->req.name, NULL,
		    &mp->cattr, &mdsio_data, mds_namespace_log,
		    NULL, mq->fid);
		if (!mp->rc)
			mdsio_release(&rootcreds, mdsio_data);
		break;
	    case SLM_FORWARD_RMDIR:
		mp->rc = slm_fcmh_get(&mq->pfg, &p);
		if (mp->rc)
			break;
		mp->rc = mdsio_rmdir(fcmh_2_mdsio_fid(p), &mp->fid,
		    mq->req.name, &rootcreds, mds_namespace_log);
		break;
	    case SLM_FORWARD_UNLINK:
		mp->rc = slm_fcmh_get(&mq->pfg, &p);
		if (mp->rc)
			break;
		mp->rc = mdsio_unlink(fcmh_2_mdsio_fid(p), &mp->fid,
		    mq->req.name, &rootcreds, mds_namespace_log);
		break;
	    case SLM_FORWARD_SETATTR:
		mp->rc = slm_fcmh_get(&mq->pfg, &p);
		if (mp->rc)
			break;
		mp->rc = mdsio_setattr(fcmh_2_mdsio_fid(p),
		    &mq->req.sstb, mq->to_set, &rootcreds, &mp->cattr, 
		    fcmh_2_mdsio_data(p), mds_namespace_log);
		break;
	}
	mds_unreserve_slot();
	if (p)
		fcmh_op_done_type(p, FCMH_OPCNT_LOOKUP_FIDC);
	return (mp->rc);
}

/**
 * slm_rmm_handler - Handle a request for MDS from another MDS.
 */
int
slm_rmm_handler(struct pscrpc_request *rq)
{
	int rc;

	rq->rq_status = SL_EXP_REGISTER_RESM(rq->rq_export,
	    slm_getmcsvcx(_resm, rq->rq_export));
	if (rq->rq_status)
		return (pscrpc_error(rq));

	switch (rq->rq_reqmsg->opc) {
	case SRMT_CONNECT:
		rc = slm_rmm_handle_connect(rq);
		break;
	case SRMT_NAMESPACE_UPDATE:
		rc = slm_rmm_handle_namespace_update(rq);
		break;
	case SRMT_NAMESPACE_FORWARD:
		rc = slm_rmm_handle_namespace_forward(rq);
		break;
	default:
		psclog_errorx("unexpected opcode %d", rq->rq_reqmsg->opc);
		rq->rq_status = -ENOSYS;
		return (pscrpc_error(rq));
	}
	authbuf_sign(rq, PSCRPC_MSG_REPLY);
	pscrpc_target_send_reply_msg(rq, rc, 0);
	return (rc);
}

int
slm_rmm_forward_namespace(int op, const struct slash_fidgen *pfg,
    char *name, uint32_t mode, const struct slash_creds *creds, 
    struct srt_stat *sstb, int32_t to_set)
{
	int rc, _siter;
	struct sl_resm *resm;
	struct sl_site *site;
	struct sl_resource *res;
	struct slashrpc_cservice *csvc;

	struct srm_forward_req *mq;
	struct srm_forward_rep *mp;
	struct pscrpc_request *rq;
	sl_siteid_t siteid;

	siteid = FID_GET_SITEID(pfg->fg_fid);

#if 1
	if (forward_not_ready)
		return (ENOSYS);
#endif
	if (op != SLM_FORWARD_MKDIR && op != SLM_FORWARD_RMDIR &&
	    op != SLM_FORWARD_CREATE && op != SLM_FORWARD_UNLINK &&
	    op != SLM_FORWARD_SETATTR)
		return (ENOSYS);

	site = libsl_siteid2site(siteid);
	if (site == NULL)
		return (EBADF);

	SITE_FOREACH_RES(site, res, _siter) {
		if (res->res_type != SLREST_MDS)
			continue;
		resm = psc_dynarray_getpos(&res->res_members, 0);
		break;
	}
	csvc = slm_getmcsvc(resm);
	if (csvc == NULL)  {
		psclog_info("Fail to connect to site %d", siteid);
		return (EIO);
	}

	rc = SL_RSX_NEWREQ(csvc, SRMT_NAMESPACE_FORWARD, rq, mq, mp);
	if (rc) {
		sl_csvc_decref(csvc);
		return (EIO);
	}

	mq->op = op;
	mq->pfg	= *pfg;

	if (op == SLM_FORWARD_SETATTR) {
		mq->to_set = to_set;
		mq->req.sstb = *sstb;
	} else
		strlcpy(mq->req.name, name, sizeof(mq->req.name));

	if (op == SLM_FORWARD_MKDIR || op == SLM_FORWARD_CREATE) {
		mq->mode = mode;
		mq->creds = *creds;
		mq->fid = slm_get_next_slashid();
	} else {
		mq->mode = 0;
		mq->creds.scr_uid = 0;		/* XXX */
		mq->creds.scr_gid = 0;
		mq->fid = 0;
	}

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc)
		goto out;

	switch (op) {
	    case SLM_FORWARD_MKDIR:
		rc = mdsio_redo_mkdir(mq->pfg.fg_fid, name, &mp->cattr);
		if (!rc)
			*sstb = mp->cattr;
		break;
	    case SLM_FORWARD_RMDIR:
		rc = mdsio_redo_rmdir(mq->pfg.fg_fid, mp->fid, name);
		break;
	    case SLM_FORWARD_CREATE:
		rc = mdsio_redo_create(mq->pfg.fg_fid, name, &mp->cattr);
		if (!rc)
			*sstb = mp->cattr;
		break;
	    case SLM_FORWARD_UNLINK:
		rc = mdsio_redo_unlink(mq->pfg.fg_fid, mp->fid, name);
		break;
	    case SLM_FORWARD_SETATTR:
		rc = mdsio_redo_setattr(mq->fid, mq->to_set, sstb);
		break;
	}

 out:
	pscrpc_req_finished(rq);
	sl_csvc_decref(csvc);

	return (rc);
}
