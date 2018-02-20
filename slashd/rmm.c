/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright (c) 2008-2015, Pittsburgh Supercomputing Center (PSC).
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

/*
 * Routines for handling RPC requests for MDS from MDS.
 */

#define PSC_SUBSYS PSS_RPC

#include <fcntl.h>
#include <stdio.h>

#include "pfl/str.h"
#include "pfl/rpc.h"
#include "pfl/rpclog.h"
#include "pfl/rsx.h"
#include "pfl/service.h"
#include "pfl/lock.h"

#include "batchrpc.h"
#include "fid.h"
#include "fidc_mds.h"
#include "journal_mds.h"
#include "rpc_mds.h"
#include "slashd.h"
#include "slashrpc.h"
#include "slconn.h"
#include "slerr.h"

#include "zfs-fuse/zfs_slashlib.h"

int
slm_rmm_apply_update(__unusedx struct srt_update_entry *entryp)
{
	psc_fatal("obsolete code path");
}

/*
 * Handle a NAMESPACE_UPDATE request from another MDS.
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
	int i, len, count;

	SL_RSX_ALLOCREP(rq, mq, mp);

	count = mq->count;
	if (count <= 0 || mq->size > LNET_MTU) {
		mp->rc = -EINVAL;
		return (mp->rc);
	}

	iov.iov_len = mq->size;
	iov.iov_base = PSCALLOC(mq->size);

	mp->rc = slrpc_bulkserver(rq, BULK_GET_SINK, SRMM_BULK_PORTAL,
	    &iov, 1);
	if (mp->rc)
		goto out;

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
		PFL_GOTOERR(out, mp->rc = -EINVAL);
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

/*
 * Handle a NAMESPACE_FORWARD request from another MDS.
 */
int
slm_rmm_handle_namespace_forward(struct pscrpc_request *rq)
{
	char *from, *to, *name, *linkname;
	struct fidc_membh *p = NULL, *op = NULL, *np = NULL;
	struct srm_forward_req *mq;
	struct srm_forward_rep *mp;
	struct slash_creds cr;
	struct srt_stat sstb;
	void *mfh;
	int vfsid;

	p = op = np = NULL;
	SL_RSX_ALLOCREP(rq, mq, mp);

	if (mq->op != SLM_FORWARD_MKDIR &&
	    mq->op != SLM_FORWARD_RMDIR &&
	    mq->op != SLM_FORWARD_CREATE &&
	    mq->op != SLM_FORWARD_UNLINK &&
	    mq->op != SLM_FORWARD_SYMLINK &&
	    mq->op != SLM_FORWARD_RENAME &&
	    mq->op != SLM_FORWARD_SETATTR) {
		mp->rc = -EINVAL;
		return (0);
	}

	psclog_info("op=%d, name=%s", mq->op, mq->req.name);

	mp->rc = slfid_to_vfsid(mq->fg.fg_fid, &vfsid);
	if (mp->rc)
		return (0);
	if (current_vfsid != vfsid) {
		mp->rc = -EINVAL;
		return (0);
	}

	cr.scr_uid = mq->creds.scr_uid;
	cr.scr_gid = mq->creds.scr_gid;

	mds_reserve_slot(2);
	switch (mq->op) {
	    case SLM_FORWARD_MKDIR:
		mp->rc = -slm_fcmh_get(&mq->fg, &p);
		if (mp->rc)
			break;
		sstb.sst_mode = mq->mode;
		sstb.sst_uid = mq->creds.scr_uid;
		sstb.sst_gid = mq->creds.scr_gid;
		mp->rc = -mdsio_mkdir(vfsid, fcmh_2_mfid(p),
		    mq->req.name, &sstb, 0, 0, &mp->attr, NULL,
		    mdslog_namespace, slm_get_next_slashfid, 0);
		break;
	    case SLM_FORWARD_CREATE:
		mp->rc = -slm_fcmh_get(&mq->fg, &p);
		if (mp->rc)
			break;
		mp->rc = -mdsio_opencreate(vfsid, fcmh_2_mfid(p),
		    &cr, O_CREAT | O_EXCL | O_RDWR, mq->mode,
		    mq->req.name, NULL, &mp->attr, &mfh,
		    mdslog_namespace, slm_get_next_slashfid, 0);
		if (!mp->rc)
			mdsio_release(vfsid, &rootcreds, mfh);
		break;
	    case SLM_FORWARD_RMDIR:
		mp->rc = -slm_fcmh_get(&mq->fg, &p);
		if (mp->rc)
			break;
		mp->rc = -mdsio_rmdir(vfsid, fcmh_2_mfid(p), NULL,
		    mq->req.name, &rootcreds, mdslog_namespace);
		break;
	    case SLM_FORWARD_UNLINK:
		mp->rc = -slm_fcmh_get(&mq->fg, &p);
		if (mp->rc)
			break;
		mp->rc = -mdsio_unlink(vfsid, fcmh_2_mfid(p), NULL,
		    mq->req.name, &rootcreds, mdslog_namespace,
		    &mp->attr);
		break;
	    case SLM_FORWARD_RENAME:
		mp->rc = -slm_fcmh_get(&mq->fg, &op);
		if (mp->rc)
			break;
		mp->rc = -slm_fcmh_get(&mq->nfg, &np);
		if (mp->rc)
			break;
		from = mq->req.name;
		to = mq->req.name + strlen(mq->req.name) + 1;
		mp->rc = -mdsio_rename(vfsid, fcmh_2_mfid(op), from,
		    fcmh_2_mfid(np), to, &rootcreds,
		    mdslog_namespace, &mp->attr);
		break;
	    case SLM_FORWARD_SETATTR:
		/*
		 * This is tough because we have some logic at the fcmh
		 * layer dealing with (partial) truncates.  It is not a
		 * pure namespace operation.
		 */
		mp->rc = -slm_fcmh_get(&mq->fg, &p);
		if (mp->rc)
			break;
		mp->rc = -mdsio_setattr(vfsid, fcmh_2_mfid(p),
		    &mq->req.sstb, mq->to_set, &rootcreds, &mp->attr,
		    fcmh_2_mfh(p), mdslog_namespace);
		break;
	    case SLM_FORWARD_SYMLINK:
		mp->rc = -slm_fcmh_get(&mq->fg, &p);
		if (mp->rc)
			break;
		name = mq->req.name;
		linkname = mq->req.name + strlen(mq->req.name) + 1;
		mp->rc = -mdsio_symlink(vfsid, linkname,
		    fcmh_2_mfid(p), name, &cr, &mp->attr, NULL,
		    NULL, slm_get_next_slashfid, 0);
		break;
	}
	mds_unreserve_slot(2);
	if (p)
		fcmh_op_done(p);
	if (op)
		fcmh_op_done(op);
	if (np)
		fcmh_op_done(np);
	return (0);
}

/*
 * Handle a request for MDS from another MDS.
 */
int
slm_rmm_handler(struct pscrpc_request *rq)
{
	int rc;

	rq->rq_status = SL_EXP_REGISTER_RESM(rq->rq_export,
	    slm_getmcsvcx(_resm, rq->rq_export, 0));
	if (rq->rq_status)
		return (pscrpc_error(rq));

	switch (rq->rq_reqmsg->opc) {
	case SRMT_CONNECT:
		rc = slrpc_handle_connect(rq, SRMM_MAGIC, SRMM_VERSION,
		    SLCONNT_MDS);
		break;
	case SRMT_BATCH_RP:
		rc = slrpc_batch_handle_reply(rq);
		break;
	case SRMT_NAMESPACE_UPDATE:
		rc = slm_rmm_handle_namespace_update(rq);
		break;
	case SRMT_NAMESPACE_FORWARD:
		rc = slm_rmm_handle_namespace_forward(rq);
		break;
	default:
		psclog_errorx("unexpected opcode %d", rq->rq_reqmsg->opc);
		rq->rq_status = -PFLERR_NOSYS;
		return (pscrpc_error(rq));
	}
	slrpc_rep_out(rq);
	pscrpc_target_send_reply_msg(rq, rc, 0);
	return (rc);
}

/*
 * Forward a name space operation to a remote MDS first before
 * replicating the operation locally by our callers.
 */
int
slm_rmm_forward_namespace(int op, struct sl_fidgen *fg,
    struct sl_fidgen *nfg, char *name, char *newname, uint32_t mode,
    const struct slash_creds *crp, struct srt_stat *sstb,
    int32_t to_set)
{
	struct slrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct sl_resm *resm = NULL;
	struct srm_forward_req *mq;
	struct srm_forward_rep *mp;
	struct sl_resource *res;
	struct sl_site *site;
	int rc, len, i, vfsid;
	sl_siteid_t siteid;

	if (op != SLM_FORWARD_MKDIR  && op != SLM_FORWARD_RMDIR &&
	    op != SLM_FORWARD_CREATE && op != SLM_FORWARD_UNLINK &&
	    op != SLM_FORWARD_RENAME && op != SLM_FORWARD_SETATTR &&
	    op != SLM_FORWARD_SYMLINK)
		return (-PFLERR_NOSYS);

	rc = slfid_to_vfsid(fg->fg_fid, &vfsid);
	if (rc)
		return (rc);

	siteid = FID_GET_SITEID(fg->fg_fid);
	site = libsl_siteid2site(siteid);
	if (site == NULL)
		return (-EBADF);

	SITE_FOREACH_RES(site, res, i) {
		if (res->res_type != SLREST_MDS)
			continue;
		resm = psc_dynarray_getpos(&res->res_members, 0);
		break;
	}
	csvc = slm_getmcsvc_wait(resm, 0);
	if (csvc == NULL) {
		psclog_info("unable to connect to site %d", siteid);
		return (-EIO);
	}

	rc = SL_RSX_NEWREQ(csvc, SRMT_NAMESPACE_FORWARD, rq, mq, mp);
	if (rc)
		goto out;

	mq->op = op;
	mq->fg = *fg;

	if (op == SLM_FORWARD_SETATTR) {
		mq->to_set = to_set;
		mq->req.sstb = *sstb;
	} else
		strlcpy(mq->req.name, name, sizeof(mq->req.name));

	if (op == SLM_FORWARD_RENAME || op == SLM_FORWARD_SYMLINK) {
		if (op == SLM_FORWARD_RENAME)
			mq->nfg	= *nfg;
		len = strlen(name) + 1;
		strlcpy(mq->req.name + len, newname,
		    sizeof(mq->req.name) - len);
	}

	if (op == SLM_FORWARD_MKDIR ||
	    op == SLM_FORWARD_CREATE ||
	    op == SLM_FORWARD_SYMLINK) {
		mq->mode = mode;
		mq->creds.scr_uid = crp->scr_uid;
		mq->creds.scr_gid = crp->scr_gid;
	}

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (!rc)
		rc = mp->rc;
	if (!rc && sstb)
		*sstb = mp->attr;

 out:
	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
	return (rc);
}
