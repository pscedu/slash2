/* $Id$ */

#include <stdio.h>

#include "psc_rpc/rpc.h"
#include "psc_util/cdefs.h"

#include "mount_slash.h"
#include "rpc.h"

int
slashrpc_timeout(__unusedx void *arg)
{
	return (0);
}

int
slash_readdir_fill(void *buf, fuse_fill_dir_t filler, int nents,
    struct slashrpc_readdir_bulk *mb)
{
	struct slashrpc_readdir_ent *me;
	struct pscrpc_request *rq;
	struct stat stb;
	int rc, j;

	for (j = 0; j < (int)mp->nents; j++) {
		me = mb->ents[j];
		memset(&stb, 0, sizeof(stb));
		stb.st_ino = me->ino;
		stb.st_mode = me->mode;
		if (filler(buf, me->name, &stb, 1))
			return (1);
	}
	return (0);
}

__inline int
slashrpc_readdir(struct pscrpc_request *rq, struct ciod_ingest *ci)
{
	struct slashrpc_readdir_bulk *mb;
	struct slashrpc_readdir_rep *mp;
	struct slashrpc_readdir_ent *me;
	struct pscrpc_bulk_desc *desc;
	struct l_wait_info lwi;
	int i, rc, comms_error;
	u8 *v1;

	comms_error = 0;

	mp = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*cw));
	if (mp == NULL) {
		zwarnx("readdir reply body is null");
		rc = errno;
		goto out;
	}

	mb = PSCALLOC(mp->nents * sizeof(*me));

	/* GET_SINK the data. */
	desc = pscrpc_prep_bulk_exp(rq, 1, BULK_GET_SINK, RPCIO_BULK_PORTAL);
	if (desc == NULL) {
		psc_warnx("pscrpc_prep_bulk_exp returned a null desc");
		rc = ENOMEM; // errno
		goto out;
	}
	desc->bd_iov_count = 1;
	desc->bd_iov[0].iov_base = mb;
	desc->bd_iov[0].iov_len = desc->bd_nob = mp->nents * sizeof(*me);

	/* Check for client eviction during previous I/O before proceeding. */
	if (desc->bd_export->exp_failed)
		rc = ENOTCONN;
	else
		rc = pscrpc_start_bulk_transfer(desc);
	if (rc == 0) {
		lwi = LWI_TIMEOUT_INTERVAL(OBD_TIMEOUT / 2,
		    HZ, slashrpc_io_bulk_timeout, desc);

		rc = psc_svr_wait_event(&desc->bd_waitq,
		    (!pscrpc_bulk_active(desc) || desc->bd_export->exp_failed),
		    &lwi, NULL);

		LASSERT(rc == 0 || rc == -ETIMEDOUT);
		if (rc == -ETIMEDOUT) {
			psc_errorx("timeout on bulk GET");
			pscrpc_abort_bulk(desc);
		} else if (desc->bd_export->exp_failed) {
			psc_warnx("eviction on bulk GET");
			rc = -ENOTCONN;
			pscrpc_abort_bulk(desc);
		} else if (!desc->bd_success ||
		    desc->bd_nob_transferred != desc->bd_nob) {
			psc_errorx("%s bulk GET %d(%d)",
			    desc->bd_success ? "truncated" : "network error on",
			    desc->bd_nob_transferred, desc->bd_nob);
			/* XXX should this be a different errno? */
			rc = -ETIMEDOUT;
		}
	} else {
		psc_errorx("pscrpc I/O bulk get failed: rc %d", rc);
	}
	comms_error = (rc != 0);

	/* count the number of bytes sent, and hold for later... */
	if (rc == 0) {
		v1 = desc->bd_iov[0].iov_base;
		if (v1 == NULL) {
			psc_errorx("desc->bd_iov[0].iov_base is NULL");
			rc = ENXIO;
			goto out;
		}

		psc_info("got %u bytes of bulk data across %d IOVs: "
		      "first byte is 0x%x",
		      desc->bd_nob, desc->bd_iov_count, *v1);

		sum = 0;
		for (i = 0; i < desc->bd_iov_count; i++)
			sum += desc->bd_iov[i].iov_len;
		if (sum != desc->bd_nob)
			psc_warnx("sum (%d) does not match bd_nob (%d)",
			    sum, desc->bd_nob);
		//rc = pscrpc_reply(rq);
	}

 out:
	if (rc == 0) {
		if (slash_readdir_fill(mb))
			/* Send back reply informing server to stop. */
	} else if (!comms_error) {
		/* Only reply if there were no comm problems with bulk. */
		rq->rq_status = rc;
		pscrpc_error(rq);
	} else {
#if 0
		// For now let's not free the reply state..
		if (rq->rq_reply_state != NULL) {
			/* reply out callback would free */
			pscrpc_rs_decref(rq->rq_reply_state);
			rq->rq_reply_state = NULL;
			rq->rq_repmsg      = NULL;
		}
#endif
		CWARN("ignoring bulk I/O comm error; "
		    "id %s - client will retry",
		    libcfs_id2str(rq->rq_peer));
	}
	pscrpc_free_bulk(desc);
	free(mb);
	return (rc);
}

int
slashrpc_handle_readdir(struct pscrpc_request *rq)
{
	struct zestion_rpciothr *zri;
	struct zestion_thread *zthr;
	struct zio_reply_body *repbody;
	struct ciod_wire *cw = NULL;
	struct zlist_head *e;
	struct zeil *zeil;
	int size, rc;

	zthr = zestion_threadtbl_get();
	zri = &zthr->zthr_rpciothr;

	if (zri->zri_ci == NULL) {
		/* Save in case we bail from error to prevent leak. */
		e = lc_getwait(&ciodiFreeList);
	}

	/* Ensure we reply back to the server. */
	size = sizeof(*repbody);
	rc = zest_pack_reply(req, 1, &size, NULL);
	if (rc) {
		zest_assert(rc == -ENOMEM);
		znotify("zest_pack_reply failed");
		/* the client will probably bomb here */
		return rc;
	}

	rc = zio_handle_write_rpc(req, zri->zri_ci);
	if (rc)
		goto done;

	cw = zest_msg_buf(req->rq_reqmsg, 0, sizeof(*cw));
	zest_assert(cw);

	/* Place the chunk on the inode's incoming list. */
	rc = ciodi_put(zri->zri_ci, zeil);
	/* Clear out ciod since it is now in use. */
	if (rc == 0)
		zri->zri_ci = NULL;

 done:
	repbody = zest_msg_buf(req->rq_repmsg, 0, size);
	zest_assert(repbody);
	/*
	 * If rc != 0 then cw was not assigned.
	 */
	if (cw != NULL) {
		repbody->nbytes = cw->ciodw_len;
		repbody->crc_meta_magic = cw->ciodw_crc_meta;
	}
	/* Grab a new ciodi for the next write(). */
	if (zri->zri_ci == NULL) {
		e = zlist_cache_get(&ciodiFreeList, 1);
		zri->zri_ci = zlist_entry(e, struct ciod_ingest,
					  ciodi_zig_entry);
	}
	return (rc);
}

int
slashrpc_io_handler(struct pscrpc_request *rq)
{
	int rc;

	rc = 0;
	switch (rq->rq_reqmsg->opc) {
	case SLASH_IOP_READDIR:
		rc = slashrpc_handle_write(req);
		break;
	default:
		psc_errorx("Unexpected opcode %d", rq->rq_reqmsg->opc);
		rq->rq_status = -ENOSYS;
		rc = pscrpc_error(rq);
		return (rc);
	}
	rq->rq_status = rc;
	target_send_reply_msg(req, rc, 0);
	return (0);
}










































int
slash_read(__unusedx const char *path, char *buf, size_t size,
    off_t offset, struct fuse_file_info *fi)
{
	struct slashrpc_read_req *mq;
	struct slashrpc_read_rep *mp;
	struct pscrpc_request *rq;
	int rc;

	if ((rc = rpc_newreq(RPCSVC_IO, SMDS_VERSION, SRMT_READ,
	    sizeof(*mq), sizeof(*mp) + size, &rq, &mq)) != 0)
		return (rc);
	mq->cfd = fi->fh;
	mq->size = size;
	mq->offset = offset;
	if ((rc = rpc_getrep(rq, sizeof(*mp) + size, &mp)) == 0)
		memcpy(buf, mp->buf, mp->size);
	pscrpc_req_finished(rq);
	if (rc)
		return (rc);
	return (mp->size);
}

int
slash_write(__unusedx const char *path, const char *buf, size_t size,
    off_t offset, struct fuse_file_info *fi)
{
	struct slashrpc_write_req *mq;
	struct slashrpc_write_rep *mp;
	struct pscrpc_request *rq;
	int rc;

	if ((rc = rpc_newreq(RPCSVC_IO, SMDS_VERSION, SRMT_WRITE,
	    sizeof(*mq) + size, sizeof(*mp), &rq, &mq)) != 0)
		return (rc);
	memcpy(mq->buf, buf, size);
	mq->cfd = fi->fh;
	mq->size = size;
	mq->offset = offset;
	rc = rpc_getrep(rq, sizeof(*mp), &mp);
	pscrpc_req_finished(rq);
	return (rc ? rc : (int)mp->size);
}

int
slashrpc_init(void)
{
	struct slash_service *svc;
	lnet_nid_t svrnid;
	char *svrname;
	int rc;

	rc = zestrpc_init_portals(ZEST_CLIENT);
	if (rc)
		zfatal("Failed to intialize portals rpc");

	/* MDS channel */
	svrname = getenv("SLASH_SERVER_NID");
	if (svrname == NULL)
		psc_fatalx("Please export ZEST_SERVER_NID");

	svrnid = libcfs_str2nid(svrname);
	if (svrnid == LNET_NID_ANY)
		psc_fatalx("SLASH_SERVER_NID is invalid: %s", svrname);
	psc_dbg("svrname %s, nid %"ZLPX64, svrname, svrnid);

	svcs[SLASH_MDS_SVC] = slash_service_create(svrnid,
	    RPCMDS_REQUEST_PORTAL, RPCMDS_REPLY_PORTAL, slashrpc_mds_connect);
	if (slashrpc_mds_connect(svrnid))
		psc_error("Failed to connect to %s", svrname);

	/* I/O channel */
	svcs[SLASH_IO_SVC] = slash_service_create(svrnid,
	    RPCIO_REQUEST_PORTAL, RPCIO_REPLY_PORTAL, slashrpc_io_connect);
	if (slashrpc_io_connect(svrnid))
		psc_error("Failed to connect to %s", svrname);

	/* Init nb_req manager for single-block, non-blocking requests */
	ioNbReqSet = nbreqset_init(slashrpc_io_interpret_set, slash_nbcallback);
	zest_assert(ioNbReqSet);
	return (0);
}

