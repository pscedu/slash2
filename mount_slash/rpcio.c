/* $Id$ */

#include <err.h>
#include <stdio.h>

#include "psc_rpc/rpc.h"
#include "psc_util/cdefs.h"

#include "mount_slash.h"
#include "rpc.h"

#define OBD_TIMEOUT 20

int
slashrpc_timeout(__unusedx void *arg)
{
	return (0);
}

int
slash_readdir_fill(struct pscrpc_export *exp,
    struct slashrpc_readdir_res_req *mrq, struct slashrpc_readdir_bulk *mb)
{
	struct slashrpc_readdir_ent *me;
	struct readdir_cache_ent *rce;
	struct stat stb;
	int j;

	/* Lookup the directory associated with the reply in memory. */
	rce = rc_lookup(exp, mrq->cfd, mrq->offset);
	if (rce == NULL)
		errx(1, "couldn't find readdir cache entry");

	/* Fill in the entries. */
	for (j = 0; j < (int)mrq->nents; j++) {
		me = &mb->ents[j];
		memset(&stb, 0, sizeof(stb));
		stb.st_ino = me->ino;
		stb.st_mode = me->mode;
		if (rce->filler(rce->buf, me->name, &stb, 1))
			return (1);
	}
	return (0);
}

__inline int
slashrpc_handle_readdir(struct pscrpc_request *rq)
{
	struct slashrpc_readdir_res_req *mrq;
	struct slashrpc_readdir_res_rep *mrp;
	struct slashrpc_readdir_bulk *mb;
	struct slashrpc_readdir_ent *me;
	struct pscrpc_bulk_desc *desc;
	struct l_wait_info lwi;
	int i, sum, size, rc, comms_error;
	u8 *v1;

	/* Ensure we reply back to the server. */
	size = sizeof(*mrp);
	rc = psc_pack_reply(rq, 1, &size, NULL);
	if (rc) {
		psc_assert(rc == -ENOMEM);
		psc_notify("psc_pack_reply failed");
		/* the client will probably bomb here */
		return (rc);
	}

	mrp = psc_msg_buf(rq->rq_repmsg, 0, size);
	psc_assert(mrp);
	mrp->flags = 0;

	comms_error = 0;

	mrq = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mrq));
	if (mrq == NULL) {
		psc_warnx("readdir reply body is null");
		rc = errno;
		goto out;
	}

	mb = PSCALLOC(mrq->nents * sizeof(*me));

	/* GET_SINK the data. */
	desc = pscrpc_prep_bulk_exp(rq, 1, BULK_GET_SINK, RPCIO_BULK_PORTAL);
	if (desc == NULL) {
		psc_warnx("pscrpc_prep_bulk_exp returned a null desc");
		rc = ENOMEM; // errno
		goto out;
	}
	desc->bd_iov_count = 1;
	desc->bd_iov[0].iov_base = mb;
	desc->bd_iov[0].iov_len = desc->bd_nob = mrq->nents * sizeof(*me);

	/* Check for client eviction during previous I/O before proceeding. */
	if (desc->bd_export->exp_failed)
		rc = ENOTCONN;
	else
		rc = pscrpc_start_bulk_transfer(desc);
	if (rc == 0) {
		lwi = LWI_TIMEOUT_INTERVAL(OBD_TIMEOUT / 2,
		    HZ, slashrpc_timeout, desc);

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
		if (slash_readdir_fill(rq->rq_export, mrq, mb))
			/* Send back reply informing server to stop. */
			mrp->flags |= SRORBF_STOP;
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
slashrpc_io_handler(struct pscrpc_request *rq)
{
	int rc;

	rc = 0;
	switch (rq->rq_reqmsg->opc) {
	case SLASH_IOP_READDIR:
		rc = slashrpc_handle_readdir(rq);
		break;
	default:
		psc_errorx("Unexpected opcode %d", rq->rq_reqmsg->opc);
		rq->rq_status = -ENOSYS;
		rc = pscrpc_error(rq);
		return (rc);
	}
	rq->rq_status = rc;
	target_send_reply_msg(rq, rc, 0);
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

	rc = pscrpc_init_portals(PSC_CLIENT);
	if (rc)
		zfatal("Failed to intialize portals rpc");

	/* MDS channel */
	svrname = getenv("SLASH_SERVER_NID");
	if (svrname == NULL)
		psc_fatalx("Please export SLASH_SERVER_NID");

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
	psc_assert(ioNbReqSet);
	return (0);
}
