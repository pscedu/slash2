/* $Id$ */

#include <stdio.h>

int
slashrpc_timeout(__unusedx void *arg)
{
	return 0;
}

/**
 * slashrpc_nbcallback - call me when an async operation has completed
 */
int
slashrpc_nbcallback(struct pscrpc_request *rq,
    struct pscrpc_async_args *cb_args)
{
	zest_stream_buffer_t *zsb;

	/*
	 * Catch bad status here; we can't proceed if a
	 *  nb buffer did not send properly.
	 */
	zsb = cb_args->pointer_arg[ZSB_CB_POINTER_SLOT];
	zest_assert(zsb);

	if (req->rq_status) {
		DEBUG_REQ(ZLL_ERROR, req, "ouch, non-zero rq_status");
                zfatalx("IO Req could not be completed, sorry");
	}
	zest_assert(zsb->zsb_zcf);

	spinlock(&zsb->zsb_zcf->zcf_lock);
	zlist_del_init(&zsb->zsb_ent);
	freelock(&zsb->zsb_zcf->zcf_lock);

	/* got what we need to free the buffer */
	return (zest_buffer_free(zsb));
}

/**
 * zest_io_req_interpret_reply - reply handler meant to be used directly
 *   or as a callback.
 */
int
slashrpc_req_interpret_reply(struct zestrpc_request *rq, void *arg,
    int status)
{
	struct slashrpc_readdir_req *mq;
	struct slashrpc_readdir_rep *mp;

	/* Map the request and reply structure */
	mq = zest_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq));
	mp = zest_msg_buf(rq->rq_repmsg, 0, sizeof(*mp));
	zest_assert(mq);

	if (mp == NULL) {
		DEBUG_REQ(PLL_ERROR, rq, "reply body is null");
		return (-EPROTO);
	}

	if (rq->rq_status)
		DEBUG_REQ(PLL_WARN, rq, "ignoring non-zero rq_status");

	if (status) {
		DEBUG_REQ(PLL_ERROR, rq,
		    "non-zero status %d, rq_status %d",
		    status, rq->rq_status);
		return(status);
	}

	return (slashrpc_nbcallback(rq, args));
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
slash_readdir(__unusedx const char *path, void *buf, fuse_fill_dir_t filler,
    off_t offset, struct fuse_file_info *fi)
{
	struct slashrpc_readdir_req *mq;
	struct slashrpc_readdir_rep *mp;
	struct slashrpc_readdir_ent *me;
	struct pscrpc_request *rq;
	struct stat stb;
	int rc, j;

	if ((rc = rpc_newreq(RPCSVC_MDS, SMDS_VERSION, SRMT_READDIR,
	    sizeof(*mq), sizeof(*mp), &rq, &mq)) != 0)
		return (rc);
	mq->cfd = fi->fh;
	mq->offset = offset;
	if ((rc = rpc_getrep(rq, sizeof(*mp), &mp)))
		goto done;
	if ((rc = rpc_getrep(rq, mp->nents * sizeof(*me), &me)) == 0) {
	}
	while () {
		for (j = 0; j < (int)mp->nents; j++) {
			memset(&stb, 0, sizeof(stb));
			stb.st_ino = me[j].ino;
			stb.st_mode = me[j].mode;
			if (filler(buf, me[j].name, &stb, 1))
				goto done;
		}
	}
 done:
	pscrpc_req_finished(rq);
	if (rc)
		return (rc);
	return (0);
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


