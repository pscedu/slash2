/* $Id$ */

/*
 * Routines for handling RPC requests for ION from CLIENT.
 */

#define _XOPEN_SOURCE 500

#include <errno.h>
#include <stdio.h>

#include "psc_rpc/rpc.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/service.h"

#include "fdbuf.h"
#include "fid.h"
#include "slashrpc.h"
#include "sliod.h"

int
slric_handle_disconnect(struct pscrpc_request *rq)
{
	struct srm_disconnect_req *mq;
	struct srm_generic_rep *mp;

	RSX_ALLOCREP(rq, mq, mp);
	return (0);
}

int
slric_handle_connect(struct pscrpc_request *rq)
{
	struct srm_connect_req *mq;
	struct srm_generic_rep *mp;

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->magic != SRIC_MAGIC ||
	    mq->version != SRIC_VERSION)
		mp->rc = -EINVAL;
	return (0);
}

int
slric_handle_read(struct pscrpc_request *rq)
{
	struct pscrpc_bulk_desc *desc;
	struct slash_fidgen fg;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	struct iovec iov;
	sl_blkno_t bmapno;
	ssize_t nbytes;
	uint64_t cfd;
	void *buf;
	int fd;

	RSX_ALLOCREP(rq, mq, mp);
#define MAX_BUFSIZ (1024 * 1024)
	if (mq->size <= 0 || mq->size > MAX_BUFSIZ) {
		mp->rc = -EINVAL;
		return (0);
	}

	mp->rc = bdbuf_decrypt(&mq->sbdb, &cfd,
	    &fg, &bmapno, rq->rq_peer, lpid.nid,
	    nodeInfo.node_res->res_id);
	if (mp->rc) {
		psc_errorx("fdbuf_decrypt failed for "FIDFMT, FIDFMTARGS(&fg));
		return (0);
	}

	if ((fd = fid_open(fg.fg_fid))) {
		psc_error("fid_open failed (%d) for "FIDFMT,
			 fd, FIDFMTARGS(&fg));
		mp->rc = fd;
		return (0);
	}
	buf = PSCALLOC(mq->size);
	nbytes = pread(fd, buf, mq->size, mq->offset);
	if (nbytes == -1) {
		psc_error("pread failed (%d) for "FIDFMT,
			 fd, FIDFMTARGS(&fg));
		mp->rc = -errno;
		close(fd);
		goto done;
	}
	close(fd);
	mp->size = nbytes;
	if (nbytes == 0)
		goto done;

	iov.iov_base = buf;
	iov.iov_len = mq->size;
	mp->rc = rsx_bulkclient(rq, &desc, BULK_GET_SOURCE,
	    SRIC_BULK_PORTAL, &iov, 1);
	if (desc)
		pscrpc_free_bulk(desc);
 done:
	free(buf);
	return (0);
}

int
slric_handle_write(struct pscrpc_request *rq)
{
	struct pscrpc_bulk_desc *desc;
	struct slash_fidgen fg;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	struct iovec iov;
	sl_blkno_t bmapno;
	ssize_t nbytes;
	uint64_t cfd;
	void *buf;
	int fd;

	RSX_ALLOCREP(rq, mq, mp);

	if (mq->size <= 0 || mq->size > MAX_BUFSIZ) {
		psc_errorx("invalid size %u, fid:"FIDFMT,
			   mq->size,  FIDFMTARGS(&fg));
		mp->rc = -EINVAL;
		return (0);
	}

	mp->rc = bdbuf_decrypt(&mq->sbdb, &cfd, &fg,
	    &bmapno, rq->rq_peer, lpid.nid,
	    nodeInfo.node_res->res_id);
	if (mp->rc) {
		psc_errorx("fdbuf_decrypt failed");
		return (0);
	}

	buf = PSCALLOC(mq->size);
	iov.iov_base = buf;
	iov.iov_len = mq->size;
	if ((mp->rc = rsx_bulkserver(rq, &desc, BULK_GET_SINK,
			     SRIC_BULK_PORTAL, &iov, 1)) == 0) {
		if ((fd = fid_ocreat(fg.fg_fid)) == -1) {
			psc_error("fid_ocreat failed "FIDFMT, FIDFMTARGS(&fg));
			mp->rc = -errno;
		} else {
			nbytes = pwrite(fd, buf, mq->size, mq->offset);
			if (nbytes == -1) {
				psc_error("pwrite failed "FIDFMT,
					  FIDFMTARGS(&fg));
				mp->rc = -errno;
			} else
				mq->size = nbytes;
			close(fd);
		}
	}
	if (desc)
		pscrpc_free_bulk(desc);
	free(buf);
	return (0);
}

int
slric_handler(struct pscrpc_request *rq)
{
	int rc;

	rc = 0; /* gcc */
	switch (rq->rq_reqmsg->opc) {
	case SRMT_DISCONNECT:
		rc = slric_handle_disconnect(rq);
		break;
	case SRMT_CONNECT:
		rc = slric_handle_connect(rq);
		break;
	case SRMT_READ:
		rc = slric_handle_read(rq);
		break;
	case SRMT_WRITE:
		rc = slric_handle_write(rq);
		break;
	default:
		psc_errorx("Unexpected opcode %d", rq->rq_reqmsg->opc);
		rq->rq_status = -ENOSYS;
		return (pscrpc_error(rq));
	}
	target_send_reply_msg(rq, rc, 0);
	return (rc);
}
