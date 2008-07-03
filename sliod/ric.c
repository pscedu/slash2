/* $Id$ */

#define _XOPEN_SOURCE 500

#include <errno.h>
#include <stdio.h>

#include "psc_rpc/rpc.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/service.h"

#include "sliod.h"
#include "slashrpc.h"
#include "fid.h"

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
	struct srcim_connect_req *mq;
	struct srm_generic_rep *mp;

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->magic != SRCI_MAGIC || mq->version != SRCI_VERSION)
		mp->rc = -EINVAL;
	return (0);
}

int
slric_handle_read(struct pscrpc_request *rq)
{
	struct pscrpc_bulk_desc *desc;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	struct iovec iov;
	char fn[PATH_MAX];
	ssize_t nbytes;
	void *buf;
	int fd;

	RSX_ALLOCREP(rq, mq, mp);
#define MAX_BUFSIZ (1024 * 1024)
	if (mq->size <= 0 || mq->size > MAX_BUFSIZ) {
		mp->rc = -EINVAL;
		return (0);
	}

#if 0
	decrypt secret
	grab fg
#endif

	fid_makepath(mq->fg.fg_fid, fn); /* XXX validity check fid */
	if ((fd = open(fn, O_RDONLY)) == -1) {
		mp->rc = -errno;
		return (0);
	}
	buf = PSCALLOC(mq->size);
	nbytes = pread(fd, buf, mq->size, mq->offset);
	if (nbytes == -1) {
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
	    SRCI_BULK_PORTAL, &iov, 1);
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
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	struct iovec iov;
	char fn[PATH_MAX];
	ssize_t nbytes;
	void *buf;
	int fd;

	RSX_ALLOCREP(rq, mq, mp);

#if 0
	decrypt secret
	grab fg
#endif

	if (mq->size <= 0 || mq->size > MAX_BUFSIZ) {
		mp->rc = -EINVAL;
		return (0);
	}
	fid_makepath(mq->fg.fg_fid, fn); /* validity check fid */
	buf = PSCALLOC(mq->size);
	iov.iov_base = buf;
	iov.iov_len = mq->size;
	if ((mp->rc = rsx_bulkserver(rq, &desc, BULK_GET_SINK,
	    SRCI_BULK_PORTAL, &iov, 1)) == 0) {
//	mq->size / pscPageSize,
		if ((fd = open(fn, O_WRONLY)) == -1)
			mp->rc = -errno;
		else {
			nbytes = pwrite(fd, buf, mq->size, mq->offset);
			if (nbytes == -1)
				mp->rc = -errno;
			else
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
