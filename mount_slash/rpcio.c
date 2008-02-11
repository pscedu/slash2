/* $Id$ */

#include <err.h>
#include <stdio.h>

#include "psc_rpc/rpc.h"
#include "psc_util/cdefs.h"

#include "mount_slash.h"
#include "slashrpc.h"
#include "rsx.h"

#define OBD_TIMEOUT 20

struct slashrpc_service *svcs[NRPCSVC];

int
slash_read(__unusedx const char *path, char *buf, size_t size,
    off_t offset, struct fuse_file_info *fi)
{
	struct slashrpc_read_req *mq;
	struct slashrpc_read_rep *mp;
	struct pscrpc_request *rq;
	int rc;

	if ((rc = rsx_newreq(rpcsvcs[RPCSVC_IO]->svc_import, SR_MDS_VERSION,
	    SRMT_READ, sizeof(*mq), sizeof(*mp) + size, &rq, &mq)) != 0)
		return (rc);
	mq->cfd = fi->fh;
	mq->size = size;
	mq->offset = offset;
	if ((rc = rsx_getrep(rq, sizeof(*mp) + size, &mp)) == 0)
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

	if ((rc = rsx_newreq(rpcsvcs[RPCSVC_IO]->svc_import, SR_MDS_VERSION,
	    SRMT_WRITE, sizeof(*mq) + size, sizeof(*mp), &rq, &mq)) != 0)
		return (rc);
	memcpy(mq->buf, buf, size);
	mq->cfd = fi->fh;
	mq->size = size;
	mq->offset = offset;
	rc = rsx_getrep(rq, sizeof(*mp), &mp);
	pscrpc_req_finished(rq);
	return (rc ? rc : (int)mp->size);
}
