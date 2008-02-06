/* $Id$ */

#define _XOPEN_SOURCE 500

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/fsuid.h>
#include <sys/vfs.h>

#include <unistd.h>
#include <errno.h>

#include "psc_rpc/rpc.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/service.h"

#include "fid.h"
#include "rpc.h"
#include "slash.h"
#include "slashrpc.h"

#define SLIO_NTHREADS  8
#define SLIO_NBUFS     1024
#define SLIO_BUFSZ     (4096+256)
#define SLIO_REPSZ     128
#define SLIO_REQPORTAL RPCIO_REQ_PORTAL
#define SLIO_REPPORTAL RPCIO_REP_PORTAL
#define SLIO_SVCNAME   "slrpciothr"

#define GENERIC_REPLY(rq, prc)								\
	do {										\
		struct slashrpc_generic_rep *_mp;					\
		int _rc, _size;								\
											\
		_size = sizeof(*(_mp));							\
		if (_size > SLIO_REPSZ)							\
			psc_fatalx("reply size greater than max");			\
		_rc = psc_pack_reply((rq), 1, &_size, NULL);				\
		if (_rc) {								\
			psc_assert(_rc == -ENOMEM);					\
			psc_errorx("psc_pack_reply failed: %s", strerror(_rc));		\
			return (_rc);							\
		}									\
		(_mp) = psc_msg_buf((rq)->rq_repmsg, 0, _size);				\
		if ((_mp) == NULL) {							\
			psc_errorx("connect repbody is null");				\
			return (-ENOMEM);						\
		}									\
		(_mp)->rc = (prc);							\
		return (0);								\
	} while (0)

#define GET_CUSTOM_REPLY_SZ(rq, mp, sz)							\
	do {										\
		int _rc, _size;								\
											\
		_size = sz;								\
		if (_size > SLIO_REPSZ)							\
			psc_fatalx("reply size greater than max");			\
		_rc = psc_pack_reply((rq), 1, &_size, NULL);				\
		if (_rc) {								\
			psc_assert(_rc == -ENOMEM);					\
			psc_errorx("psc_pack_reply failed: %s", strerror(_rc));		\
			return (_rc);							\
		}									\
		(mp) = psc_msg_buf((rq)->rq_repmsg, 0, _size);				\
		if ((mp) == NULL) {							\
			psc_errorx("connect repbody is null");				\
			return (-ENOMEM);						\
		}									\
	} while (0)

#define GET_CUSTOM_REPLY(rq, mp) GET_CUSTOM_REPLY_SZ(rq, mp, sizeof(*(mp)))

#define GET_GEN_REQ(rq, mq)								\
	do {										\
		(mq) = psc_msg_buf((rq)->rq_reqmsg, 0, sizeof(*(mq)));			\
		if ((mq) == NULL) {							\
			psc_warnx("reqbody is null");					\
			GENERIC_REPLY((rq), -ENOMSG);					\
		}									\
	} while (0)

psc_spinlock_t fsidlock = LOCK_INITIALIZER;

int
slio_connect(struct pscrpc_request *rq)
{
	struct slashrpc_connect_req *mq;
	int rc;

	rc = 0;
	GET_GEN_REQ(rq, mq);
	if (mq->magic != SIO_MAGIC || mq->version != SIO_VERSION)
		rc = -EINVAL;
	GENERIC_REPLY(rq, rc);
}

int
slio_read(struct pscrpc_request *rq)
{
	struct pscrpc_bulk_desc *desc;
	struct slashrpc_read_req *mq;
	struct slashrpc_read_rep *mp;
	struct l_wait_info lwi;
	int comms_error, fd, rc;
	char fn[PATH_MAX];
	slash_fid_t fid;
	ssize_t nbytes;
	void *buf;

	if ((mq = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq))) == NULL)
		return (-ENOMSG);
	GET_CUSTOM_REPLY(rq, mp);
	mp->rc = 0;
#define MAX_BUFSIZ (1024 * 1024)
	if (mq->size <= 0 || mq->size > MAX_BUFSIZ) {
		mp->rc = -EINVAL;
		return (0);
	}
	if (cfd2fid_cache(&fid, rq->rq_export, mq->cfd) || fid_makepath(&fid, fn)) {
		mp->rc = -errno;
		return (0);
	}
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

	desc = pscrpc_prep_bulk_exp(rq, mq->size / pscPageSize,
	    BULK_PUT_SOURCE, RPCMDS_BULK_PORTAL);
	if (desc == NULL) {
		psc_warnx("pscrpc_prep_bulk_exp returned a null desc");
		mp->rc = -ENOMEM;
		goto done;
	}
	desc->bd_iov[0].iov_base = buf;
	desc->bd_iov[0].iov_len = mq->size;
	desc->bd_iov_count = 1;
	desc->bd_nob = mq->size;

	if (desc->bd_export->exp_failed)
		rc = -ENOTCONN;
	else
		rc = pscrpc_start_bulk_transfer(desc);

	if (rc == 0) {
		lwi = LWI_TIMEOUT_INTERVAL(20 * HZ / 2, HZ, NULL, desc);

		rc = psc_svr_wait_event(&desc->bd_waitq,
		    !pscrpc_bulk_active(desc) || desc->bd_export->exp_failed,
		    &lwi, NULL);
		LASSERT(rc == 0 || rc == -ETIMEDOUT);
		if (rc == -ETIMEDOUT) {
			psc_info("timeout on bulk PUT");
			pscrpc_abort_bulk(desc);
		} else if (desc->bd_export->exp_failed) {
			psc_info("eviction on bulk PUT");
			rc = -ENOTCONN;
			pscrpc_abort_bulk(desc);
		} else if (!desc->bd_success ||
		    desc->bd_nob_transferred != desc->bd_nob) {
			psc_info("%s bulk PUT %d(%d)",
			    desc->bd_success ? "truncated" : "network err",
			    desc->bd_nob_transferred, desc->bd_nob);
			/* XXX should this be a different errno? */
			rc = -ETIMEDOUT;
		}
	} else
		psc_info("pscrpc bulk PUT failed: rc %d", rc);
	comms_error = (rc != 0);
	if (rc == 0)
		psc_info("PUT READ contents successfully");
	else if (!comms_error) {
		/* Only reply if there was no comms problem with bulk */
		rq->rq_status = rc;
		pscrpc_error(rq);
	}
	pscrpc_free_bulk(desc);
	mp->rc = rc;
 done:
	free(buf);
	return (0);
}

int
slio_write(struct pscrpc_request *rq)
{
	struct slashrpc_write_req *mq;
	struct slashrpc_write_rep *mp;
	struct pscrpc_bulk_desc *desc;
	struct l_wait_info lwi;
	int fd, rc, comms_error;
	char fn[PATH_MAX];
	slash_fid_t fid;
	ssize_t nbytes;
	void *buf;

	if ((mq = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq))) == NULL)
		return (-ENOMSG);
	GET_CUSTOM_REPLY(rq, mp);
	mp->rc = 0;
	if (cfd2fid_cache(&fid, rq->rq_export, mq->cfd) || fid_makepath(&fid, fn)) {
		mp->rc = -errno;
		return (0);
	}
	if (mq->size <= 0 || mq->size > MAX_BUFSIZ) {
		mp->rc = -EINVAL;
		return (0);
	}
	buf = PSCALLOC(mq->size);

	desc = pscrpc_prep_bulk_exp(rq, mq->size / pscPageSize,
	    BULK_GET_SINK, RPCMDS_BULK_PORTAL);
	if (desc == NULL) {
		psc_warnx("pscrpc_prep_bulk_exp returned a null desc");
		mp->rc = -ENOMEM;
		goto done;
	}
	desc->bd_iov[0].iov_base = buf;
	desc->bd_iov[0].iov_len = mq->size;
	desc->bd_iov_count = 1;
	desc->bd_nob = mq->size;

	if (desc->bd_export->exp_failed)
		rc = -ENOTCONN;
	else
		rc = pscrpc_start_bulk_transfer(desc);

	if (rc == 0) {
		lwi = LWI_TIMEOUT_INTERVAL(20 * HZ / 2, HZ, NULL, desc);

		rc = psc_svr_wait_event(&desc->bd_waitq,
		    !pscrpc_bulk_active(desc) || desc->bd_export->exp_failed,
		    &lwi, NULL);
		LASSERT(rc == 0 || rc == -ETIMEDOUT);
		if (rc == -ETIMEDOUT) {
			psc_info("timeout on bulk GET");
			pscrpc_abort_bulk(desc);
		} else if (desc->bd_export->exp_failed) {
			psc_info("eviction on bulk GET");
			rc = -ENOTCONN;
			pscrpc_abort_bulk(desc);
		} else if (!desc->bd_success ||
		    desc->bd_nob_transferred != desc->bd_nob) {
			psc_info("%s bulk GET %d(%d)",
			    desc->bd_success ? "truncated" : "network err",
			    desc->bd_nob_transferred, desc->bd_nob);
			/* XXX should this be a different errno? */
			rc = -ETIMEDOUT;
		}
	} else
		psc_info("pscrpc bulk GET failed: rc %d", rc);
	comms_error = (rc != 0);
	if (rc == 0) {
		if ((fd = open(fn, O_WRONLY)) == -1) {
			rc = -errno;
			goto done;
		}
		nbytes = pwrite(fd, buf, mq->size, mq->offset);
		if (nbytes == -1)
			rc = -errno;
		else
			mq->size = nbytes;
		close(fd);
	}
	else if (!comms_error) {
		/* Only reply if there was no comms problem with bulk */
		rq->rq_status = rc;
		pscrpc_error(rq);
	}
	pscrpc_free_bulk(desc);
	mp->rc = rc;
 done:
	free(buf);
	return (0);
}

int
setcred(uid_t uid, gid_t gid, uid_t *myuid, gid_t *mygid)
{
	uid_t tuid;
	gid_t tgid;

	/* Set fs credentials */
	spinlock(&fsidlock);
	*myuid = getuid();
	*mygid = getgid();

	if ((tuid = setfsuid(uid)) != *myuid)
		psc_fatal("invalid fsuid %u", tuid);
	if (setfsuid(uid) != (int)uid) {
		psc_error("setfsuid %u", uid);
		return (-1);
	}

	if ((tgid = setfsgid(gid)) != *mygid)
		psc_fatal("invalid fsgid %u", tgid);
	if (setfsgid(gid) != (int)gid) {
		psc_error("setfsgid %u", gid);
		return (-1);
	}
	return (0);
}

void
revokecred(uid_t uid, gid_t gid)
{
	setfsuid(uid);
	if (setfsuid(uid) != (int)uid)
		psc_fatal("setfsuid %d", uid);
	setfsgid(gid);
	if (setfsgid(gid) != (int)gid)
		psc_fatal("setfsgid %d", gid);
	freelock(&fsidlock);
}

int
slio_svc_handler(struct pscrpc_request *rq)
{
	int rc = 0;

	ENTRY;
	DEBUG_REQ(PLL_TRACE, rq, "new req");
	switch (rq->rq_reqmsg->opc) {
	case SRMT_CONNECT:
		rc = slio_connect(rq);
		break;
	case SRMT_READ:
		rc = slio_read(rq);
		break;
	case SRMT_WRITE:
		rc = slio_write(rq);
		break;
	default:
		psc_errorx("Unexpected opcode %d", rq->rq_reqmsg->opc);
		rq->rq_status = -ENOSYS;
		rc = pscrpc_error(rq);
		goto done;
	}
	psc_info("rq->rq_status == %d", rq->rq_status);
	target_send_reply_msg(rq, rc, 0);

 done:
	RETURN(rc);
}

/**
 * slio_init - start up the I/O threads via pscrpc_thread_spawn()
 */
void
slio_init(void)
{
	pscrpc_svc_handle_t *svh = PSCALLOC(sizeof(*svh));

	svh->svh_nbufs      = SLIO_NBUFS;
	svh->svh_bufsz      = SLIO_BUFSZ;
	svh->svh_reqsz      = SLIO_BUFSZ;
	svh->svh_repsz      = SLIO_REPSZ;
	svh->svh_req_portal = SLIO_REQPORTAL;
	svh->svh_rep_portal = SLIO_REPPORTAL;
	svh->svh_type       = SLTHRT_RPCIO;
	svh->svh_nthreads   = SLIO_NTHREADS;
	svh->svh_handler    = slio_svc_handler;

	strncpy(svh->svh_svc_name, SLIO_SVCNAME, PSCRPC_SVCNAME_MAX);

	pscrpc_thread_spawn(svh);
}
