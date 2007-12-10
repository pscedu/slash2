/* $Id$ */

#include "mount_slash.h"

#include "psc_rpc/rpc.h"

struct rpcsvc rpcsvcs[NRPCSVC];

int
rpc_svc_init(void)
{
	struct rpc_service *svc;
	lnet_nid_t nid;
	char *snid;

	/* Setup client MDS service */
	snid = getenv("SLASH_SERVER_NID");
	if (snid == NULL)
		psc_fatalx("SLASH_RPC_SERVER_NID not set");
	nid = libcfs_str2nid(snid);
	if (nid == LNET_NID_ANY)
		psc_fatalx("invalid SLASH_SERVER_NID: %s", snid);

	svc = rpc_svc_create(nid, RPCMDS_REQ_PORTAL,
	    RPCMDS_REP_PORTAL, rpc_mds_connect);

	rpcsvcs[RPCSVC_MDS] = csvc;
	if (rpc_mds_connect(nid))
		psc_error("rpc_mds_connect %s", snid);

	/* Setup client I/O service */
	svc = rpc_svc_create(nid, RPCIO_REQ_PORTAL,
	    RPCIO_REP_PORTAL, rpc_io_connect);

	rpcsvcs[RPCSRV_IO] = svc;
	if (rpc_io_connect(nid))
		psc_error("rpc_io_connect %s", snid);

	/* Initialize manager for single-block, non-blocking requests */
	ioNbReqSet = nbreqset_init(rpc_io_interpret_set, rpc_nbcallback);
	if (ioNbReqSet == NULL)
		psc_fatal("nbreqset_init");
	return (0);
}

int
rpc_sendmsg(int op, ...)
{
	union {
		struct slashrpc_access_req	*m_access;
		struct slashrpc_chmod_req	*m_chmod;
		struct slashrpc_chown_req	*m_chown;
		struct slashrpc_link_req	*m_link;
		struct slashrpc_mkdir_req	*m_mkdir;
		struct slashrpc_rename_req	*m_rename;
		struct slashrpc_rmdir_req	*m_rmdir;
		struct slashrpc_symlink_req	*m_symlink;
		struct slashrpc_truncate_req	*m_truncate;
		struct slashrpc_unlink_req	*m_unlink;
		void				*m;
	} u;
	struct pscrpc_request *rq;
	struct pscrpc_import *imp;
	int rc, msglen;
	va_list ap;

	imp = rpcsvcs[RPCSVC_MDS]->csvc_import;

#define SRM_ALLOC(var)							\
	do {								\
		msglen = sizeof(*(var));				\
		(var) = psc_msg_buf(rq->rq_reqmsg, 0, msglen);	\
	} while (0)

	va_start(ap, op);
	switch (op) {
	case SRM_ACCESS:
		SRM_ALLOC(u.m_access);
		u.m_access->path = va_arg(ap, const char *);
		u.m_access->mask = va_arg(ap, int);
		break;
	case SRM_CHMOD:
		SRM_ALLOC(u.m_chmod);
		u.m_chmod->path = va_arg(ap, const char *);
		u.m_chmod->mode = va_arg(ap, mode_t);
		break;
	case SRM_CHOWN:
		SRM_ALLOC(u.m_chown);
		u.m_chown->path = va_arg(ap, const char *);
		u.m_chown->uid = va_arg(ap, uid_t);
		u.m_chown->gid = va_arg(ap, gid_t);
		break;
	case SRM_LINK:
		SRM_ALLOC(u.m_link);
		u.m_link->from = va_arg(ap, const char *);
		u.m_link->to = va_arg(ap, const char *);
		break;
	case SRM_MKDIR:
		SRM_ALLOC(u.m_mkdir);
		u.m_mkdir->path = va_arg(ap, const char *);
		u.m_mkdir->mode = va_arg(ap, mode_t);
		break;
	case SRM_RENAME:
		SRM_ALLOC(u.m_rename);
		u.m_rename->from = va_arg(ap, const char *);
		u.m_rename->to = va_arg(ap, const char *);
		break;
	case SRM_RMDIR:
		SRM_ALLOC(u.m_rmdir);
		u.m_rmdir->path = va_arg(ap, const char *);
		break;
	case SRM_SYMLINK:
		SRM_ALLOC(u.m_symlink);
		u.m_symlink->from = va_arg(ap, const char *);
		u.m_symlink->to = va_arg(ap, const char *);
		break;
	case SRM_TRUNCATE:
		SRM_ALLOC(u.m_truncate);
		u.m_truncate->path = va_arg(ap, const char *);
		u.m_truncate->size = va_arg(ap, size_t);
		break;
	case SRM_UNLINK:
		SRM_ALLOC(u.m_unlink);
		u.m_unlink->path = va_arg(ap, const char *);
		break;
	default:
		psc_fatalx("unknown op: %d", op);
	}
	va_end(ap);

	/* Create the request and associate it with the import.  */
	rq = pscrpc_prep_req(imp, SMDS_VERSION, op, 1, &msglen, NULL);
	if (rq == NULL)
		return (-ENOMEM);

	/* No reply buffer expected; only return code is needed. */
	msglen = 0;
	rq->rq_replen = psc_msg_size(1, &msglen);

	/* Send the request and block on its completion. */
	rc = pscrpc_queue_wait(rq);
	pscrpc_req_finished(rq);
	return (rc);
}
