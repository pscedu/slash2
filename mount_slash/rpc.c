/* $Id$ */

#include "psc_types.h"
#include "psc_rpc/rpc.h"
#include "psc_util/cdefs.h"
#include "psc_ds/list.h"

#include "mount_slash.h"
#include "slashrpc.h"

struct slashrpc_service *rpcsvcs[NRPCSVC];
struct pscrpc_nbreqset *ioNbReqSet;

int
rpc_io_interpret_set(struct pscrpc_request_set *set, __unusedx void *arg,
    int status)
{
	struct pscrpc_request *req;
	int rc = 0;

	/*
	 * pscrpc_set_wait() already does this for us but it
	 * doesn't abort.
	 */
	psclist_for_each_entry(req, &set->set_requests, rq_set_chain_lentry) {
		LASSERT(req->rq_phase == ZRQ_PHASE_COMPLETE);
		if (req->rq_status != 0) {
			/* sanity check */
			psc_assert(status);
			rc = req->rq_status;
		}
	}
	if (rc)
		psc_fatalx("Some I/O reqs could not be completed");
	return (rc);
}

/**
 * rpc_nbcallback - async op completion callback
 */
int
rpc_nbcallback(__unusedx struct pscrpc_request *req,
    __unusedx struct pscrpc_async_args *cb_args)
{
#if 0
	psc_stream_buffer_t *zsb;

	/*
	 * Catch bad status here, we can't proceed if a
	 *  nb buffer did not send properly.
	 */
	if (req->rq_status)
		zfatalx("I/O req could not be completed");

	zsb = cb_args->pointer_arg[ZSB_CB_POINTER_SLOT];
	zest_assert(zsb);
	zest_assert(zsb->zsb_zcf);
	zlist_del(zsb->zsb_ent);
	return (zest_buffer_free(zsb));
#endif
return 0;
}

/*
 * rpcmds_connect - attempt connection initiation with a peer.
 * @server: NID of server peer.
 * @ptl: portal ID to initiate over.
 * @magic: agreed-upon connection message key.
 * @version: communication protocol version.
 */
int
rpcmds_connect(lnet_nid_t server, int ptl, u64 magic, u32 version)
{
	lnet_process_id_t server_id = { server, 0 };
	struct pscrpc_request *rq;
	struct pscrpc_import *imp;
	struct fuse_context *ctx;
	lnet_process_id_t id;

	if (LNetGetId(1, &id))
		psc_fatalx("LNetGetId");

	imp = rpcsvcs[ptl]->svc_import;
	imp->imp_connection = pscrpc_get_connection(server_id, id.nid, NULL);
	imp->imp_connection->c_peer.pid = SLASH_SVR_PID;

	ctx = fuse_get_context();
	if (rpc_sendmsg(SRMT_CONNECT, magic, version, ctx->uid, ctx->gid) == -1)
		return (-errno);

#if 0
	struct slashrpc_connect_req *mq;

	rc = rpc_newreq(imp, version, op, sizeof(*mq), 0, &rq, &u.m);
	if (rc)
		return (rc);
	mq->magic = magic;
	mq->version = version;
	mq->uid = ctx->uid;
	mq->gid = ctx->gid;
	rc = rpc_getrep(rq, 0, &dummy);
	pscrpc_req_finished(rq);
	if (rc == 0)
		rc = mp->rc;
#endif


	/* Save server PID from reply callback and mark initialized.  */
//	imp->imp_connection->c_peer.pid = rq->rq_peer.pid;
	imp->imp_state = PSC_IMP_FULL;
	return (0);
}

/*
 * rpc_svc_create - create a client RPC service.
 * @rqptl: request portal ID.
 * @rpptl: reply portal ID.
 */
struct slashrpc_service *
rpc_svc_create(u32 rqptl, u32 rpptl)
{
	struct slashrpc_service *svc;

	svc = PSCALLOC(sizeof(*svc));

	INIT_PSCLIST_HEAD(&svc->svc_old_imports);
	LOCK_INIT(&svc->svc_lock);

	svc->svc_failed = 0;
	svc->svc_initialized = 0;

	if ((svc->svc_import = new_import()) == NULL)
		psc_fatalx("new_import");

	svc->svc_import->imp_client =
	    PSCALLOC(sizeof(*svc->svc_import->imp_client));
	svc->svc_import->imp_client->cli_request_portal = rqptl;
	svc->svc_import->imp_client->cli_reply_portal = rpptl;

	svc->svc_import->imp_max_retries = 2;
	return (svc);
}

/*
 * rpc_svc_init: initialize client RPC services.
 */
void
rpc_svc_init(void)
{
	lnet_nid_t nid;
	char *snid;
	int rc;

	rc = pscrpc_init_portals(PSC_CLIENT);
	if (rc)
		psc_fatal("Failed to intialize portals");

	snid = getenv("SLASH_SERVER_NID");
	if (snid == NULL)
		psc_fatalx("SLASH_RPC_SERVER_NID not set");
	nid = libcfs_str2nid(snid);
	if (nid == LNET_NID_ANY)
		psc_fatalx("invalid SLASH_SERVER_NID: %s", snid);

	/* Setup client MDS service */
	rpcsvcs[RPCSVC_MDS] = rpc_svc_create(SR_MDS_REQ_PORTAL,
	    SR_MDS_REP_PORTAL);
	if (rpcmds_connect(nid, RPCSVC_MDS, SR_MDS_MAGIC, SR_MDS_VERSION))
		psc_error("rpc_mds_connect %s", snid);

#if 0
	/* Setup client I/O service */
	rpcsvcs[RPCSVC_IO] = rpc_svc_create(RPCIO_REQ_PORTAL,
	    RPCIO_REP_PORTAL);
	if (rpc_connect(nid, RPCSVC_IO, SIO_MAGIC, SIO_VERSION))
		psc_error("rpc_io_connect %s", snid);
#endif

	/* Initialize manager for single-block, non-blocking requests */
	ioNbReqSet = nbreqset_init(rpc_io_interpret_set, rpc_nbcallback);
	if (ioNbReqSet == NULL)
		psc_fatal("nbreqset_init");
}

int simpleop_sizes[] = {
	sizeof(struct slashrpc_access_req),		/* 0 */
	sizeof(struct slashrpc_chmod_req),		/* 1 */
	sizeof(struct slashrpc_chown_req),		/* 2 */
	sizeof(struct slashrpc_connect_req),		/* 3 */
	0,						/* 4 - creat */
	sizeof(struct slashrpc_destroy_req),		/* 5 */
	0,						/* 6 - fgetattr */
	sizeof(struct slashrpc_ftruncate_req),		/* 7 */
	0,						/* 8 - getattr */
	sizeof(struct slashrpc_link_req),		/* 9 */
	0,						/* 10 - lock */
	sizeof(struct slashrpc_mkdir_req),		/* 11 */
	sizeof(struct slashrpc_mknod_req),		/* 12 */
	0,						/* 13 - open */
	0,						/* 14 - opendir */
	0,						/* 15 - readdir */
	0,						/* 16 - readlink */
	sizeof(struct slashrpc_release_req),		/* 17 */
	sizeof(struct slashrpc_releasedir_req),		/* 18 */
	sizeof(struct slashrpc_rename_req),		/* 19 */
	sizeof(struct slashrpc_rmdir_req),		/* 20 */
	0,						/* 21 - statfs */
	sizeof(struct slashrpc_symlink_req),		/* 22 */
	sizeof(struct slashrpc_truncate_req),		/* 23 */
	sizeof(struct slashrpc_unlink_req),		/* 24 */
	sizeof(struct slashrpc_utimes_req),		/* 25 */
	0,						/* 26 - read */
	0,						/* 27 - write */
	0						/* 28 - GETFID */
#if SNRT != 29
# error "RPC ops out of sync"
#endif
};

/*
 * rpc_sendmsg - Initiate I/O for "simple" command, the request format
 *	of which many commands have in common.
 * @op: operation ID.
 * Notes: Subsequent arguments depend on specific operation.
 */
int
rpc_sendmsg(int op, ...)
{
	union {
		struct slashrpc_access_req	*m_access;
		struct slashrpc_chmod_req	*m_chmod;
		struct slashrpc_chown_req	*m_chown;
		struct slashrpc_connect_req	*m_connect;
		struct slashrpc_destroy_req	*m_destroy;
		struct slashrpc_ftruncate_req	*m_ftruncate;
		struct slashrpc_link_req	*m_link;
		struct slashrpc_mkdir_req	*m_mkdir;
		struct slashrpc_mknod_req	*m_mknod;
		struct slashrpc_release_req	*m_release;
		struct slashrpc_releasedir_req	*m_releasedir;
		struct slashrpc_rename_req	*m_rename;
		struct slashrpc_rmdir_req	*m_rmdir;
		struct slashrpc_symlink_req	*m_symlink;
		struct slashrpc_truncate_req	*m_truncate;
		struct slashrpc_unlink_req	*m_unlink;
		struct slashrpc_utimes_req	*m_utimes;
		void				*m;
	} u;
	struct slashrpc_generic_rep *mp;
	struct pscrpc_request *rq;
	va_list ap;
	int rc;

	if (op < 0 || op > SNRT || simpleop_sizes[op] == 0)
		goto badop;

	rc = rsx_newreq(rpcsvcs[RPCSVC_MDS]->svc_import, SR_MDS_VERSION,
	    op, simpleop_sizes[op], sizeof(*mp), &rq, &u.m);
	if (rc)
		return (rc);

	va_start(ap, op);
	switch (op) {
	case SRMT_ACCESS:
		snprintf(u.m_access->path, sizeof(u.m_access->path),
		    "%s", va_arg(ap, const char *));
		u.m_access->mask = va_arg(ap, int);
		break;
	case SRMT_CHMOD:
		snprintf(u.m_chmod->path, sizeof(u.m_chmod->path),
		    "%s", va_arg(ap, const char *));
		u.m_chmod->mode = va_arg(ap, mode_t);
		break;
	case SRMT_CHOWN:
		snprintf(u.m_chown->path, sizeof(u.m_chown->path),
		    "%s", va_arg(ap, const char *));
		u.m_chown->uid = va_arg(ap, uid_t);
		u.m_chown->gid = va_arg(ap, gid_t);
		break;
	case SRMT_CONNECT:
		u.m_connect->magic = va_arg(ap, u64);
		u.m_connect->version = va_arg(ap, u32);
		u.m_connect->uid = va_arg(ap, uid_t);
		u.m_connect->gid = va_arg(ap, gid_t);
		break;
	case SRMT_DESTROY:
		break;
	case SRMT_FTRUNCATE:
		u.m_ftruncate->cfd = va_arg(ap, u64);
		u.m_ftruncate->size = va_arg(ap, u64);
		break;
	case SRMT_LINK:
		snprintf(u.m_link->from, sizeof(u.m_link->from),
		    "%s", va_arg(ap, const char *));
		snprintf(u.m_link->to, sizeof(u.m_link->to),
		    "%s", va_arg(ap, const char *));
		break;
	case SRMT_MKDIR:
		snprintf(u.m_mkdir->path, sizeof(u.m_mkdir->path),
		    "%s", va_arg(ap, const char *));
		u.m_mkdir->mode = va_arg(ap, mode_t);
		break;
	case SRMT_MKNOD:
		snprintf(u.m_mkdir->path, sizeof(u.m_mkdir->path),
		    "%s", va_arg(ap, const char *));
		u.m_mkdir->mode = va_arg(ap, mode_t);
		break;
	case SRMT_RELEASE:
		u.m_release->cfd = va_arg(ap, u64);
		break;
	case SRMT_RELEASEDIR:
		u.m_releasedir->cfd = va_arg(ap, u64);
		break;
	case SRMT_RENAME:
		snprintf(u.m_rename->from, sizeof(u.m_rename->from),
		    "%s", va_arg(ap, const char *));
		snprintf(u.m_rename->to, sizeof(u.m_rename->to),
		    "%s", va_arg(ap, const char *));
		break;
	case SRMT_RMDIR:
		snprintf(u.m_rmdir->path, sizeof(u.m_rmdir->path),
		    "%s", va_arg(ap, const char *));
		break;
	case SRMT_SYMLINK:
		snprintf(u.m_symlink->from, sizeof(u.m_symlink->from),
		    "%s", va_arg(ap, const char *));
		snprintf(u.m_symlink->to, sizeof(u.m_symlink->to),
		    "%s", va_arg(ap, const char *));
		break;
	case SRMT_TRUNCATE:
		snprintf(u.m_truncate->path, sizeof(u.m_truncate->path),
		    "%s", va_arg(ap, const char *));
		u.m_truncate->size = va_arg(ap, size_t);
		break;
	case SRMT_UNLINK:
		snprintf(u.m_unlink->path, sizeof(u.m_unlink->path),
		    "%s", va_arg(ap, const char *));
		break;
	case SRMT_UTIMES:
		snprintf(u.m_utimes->path, sizeof(u.m_utimes->path),
		    "%s", va_arg(ap, const char *));
		memcpy(u.m_utimes->times, va_arg(ap, struct timespec *),
		    sizeof(u.m_utimes->times));
		break;
	default:
 badop:
		psc_fatalx("unknown op: %d", op);
	}
	va_end(ap);

	rc = rsx_getrep(rq, sizeof(*mp), &mp);
	pscrpc_req_finished(rq);
	if (rc == 0) {
		errno = -mp->rc;
		if (errno)
			rc = -1;
	}
	else if (rc != -1) {
		errno = -rc;
		rc = -1;
	}
	return (rc);
}
