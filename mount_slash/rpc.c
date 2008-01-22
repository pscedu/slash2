/* $Id$ */

#include "psc_types.h"
#include "psc_rpc/rpc.h"
#include "psc_util/cdefs.h"
#include "psc_ds/list.h"

#include "mount_slash.h"
#include "slashrpc.h"

#define SLASH_SVR_PID 54321

typedef int (*rpcsvc_connect_t)(lnet_nid_t, int, u64, u32);

struct rpcsvc {
	struct pscrpc_import	*svc_import;
	lnet_nid_t		 svc_default_server_id;
	struct psclist_head	 svc_old_imports;
	psc_spinlock_t		 svc_lock;
	int			 svc_failed;
	int			 svc_initialized;
	rpcsvc_connect_t	 svc_connect;
};

struct rpcsvc *rpcsvcs[NRPCSVC];
struct pscrpc_nbreqset *ioNbReqSet;

int
rpc_io_interpret_set(struct pscrpc_request_set *set, __unusedx void *arg,
    int status)
{
        struct pscrpc_request *req;
        int rc = 0;

        /*
         * pscrpc_set_wait() already does this for us but it
         *  doesn't abort.
         */
        psclist_for_each_entry(req, &set->set_requests, rq_set_chain) {
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
	if (rpc_sendmsg(SRMT_CONNECT, version, magic, ctx->uid, ctx->gid) == -1)
		return (-errno);

#if 0
	struct slashrpc_connect_req *mq;

	rc = rpc_newreq(ptl, version, op, sizeof(*mq), 0, &rq, &u.m);
	if (rc)
		return (rc);
	mq->magic = magic;
	mq->version = version;
	mq->uid = ctx->uid;
	mq->gid = ctx->gid;
	rc = rpc_getrep(rq, 0, &dummy);
	pscrpc_req_finished(rq);
	if (rc)
#endif


	/* Save server PID from reply callback and mark initialized.  */
	imp->imp_connection->c_peer.pid = rq->rq_peer.pid;
	imp->imp_state = PSC_IMP_FULL;
	return (0);
}

/*
 * rpc_svc_create - create a client RPC service.
 * @rqptl: request portal ID.
 * @rpptl: reply portal ID.
 */
struct rpcsvc *
rpc_svc_create(u32 rqptl, u32 rpptl)
{
	struct rpcsvc *svc;

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
int
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
	rpcsvcs[RPCSVC_MDS] = rpc_svc_create(RPCMDS_REQ_PORTAL,
	    RPCMDS_REP_PORTAL);
	if (rpcmds_connect(nid, RPCSVC_MDS, SMDS_MAGIC, SMDS_VERSION))
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
	return (0);
}

/*
 * rpc_newreq - Create a new request and associate it with the import.
 * @ptl: which portal to create the request on, either RPCSVC_MDS or IO.
 * @version: version of communication protocol of channel.
 * @op: operation ID of command to send.
 * @reqlen: length of request buffer.
 * @replen: length of expected reply buffer.
 * @rqp: value-result of pointer to RPC request.
 * @mqp: value-result of pointer to start of request buffer.
 */
int
rpc_newreq(int ptl, int version, int op, int reqlen, int replen,
    struct pscrpc_request **rqp, void *mqp)
{
	struct pscrpc_import *imp;
	int rc;

	rc = 0;
	imp = rpcsvcs[ptl]->svc_import;
	*rqp = pscrpc_prep_req(imp, version, op, 1, &reqlen, NULL);
	if (*rqp == NULL)
		return (-ENOMEM);

	/* Setup request buffer. */
	*(void **)mqp = psc_msg_buf((*rqp)->rq_reqmsg, 0, reqlen);
	if (*(void **)mqp == NULL)
		psc_fatalx("psc_msg_buf");

	/* Setup reply buffer. */
	(*rqp)->rq_replen = psc_msg_size(1, &replen);
	return (0);
}

/*
 * rpc_getrep - Wait for a reply of a "simple" command, i.e. an error code.
 * @rq: the RPC request we sent.
 * @replen: anticipated size of response.
 * @mpp: value-result pointer where reply buffer start will be set.
 */
int
rpc_getrep(struct pscrpc_request *rq, int replen, void *mpp)
{
	int rc;

	/* Send the request and block on its completion. */
	rc = pscrpc_queue_wait(rq);
	if (rc)
		return (rc);
	*(void **)mpp = psc_msg_buf(rq->rq_repmsg, 0, replen);
	if (*(void **)mpp == NULL)
		return (-ENOMEM);
	return (0);
}

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
		struct slashrpc_link_req	*m_link;
		struct slashrpc_mkdir_req	*m_mkdir;
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
	struct pscrpc_request *rq;
	void *dummy;
	va_list ap;
	int rc;

	va_start(ap, op);
	switch (op) {
	case SRMT_ACCESS:
		rc = rpc_newreq(RPCSVC_MDS, SMDS_VERSION, op,
		    sizeof(*u.m_access), 0, &rq, &u.m);
		if (rc)
			return (rc);
		snprintf(u.m_access->path, sizeof(u.m_access->path),
		    "%s", va_arg(ap, const char *));
		u.m_access->mask = va_arg(ap, int);
		break;
	case SRMT_CHMOD:
		rc = rpc_newreq(RPCSVC_MDS, SMDS_VERSION, op,
		    sizeof(*u.m_chmod), 0, &rq, &u.m);
		if (rc)
			return (rc);
		snprintf(u.m_chmod->path, sizeof(u.m_chmod->path),
		    "%s", va_arg(ap, const char *));
		u.m_chmod->mode = va_arg(ap, mode_t);
		break;
	case SRMT_CHOWN:
		rc = rpc_newreq(RPCSVC_MDS, SMDS_VERSION, op,
		    sizeof(*u.m_chown), 0, &rq, &u.m);
		if (rc)
			return (rc);
		snprintf(u.m_chown->path, sizeof(u.m_chown->path),
		    "%s", va_arg(ap, const char *));
		u.m_chown->uid = va_arg(ap, uid_t);
		u.m_chown->gid = va_arg(ap, gid_t);
		break;
	case SRMT_CONNECT:
		rc = rpc_newreq(RPCSVC_MDS, SMDS_VERSION, op,
		    sizeof(*u.m_connect), 0, &rq, &u.m);
		if (rc)
			return (rc);
		u.m_connect->magic = va_arg(ap, u64);
		u.m_connect->version = va_arg(ap, u64);
		u.m_connect->uid = va_arg(ap, uid_t);
		u.m_connect->gid = va_arg(ap, gid_t);
		break;
	case SRMT_DESTROY:
		rc = rpc_newreq(RPCSVC_MDS, SMDS_VERSION, op,
		    sizeof(*u.m_destroy), 0, &rq, &u.m);
		if (rc)
			return (rc);
		break;
	case SRMT_LINK:
		rc = rpc_newreq(RPCSVC_MDS, SMDS_VERSION, op,
		    sizeof(*u.m_link), 0, &rq, &u.m);
		if (rc)
			return (rc);
		snprintf(u.m_link->from, sizeof(u.m_link->from),
		    "%s", va_arg(ap, const char *));
		snprintf(u.m_link->to, sizeof(u.m_link->to),
		    "%s", va_arg(ap, const char *));
		break;
	case SRMT_MKDIR:
		rc = rpc_newreq(RPCSVC_MDS, SMDS_VERSION, op,
		    sizeof(*u.m_mkdir), 0, &rq, &u.m);
		if (rc)
			return (rc);
		snprintf(u.m_mkdir->path, sizeof(u.m_mkdir->path),
		    "%s", va_arg(ap, const char *));
		u.m_mkdir->mode = va_arg(ap, mode_t);
		break;
	case SRMT_RELEASE:
		rc = rpc_newreq(RPCSVC_MDS, SMDS_VERSION, op,
		    sizeof(*u.m_release), 0, &rq, &u.m);
		if (rc)
			return (rc);
		u.m_release->cfd = va_arg(ap, u64);
		break;
	case SRMT_RELEASEDIR:
		rc = rpc_newreq(RPCSVC_MDS, SMDS_VERSION, op,
		    sizeof(*u.m_releasedir), 0, &rq, &u.m);
		if (rc)
			return (rc);
		u.m_releasedir->cfd = va_arg(ap, u64);
		break;
	case SRMT_RENAME:
		rc = rpc_newreq(RPCSVC_MDS, SMDS_VERSION, op,
		    sizeof(*u.m_rename), 0, &rq, &u.m);
		if (rc)
			return (rc);
		snprintf(u.m_rename->from, sizeof(u.m_rename->from),
		    "%s", va_arg(ap, const char *));
		snprintf(u.m_rename->to, sizeof(u.m_rename->to),
		    "%s", va_arg(ap, const char *));
		break;
	case SRMT_RMDIR:
		rc = rpc_newreq(RPCSVC_MDS, SMDS_VERSION, op,
		    sizeof(*u.m_rmdir), 0, &rq, &u.m);
		if (rc)
			return (rc);
		snprintf(u.m_rmdir->path, sizeof(u.m_rmdir->path),
		    "%s", va_arg(ap, const char *));
		break;
	case SRMT_SYMLINK:
		rc = rpc_newreq(RPCSVC_MDS, SMDS_VERSION, op,
		    sizeof(*u.m_symlink), 0, &rq, &u.m);
		if (rc)
			return (rc);
		snprintf(u.m_symlink->from, sizeof(u.m_symlink->from),
		    "%s", va_arg(ap, const char *));
		snprintf(u.m_symlink->to, sizeof(u.m_symlink->to),
		    "%s", va_arg(ap, const char *));
		break;
	case SRMT_TRUNCATE:
		rc = rpc_newreq(RPCSVC_MDS, SMDS_VERSION, op,
		    sizeof(*u.m_truncate), 0, &rq, &u.m);
		if (rc)
			return (rc);
		snprintf(u.m_truncate->path, sizeof(u.m_truncate->path),
		    "%s", va_arg(ap, const char *));
		u.m_truncate->size = va_arg(ap, size_t);
		break;
	case SRMT_UNLINK:
		rc = rpc_newreq(RPCSVC_MDS, SMDS_VERSION, op,
		    sizeof(*u.m_unlink), 0, &rq, &u.m);
		if (rc)
			return (rc);
		snprintf(u.m_unlink->path, sizeof(u.m_unlink->path),
		    "%s", va_arg(ap, const char *));
		break;
	case SRMT_UTIMES:
		rc = rpc_newreq(RPCSVC_MDS, SMDS_VERSION, op,
		    sizeof(*u.m_utimes), 0, &rq, &u.m);
		if (rc)
			return (rc);
		snprintf(u.m_utimes->path, sizeof(u.m_utimes->path),
		    "%s", va_arg(ap, const char *));
		memcpy(u.m_utimes->times, va_arg(ap, struct timespec *),
		    sizeof(u.m_utimes->times));
		break;
	default:
		psc_fatalx("unknown op: %d", op);
	}
	va_end(ap);

	rc = rpc_getrep(rq, 0, &dummy);
	pscrpc_req_finished(rq);
	return (rc);
}
