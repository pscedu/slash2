/* $Id$ */

/*
 * Routines for handling RPC requests for MDS from CLIENT.
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/fsuid.h>
#include <sys/vfs.h>

#define __USE_GNU
#include <fcntl.h>
#undef __USE_GNU

#include <errno.h>
#include <inttypes.h>
#include <unistd.h>

#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/service.h"
#include "psc_util/acsvc.h"
#include "psc_util/lock.h"
#include "psc_util/strlcpy.h"

#include "fidcache.h"
#include "fidc_common.h"
#include "fidc_mds.h"
#include "slashrpc.h"
#include "slashexport.h"
#include "cfd.h"
#include "mdsexpc.h"
#include "mds.h"
#include "../../zfs/zfs-fuse-0.5.0_slash/src/zfs-fuse/zfs_slashlib.h"

extern void *zfsVfs;
psc_spinlock_t fsidlock = LOCK_INITIALIZER;

static int
slrmc_inode_cacheput(struct slash_fidgen *fg, struct stat *stb, 
		     struct slash_creds *creds)
{
	struct fidc_memb fcm;
	struct fidc_membh *fcmh;	
        int flags=(FIDC_LOOKUP_CREATE | FIDC_LOOKUP_LOAD);
	
	ENTRY;
	if (stb) {
		flags |= FIDC_LOOKUP_COPY;
		FCM_FROM_FG_ATTR(&fcm, fg, stb);
	}
		
	fcmh = __fidc_lookup_inode(fg, flags, (stb ? (&fcm) : NULL),  creds);
	
	if (fcmh) {
		fidc_membh_dropref(fcmh);
		RETURN(0);
	} else
		RETURN(-1);
}

int
slrmc_connect(struct pscrpc_request *rq)
{
	struct pscrpc_export *e=rq->rq_export;
        struct srm_connect_req *mq;
        struct srm_generic_rep *mp;   
	struct slashrpc_export *sexp;
   	struct mexp_cli *mexp_cli;

	ENTRY;

        RSX_ALLOCREP(rq, mq, mp);
        if (mq->magic != SRMC_MAGIC || mq->version != SRMC_VERSION)
                mp->rc = -EINVAL;

	if (e->exp_private) {
		/* Client has issued a reconnect.  For now just dump
		 *   the remaining cached bmaps.
		 */
		spinlock(&e->exp_lock);
		sexp = slashrpc_export_get(e);
		psc_assert(sexp->sexp_data);
		sexp->sexp_type |= EXP_CLOSING;
		freelock(&e->exp_lock);
		DEBUG_REQ(PLL_WARN, rq, 
 			  "connect rq but export already exists");
		slashrpc_export_destroy((void *)sexp);
	}
	spinlock(&e->exp_lock);
	sexp = slashrpc_export_get(e);

	psc_assert(!sexp->sexp_data);
	sexp->sexp_type = MDS_CLI_EXP;
	sexp->sexp_export = e;
	/* XXX allocated twice? slashrpc_export_get() */
	mexp_cli = sexp->sexp_data = PSCALLOC(sizeof(*mexp_cli));
	/* Allocate client service for callbacks.
	 */
	mexp_cli->mc_csvc = rpc_csvc_create(SRCM_REQ_PORTAL, SRCM_REP_PORTAL);
	if (!mexp_cli->mc_csvc)
		psc_fatal("rpc_csvc_create() failed");
	/* See how this works out, I'm just going to borrow
	 *   the export's  connection structure.
	 */
	psc_assert(e->exp_connection);
	mexp_cli->mc_csvc->csvc_import->imp_connection = e->exp_connection;
	freelock(&e->exp_lock);

	mp->data = (u64)sexp->sexp_data;

        RETURN(0);
}


static int 
slrmc_access(struct pscrpc_request *rq)
{
	struct srm_access_req *mq;
	struct srm_generic_rep *mp;

	ENTRY;

	RSX_ALLOCREP(rq, mq, mp);
	mp->rc = zfsslash2_access(zfsVfs, mq->ino, mq->mask, &mq->creds);

	RETURN(0);
}


static int
slrmc_getattr(struct pscrpc_request *rq)
{
	struct srm_getattr_req *mq;
	struct srm_getattr_rep *mp;

	ENTRY;

	RSX_ALLOCREP(rq, mq, mp);
	mp->rc = zfsslash2_getattr(zfsVfs, mq->ino, &mq->creds, &mp->attr, 
				   &mp->gen);

	psc_info("zfsslash2_getattr() ino=%"_P_U64"d gen=%"_P_U64"d rc=%d", 
		 mq->ino, mp->gen, mp->rc);

	RETURN(0);
}

static int
slrmc_getbmap(struct pscrpc_request *rq)
{
	struct srm_bmap_req *mq;
	struct srm_bmap_rep *mp;
	struct mexpfcm *fref;
	slfid_t fid;
	struct bmapc_memb *bmap;

	ENTRY;

	RSX_ALLOCREP(rq, mq, mp);
	/* Access the reference 
	 */
	if (cfd2fid_p(rq->rq_export, mq->cfd, &fid, (void **)&fref))
		mp->rc = -errno;
	else {
		psc_assert(fref);
		mp->rc = mds_bmap_load(fref, mq, &bmap);
	}
	RETURN(0);
}

static int
slrmc_link(struct pscrpc_request *rq)
{
	struct srm_link_req *mq;
        struct srm_link_rep *mp;

	ENTRY;

	RSX_ALLOCREP(rq, mq, mp);
	mp->rc = zfsslash2_link(zfsVfs, mq->ino, mq->pino, mq->name, 
				&mp->fg, &mq->creds, &mp->attr);

        RETURN(0);
}

static int
slrmc_lookup(struct pscrpc_request *rq)
{
	struct srm_lookup_req *mq;
        struct srm_lookup_rep *mp;

	ENTRY;

	RSX_ALLOCREP(rq, mq, mp);
	mp->rc = zfsslash2_lookup(zfsVfs, mq->pino, mq->name, &mp->fg, &mq->creds, 
				  &mp->attr);

        RETURN(0);
}

static int
slrmc_mkdir(struct pscrpc_request *rq)
{
	struct srm_mkdir_req *mq;
	struct srm_mkdir_rep *mp;
       
	ENTRY;

	RSX_ALLOCREP(rq, mq, mp);
	mp->rc = zfsslash2_mkdir(zfsVfs, mq->pino, mq->name, mq->mode, 
				 &mq->creds, &mp->attr, &mp->fg);

	RETURN(0);
}

static int
slrmc_create(struct pscrpc_request *rq)
{
	struct srm_create_req *mq;
	struct srm_opencreate_rep *mp;
	void *data;

	ENTRY;

	RSX_ALLOCREP(rq, mq, mp);
	mp->rc = zfsslash2_opencreate(zfsVfs, mq->pino, &mq->creds, mq->flags, 
				      mq->mode, mq->name, &mp->fg, 
				      &mp->attr, &data);
	if (!mp->rc) {
		extern struct cfdops mdsCfdOps;
		struct cfdent *cfd=NULL;

		mp->rc = slrmc_inode_cacheput(&mp->fg, &mp->attr, &mq->creds);
		if (!mp->rc) {
			cfdnew(mp->fg.fg_fid, rq->rq_export, data, &cfd, 
			       &mdsCfdOps);

			if (!mp->rc && cfd)
				mp->cfd = cfd->cfd;

			psc_info("cfdnew() fid %"_P_U64"d CFD (%"_P_U64"d) rc=%d",
				 mp->fg.fg_fid, mp->cfd, mp->rc);
		}
	}

	RETURN(0);
}
static int 
slrmc_open(struct pscrpc_request *rq) {
	struct srm_open_req *mq;
	struct srm_opencreate_rep *mp;
	void *data;

	ENTRY;

	RSX_ALLOCREP(rq, mq, mp);

	mp->rc = zfsslash2_opencreate(zfsVfs, mq->ino, &mq->creds, mq->flags, 
				      0, NULL, &mp->fg, &mp->attr, &data);

	psc_info("zfsslash2_opencreate() fid %"_P_U64"d rc=%d", 
		 mq->ino, mp->rc);

	if (!mp->rc) {
		extern struct cfdops mdsCfdOps;
		struct cfdent *cfd=NULL;

		mp->rc = slrmc_inode_cacheput(&mp->fg, &mp->attr, &mq->creds);
		
		psc_info("slrmc_inode_cacheput() fid %"_P_U64"d rc=%d", 
			 mq->ino, mp->rc);

		if (!mp->rc) {
			mp->rc = cfdnew(mp->fg.fg_fid, rq->rq_export, data, 
					&cfd, &mdsCfdOps);

			if (!mp->rc && cfd)
				mp->cfd = cfd->cfd;	

			psc_info("cfdnew() fid %"_P_U64"d CFD (%"_P_U64"d) rc=%d",
				 mp->fg.fg_fid, mp->cfd, mp->rc);
		}
	}
	RETURN(0);
}

static int
slrmc_opendir(struct pscrpc_request *rq)
{
	struct srm_opendir_req *mq;
	struct srm_opencreate_rep *mp;
	void *data;

	ENTRY;

	RSX_ALLOCREP(rq, mq, mp);
	mp->rc = zfsslash2_opendir(zfsVfs, mq->ino, &mq->creds, &mp->fg, 
				   &data);

	psc_info("zfs opendir data (%p)", data);

	if (!mp->rc) {
		extern struct cfdops mdsCfdOps;
		struct cfdent *cfd;

		mp->rc = slrmc_inode_cacheput(&mp->fg, NULL, &mq->creds);
		if (!mp->rc) {
			mp->rc = cfdnew(mp->fg.fg_fid, rq->rq_export, data, 
					&cfd, &mdsCfdOps);
			if (mp->rc) {
				psc_error("cfdnew failed rc=%d", mp->rc);
				RETURN(0);
			}
			mp->cfd = cfd->cfd;
		}
	}
	RETURN(0);
}

static int
slrmc_readdir(struct pscrpc_request *rq)
{
	struct pscrpc_bulk_desc *desc;
	struct srm_readdir_req *mq;
	struct srm_readdir_rep *mp;
	struct iovec iov;
	slfid_t fid;
	void *data;

	ENTRY;

	RSX_ALLOCREP(rq, mq, mp);
	if (__cfd2fid(rq->rq_export, mq->cfd, &fid, &data)) {
		mp->rc = -errno;
		RETURN(mp->rc);
	}
	iov.iov_base = PSCALLOC(mq->size);
	iov.iov_len = mq->size;
	
	psc_info("zfs pri data (%p)", data);

	mp->rc = zfsslash2_readdir(zfsVfs, fid, &mq->creds, mq->size, 
	   mq->offset, (char *)iov.iov_base, &mp->size, data);

	if (mp->rc) {
		PSCFREE(iov.iov_base);
		RETURN(mp->rc);
	}

	mp->rc = rsx_bulkserver(rq, &desc, BULK_PUT_SOURCE,
	    SRMC_BULK_PORTAL, &iov, 1);

	if (desc)
		pscrpc_free_bulk(desc);

	PSCFREE(iov.iov_base);
	RETURN(0);
}

static int
slrmc_readlink(struct pscrpc_request *rq)
{
	struct srm_readlink_req *mq;
        struct srm_readlink_rep *mp;

	ENTRY;

	RSX_ALLOCREP(rq, mq, mp);
	mp->rc = zfsslash2_readlink(zfsVfs, mq->ino, mp->buf, &mq->creds);
	
	RETURN(0);
}

static int 
slrmc_release(struct pscrpc_request *rq)
{
	struct srm_release_req *mq;
        struct srm_generic_rep *mp;
	struct mexpfcm *m;
	struct fidc_membh *f;
	struct cfdent *c;
	slfid_t fid;
	int rc;

	ENTRY;

	RSX_ALLOCREP(rq, mq, mp);
	
	c = cfdget(rq->rq_export, mq->cfd);
	if (!c) {
		psc_info("cfdget() failed cfd %"_P_U64"d", mq->cfd);
		mp->rc = ENOENT;
		RETURN(0);
	}
	fid = c->fid;
	psc_assert(c->pri);
	m = c->pri;
	psc_assert(m->mexpfcm_fcmh);
	f = m->mexpfcm_fcmh;
	
	rc = cfdfree(rq->rq_export, mq->cfd);
	psc_info("cfdfree() cfd %"_P_U64"d rc=%d", 
		 mq->cfd, rc);
	/* Serialize the test for releasing the zfs inode
	 *  so that this segment is re-entered.  Also, note that
	 *  'm' may have been freed already.
	 */	
	spinlock(&f->fcmh_lock);
	
	DEBUG_FCMH(PLL_INFO, f, "slrmc_release");

	if (f->fcmh_state & FCMH_FCOO_CLOSING) {
		struct fidc_mds_info *i;
		i = f->fcmh_fcoo->fcoo_pri;
		mp->rc = zfsslash2_release(zfsVfs, fid, &mq->creds, 
					   i->fmdsi_data);
		fidc_fcoo_remove(f);
	} else
		mp->rc = 0;
	freelock(&f->fcmh_lock);

	RETURN(0);
}

static int
slrmc_rename(struct pscrpc_request *rq)
{
	char from[NAME_MAX+1], to[NAME_MAX+1];
	struct pscrpc_bulk_desc *desc;
	struct srm_rename_req *mq;
	struct srm_generic_rep *mp;
	struct iovec iov[2];

	ENTRY;

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->fromlen == 0 || mq->fromlen >= NAME_MAX ||
	    mq->tolen == 0 || mq->tolen >= NAME_MAX) {
		mp->rc = -EINVAL;
		RETURN(0);
	}
	iov[0].iov_base = (void *)from;
	iov[0].iov_len = mq->fromlen;
	iov[1].iov_base = (void *)to;
	iov[1].iov_len = mq->tolen;

	if ((mp->rc = rsx_bulkserver(rq, &desc, BULK_GET_SINK,
	    SRMC_BULK_PORTAL, iov, 2)) != 0)
		RETURN(0);
	from[mq->fromlen] = '\0';
	to[mq->tolen] = '\0';
	pscrpc_free_bulk(desc);

	mp->rc = zfsslash2_rename(zfsVfs, mq->opino, from, 
				  mq->npino, to, &mq->creds);
	RETURN(0);
}

static int
slrmc_setattr(struct pscrpc_request *rq)
{
        struct srm_setattr_req *mq;
        struct srm_setattr_rep *mp;
	struct fidc_membh *fcmh;
	struct fidc_mds_info *fmdsi;

	ENTRY;

	RSX_ALLOCREP(rq, mq, mp);

	fmdsi = fidc_fid2fmdsi((slfid_t)mq->ino, &fcmh);
	if (fmdsi)
		psc_assert(fcmh);
	/* An fmdsi means that the file is 'open' and therefore
	 *  we have a valid zfs handle. 
	 * A null fmdsi means that the file is either not opened
	 *  or not cached.  In that case try to pass the inode
	 *  into zfs with the hope that it has it cached.
	 */
	mp->rc = zfsslash2_setattr(zfsVfs, mq->ino, &mq->attr, 
		   mq->to_set, &mq->creds, &mp->attr, 
		   (fmdsi) ? fmdsi->fmdsi_data : NULL);
	
	if (mp->rc == ENOENT) {
		//XXX need to figure out how to 'lookup' via the immns.
		// right now I don't know how to start a lookup from "/"?
		//  it's either inode # 1 or 3
	}

	if (fmdsi)
		fidc_membh_dropref(fcmh);

	RETURN(0);
}

static int
slrmc_statfs(struct pscrpc_request *rq)
{
	struct srm_statfs_req *mq;
	struct srm_statfs_rep *mp;

	ENTRY;

	RSX_ALLOCREP(rq, mq, mp);
	mp->rc = zfsslash2_statfs(zfsVfs, &mp->stbv);

	RETURN(0);
}

static int
slrmc_symlink(struct pscrpc_request *rq)
{
	char link[PATH_MAX];
	struct pscrpc_bulk_desc *desc;
	struct srm_symlink_req *mq;
	struct srm_symlink_rep *mp;
	struct iovec iov[1];

	ENTRY;

	RSX_ALLOCREP(rq, mq, mp);
	if ((mq->linklen >= PATH_MAX) ||
	    (mq->namelen >= NAME_MAX)) {
		mp->rc = -ENAMETOOLONG;
		RETURN(0);
	}
	iov[0].iov_base = link;
	iov[0].iov_len = mq->linklen;
	if ((mp->rc = rsx_bulkserver(rq, &desc, BULK_GET_SINK,
	    SRMC_BULK_PORTAL, iov, 1)) != 0)
		RETURN(0);
	link[mq->linklen] = '\0';
	pscrpc_free_bulk(desc);

	mp->rc = zfsslash2_symlink(zfsVfs, link, mq->pino, mq->name, 
				   &mq->creds, &mp->attr, &mp->fg);
	RETURN(0);
}

static int
slrmc_unlink(struct pscrpc_request *rq, int ford)
{
	struct srm_unlink_req *mq;
        struct srm_unlink_rep *mp;

	ENTRY;

	RSX_ALLOCREP(rq, mq, mp);
	if (ford)
		mp->rc = zfsslash2_unlink(zfsVfs, mq->pino, 
					  mq->name, &mq->creds);
	else 
		mp->rc = zfsslash2_rmdir(zfsVfs, mq->pino, 
					 mq->name, &mq->creds);
	
        RETURN(0);
}
int
slrmc_handler(struct pscrpc_request *rq)
{
	int rc = 0;

	switch (rq->rq_reqmsg->opc) {
	case SRMT_ACCESS:
		rc = slrmc_access(rq);
		break;
	case SRMT_BMAPCHMODE:
		break;
	case SRMT_CONNECT:
		rc = slrmc_connect(rq);
		break;
	case SRMT_CREATE:
		rc = slrmc_create(rq);
		break;
	case SRMT_DESTROY:	/* client has unmounted */
		break;
	case SRMT_GETATTR:
		rc = slrmc_getattr(rq);
		break;
	case SRMT_GETBMAP:
		rc = slrmc_getbmap(rq);
		break;
	case SRMT_LINK:
		rc = slrmc_link(rq);
		break;
	case SRMT_LOCK:
		break;
	case SRMT_MKDIR:
		rc = slrmc_mkdir(rq);
		break;
	case SRMT_MKNOD:
		//rc = slrmc_mknod(rq);
		break;
	case SRMT_LOOKUP:
		rc = slrmc_lookup(rq);
		break;
	case SRMT_OPEN:
		rc = slrmc_open(rq);
		break;
	case SRMT_OPENDIR:
		rc = slrmc_opendir(rq);
		break;
	case SRMT_READDIR:
		rc = slrmc_readdir(rq);
		break;
	case SRMT_READLINK:
		rc = slrmc_readlink(rq);
		break;
	case SRMT_RELEASE:
		rc = slrmc_release(rq);
		break;
	case SRMT_RENAME:
		rc = slrmc_rename(rq);
		break;
	case SRMT_RMDIR:
		rc = slrmc_unlink(rq, 0);
		break;
	case SRMT_SETATTR:
		rc = slrmc_setattr(rq);
                break;
	case SRMT_STATFS:
		rc = slrmc_statfs(rq);
		break;
	case SRMT_SYMLINK:
		rc = slrmc_symlink(rq);
		break;
	case SRMT_UNLINK:
		rc = slrmc_unlink(rq, 1);
		break;
	default:
		psc_errorx("Unexpected opcode %d", rq->rq_reqmsg->opc);
		rq->rq_status = -ENOSYS;
		return (pscrpc_error(rq));
	}
	target_send_reply_msg(rq, rc, 0);
	return (rc);
}
