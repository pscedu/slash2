/* $Id$ */

/*
 * Routines for handling RPC requests for MDS from CLIENT.
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/fsuid.h>
#include <sys/vfs.h>

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <unistd.h>

#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/service.h"
#include "psc_util/lock.h"
#include "psc_util/strlcpy.h"

#include "cfd.h"
#include "fdbuf.h"
#include "fidc_common.h"
#include "fidc_mds.h"
#include "fidcache.h"
#include "mds.h"
#include "mdsexpc.h"
#include "slashexport.h"
#include "slashrpc.h"

#include "zfs-fuse/zfs_slashlib.h"

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
	struct pscrpc_bulk_desc *desc;
	struct bmap_mds_info *bmdsi;
	struct srm_bmap_req *mq;
	struct srm_bmap_rep *mp;
	struct bmapc_memb *bmap;
	struct mexpfcm *m;
	struct iovec iov;
	uint64_t cfd;

	ENTRY;

	bmap = NULL;
	RSX_ALLOCREP(rq, mq, mp);

	mp->rc = fdbuf_decrypt(&mq->sfdb, &cfd, NULL, rq->rq_peer);
	if (mp->rc)
		RETURN(0);

	/* Access the reference
	 */
	mp->rc = cfdlookup(rq->rq_export, cfd, &m);
	if (mp->rc == 0) {
		bmap = NULL;
		mp->rc = mds_bmap_load(m, mq, &bmap);
		if (mp->rc == 0) {
			bmdsi = bmap->bcm_pri;
			iov.iov_base = bmdsi->bmdsi_od;
			iov.iov_len = sizeof(*bmdsi->bmdsi_od);
			mp->rc = rsx_bulkserver(rq, &desc,
			    BULK_PUT_SOURCE, SRMC_BULK_PORTAL, &iov, 1);
			pscrpc_free_bulk(desc);
//			mds_bmap_ref_del(m);
			mp->nblks = 1;
		}
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
	struct slash_fidgen fg;
	void *data;

	ENTRY;

	RSX_ALLOCREP(rq, mq, mp);
	mp->rc = zfsslash2_opencreate(zfsVfs, mq->pino, &mq->creds, mq->flags,
				      mq->mode, mq->name, &fg,
				      &mp->attr, &data);
	if (!mp->rc) {
		extern struct cfdops mdsCfdOps;
		struct cfdent *cfd=NULL;

		mp->rc = slrmc_inode_cacheput(&fg, &mp->attr, &mq->creds);
		if (!mp->rc) {
			mp->rc = cfdnew(fg.fg_fid, rq->rq_export,
			    data, &cfd, &mdsCfdOps);

			if (!mp->rc && cfd) {
				fdbuf_encrypt(&cfd->fdb, &fg,
				    rq->rq_peer);
				memcpy(&mp->sfdb, &cfd->fdb,
				    sizeof(mp->sfdb));
			}

			psc_info("cfdnew() fid %"_P_U64"d rc=%d",
				 fg.fg_fid, mp->rc);
		}
	}

	RETURN(0);
}

static int
slrmc_open(struct pscrpc_request *rq)
{
	struct srm_open_req *mq;
	struct srm_opencreate_rep *mp;
	struct slash_fidgen fg;
	void *data;

	ENTRY;

	RSX_ALLOCREP(rq, mq, mp);

	mp->rc = zfsslash2_opencreate(zfsVfs, mq->ino, &mq->creds, mq->flags,
				      0, NULL, &fg, &mp->attr, &data);

	psc_info("zfsslash2_opencreate() fid %"_P_U64"d rc=%d",
		 mq->ino, mp->rc);

	if (!mp->rc) {
		extern struct cfdops mdsCfdOps;
		struct cfdent *cfd=NULL;

		mp->rc = slrmc_inode_cacheput(&fg, &mp->attr, &mq->creds);

		psc_info("slrmc_inode_cacheput() fid %"_P_U64"d rc=%d",
			 mq->ino, mp->rc);

		if (!mp->rc) {
			mp->rc = cfdnew(fg.fg_fid, rq->rq_export,
			    data, &cfd, &mdsCfdOps);

			if (!mp->rc && cfd) {
				fdbuf_encrypt(&cfd->fdb, &fg,
				    rq->rq_peer);
				memcpy(&mp->sfdb, &cfd->fdb,
				    sizeof(mp->sfdb));
			}

			psc_info("cfdnew() fid %"_P_U64"d rc=%d",
			    fg.fg_fid, mp->rc);
		}
	}
	RETURN(0);
}

static int
slrmc_opendir(struct pscrpc_request *rq)
{
	struct srm_opendir_req *mq;
	struct srm_opendir_rep *mp;
	struct slash_fidgen fg;
	void *data;

	ENTRY;

	RSX_ALLOCREP(rq, mq, mp);
	mp->rc = zfsslash2_opendir(zfsVfs, mq->ino, &mq->creds, &fg,
				   &data);

	psc_info("zfs opendir data (%p)", data);

	if (!mp->rc) {
		extern struct cfdops mdsCfdOps;
		struct cfdent *cfd;

		mp->rc = slrmc_inode_cacheput(&fg, NULL, &mq->creds);
		if (!mp->rc) {
			mp->rc = cfdnew(fg.fg_fid, rq->rq_export,
			    data, &cfd, &mdsCfdOps);

			if (mp->rc) {
				psc_error("cfdnew failed rc=%d", mp->rc);
				RETURN(0);
			}
			fdbuf_encrypt(&cfd->fdb, &fg, rq->rq_peer);
			memcpy(&mp->sfdb, &cfd->fdb, sizeof(mp->sfdb));
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
	struct slash_fidgen fg;
	struct iovec iov[2];
	struct mexpfcm *m;
	uint64_t cfd;

	ENTRY;

	RSX_ALLOCREP(rq, mq, mp);

	mp->rc = fdbuf_decrypt(&mq->sfdb, &cfd, &fg, rq->rq_peer);
	if (mp->rc)
		RETURN(0);

	if (cfdlookup(rq->rq_export, cfd, &m)) {
		mp->rc = -errno;
		RETURN(mp->rc);
	}
	iov[0].iov_base = PSCALLOC(mq->size);
	iov[0].iov_len = mq->size;

	if (mq->nstbpref) {
		iov[1].iov_len = mq->nstbpref * sizeof(struct srm_getattr_rep);
		iov[1].iov_base = PSCALLOC(iov[1].iov_len);
	} else {
		iov[1].iov_len = 0;
		iov[1].iov_base = NULL;
	}

	psc_info("zfs pri data (%p)", m);

	mp->rc = zfsslash2_readdir(zfsVfs, fg.fg_fid, &mq->creds, mq->size,
				   mq->offset, (char *)iov[0].iov_base,
				   &mp->size,
				   (struct srm_getattr_rep *)iov[1].iov_base,
				   mq->nstbpref,
				   ((struct fidc_mds_info *)m->mexpfcm_fcmh->fcmh_fcoo->fcoo_pri)->fmdsi_data);

	if (mp->rc) {
		PSCFREE(iov[0].iov_base);
		if (mq->nstbpref)
			PSCFREE(iov[1].iov_base);
		RETURN(mp->rc);
	}

	if (mq->nstbpref) {
		mp->rc = rsx_bulkserver(rq, &desc, BULK_PUT_SOURCE,
					SRMC_BULK_PORTAL, iov, 2);

	} else
		mp->rc = rsx_bulkserver(rq, &desc, BULK_PUT_SOURCE,
					SRMC_BULK_PORTAL, iov, 1);

	if (desc)
		pscrpc_free_bulk(desc);

	PSCFREE(iov[0].iov_base);
	if (mq->nstbpref)
		PSCFREE(iov[1].iov_base);
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
	struct slash_fidgen fg;
	struct mexpfcm *m;
	struct fidc_membh *f;
	struct cfdent *c;
	struct fidc_mds_info *i;
	uint64_t cfd;
	int rc;

	ENTRY;

	RSX_ALLOCREP(rq, mq, mp);

	mp->rc = fdbuf_decrypt(&mq->sfdb, &cfd, &fg, rq->rq_peer);
	if (mp->rc)
		RETURN(0);

	c = cfdget(rq->rq_export, cfd);
	if (!c) {
		psc_info("cfdget() failed cfd %"_P_U64"d", cfd);
		mp->rc = ENOENT;
		RETURN(0);
	}
	psc_assert(c->pri);
	m = c->pri;
	psc_assert(m->mexpfcm_fcmh);
	f = m->mexpfcm_fcmh;

	psc_assert(f->fcmh_fcoo);
	i = f->fcmh_fcoo->fcoo_pri;

	rc = cfdfree(rq->rq_export, cfd);
	psc_info("cfdfree() cfd %"_P_U64"d rc=%d",
		 cfd, rc);
	/* Serialize the test for releasing the zfs inode
	 *  so that this segment is re-entered.  Also, note that
	 *  'm' may have been freed already.
	 */
	spinlock(&f->fcmh_lock);

	DEBUG_FCMH(PLL_DEBUG, f, "slrmc_release i->fmdsi_ref (%d) (oref=%d)",
		   atomic_read(&i->fmdsi_ref), f->fcmh_fcoo->fcoo_oref_rw[0]);

	if (atomic_dec_and_test(&i->fmdsi_ref)) {
		psc_assert(SPLAY_EMPTY(&i->fmdsi_exports));
		f->fcmh_state |= FCMH_FCOO_CLOSING;

		DEBUG_FCMH(PLL_DEBUG, f, "calling zfsslash2_release");
		mp->rc = zfsslash2_release(zfsVfs, fg.fg_fid, &mq->creds,
					   i->fmdsi_data);
		/* Remove the fcoo but first make sure the open ref's
		 *  are ok.  This value is bogus, fmdsi_ref has the
		 *  the real open ref.
		 */
		PSCFREE(i);
		f->fcmh_fcoo->fcoo_pri = NULL;
		f->fcmh_fcoo->fcoo_oref_rw[0] = 0;
		freelock(&f->fcmh_lock);
		fidc_fcoo_remove(f);
	} else {
		mp->rc = 0;
		freelock(&f->fcmh_lock);
	}
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

	if (fcmh)
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
