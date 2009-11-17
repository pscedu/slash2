/* $Id$ */

/*
 * Routines for handling RPC requests for MDS from CLIENT.
 */

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
#include "fidc_mds.h"
#include "fidcache.h"
#include "mdsexpc.h"
#include "mdsio_zfs.h"
#include "pathnames.h"
#include "repl_mds.h"
#include "rpc_mds.h"
#include "slashd.h"
#include "slashexport.h"
#include "slashrpc.h"
#include "slerr.h"

#include "zfs-fuse/zfs_slashlib.h"

psc_spinlock_t fsidlock = LOCK_INITIALIZER;

int
slmrmcthr_inode_cacheput(struct slash_fidgen *fg, struct stat *stb,
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

	fidc_lookup(fg, flags, stb ? &fcm : NULL, creds, &fcmh);

	if (fcmh) {
		fidc_membh_dropref(fcmh);
		RETURN(0);
	} else
		RETURN(-1);
}

int
slm_rmc_handle_connect(struct pscrpc_request *rq)
{
	struct pscrpc_export *e=rq->rq_export;
	struct srm_connect_req *mq;
	struct srm_generic_rep *mp;
	struct slashrpc_export *slexp;
	struct mexp_cli *mexp_cli;

	ENTRY;

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->magic != SRMC_MAGIC || mq->version != SRMC_VERSION)
		mp->rc = -EINVAL;

	if (e->exp_private) {
		/*
		 * XXX this should never happen; it should have been
		 * cleaned up by the hldrop routine.
		 */

		/* Client has issued a reconnect.  For now just dump
		 *   the remaining cached bmaps.
		 */
		spinlock(&e->exp_lock);
		slexp = slashrpc_export_get(e);
		psc_assert(slexp->slexp_data);
		slexp->slexp_type |= EXP_CLOSING;
		freelock(&e->exp_lock);
		DEBUG_REQ(PLL_WARN, rq,
			  "connect rq but export already exists");
		slashrpc_export_destroy(slexp);
	}
	spinlock(&e->exp_lock);
	slexp = slashrpc_export_get(e);

	psc_assert(!slexp->slexp_data);
	slexp->slexp_type = MDS_CLI_EXP;
	slexp->slexp_export = e;

	mexp_cli = slexp->slexp_data = PSCALLOC(sizeof(*mexp_cli));
	LOCK_INIT(&mexp_cli->mc_lock);
	slm_initclconn(mexp_cli, e);
	freelock(&e->exp_lock);

	RETURN(0);
}

int
slm_rmc_handle_access(struct pscrpc_request *rq)
{
	struct srm_access_req *mq;
	struct srm_generic_rep *mp;

	ENTRY;

	RSX_ALLOCREP(rq, mq, mp);
	mp->rc = zfsslash2_access(zfsVfs, mq->ino, mq->mask, &mq->creds);

	RETURN(0);
}

int
slm_rmc_handle_getattr(struct pscrpc_request *rq)
{
	struct srm_getattr_req *mq;
	struct srm_getattr_rep *mp;

	ENTRY;

	RSX_ALLOCREP(rq, mq, mp);
	mp->rc = zfsslash2_getattr(zfsVfs, mq->ino, &mq->creds, &mp->attr,
				   &mp->gen);

	psc_info("zfsslash2_getattr() ino=%"PRId64" gen=%"PRId64" rc=%d",
		 mq->ino, mp->gen, mp->rc);

	RETURN(0);
}

int
slm_rmc_handle_getbmap(struct pscrpc_request *rq)
{
	struct slash_bmap_cli_wire *cw;
	struct pscrpc_bulk_desc *desc;
	struct bmap_mds_info *bmdsi;
	struct srt_bmapdesc_buf bdb;
	const struct srm_bmap_req *mq;
	struct srm_bmap_rep *mp;
	struct bmapc_memb *bmap;
	struct iovec iov[4];
	struct mexpfcm *m;
	uint64_t cfd;
	int niov;

	ENTRY;

	bmap = NULL;
	RSX_ALLOCREP(rq, mq, mp);

	mp->rc = fdbuf_check(&mq->sfdb, &cfd, NULL, rq->rq_peer);
	if (mp->rc)
		RETURN(mp->rc);

	/* Access the reference
	 */
	mp->rc = cfdlookup(rq->rq_export, cfd, &m);
	if (mp->rc)
		RETURN(mp->rc);

	bmap = NULL;
	mp->rc = mds_bmap_load_cli(m, mq, &bmap);
	if (mp->rc)
		RETURN(mp->rc);

	bmdsi = bmap->bcm_pri;
	cw = (struct slash_bmap_cli_wire *)bmdsi->bmdsi_od->bh_crcstates;

	niov = 2;
	iov[0].iov_base = cw;
	iov[0].iov_len = sizeof(*cw);
	iov[1].iov_base = &bdb;
	iov[1].iov_len = sizeof(bdb);

	if (mq->getreptbl) {
		struct slash_inode_handle *ih;

		niov++;
		ih = fcmh_2_inoh(bmap->bcm_fcmh);
		iov[2].iov_base = &ih->inoh_ino.ino_repls;
		iov[2].iov_len = sizeof(sl_replica_t) * INO_DEF_NREPLS;
	}

	if (bmap->bcm_mode & BMAP_WR) {
		/* Always return the write IOS if the bmap is in write mode.
		 */
		psc_assert(bmdsi->bmdsi_wr_ion);
		mp->ios_nid = bmdsi->bmdsi_wr_ion->mi_resm->resm_nid;
	}

	bdbuf_sign(&bdb, &mq->sfdb.sfdb_secret.sfs_fg, rq->rq_peer,
	   (mq->rw == SRIC_BMAP_WRITE ? mp->ios_nid : LNET_NID_ANY),
	   (mq->rw == SRIC_BMAP_WRITE ?
	    bmdsi->bmdsi_wr_ion->mi_resm->resm_res->res_id : IOS_ID_ANY),
	   bmap->bcm_blkno);

	mp->rc = rsx_bulkserver(rq, &desc, BULK_PUT_SOURCE,
	    SRMC_BULK_PORTAL, iov, niov);
	if (desc)
		pscrpc_free_bulk(desc);
	mp->nblks = 1;

	RETURN(mp->rc);
}

int
slm_rmc_handle_link(struct pscrpc_request *rq)
{
	struct srm_link_req *mq;
	struct srm_link_rep *mp;

	ENTRY;

	RSX_ALLOCREP(rq, mq, mp);
	mq->name[sizeof(mq->name) - 1] = '\0';
	mp->rc = zfsslash2_link(zfsVfs, mq->ino, mq->pino, mq->name,
				&mp->fg, &mq->creds, &mp->attr);

	RETURN(0);
}

int
slm_rmc_handle_lookup(struct pscrpc_request *rq)
{
	struct srm_lookup_req *mq;
	struct srm_lookup_rep *mp;

	ENTRY;

	RSX_ALLOCREP(rq, mq, mp);
	mq->name[sizeof(mq->name) - 1] = '\0';
	if (mq->pino == SL_ROOT_INUM &&
	    strncmp(mq->name, SL_PATH_PREFIX,
	     strlen(SL_PATH_PREFIX)) == 0)
		mp->rc = EINVAL;
	else
		mp->rc = zfsslash2_lookup(zfsVfs, mq->pino,
		    mq->name, &mp->fg, &mq->creds, &mp->attr);
	RETURN(0);
}

int
slm_rmc_handle_mkdir(struct pscrpc_request *rq)
{
	struct srm_mkdir_req *mq;
	struct srm_mkdir_rep *mp;

	ENTRY;

	RSX_ALLOCREP(rq, mq, mp);
	mq->name[sizeof(mq->name) - 1] = '\0';
	mp->rc = zfsslash2_mkdir(zfsVfs, mq->pino, mq->name,
	    mq->mode, &mq->creds, &mp->attr, &mp->fg, 0);
	RETURN(0);
}

int
slm_rmc_translate_flags(int in, int *out)
{
	*out = 0;

	if (in & ~(SL_FREAD | SL_FWRITE | SL_FAPPEND | SL_FCREAT |
	    SL_FTRUNC | SL_FOFFMAX | SL_FSYNC | SL_FDSYNC |
	    SL_FRSYNC | SL_FEXCL | SL_FNODSYNC))
		return (EINVAL);
	/* XXX SL_FNOFOLLOW not implemented */
	if ((in & (SL_FREAD | SL_FWRITE)) == 0)
		return (EINVAL);

	if (in & SL_FREAD)
		*out |= SL_FREAD;
	if (in & SL_FWRITE)
		*out |= SL_FWRITE;
	if (in & SL_FCREAT)
		*out |= SL_FCREAT;
	if (in & SL_FEXCL)
		*out |= SL_FEXCL;
	*out |= SL_FOFFMAX;
	return (0);
}

int
slm_rmc_handle_create(struct pscrpc_request *rq)
{
	struct srm_create_req *mq;
	struct srm_opencreate_rep *mp;
	struct slash_fidgen fg;
	void *data;
	int fl;

	ENTRY;

	RSX_ALLOCREP(rq, mq, mp);
	mq->name[sizeof(mq->name) - 1] = '\0';
	mp->rc = slm_rmc_translate_flags(mq->flags, &fl);
	if (mp->rc)
		RETURN(0);
	mp->rc = zfsslash2_opencreate(zfsVfs, mq->pino, &mq->creds, fl,
	    mq->mode, mq->name, &fg, &mp->attr, &data);
	if (!mp->rc) {
		struct cfdent *cfd=NULL;

		mp->rc = slmrmcthr_inode_cacheput(&fg, &mp->attr, &mq->creds);
		if (!mp->rc) {
			mp->rc = cfdnew(fg.fg_fid, rq->rq_export,
				data, &cfd, &mdsCfdOps, CFD_FILE);

			if (!mp->rc && cfd) {
				fdbuf_sign(&cfd->cfd_fdb,
				    &fg, rq->rq_peer);
				memcpy(&mp->sfdb, &cfd->cfd_fdb,
				    sizeof(mp->sfdb));
			}

			psc_info("cfdnew() fid %"PRId64" rc=%d",
				 fg.fg_fid, mp->rc);
		}

		/*
		 * On success, the cfd private data, originally the ZFS
		 * handle private data, is overwritten with an fmdsi,
		 * so release the ZFS handle if we failed or didn't use it.
		 */
		if (cfd == NULL || cfd_2_zfsdata(cfd) != data)
			zfsslash2_release(zfsVfs, fg.fg_fid,
			    &mq->creds, data);
	}
	RETURN(0);
}

int
slm_rmc_handle_open(struct pscrpc_request *rq)
{
	struct srm_open_req *mq;
	struct srm_opencreate_rep *mp;
	struct slash_fidgen fg;
	void *data;
	int fl;

	ENTRY;

	RSX_ALLOCREP(rq, mq, mp);
	mp->rc = slm_rmc_translate_flags(mq->flags, &fl);
	if (mp->rc)
		RETURN(0);
	mp->rc = zfsslash2_opencreate(zfsVfs, mq->ino, &mq->creds,
	    fl, 0, NULL, &fg, &mp->attr, &data);

	psc_info("zfsslash2_opencreate() fid %"PRId64" rc=%d",
		 mq->ino, mp->rc);

	if (!mp->rc) {
		struct cfdent *cfd=NULL;

		mp->rc = slmrmcthr_inode_cacheput(&fg, &mp->attr, &mq->creds);

		psc_info("slmrmcthr_inode_cacheput() fid %"PRId64" rc=%d",
			 mq->ino, mp->rc);

		if (!mp->rc) {
			mp->rc = cfdnew(fg.fg_fid, rq->rq_export,
				data, &cfd, &mdsCfdOps, CFD_FILE);

			if (!mp->rc && cfd) {
				fdbuf_sign(&cfd->cfd_fdb,
				    &fg, rq->rq_peer);
				memcpy(&mp->sfdb, &cfd->cfd_fdb,
				    sizeof(mp->sfdb));
			}

			psc_info("cfdnew() fid %"PRId64" rc=%d",
			    fg.fg_fid, mp->rc);
		}

		/*
		 * On success, the cfd private data, originally the ZFS
		 * handle private data, is overwritten with an fmdsi,
		 * so release the ZFS handle if we failed or didn't use it.
		 */
		if (cfd == NULL || cfd_2_zfsdata(cfd) != data)
			zfsslash2_release(zfsVfs, fg.fg_fid,
			    &mq->creds, data);
	}
	RETURN(0);
}

int
slm_rmc_handle_opendir(struct pscrpc_request *rq)
{
	struct srm_opendir_req *mq;
	struct srm_opendir_rep *mp;
	struct slash_fidgen fg;
	struct stat stb;
	void *data;

	ENTRY;

	RSX_ALLOCREP(rq, mq, mp);
	mp->rc = zfsslash2_opendir(zfsVfs, mq->ino, &mq->creds, &fg,
				   &stb, &data);

	psc_info("zfs opendir data (%p)", data);

	if (!mp->rc) {
		struct cfdent *cfd;

		mp->rc = slmrmcthr_inode_cacheput(&fg, &stb, &mq->creds);
		if (!mp->rc) {
			mp->rc = cfdnew(fg.fg_fid, rq->rq_export,
				data, &cfd, &mdsCfdOps, CFD_DIR);

			if (mp->rc) {
				psc_error("cfdnew failed rc=%d", mp->rc);
				RETURN(0);
			}
			fdbuf_sign(&cfd->cfd_fdb, &fg, rq->rq_peer);
			memcpy(&mp->sfdb, &cfd->cfd_fdb, sizeof(mp->sfdb));
		}

		/*
		 * On success, the cfd private data, originally the ZFS
		 * handle private data, is overwritten with an fmdsi,
		 * so release the ZFS handle if we failed or didn't use it.
		 */
		if (cfd == NULL || cfd_2_zfsdata(cfd) != data)
			zfsslash2_release(zfsVfs, fg.fg_fid,
			    &mq->creds, data);
	}
	RETURN(0);
}

int
slm_rmc_handle_readdir(struct pscrpc_request *rq)
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

	mp->rc = fdbuf_check(&mq->sfdb, &cfd, &fg, rq->rq_peer);
	if (mp->rc)
		RETURN(0);

	if (cfdlookup(rq->rq_export, cfd, &m)) {
		mp->rc = -errno;
		RETURN(mp->rc);
	}

#define MAX_READDIR_NENTS	1000
#define MAX_READDIR_BUFSIZ	(sizeof(struct stat) * MAX_READDIR_NENTS)
	if (mq->size > MAX_READDIR_BUFSIZ ||
	    mq->nstbpref > MAX_READDIR_NENTS) {
		mp->rc = EINVAL;
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

	mp->rc = zfsslash2_readdir(zfsVfs, fg.fg_fid, &mq->creds,
	    mq->size, mq->offset, iov[0].iov_base, &mp->size,
	    iov[1].iov_base, mq->nstbpref, fcmh_2_zfsdata(m->mexpfcm_fcmh));

	if (mp->rc) {
		PSCFREE(iov[0].iov_base);
		if (mq->nstbpref)
			PSCFREE(iov[1].iov_base);
		RETURN(mp->rc);
	}

	if (mq->nstbpref)
		mp->rc = rsx_bulkserver(rq, &desc,
		    BULK_PUT_SOURCE, SRMC_BULK_PORTAL, iov, 2);
	else
		mp->rc = rsx_bulkserver(rq, &desc,
		    BULK_PUT_SOURCE, SRMC_BULK_PORTAL, iov, 1);

	if (desc)
		pscrpc_free_bulk(desc);

	PSCFREE(iov[0].iov_base);
	if (mq->nstbpref)
		PSCFREE(iov[1].iov_base);
	RETURN(0);
}

int
slm_rmc_handle_readlink(struct pscrpc_request *rq)
{
	struct pscrpc_bulk_desc *desc;
	struct srm_readlink_req *mq;
	struct srm_readlink_rep *mp;
	struct iovec iov;
	char buf[PATH_MAX];

	ENTRY;

	iov.iov_base = buf;
	iov.iov_len = sizeof(buf);

	RSX_ALLOCREP(rq, mq, mp);
	mp->rc = zfsslash2_readlink(zfsVfs, mq->ino, buf, &mq->creds);
	if (mp->rc)
		RETURN(0);
	mp->rc = rsx_bulkserver(rq, &desc, BULK_PUT_SOURCE,
	    SRMC_BULK_PORTAL, &iov, 1);
	if (desc)
		pscrpc_free_bulk(desc);
	RETURN(0);
}

int
slm_rmc_handle_release(struct pscrpc_request *rq)
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

	mp->rc = fdbuf_check(&mq->sfdb, &cfd, &fg, rq->rq_peer);
	if (mp->rc)
		RETURN(0);

	c = cfdget(rq->rq_export, cfd);
	if (!c) {
		psc_info("cfdget() failed cfd %"PRId64, cfd);
		mp->rc = ENOENT;
		RETURN(0);
	}
	psc_assert(c->cfd_pri);
	m = c->cfd_pri;

	f = m->mexpfcm_fcmh;
	psc_assert(f->fcmh_fcoo);

	i = fcmh_2_fmdsi(f);

	MEXPFCM_LOCK(m);
	psc_assert(m->mexpfcm_fcmh);
	/* Prevent others from trying to access the mexpfcm.
	 */
	m->mexpfcm_flags |= MEXPFCM_CLOSING;
	MEXPFCM_ULOCK(m);

	rc = cfdfree(rq->rq_export, cfd);
	psc_warnx("cfdfree() cfd %"PRId64" rc=%d",
		 cfd, rc);

	mp->rc = mds_inode_release(f);

	RETURN(0);
}

int
slm_rmc_handle_rename(struct pscrpc_request *rq)
{
	char from[NAME_MAX+1], to[NAME_MAX+1];
	struct pscrpc_bulk_desc *desc;
	struct srm_rename_req *mq;
	struct srm_generic_rep *mp;
	struct iovec iov[2];

	ENTRY;

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->fromlen <= 1 ||
	    mq->tolen <= 1) {
		mp->rc = -ENOENT;
		RETURN(0);
	}
	if (mq->fromlen > NAME_MAX ||
	    mq->tolen > NAME_MAX) {
		mp->rc = -EINVAL;
		RETURN(0);
	}
	iov[0].iov_base = from;
	iov[0].iov_len = mq->fromlen;
	iov[1].iov_base = to;
	iov[1].iov_len = mq->tolen;

	if ((mp->rc = rsx_bulkserver(rq, &desc, BULK_GET_SINK,
	    SRMC_BULK_PORTAL, iov, 2)) != 0)
		RETURN(0);
	from[sizeof(from) - 1] = '\0';
	to[sizeof(to) - 1] = '\0';
	pscrpc_free_bulk(desc);

	mp->rc = zfsslash2_rename(zfsVfs, mq->opino, from,
				  mq->npino, to, &mq->creds);
	RETURN(0);
}

int
slm_rmc_handle_setattr(struct pscrpc_request *rq)
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

int
slm_rmc_handle_statfs(struct pscrpc_request *rq)
{
	struct srm_statfs_req *mq;
	struct srm_statfs_rep *mp;

	ENTRY;

	RSX_ALLOCREP(rq, mq, mp);
	mp->rc = zfsslash2_statfs(zfsVfs, &mp->stbv);

	RETURN(0);
}

int
slm_rmc_handle_symlink(struct pscrpc_request *rq)
{
	struct pscrpc_bulk_desc *desc;
	struct srm_symlink_req *mq;
	struct srm_symlink_rep *mp;
	struct iovec iov;
	char linkname[PATH_MAX];

	ENTRY;

	RSX_ALLOCREP(rq, mq, mp);
	mq->name[sizeof(mq->name) - 1] = '\0';
	if (mq->linklen == 0) {
		mp->rc = -ENOENT;
		RETURN(0);
	}
	if (mq->linklen >= PATH_MAX) {
		mp->rc = -ENAMETOOLONG;
		RETURN(0);
	}
	iov.iov_base = linkname;
	iov.iov_len = mq->linklen;
	if ((mp->rc = rsx_bulkserver(rq, &desc, BULK_GET_SINK,
	    SRMC_BULK_PORTAL, &iov, 1)) != 0)
		RETURN(0);
	linkname[sizeof(linkname) - 1] = '\0';
	pscrpc_free_bulk(desc);

	mp->rc = zfsslash2_symlink(zfsVfs, linkname, mq->pino, mq->name,
				   &mq->creds, &mp->attr, &mp->fg);
	RETURN(0);
}

int
slm_rmc_handle_unlink(struct pscrpc_request *rq, int isfile)
{
	struct srm_unlink_req *mq;
	struct srm_unlink_rep *mp;

	ENTRY;

	RSX_ALLOCREP(rq, mq, mp);
	mq->name[sizeof(mq->name) - 1] = '\0';
	if (isfile)
		mp->rc = zfsslash2_unlink(zfsVfs, mq->pino,
					  mq->name, &mq->creds);
	else
		mp->rc = zfsslash2_rmdir(zfsVfs, mq->pino,
					 mq->name, &mq->creds);

	RETURN(0);
}

int
slm_rmc_handle_addreplrq(struct pscrpc_request *rq)
{
	struct srm_generic_rep *mp;
	struct srm_replrq_req *mq;

	RSX_ALLOCREP(rq, mq, mp);
	mp->rc = mds_repl_addrq(&mq->fg,
	    mq->bmapno, mq->repls, mq->nrepls);
	return (0);
}

int
slm_rmc_handle_delreplrq(struct pscrpc_request *rq)
{
	struct srm_generic_rep *mp;
	struct srm_replrq_req *mq;

	RSX_ALLOCREP(rq, mq, mp);
	mp->rc = mds_repl_delrq(&mq->fg,
	    mq->bmapno, mq->repls, mq->nrepls);
	return (0);
}

int
slm_rmc_handle_getreplst(struct pscrpc_request *rq)
{
	struct srm_replst_master_req *mq;
	struct srm_replst_master_rep *mp;
	struct slmrcm_thread *srcm;
	struct psc_thread *thr;
	size_t id;

	RSX_ALLOCREP(rq, mq, mp);

	spinlock(&slmrcmthr_uniqidmap_lock);
	if (vbitmap_next(&slmrcmthr_uniqidmap, &id) == -1)
		psc_fatal("vbitmap_next");
	freelock(&slmrcmthr_uniqidmap_lock);

	thr = pscthr_init(SLMTHRT_RCM, 0, slmrcmthr_main,
	    NULL, sizeof(*srcm), "slmrcmthr%02zu", id);
	srcm = thr->pscthr_private;
	srcm->srcm_fg = mq->fg;
	srcm->srcm_id = mq->id;
	srcm->srcm_csvc = rpc_csvc_fromexp(rq->rq_export,
	    SRCM_REQ_PORTAL, SRCM_REP_PORTAL);
	pscthr_setready(thr);
	return (0);
}

int
slm_rmc_handler(struct pscrpc_request *rq)
{
	int rc = 0;

	switch (rq->rq_reqmsg->opc) {
	case SRMT_ACCESS:
		rc = slm_rmc_handle_access(rq);
		break;
	case SRMT_REPL_ADDRQ:
		rc = slm_rmc_handle_addreplrq(rq);
		break;
	case SRMT_CONNECT:
		rc = slm_rmc_handle_connect(rq);
		break;
	case SRMT_CREATE:
		rc = slm_rmc_handle_create(rq);
		break;
	case SRMT_REPL_DELRQ:
		rc = slm_rmc_handle_delreplrq(rq);
		break;
	case SRMT_GETATTR:
		rc = slm_rmc_handle_getattr(rq);
		break;
	case SRMT_GETBMAP:
		rc = slm_rmc_handle_getbmap(rq);
		break;
	case SRMT_REPL_GETST:
		rc = slm_rmc_handle_getreplst(rq);
		break;
	case SRMT_LINK:
		rc = slm_rmc_handle_link(rq);
		break;
	case SRMT_MKDIR:
		rc = slm_rmc_handle_mkdir(rq);
		break;
	case SRMT_LOOKUP:
		rc = slm_rmc_handle_lookup(rq);
		break;
	case SRMT_OPEN:
		rc = slm_rmc_handle_open(rq);
		break;
	case SRMT_OPENDIR:
		rc = slm_rmc_handle_opendir(rq);
		break;
	case SRMT_READDIR:
		rc = slm_rmc_handle_readdir(rq);
		break;
	case SRMT_READLINK:
		rc = slm_rmc_handle_readlink(rq);
		break;
	case SRMT_RELEASE:
		rc = slm_rmc_handle_release(rq);
		break;
	case SRMT_RENAME:
		rc = slm_rmc_handle_rename(rq);
		break;
	case SRMT_RMDIR:
		rc = slm_rmc_handle_unlink(rq, 0);
		break;
	case SRMT_SETATTR:
		rc = slm_rmc_handle_setattr(rq);
		break;
	case SRMT_STATFS:
		rc = slm_rmc_handle_statfs(rq);
		break;
	case SRMT_SYMLINK:
		rc = slm_rmc_handle_symlink(rq);
		break;
	case SRMT_UNLINK:
		rc = slm_rmc_handle_unlink(rq, 1);
		break;
	default:
		psc_errorx("Unexpected opcode %d", rq->rq_reqmsg->opc);
		rq->rq_status = -ENOSYS;
		return (pscrpc_error(rq));
	}
	target_send_reply_msg(rq, rc, 0);
	return (rc);
}
