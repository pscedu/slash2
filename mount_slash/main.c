/* $Id$ */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "pfl.h"
#include "psc_types.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_util/cdefs.h"
#include "psc_util/ctlsvr.h"
#include "psc_util/log.h"
#include "psc_util/strlcpy.h"
#include "psc_util/thread.h"
#include "psc_util/usklndthr.h"

#include "control.h"
#include "fidc_client.h"
#include "fidc_common.h"
#include "fidcache.h"
#include "fuse_listener.h"
#include "mount_slash.h"
#include "msl_fuse.h"
#include "slashrpc.h"

sl_ios_id_t prefIOS = IOS_ID_ANY;
const char *progname;
char ctlsockfn[] = _PATH_MSCTLSOCK;

#if 0
static void exit_handler(int sig)
{
	exit_fuse_listener = B_TRUE;
}

static int set_signal_handler(int sig, void (*handler)(int))
{
	struct sigaction sa;

	memset(&sa, 0, sizeof(struct sigaction));

	sa.sa_handler = handler;
	sigemptyset(&(sa.sa_mask));
	sa.sa_flags = 0;

	if (sigaction(sig, &sa, NULL) == -1) {
		perror("sigaction");
		return -1;
	}

	return 0;
}
#endif

static void
slash2fuse_getcred(fuse_req_t req, struct slash_creds *cred)
{
	const struct fuse_ctx *ctx = fuse_req_ctx(req);

	cred->uid = ctx->uid;
	cred->gid = ctx->gid;
}

static void
msfsthr_teardown(void *arg)
{
	struct msfs_thread *mft = arg;

	free(mft);
}

static void
msfsthr_ensure(void)
{
	static atomic_t thrid; /* XXX maintain bitmap for transiency */
	struct psc_thread *thr;

	thr = pscthr_get_canfail();
	if (thr == NULL) {
		thr = pscthr_init(MSTHRT_FS, PTF_FREE, NULL,
		    msfsthr_teardown, sizeof(struct msfs_thread),
		    "msfsthr%d", atomic_inc_return(&thrid) - 1);
		pscthr_setready(thr);
	}
	psc_assert(thr->pscthr_type == MSTHRT_FS);
}

static void
slash2fuse_reply_create(fuse_req_t req, const struct slash_fidgen *fg,
			const struct stat *stb,
			const struct fuse_file_info *fi)
{
	struct fuse_entry_param e;

	memset(&e, 0, sizeof(e));
	e.attr_timeout = 0.0;
	e.entry_timeout = 0.0;
	e.ino = fg->fg_fid;
	e.generation = fg->fg_gen;

	memcpy(&e.attr, stb, sizeof(e.attr));

	psc_trace("inode:%"PRId64" generation:%lu", e.ino, e.generation);
	fcm_dump_stb(&e.attr, PLL_TRACE);

	fuse_reply_create(req, &e, fi);
}

static void
slash2fuse_reply_entry(fuse_req_t req, const struct slash_fidgen *fg,
		       const struct stat *stb)
{
	struct fuse_entry_param e;

	memset(&e, 0, sizeof(e));
	e.attr_timeout = 0.0;
	e.entry_timeout = 0.0;
	e.ino = fg->fg_fid;
	e.generation = fg->fg_gen;

	memcpy(&e.attr, stb, sizeof(e.attr));

	psc_trace("inode:%"PRId64" generation:%lu",
		  e.ino, e.generation);
	fcm_dump_stb(&e.attr, PLL_TRACE);

	fuse_reply_entry(req, &e);
}

/**
 * slash2fuse_fidc_putget - create a new fcmh based on the attrs provided,
 *  return a ref'd fcmh.
 */
static struct fidc_membh *
slash2fuse_fidc_putget(const struct slash_fidgen *fg, const struct stat *stb,
		       const char *name, struct fidc_membh *parent,
		       const struct slash_creds *creds, int flags)
{
	struct fidc_membh *c;
	struct fidc_memb fcm;

	int lookupflags = (FIDC_LOOKUP_CREATE |
			   FIDC_LOOKUP_COPY   |
			   flags);

	FCM_FROM_FG_ATTR(&fcm, fg, stb);

	c = __fidc_lookup_inode(fg, lookupflags, &fcm, creds);

	psc_assert(c);

	if (name) {
		psc_assert(parent);
		fidc_child_add(parent, c, name);
	}
	return (c);
}

/**
 * slash2fuse_fidc_put - wrapper around slash2fuse_fidc_putget(), it makes
 *  same call but deref's the fcmh and returns void.  For callers who don't
 *  want a pointer back the new fcmh.
 */
static void
slash2fuse_fidc_put(const struct slash_fidgen *fg, const struct stat *stb,
		    const char *name, struct fidc_membh *parent,
		    const struct slash_creds *creds, int flags)
{
	struct fidc_membh *c;

	c = slash2fuse_fidc_putget(fg, stb, name, parent, creds, flags);
	fidc_membh_dropref(c);
}

static void
slash2fuse_access(fuse_req_t req, fuse_ino_t ino, int mask)
{
	struct srm_generic_rep *mp;
	struct srm_access_req *mq;
	struct pscrpc_request *rq;
	struct fidc_membh *c;
	int rc=0;

	msfsthr_ensure();

	c = NULL;

	rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_ACCESS, rq, mq, mp);
	if (rc)
		goto out;

	slash2fuse_getcred(req, &mq->creds);

	mq->ino = ino;
	mq->mask = mask;

	c = fidc_lookup_load_inode((slfid_t)ino, &mq->creds);
	if (!c) {
		rc = ENOENT;
		goto out;
	}

	rc = RSX_WAITREP(rq, mp);
	if (rc || mp->rc)
		rc = rc ? rc : mp->rc;

 out:
	fuse_reply_err(req, rc);
	if (c)
		fidc_membh_dropref(c);
	if (rq)
		pscrpc_req_finished(rq);
}

static void
slash2fuse_openref_update(struct fidc_membh *fcmh, int flags, int *uord)
{
	struct fidc_open_obj *o=fcmh->fcmh_fcoo;
	int l=reqlock(&fcmh->fcmh_lock);

#define SL2F_UPOPREF_READ() do {				\
		if (!o->fcoo_oref_rw[0])			\
			*uord |= SL_FREAD;			\
		o->fcoo_oref_rw[0]++;				\
	} while (0)

#define SL2F_UPOPREF_WRITE() do {				\
		if (!o->fcoo_oref_rw[1])			\
			*uord |= SL_FWRITE;			\
		o->fcoo_oref_rw[1]++;				\
	} while (0)

#define SL2F_DOWNOPREF_READ() do {				\
		o->fcoo_oref_rw[0]--;				\
		if (!o->fcoo_oref_rw[0])			\
			*uord |= SL_FREAD;			\
	} while (0)

#define SL2F_DOWNOPREF_WRITE() do {				\
		o->fcoo_oref_rw[1]--;				\
		if (!o->fcoo_oref_rw[1])			\
			*uord |= SL_FWRITE;			\
	} while (0)

	psc_assert(o->fcoo_oref_rw[0] >= 0);
	psc_assert(o->fcoo_oref_rw[1] >= 0);

	if (*uord) {
		*uord = 0;
		if (flags & O_WRONLY)
			SL2F_UPOPREF_WRITE();
		else if (flags & O_RDWR) {
			SL2F_UPOPREF_WRITE();
			SL2F_UPOPREF_READ();
		} else
			SL2F_UPOPREF_READ();
	} else {
		if (flags & O_WRONLY)
			SL2F_DOWNOPREF_WRITE();
		else if (flags & O_RDWR) {
			SL2F_DOWNOPREF_WRITE();
			SL2F_DOWNOPREF_READ();
		} else
			SL2F_DOWNOPREF_READ();
	}

	if (!(o->fcoo_oref_rw[0] || o->fcoo_oref_rw[1])) {
		fcmh->fcmh_state |= FCMH_FCOO_CLOSING;
		*uord = 1;
	}

	DEBUG_FCMH(PLL_TRACE, fcmh, "fdstate (%d)", *uord);
	ureqlock(&fcmh->fcmh_lock, l);
}

static void
slash2fuse_transflags(u32 flags, u32 *nflags, u32 *nmode)
{
	if (flags & O_WRONLY) {
		*nmode = SL_WRITE;
		*nflags = SL_FWRITE;

	} else if (flags & O_RDWR) {
		*nmode = SL_WRITE | SL_READ;
		*nflags = SL_FREAD | SL_FWRITE;
	} else {
		*nmode = SL_READ;
		*nflags = SL_FREAD;
	}

	if (flags & O_CREAT)
		*nflags |= SL_FCREAT;
	if (flags & O_SYNC)
		*nflags |= SL_FSYNC;
	if (flags & O_DSYNC)
		*nflags |= SL_FDSYNC;
	if (flags & O_RSYNC)
		*nflags |= SL_FRSYNC;
	if (flags & O_APPEND)
		*nflags |= SL_FAPPEND;
	//if (flags & O_LARGEFILE)
	*nflags |= SL_FOFFMAX;
	if (flags & O_NOFOLLOW)
		*nflags |= SL_FNOFOLLOW;
	if (flags & O_TRUNC)
		*nflags |= SL_FTRUNC;
	if (flags & O_EXCL)
		*nflags |= SL_FEXCL;
	if (flags & O_DIRECTORY)
		*nflags |= SL_DIRECTORY;
}

static int
slash2fuse_openrpc(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
	struct pscrpc_request *rq;
	struct srm_open_req *mq;
	struct srm_opencreate_rep *mp;
	struct msl_fhent *mfh;
	struct fidc_membh *h;
	int rc;

	mfh = (void *)fi->fh;
	h = mfh->mfh_fcmh;
	psc_assert(ino == fcmh_2_fid(h));

	rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    (fi->flags & O_DIRECTORY) ? SRMT_OPENDIR : SRMT_OPEN,
	    rq, mq, mp);
	if (rc)
		return (rc);

	slash2fuse_getcred(req, &mq->creds);
	slash2fuse_transflags(fi->flags, &mq->flags, &mq->mode);
	mq->ino = ino;

	rc = RSX_WAITREP(rq, mp);
	if (rc || mp->rc)
		rc = rc ? rc : mp->rc;
	else {
		memcpy(&h->fcmh_fcoo->fcoo_fdb,
		    &mp->sfdb, sizeof(mp->sfdb));
		//fidc_fcm_setattr(h, &mp->attr);
	}
	pscrpc_req_finished(rq);
	return (rc);
}

/**
 * slash2fuse_fcoo_start - helper for slash2fuse_create and slash2fuse_open.
 *  Starts the fcoo if one doesn't already exist.
 */
static int
slash2fuse_fcoo_start(fuse_req_t req, fuse_ino_t ino,
		      struct fuse_file_info *fi)
{
	struct fidc_membh *c;
	struct msl_fhent *mfh;
	int flags=1, rc=0;

	mfh = (void *)fi->fh;
	c = mfh->mfh_fcmh;
	psc_assert(ino == fcmh_2_fid(c));

	spinlock(&c->fcmh_lock);
	if (c->fcmh_fcoo || (c->fcmh_state & FCMH_FCOO_CLOSING)) {
		/* Barrier.  Wait for rpc to complete before
		 *  proceeding.  If we're blocked on a release rpc
		 *  then fidc_fcoo_wait_locked() will kick us right
		 *  into startlocked and return '1'.
		 */
		rc = fidc_fcoo_wait_locked(c, FCOO_START);
		/* rc of '1' means that we have to complete the open.
		 *  Otherwise free the lock and return.
		 */
		if (rc < 0) {
			freelock(&c->fcmh_lock);
			goto out;
		}
	} else
		fidc_fcoo_start_locked(c);

	/* Hold the lock until the read / write ref count is updated.
	 *  Fcoo's with no refs are invalid.
	 */
	slash2fuse_openref_update(c, fi->flags, &flags);
	freelock(&c->fcmh_lock);
	/* Key off the 'flags' int to determine whether an RPC
	 *  is warranted.  Having an fcoo may not prevent RPC if the
	 *  file is being opened for a new mode. (i.e. already opened
	 *  read but is now being open for write).
	 */
	if (flags & SL_FWRITE || flags & SL_FREAD)
		rc = slash2fuse_openrpc(req, ino, fi);

	if (c->fcmh_state & FCMH_FCOO_STARTING) {
		/* FCMH_FCOO_STARTING means that we must have
		 *  sent an RPC above because either the read or
		 *  write refcnt must have went to 1.
		 */
		psc_assert((flags & SL_FWRITE) || (flags & SL_FREAD));

		if (!rc)
			fidc_fcoo_startdone(c);
		else
			/* Ugh, have to cleanup the half started fcoo.
			 *  Tricky because others may be waiting on it.
			 */
			fidc_fcoo_startfailed(c);
	}
 out:
	return (rc);
}

static void
slash2fuse_create(fuse_req_t req, fuse_ino_t parent, const char *name,
		  mode_t mode, struct fuse_file_info *fi)
{
	struct pscrpc_request *rq=NULL;
	struct srm_create_req *mq;
	struct srm_opencreate_rep *mp;
	struct fidc_membh *p, *m;
	struct msl_fhent *mfh;
	int rc=0, flags=1;

	msfsthr_ensure();

	p = NULL;

	if (strlen(name) > NAME_MAX) {
		rc = ENAMETOOLONG;
		goto out;
	}

	p = fidc_lookup_inode((slfid_t)parent);
	if (!p) {
		/* Parent inode must exist in the cache.
		 */
		rc = ENOENT;
		goto out;

	} else if (!fcmh_2_isdir(p)) {
		rc = ENOTDIR;
		goto out;
	}
	/* Now we've established a local placeholder for this create.
	 *  any other creates to this pathame will block in
	 *  fidc_child_wait_locked() until we release the fcc.
	 */
	rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_CREATE, rq, mq, mp);
	if (rc)
		goto out;

	slash2fuse_getcred(req, &mq->creds);
	slash2fuse_transflags((fi->flags | O_CREAT),
			      &mq->flags, &mq->mode);
	mq->mode = mode;
	mq->pino = parent;
	strlcpy(mq->name, name, sizeof(mq->name));

	rc = RSX_WAITREP(rq, mp);
	if (rc)
		goto out;
	if (mp->rc == EEXIST) {
		psc_info("fid %"PRId64" already existed on mds",
			 mp->sfdb.sfdb_secret.sfs_fg.fg_fid);
		/*  Handle the network side of O_EXCL.
		 */
		if (fi->flags & O_EXCL) {
			rc = EEXIST;
			goto out;
		}
	} else if (mp->rc) {
		rc = mp->rc;
		goto out;
	}

	psc_warnx("FID %"PRId64" %s", mp->sfdb.sfdb_secret.sfs_fg.fg_fid,
	    name);

	m = slash2fuse_fidc_putget(&mp->sfdb.sfdb_secret.sfs_fg, &mp->attr, name, p, &mq->creds,
				   (FIDC_LOOKUP_EXCL | FIDC_LOOKUP_FCOOSTART));
	psc_assert(m);
	psc_assert(m->fcmh_fcoo && (m->fcmh_state & FCMH_FCOO_STARTING));
	memcpy(&m->fcmh_fcoo->fcoo_fdb, &mp->sfdb, sizeof(mp->sfdb));

	mfh = msl_fhent_new(m);
	fi->fh = (uint64_t)mfh;
	fi->keep_cache = 1;
	/* Increment the fcoo #refs.  The RPC has already taken place.
	 */
	slash2fuse_openref_update(m, fi->flags, &flags);
	fidc_fcoo_startdone(m);
	slash2fuse_reply_create(req, &mp->sfdb.sfdb_secret.sfs_fg, &mp->attr, fi);
	fidc_membh_dropref(m);		/* slash2fuse_fidc_putget() bumped it. */

 out:
	if (rc)
		fuse_reply_err(req, rc);
	if (p)
		fidc_membh_dropref(p);
	if (rq)
		pscrpc_req_finished(rq);
}

static void
slash2fuse_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
	struct slash_creds creds;
	struct msl_fhent *mfh;
	struct fidc_membh *c;
	int rc=0;

	slash2fuse_getcred(req, &creds);

	msfsthr_ensure();

	psc_warnx("FID %"PRId64, (slfid_t)ino);

	c = fidc_lookup_load_inode((slfid_t)ino, &creds);
	if (!c) {
		rc = ENOENT;
		goto out;
	}
	/* Don't allow writes on directory inodes.
	 */
	if (fcmh_2_isdir(c)) {
		if ((fi->flags & O_WRONLY) || (fi->flags & O_RDWR)) {
			rc = EBADF;
			goto out;
		}
		if (!(fi->flags & O_DIRECTORY)) {
			rc = EISDIR;
			goto out;
		}
	} else {
		if (fi->flags & O_DIRECTORY) {
			rc = ENOTDIR;
			goto out;
		}
	}

	mfh = msl_fhent_new(c);
	fi->fh = (uint64_t)mfh;
	fi->keep_cache = 1;
	rc = slash2fuse_fcoo_start(req, ino, fi);
	fuse_reply_open(req, fi);
	fidc_membh_dropref(c);

 out:
	if (rc)
		fuse_reply_err(req, rc);
}

static void
slash2fuse_opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
	fi->flags |= O_DIRECTORY;
	slash2fuse_open(req, ino, fi);
}

int
slash2fuse_stat(struct fidc_membh *fcmh, const struct slash_creds *creds)
{
	struct pscrpc_request *rq;
	struct srm_getattr_req *mq;
	struct srm_getattr_rep *mp;
	int rc=0, l;

	if ((fcmh->fcmh_state & FCMH_HAVE_ATTRS) &&
	    fidc_gettime() < (fcmh_2_age(fcmh) + FCMH_ATTR_TIMEO)) {
		DEBUG_FCMH(PLL_DEBUG, fcmh, "attrs cached - YES");
		/* XXX Need to check creds here.
		 */
		return (0);
	}

	l = reqlock(&fcmh->fcmh_lock);
	fcmh->fcmh_state |= FCMH_GETTING_ATTRS;
	ureqlock(&fcmh->fcmh_lock, l);

	rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_GETATTR, rq, mq, mp);
	if (rc)
		return (rc);

	memcpy(&mq->creds, creds, sizeof(*creds));
	mq->ino = fcmh_2_fid(fcmh);

	rc = RSX_WAITREP(rq, mp);
	if (rc || mp->rc)
		rc = rc ? rc : mp->rc;
	else {
		if (fcmh_2_gen(fcmh) == FID_ANY) {
			psc_assert(fcmh->fcmh_state & FCMH_GETTING_ATTRS);
			fcmh_2_gen(fcmh) = mp->gen;
		}
		fidc_fcm_setattr(fcmh, &mp->attr);
	}

	pscrpc_req_finished(rq);

	DEBUG_FCMH(PLL_DEBUG, fcmh, "attrs retrieved via rpc rc=%d", rc);

	return (rc);
}

static void
slash2fuse_getattr(__unusedx fuse_req_t req, fuse_ino_t ino,
		   __unusedx struct fuse_file_info *fi)
{
	struct fidc_membh *f;
	struct slash_creds creds;
	int rc=0;

	msfsthr_ensure();

	slash2fuse_getcred(req, &creds);
	/* Lookup and possibly create a new fidcache handle for ino.
	 *  If the fid does not exist in the cache then a placeholder will
	 *  be allocated.  slash2fuse_stat() will detect incomplete attrs via
	 *  FCMH_GETTING_ATTRS flag and rpc for them.
	 */
	f = fidc_lookup_load_inode((slfid_t)ino, &creds);
	if (!f)
		rc = ENOMEM;
	else
		rc = slash2fuse_stat(f, &creds);

	if (rc)
		fuse_reply_err(req, rc);
	else {
		fcm_dump_stb(fcmh_2_stb(f), PLL_INFO);
		fuse_reply_attr(req, fcmh_2_stb(f), 0.0);
		fidc_membh_dropref(f);
	}
}

static void
slash2fuse_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent,
		const char *newname)
{
	struct pscrpc_request *rq;
	struct fidc_membh *p, *c;
	struct slash_creds creds;
	struct srm_link_req *mq;
	struct srm_link_rep *mp;
	int rc=0;

	msfsthr_ensure();

	p = c = NULL;
	rq = NULL;

	rc = EOPNOTSUPP;
	goto out;

	if (strlen(newname) > NAME_MAX) {
		rc = ENAMETOOLONG;
		goto out;
	}

	slash2fuse_getcred(req, &creds);
	/* Check the newparent inode.
	 */
	p = fidc_lookup_load_inode((slfid_t)newparent, &creds);
	if (!p) {
		rc = ENOMEM;
		goto out;
	}

	if (!fcmh_2_isdir(p)) {
		rc = ENOTDIR;
		goto out;
	}
	/* Check the child inode.
	 */
	c = fidc_lookup_load_inode((slfid_t)ino, &creds);
	if (!c) {
		rc = ENOMEM;
		goto out;
	} else
		psc_assert(fcmh_2_fid(c) == ino);

	if (fcmh_2_isdir(c)) {
		rc = EISDIR;
		goto out;
	}
	/* Create and initialize the LINK RPC.
	 */
	rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_LINK, rq, mq, mp);
	if (rc)
		goto out;

	memcpy(&mq->creds, &creds, sizeof(mq->creds));
	mq->pino = fcmh_2_fid(p);
	mq->ino = ino;
	strlcpy(mq->name, newname, sizeof(mq->name));

	rc = RSX_WAITREP(rq, mp);
	if (rc || mp->rc) {
		rc = rc ? rc : mp->rc;
		goto out;
	}
	slash2fuse_reply_entry(req, &mp->fg, &mp->attr);
	//slash2fuse_fidc_put(&mp->fg, &mp->attr, name, p, &mq->creds);
	fidc_fcm_setattr(c, &mp->attr);

 out:
	if (rc)
		fuse_reply_err(req, rc);
	if (c)
		fidc_membh_dropref(c);
	if (p)
		fidc_membh_dropref(p);
	if (rq)
		pscrpc_req_finished(rq);
}

static void
slash2fuse_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name,
		 mode_t mode)
{
	struct pscrpc_request *rq=NULL;
	struct srm_mkdir_req *mq;
	struct srm_mkdir_rep *mp;
	struct fidc_membh *p;
	int rc;

	msfsthr_ensure();

	p = NULL;

	if (strlen(name) > NAME_MAX) {
		rc = ENAMETOOLONG;
		goto out;
	}
	/* Check the parent inode.
	 */
	p = fidc_lookup_inode((slfid_t)parent);
	if (!p) {
		rc = EINVAL;
		goto out;
	}

	if (!fcmh_2_isdir(p)) {
		rc = ENOTDIR;
		goto out;
	}
	/* Create and initialize the MKDIR RPC.
	 */
	rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_MKDIR, rq, mq, mp);
	if (rc)
		goto out;

	slash2fuse_getcred(req, &mq->creds);

	psc_assert(fcmh_2_fid(p) == parent);
	mq->pino = parent;
	mq->mode = mode;
	strlcpy(mq->name, name, sizeof(mq->name));

	rc = RSX_WAITREP(rq, mp);

	psc_info("pino=%"PRIx64" mode=0%o name='%s' rc=%d mp->rc=%d",
		 mq->pino, mq->mode, mq->name, rc, mp->rc);

	if (rc || mp->rc) {
		rc = rc ? rc : mp->rc;
		goto out;
	}
	slash2fuse_reply_entry(req, &mp->fg, &mp->attr);
	slash2fuse_fidc_put(&mp->fg, &mp->attr, name, p, &mq->creds, 0);

 out:
	if (rc)
		fuse_reply_err(req, rc);
	if (p)
		fidc_membh_dropref(p);
	if (rq)
		pscrpc_req_finished(rq);
}

static int
slash2fuse_unlink(fuse_req_t req, fuse_ino_t parent, const char *name,
		  int isfile)
{
	struct pscrpc_request *rq;
	struct srm_unlink_req *mq;
	struct srm_unlink_rep *mp;
	struct fidc_membh *p;
	int rc;

	msfsthr_ensure();

	rq = NULL;
	p = NULL;

	if (strlen(name) > NAME_MAX) {
		rc = ENAMETOOLONG;
		goto out;
	}

	rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    (isfile ? SRMT_UNLINK : SRMT_RMDIR), rq, mq, mp);
	if (rc)
		goto out;

	slash2fuse_getcred(req, &mq->creds);
	/* Check the parent inode.
	 */
	p = fidc_lookup_load_inode((slfid_t)parent, &mq->creds);
	if (!p) {
		rc = EINVAL;
		goto out;
	}

	if (!fcmh_2_isdir(p)) {
		rc = ENOTDIR;
		goto out;
	}

	mq->pino = parent;
	strlcpy(mq->name, name, sizeof(mq->name));

	rc = RSX_WAITREP(rq, mp);
	if (rc || mp->rc) {
		rc = rc ? rc : mp->rc;
		goto out;
	}
	/* Remove ourselves from the namespace cache.
	 */
	fidc_child_unlink(p, name);

 out:
	if (p)
		fidc_membh_dropref(p);
	if (rq)
		pscrpc_req_finished(rq);
	return (rc);
}

static void
slash2fuse_rmdir_helper(fuse_req_t req, fuse_ino_t parent, const char *name)
{
	int error = slash2fuse_unlink(req, parent, name, 0);

	/* rmdir events always reply_err
	 */
	fuse_reply_err(req, error);
}

static void
slash2fuse_mknod_helper(fuse_req_t req,
			__unusedx fuse_ino_t parent,
			__unusedx const char *name,
			__unusedx mode_t mode,
			__unusedx dev_t rdev)
{
	msfsthr_ensure();

	fuse_reply_err(req, EOPNOTSUPP);
}

static int
slash2fuse_readdir(fuse_req_t req, __unusedx fuse_ino_t ino, size_t size,
		   off_t off, struct fuse_file_info *fi)
{
	struct pscrpc_bulk_desc *desc;
	struct srm_readdir_req *mq;
	struct srm_readdir_rep *mp;
	struct pscrpc_request *rq;
	struct srt_fd_buf fdb;
	struct msl_fhent *mfh;
	struct fidc_membh *d;
	struct iovec iov[2];
	int rc;

	msfsthr_ensure();

	iov[0].iov_base = NULL;
	iov[1].iov_base = NULL;

	/* Don't allow writes on directory inodes.
	 */
	if (fi->flags & (O_WRONLY | O_RDWR))
		return (EINVAL);

	mfh = (void *)fi->fh;
	d = mfh->mfh_fcmh;
	psc_assert(d);

	/* Ensure that the fcmh is still valid, we can't rely
	 *  only on the inode number, the generation # number
	 *  must be taken into account.
	 * NOTE: 'd' must be decref'd.
	 */
	if (fidc_lookup_fg(fcmh_2_fgp(d)) != d)
		return (EBADF);

	if (fidc_fcmh2fdb(d, &fdb) < 0) {
		fidc_membh_dropref(d);
		return (EBADF);
	}

	if (!fcmh_2_isdir(d)) {
		fidc_membh_dropref(d);
		return (ENOTDIR);
	}

	rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_READDIR, rq, mq, mp);
	if (rc) {
		fidc_membh_dropref(d);
		return (rc);
	}

	slash2fuse_getcred(req, &mq->creds);
	memcpy(&mq->sfdb, &fdb, sizeof(fdb));
	mq->size = size;
	mq->offset = off;

	iov[0].iov_base = PSCALLOC(size);
	iov[0].iov_len = size;

	mq->nstbpref = 100;
	if (mq->nstbpref) {
		iov[1].iov_len = mq->nstbpref * sizeof(struct srm_getattr_rep);
		iov[1].iov_base = PSCALLOC(iov[1].iov_len);
		rsx_bulkclient(rq, &desc, BULK_PUT_SINK, SRMC_BULK_PORTAL, iov, 2);
	} else
		rsx_bulkclient(rq, &desc, BULK_PUT_SINK, SRMC_BULK_PORTAL, iov, 1);

	rc = RSX_WAITREP(rq, mp);
	if (rc || mp->rc) {
		rc = rc ? rc : mp->rc;
		goto out;
	}

	if (mq->nstbpref) {
		u32 i;
		struct fidc_memb fcm;
		struct fidc_membh *fcmh;
		struct srm_getattr_rep *attr =
			(struct srm_getattr_rep *)iov[1].iov_base;

		for (i=0; i < mq->nstbpref; i++, attr++) {
			if (attr->rc || !attr->attr.st_ino)
				continue;

			memset(&fcm, 0, sizeof(struct fidc_memb));
			memcpy(&fcm.fcm_stb, &attr->attr, sizeof(struct stat));
			fcm.fcm_fg.fg_fid = attr->attr.st_ino;
			fcm.fcm_fg.fg_gen = attr->gen;

			psc_trace("adding i+g:%"PRId64"+%"PRId64" rc=%d",
				  fcm.fcm_fg.fg_fid, fcm.fcm_fg.fg_gen,
				  attr->rc);

			fcm_2_age(&fcm) = fidc_gettime() + FCMH_ATTR_TIMEO;
			fcmh = fidc_lookup_copy_inode(&fcm.fcm_fg, &fcm,
						      &mq->creds);
			if (fcmh)
				fidc_membh_dropref(fcmh);
			else
				psc_warnx("fcmh is NULL");
		}
	}

	fuse_reply_buf(req, (char *)iov[0].iov_base, (size_t)mp->size);
 out:
	fidc_membh_dropref(d);
	pscrpc_req_finished(rq);
	PSCFREE(iov[0].iov_base);
	if (mq->nstbpref)
		PSCFREE(iov[1].iov_base);
	return (rc);
}

static void
slash2fuse_readdir_helper(fuse_req_t req, fuse_ino_t ino, size_t size,
		       off_t off, struct fuse_file_info *fi)
{
	int error = slash2fuse_readdir(req, ino, size, off, fi);

	if (error)
		fuse_reply_err(req, error);
}

static int
slash2fuse_lookuprpc(fuse_req_t req, struct fidc_membh *p, const char *name)
{
	struct pscrpc_request *rq;
	struct srm_lookup_req *mq;
	struct srm_lookup_rep *mp;
	int rc;

	if (strlen(name) > NAME_MAX)
		return (ENAMETOOLONG);

	rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_LOOKUP, rq, mq, mp);
	if (rc)
		return (rc);

	slash2fuse_getcred(req, &mq->creds);
	mq->pino = fcmh_2_fid(p);
	strlcpy(mq->name, name, sizeof(mq->name));

	rc = RSX_WAITREP(rq, mp);
	if (rc || mp->rc)
		rc = rc ? rc : mp->rc;
	else {
		/* Add the inode to the cache first, otherwise fuse may
		 *  come to us with another request for the inode it won't
		 *  yet be visible in the cache.
		 */
		slash2fuse_fidc_put(&mp->fg, &mp->attr, name, p,
				    &mq->creds, 0);
		slash2fuse_reply_entry(req, &mp->fg, &mp->attr);
	}

	pscrpc_req_finished(rq);
	return (rc);
}

static void
slash2fuse_lookup_helper(fuse_req_t req, fuse_ino_t parent, const char *name)
{
	int error=0;
	struct fidc_membh *p, *m;
	struct slash_creds creds;

	msfsthr_ensure();

	slash2fuse_getcred(req, &creds);

	psc_infos(PSS_OTHER, "name %s inode %"PRId64,
		  name, parent);

	p = fidc_lookup_load_inode((slfid_t)parent, &creds);
	if (!p) {
		/* Parent inode must exist in the cache.
		 */
		psc_warnx("name %s - failed to load inode %"PRIx64,
			  name, parent);
		error = EINVAL;
		goto out;

	} else if (!fcmh_2_isdir(p)) {
		fidc_membh_dropref(p);
		error = ENOTDIR;
		goto out;
	}

	if ((m = fidc_child_lookup(p, name))) {
		/* At this point the namespace reference is still valid but
		 *  the fcmh contents may be old, use slash2fuse_stat() to
		 *  determine attr age and possibly invoke an rpc to refresh
		 *  the fcmh contents.
		 */
		error = slash2fuse_stat(m, &creds);
		if (error)
			goto out;

		slash2fuse_reply_entry(req, fcmh_2_fgp(m), fcmh_2_attrp(m));
		fidc_membh_dropref(m);
	} else
		error = slash2fuse_lookuprpc(req, p, name);

	/* Drop the parent's refcnt.
	 */
	fidc_membh_dropref(p);
 out:
	if (error)
		fuse_reply_err(req, error);
}

static void
slash2fuse_readlink(fuse_req_t req, fuse_ino_t ino)
{
	struct pscrpc_bulk_desc *desc;
	struct srm_readlink_req *mq;
	struct srm_readlink_rep *mp;
	struct pscrpc_request *rq;
	struct iovec iov;
	char buf[PATH_MAX];
	int rc;

	msfsthr_ensure();

	rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_READLINK, rq, mq, mp);
	if (rc)
		goto out;

	slash2fuse_getcred(req, &mq->creds);
	mq->ino = ino;

	iov.iov_base = buf;
	iov.iov_len = sizeof(buf);
	rsx_bulkclient(rq, &desc, BULK_PUT_SINK,
	    SRMC_BULK_PORTAL, &iov, 1);

	rc = RSX_WAITREP(rq, mp);
	if (rc || mp->rc)
		rc = rc ? rc : mp->rc;

 out:
	if (rc)
		fuse_reply_err(req, rc);
	else {
		buf[sizeof(buf) - 1] = '\0';
		fuse_reply_readlink(req, buf);
	}
	if (rq)
		pscrpc_req_finished(rq);
}

static int
slash2fuse_releaserpc(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
	struct srm_release_req *mq;
	struct srm_generic_rep *mp;
	struct pscrpc_request *rq;
	struct msl_fhent *mfh;
	struct fidc_membh *h;
	int rc=0;
	u32 mode;

	mfh = (void *)fi->fh;
	h = mfh->mfh_fcmh;

	spinlock(&h->fcmh_lock);
	DEBUG_FCMH(PLL_INFO, h, "releaserpc");
	psc_assert(ino == fcmh_2_fid(h));
	psc_assert(h->fcmh_fcoo);
	psc_assert(h->fcmh_state & FCMH_FCOO_CLOSING);
	freelock(&h->fcmh_lock);

	rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_RELEASE, rq, mq, mp);
	if (rc)
		return (rc);

	slash2fuse_getcred(req, &mq->creds);
	slash2fuse_transflags(fi->flags, &mq->flags, &mode);
	memcpy(&mq->sfdb, &h->fcmh_fcoo->fcoo_fdb, sizeof(mq->sfdb));

	rc = RSX_WAITREP(rq, mp);
	if (rc || mp->rc)
		rc = rc ? rc : mp->rc;

	pscrpc_req_finished(rq);
	return (rc);
}

static void
slash2fuse_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
	int rc=0, fdstate=0;
	struct msl_fhent *mfh;
	struct fidc_membh *c;

	msfsthr_ensure();
	psc_warnx("FID %"PRId64, (slfid_t)ino);

	mfh = (void *)fi->fh;
	c = mfh->mfh_fcmh;
	spinlock(&c->fcmh_lock);
	psc_assert(c->fcmh_fcoo);
	/* If the fcoo is going away FCMH_FCOO_CLOSING will be set.
	 */
	slash2fuse_openref_update(c, fi->flags, &fdstate);

	DEBUG_FCMH(PLL_INFO, c, "slash2fuse_release");
	freelock(&c->fcmh_lock);

	if ((c->fcmh_state & FCMH_FCOO_CLOSING) && fdstate) {
		rc = slash2fuse_releaserpc(req, ino, fi);
		fidc_fcoo_remove(c);
	}
	DEBUG_FCMH(PLL_INFO, c, "done with slash2fuse_release");
	fuse_reply_err(req, rc);
}

static int
slash2fuse_rename(__unusedx fuse_req_t req, fuse_ino_t parent,
    const char *name, fuse_ino_t newparent, const char *newname)
{
	struct pscrpc_bulk_desc *desc;
	struct fidc_membh *op, *np;
	struct srm_generic_rep *mp;
	struct srm_rename_req *mq;
	struct pscrpc_request *rq;
	struct iovec iov[2];
	int rc;

	msfsthr_ensure();

	op = np = NULL;
	rq = NULL;

	if (strlen(name) > NAME_MAX ||
	    strlen(newname) > NAME_MAX)
		return (ENAMETOOLONG);

	rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_RENAME, rq, mq, mp);
	if (rc)
		return (rc);

	mq->opino = parent;
	mq->npino = newparent;
	mq->fromlen = strlen(name) + 1;
	mq->tolen = strlen(newname) + 1;

	iov[0].iov_base = (char *)name;
	iov[0].iov_len = mq->fromlen;
	iov[1].iov_base = (char *)newname;
	iov[1].iov_len = mq->tolen;

	op = fidc_lookup_load_inode(parent, &mq->creds);
	if (op == NULL) {
		rc = ENOENT;
		goto out;
	}

	if (!fcmh_2_isdir(op)) {
		rc = ENOTDIR;
		goto out;
	}

	np = fidc_lookup_load_inode(newparent, &mq->creds);
	if (np == NULL) {
		rc = ENOENT;
		goto out;
	}

	if (!fcmh_2_isdir(np)) {
		rc = ENOTDIR;
		goto out;
	}

	rsx_bulkclient(rq, &desc, BULK_GET_SOURCE, SRMC_BULK_PORTAL, iov, 2);

	rc = RSX_WAITREP(rq, mp);
	if (rc || mp->rc)
		rc = rc ? rc : mp->rc;
	else
		fidc_child_rename(op, name, np, newname);

 out:
	if (op)
		fidc_membh_dropref(op);
	if (np)
		fidc_membh_dropref(np);
	if (rq)
		pscrpc_req_finished(rq);
	return (rc);
}

static void
slash2fuse_rename_helper(fuse_req_t req, fuse_ino_t parent, const char *name,
			 fuse_ino_t newparent, const char *newname)
{
	int error = slash2fuse_rename(req, parent, name, newparent, newname);

	/* rename events always reply_err */
	fuse_reply_err(req, error);
}

static void
slash2fuse_statfs(fuse_req_t req, __unusedx fuse_ino_t ino)
{
	struct pscrpc_request *rq;
	struct srm_statfs_req *mq;
	struct srm_statfs_rep *mp;
	int rc;

	msfsthr_ensure();

	rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_STATFS, rq, mq, mp);
	if (rc)
		goto out;
	rc = RSX_WAITREP(rq, mp);
	if (rc || mp->rc)
		rc = rc ? rc : mp->rc;
	else
		fuse_reply_statfs(req, &mp->stbv);

 out:
	if (rc)
		fuse_reply_err(req, rc);
	if (rq)
		pscrpc_req_finished(rq);
}

static int
slash2fuse_symlink(fuse_req_t req, const char *link, fuse_ino_t parent,
    const char *name)
{
	struct pscrpc_bulk_desc *desc;
	struct pscrpc_request *rq=NULL;
	struct srm_symlink_req *mq;
	struct srm_symlink_rep *mp;
	struct fidc_membh *p;
	struct iovec iov;
	int rc;

	msfsthr_ensure();

	if (strlen(link) >= PATH_MAX ||
	    strlen(name) > NAME_MAX)
		return (ENAMETOOLONG);

	p = fidc_lookup_inode((slfid_t)parent);
	if (!p)
		return (ENOMEM);

	rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_SYMLINK, rq, mq, mp);
	if (rc) {
		fidc_membh_dropref(p);
		return (rc);
	}

	slash2fuse_getcred(req, &mq->creds);
	mq->pino = parent;
	mq->linklen = strlen(link) + 1;
	strlcpy(mq->name, name, sizeof(mq->name));

	iov.iov_base = (char *)link;
	iov.iov_len = mq->linklen;

	rsx_bulkclient(rq, &desc, BULK_GET_SOURCE,
	    SRMC_BULK_PORTAL, &iov, 1);

	rc = RSX_WAITREP(rq, mp);
	if (rc || mp->rc)
		rc = rc ? rc : mp->rc;
	else {
		slash2fuse_reply_entry(req, &mp->fg, &mp->attr);
		slash2fuse_fidc_put(&mp->fg, &mp->attr,
		    name, p, &mq->creds, 0);
	}
	fidc_membh_dropref(p);
	pscrpc_req_finished(rq);
	return (rc);
}

static void
slash2fuse_symlink_helper(fuse_req_t req, const char *link,
    fuse_ino_t parent, const char *name)
{
	int error = slash2fuse_symlink(req, link, parent, name);

	if (error)
		fuse_reply_err(req, error);
}

static void
slash2fuse_unlink_helper(fuse_req_t req, fuse_ino_t parent, const char *name)
{
	int error = slash2fuse_unlink(req, parent, name, 1);

	/* unlink events always reply_err */
	fuse_reply_err(req, error);
}

static void
slash2fuse_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,
		int to_set, struct fuse_file_info *fi)
{
	struct pscrpc_request *rq;
	struct srm_setattr_req *mq;
	struct srm_setattr_rep *mp;
	struct msl_fhent *mfh;
	struct fidc_membh *c;
	int rc;

	ENTRY;

	msfsthr_ensure();

	c = NULL;

	rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_SETATTR, rq, mq, mp);
	if (rc)
		goto out;

	c = fidc_lookup_inode((slfid_t)ino);
	if (!c) {
		rc = EINVAL;
		goto out;
	}

	spinlock(&c->fcmh_lock);
	/* We're obtaining the attributes now.
	 */
	if (!((c->fcmh_state & FCMH_GETTING_ATTRS) ||
	      (c->fcmh_state & FCMH_HAVE_ATTRS)))
		c->fcmh_state |= FCMH_GETTING_ATTRS;
	freelock(&c->fcmh_lock);

	if (fi && fi->fh) {
		mfh = (void *)fi->fh;
		psc_assert(c == mfh->mfh_fcmh);
	}

	fidc_fcmh2fdb(c, &mq->sfdb);

	slash2fuse_getcred(req, &mq->creds);
	mq->ino = ino;
	mq->to_set = to_set;
	memcpy(&mq->attr, attr, sizeof(*attr));

	rc = RSX_WAITREP(rq, mp);
	if (rc || mp->rc) {
		rc = rc ? rc : mp->rc;
		goto out;
	}
	fidc_fcm_setattr(c, &mp->attr);
	fuse_reply_attr(req, &mp->attr, 0.0);

 out:
	if (rc)
		fuse_reply_err(req, rc);
	if (c)
		fidc_membh_dropref(c);
	if (rq)
		pscrpc_req_finished(rq);
	EXIT;
}

//XXX convert me
static int
slash2fuse_fsync(__unusedx fuse_req_t req, __unusedx fuse_ino_t ino,
		 __unusedx int datasync, __unusedx struct fuse_file_info *fi)
{

	msfsthr_ensure();

	return (EOPNOTSUPP);
}

static void
slash2fuse_fsync_helper(fuse_req_t req, fuse_ino_t ino, int datasync,
		     struct fuse_file_info *fi)
{
	int error = slash2fuse_fsync(req, ino, datasync, fi);

	/* fsync events always reply_err */
	fuse_reply_err(req, error);
}

static void
slash2fuse_destroy(__unusedx void *userdata)
{
	//do an unmount of slash2
	//fuse_reply_err(req, EOPNOTSUPP);
}

void
slash2fuse_write(fuse_req_t req, __unusedx fuse_ino_t ino,
    const char *buf, size_t size, off_t off,
    struct fuse_file_info *fi)
{
	struct msl_fhent *mfh;
	int rc;

	msfsthr_ensure();

	mfh = (void *)fi->fh;
	if (fidc_lookup_fg(fcmh_2_fgp(mfh->mfh_fcmh)) != mfh->mfh_fcmh) {
		rc = EBADF;
		goto out;
	}
	/* XXX EBADF if fd is not open for writing */
	if (fcmh_2_isdir(mfh->mfh_fcmh)) {
		fidc_membh_dropref(mfh->mfh_fcmh);
		rc = EISDIR;
		goto out;
	}

	rc = msl_write(mfh, (char *)buf, size, off);
	fidc_membh_dropref(mfh->mfh_fcmh);
	if (rc < 0)
		rc = -rc;
	else {
		fuse_reply_buf(req, buf, rc);
		rc = 0;
	}
 out:
	if (rc)
		fuse_reply_err(req, rc);
}

void
slash2fuse_read(fuse_req_t req, __unusedx fuse_ino_t ino,
    size_t size, off_t off, struct fuse_file_info *fi)
{
	struct msl_fhent *mfh;
	void *buf;
	int rc;

	msfsthr_ensure();

	mfh = (void *)fi->fh;
	if (fidc_lookup_fg(fcmh_2_fgp(mfh->mfh_fcmh)) != mfh->mfh_fcmh) {
		rc = EBADF;
		goto out;
	}

	if (fcmh_2_isdir(mfh->mfh_fcmh)) {
		fidc_membh_dropref(mfh->mfh_fcmh);
		rc = EISDIR;
		goto out;
	}

	buf = PSCALLOC(size);
	rc = msl_read(mfh, buf, size, off);
	fidc_membh_dropref(mfh->mfh_fcmh);
	if (rc < 0)
		rc = -rc;
	else {
		fuse_reply_buf(req, buf, rc);
		rc = 0;
	}
	free(buf);
 out:
	if (rc)
		fuse_reply_err(req, rc);
}

void *
slash_init(__unusedx struct fuse_conn_info *conn)
{
	char *name;

	if (getenv("LNET_NETWORKS") == NULL)
		psc_fatalx("please export LNET_NETWORKS");
	if ((name = getenv("SLASH_MDS_NID")) == NULL)
		psc_fatalx("please export SLASH_MDS_NID");

	fidcache_init(FIDC_USER_CLI, fidc_child_reap_cb);
	sl_buffer_cache_init();
	rpc_initsvc();

	/* Start up service threads. */
	mseqpollthr_spawn();
	msctlthr_spawn();
	mstimerthr_spawn();

	if (msrmc_connect(name))
		psc_fatal("unable to connect to MDS");
	if ((name = getenv("SLASH2_PIOS_ID")) != NULL) {
		if ((prefIOS = libsl_str2id(name)) == IOS_ID_ANY)
			psc_warnx("SLASH2_PIOS_ID (%s) does not resolve to "
				  "a valid IOS, defaulting to IOS_ID_ANY", name);
	}

	slFsops = PSCALLOC(sizeof(*slFsops));
	slFsops->slfsop_getattr = slash2fuse_stat;

	return (NULL);
}

struct fuse_lowlevel_ops zfs_operations = {
	.access		= slash2fuse_access,
	.create		= slash2fuse_create,
	.destroy	= slash2fuse_destroy,
	.fsync		= slash2fuse_fsync_helper,
	.fsyncdir	= slash2fuse_fsync_helper,
	.getattr	= slash2fuse_getattr,
	.link		= slash2fuse_link,
	.lookup		= slash2fuse_lookup_helper,
	.mkdir		= slash2fuse_mkdir,
	.mknod		= slash2fuse_mknod_helper,
	.open		= slash2fuse_open,
	.opendir	= slash2fuse_opendir,
	.read		= slash2fuse_read,
	.readdir	= slash2fuse_readdir_helper,
	.readlink	= slash2fuse_readlink,
	.release	= slash2fuse_release,
	.releasedir	= slash2fuse_release,
	.rename		= slash2fuse_rename_helper,
	.rmdir		= slash2fuse_rmdir_helper,
	.setattr	= slash2fuse_setattr,
	.statfs		= slash2fuse_statfs,
	.symlink	= slash2fuse_symlink_helper,
	.unlink		= slash2fuse_unlink_helper,
	.write		= slash2fuse_write
};

int
psc_usklndthr_get_type(const char *namefmt)
{
	if (namefmt[0] == 'a')
		return (MSTHRT_LNETAC);
	return (MSTHRT_USKLNDPL);
}

void
psc_usklndthr_get_namev(char buf[PSC_THRNAME_MAX], const char *namefmt,
    va_list ap)
{
	size_t n;

	n = strlcpy(buf, "ms", PSC_THRNAME_MAX);
	if (n < PSC_THRNAME_MAX)
		vsnprintf(buf + n, PSC_THRNAME_MAX - n, namefmt, ap);
}

static int
msl_fuse_lowlevel_mount(const char *mp)
{
	struct fuse_session *se;
	struct fuse_chan *ch;
	struct fuse_args args = FUSE_ARGS_INIT(0, NULL);
	char *fuse_opts;

	slash2fuse_listener_init();

	if (asprintf(&fuse_opts, FUSE_OPTIONS, mp) == -1)
		return ENOMEM;

	if (fuse_opt_add_arg(&args, "") == -1 ||
	    fuse_opt_add_arg(&args, "-o") == -1 ||
	    fuse_opt_add_arg(&args, fuse_opts) == -1) {
		fuse_opt_free_args(&args);
		free(fuse_opts);
		return ENOMEM;
	}
	free(fuse_opts);

	ch = fuse_mount(mp, &args);
	if (ch == NULL) {
		fuse_opt_free_args(&args);
		return (errno);
	}

	se = fuse_lowlevel_new(&args, &zfs_operations, sizeof(zfs_operations),
	    NULL);
	fuse_opt_free_args(&args);

	if (se == NULL) {
		fuse_unmount(mp, ch);
		return EIO;
	}

	fuse_session_add_chan(se, ch);

	if (slash2fuse_newfs(mp, ch) != 0) {
		fuse_session_destroy(se);
		fuse_unmount(mp, ch);
		return EIO;
	}

	return (0);
}

__dead void
usage(void)
{
	fprintf(stderr, "usage: %s [-U] [-f conf] [-S socket] node\n", progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	char c, mp[PATH_MAX], *nc_mp, *cfg = _PATH_SLASHCONF;
	int rc, unmount;

	pfl_init();

	unmount = 0;
	nc_mp = NULL;
	progname = argv[0];
	while ((c = getopt(argc, argv, "f:m:S:U")) != -1)
		switch (c) {
		case 'f':
			cfg = optarg;
			break;
		case 'm':
			psc_errorx("-m is deprecated");
			nc_mp = optarg;
			break;
		case 'S':
			if (strlcpy(ctlsockfn, optarg,
			    PATH_MAX) >= PATH_MAX)
				psc_fatalx("%s: too long", optarg);
			break;
		case 'U':
			unmount = 1;
			break;
		default:
			usage();
		}
	argc -= optind;
	argv += optind;
	if (argc == 1)
		nc_mp = argv[0];
	else if (argc || nc_mp == NULL)
		usage();

	pscthr_init(MSTHRT_FUSE, 0, NULL, NULL, 0, "msfusethr");

	slashGetConfig(cfg);
	slash_init(NULL);

	if (unmount) {
		char cmdbuf[BUFSIZ];

		if ((size_t)snprintf(cmdbuf, sizeof(cmdbuf),
		    "umount %s", nc_mp) >= sizeof(cmdbuf))
			psc_error("snprintf");
		else if (system(cmdbuf) == -1)
			psc_error("%s", cmdbuf);
	}
	/* canonicalize mount path */
	if (realpath(nc_mp, mp) == NULL)
		psc_fatal("realpath %s", nc_mp);

	if (msl_fuse_lowlevel_mount(mp))
		return (-1);

#if 0
	if (set_signal_handler(SIGHUP, exit_handler) != 0 ||
	    set_signal_handler(SIGINT, exit_handler) != 0 ||
	    set_signal_handler(SIGTERM, exit_handler) != 0 ||
	    set_signal_handler(SIGPIPE, SIG_IGN) != 0) {
		return 2;
	}
#endif
	rc = slash2fuse_listener_start();

	return (0);
}
