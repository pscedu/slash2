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
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "pfl.h"
#include "psc_types.h"
#include "psc_mount/dhfh.h"
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

struct slfuse_dirent {
	u64   ino;
        u64   off;
	u32   namelen;
        u32   type;
	char name[0];
};

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

        if(sigaction(sig, &sa, NULL) == -1) {
                perror("sigaction");
                return -1;
        }

        return 0;
}
#endif

sl_ios_id_t prefIOS = IOS_ID_ANY;
const char *progname;
char ctlsockfn[] = _PATH_MSCTLSOCK;

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
	
	psc_trace("inode:%"_P_U64"d generation:%lu", e.ino, e.generation);
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

	psc_trace("inode:%"_P_U64"d generation:%lu", 
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
		if (!fidc_child_get(parent, name, strnlen(name, NAME_MAX)))
			(struct fidc_child *)fidc_child_add(parent, c, name);
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
	struct pscrpc_request *rq;
	struct srm_access_req *mq;
	struct srm_generic_rep *mp;
	struct fidc_membh *c;
	int rc=0;

	msfsthr_ensure();

	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION, SRMT_ACCESS, 
			     rq, mq, mp)) != 0)
		goto out;

	slash2fuse_getcred(req, &mq->creds);

	mq->ino = ino;
	mq->mask = mask;

	c = fidc_lookup_load_inode((slfid_t)ino, &mq->creds);
	if (!c) {
		rc = ENOENT;
		goto out;
	}

	rc = rsx_waitrep(rq, sizeof(*mp), &mp);
	if (rc || mp->rc)
		rc = rc ? rc : mp->rc;
 out:
	fuse_reply_err(req, rc);
	fidc_membh_dropref(c);
}

static void
slash2fuse_openref_update(struct fidc_membh *fcmh, int flags, int *uord)
{
	struct fidc_open_obj *o=fcmh->fcmh_fcoo;
	int l=reqlock(&fcmh->fcmh_lock);
	
#define SL2F_UPOPREF_READ {					\
		if (!o->fcoo_oref_rw[0])			\
			*uord |= SL_FREAD;			\
		o->fcoo_oref_rw[0]++;				\
	}
	
#define SL2F_UPOPREF_WRITE {					\
		if (!o->fcoo_oref_rw[1])			\
			*uord |= SL_FWRITE;			\
		o->fcoo_oref_rw[1]++;				\
	}
	
#define SL2F_DOWNOPREF_READ {					\
		o->fcoo_oref_rw[0]--;				\
		if (!o->fcoo_oref_rw[0])			\
			*uord |= SL_FREAD;			\
	}
	
#define SL2F_DOWNOPREF_WRITE {					\
		o->fcoo_oref_rw[1]--;				\
		if (!o->fcoo_oref_rw[1])			\
			*uord |= SL_FWRITE;			\
	}
	
	psc_assert(o->fcoo_oref_rw[0] >= 0);
	psc_assert(o->fcoo_oref_rw[1] >= 0);

	if (*uord) {
		*uord = 0;
		if (flags & O_WRONLY)
			{SL2F_UPOPREF_WRITE;}
		
		else if (flags & O_RDWR) {
			{SL2F_UPOPREF_WRITE;}
			{SL2F_UPOPREF_READ;}
		} else 
			{SL2F_UPOPREF_READ;}

	} else {
		if (flags & O_WRONLY) {
			{SL2F_DOWNOPREF_WRITE;}
			
 		} else if (flags & O_RDWR) {
			{SL2F_DOWNOPREF_WRITE;}
			{SL2F_DOWNOPREF_READ;}
		} else
			{SL2F_DOWNOPREF_READ;}
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

	if(flags & O_CREAT)
		*nflags |= SL_FCREAT;
	if(flags & O_SYNC)
		*nflags |= SL_FSYNC;
	if(flags & O_DSYNC)
		*nflags |= SL_FDSYNC;
	if(flags & O_RSYNC)
		*nflags |= SL_FRSYNC;
	if(flags & O_APPEND)
		*nflags |= SL_FAPPEND;
	//if(flags & O_LARGEFILE)
	*nflags |= SL_FOFFMAX;
	if(flags & O_NOFOLLOW)
		*nflags |= SL_FNOFOLLOW;
	if(flags & O_TRUNC)
		*nflags |= SL_FTRUNC;
	if(flags & O_EXCL)
		*nflags |= SL_FEXCL;
	if(flags & O_DIRECTORY)
		*nflags |= SL_DIRECTORY;

}

static int
slash2fuse_openrpc(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
	struct pscrpc_request *rq;
	struct srm_open_req *mq;
	struct srm_opencreate_rep *mp;
	struct fidc_membh *h=(struct fidc_membh *)fi->fh;
	int rc=0;

	psc_assert(fi->fh);
	psc_assert(ino == fcmh_2_fid(h));
		
	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
		   (fi->flags & O_DIRECTORY) ? SRMT_OPENDIR : SRMT_OPEN,  
		   rq, mq, mp)) != 0)
		return (rc);

        slash2fuse_getcred(req, &mq->creds);
	slash2fuse_transflags(fi->flags, &mq->flags, &mq->mode);
	mq->ino = ino;

	if (!(rc = rsx_waitrep(rq, sizeof(*mp), &mp))) {
		if (mp->rc)
			rc = mp->rc;
		else {
			psc_assert(h->fcmh_fcoo->fcoo_cfd == FID_ANY);
			h->fcmh_fcoo->fcoo_cfd = mp->cfd;
			//fidc_fcm_setattr(h, &mp->attr);
		}
	}
	pscrpc_req_finished(rq);

	return (rc);
}

/** 
 * slash2fuse_fcoo_start - helper for slash2fuse_create and slash2fuse_open.
 *  Starts the fcoo if one doesn't already exist.
 */
static int
slash2fuse_fcoo_start(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
	struct fidc_membh *c=(struct fidc_membh *)fi->fh;
	int flags=1, rc=0;

	psc_assert(fi->fh);
	psc_assert(ino == fcmh_2_fid(c));

	spinlock(&c->fcmh_lock);
	if (!c->fcmh_fcoo)
		fidc_fcoo_start_locked(c);
	else {		
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
	}
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
	struct fidc_membh *p, *n;
	struct fidc_child *c;
	int rc=0, flags=1;

	msfsthr_ensure();

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

	/* Lock here so that we can do an atomic fidc_child_get and possible
	 *  add.
	 */
	spinlock(&p->fcmh_lock);	
	if ((c = fidc_child_get(p, name, strlen(name)))) {
		/* The cached basename already exists.  This could mean that
		 *  another create operation is in progress.  If that's the 
		 *  case then c->fcc_fcmh will be NULL.
		 * NOTE: the parent's fcmh_lock and waitq will be used here. 
		 */
		if (fi->flags & O_EXCL) {
			freelock(&p->fcmh_lock);
			fidc_membh_dropref(c->fcc_fcmh);
			rc = EEXIST;
			goto out;
		} 	

		fidc_child_wait_locked(p, c);

		/* Treat this as a standard open()
		 */
		fi->fh = (uint64_t)c->fcc_fcmh;
		slash2fuse_fcoo_start(req, (fuse_ino_t)fcmh_2_fid(c->fcc_fcmh), fi);
		slash2fuse_reply_create(req, fcmh_2_fgp(c->fcc_fcmh), 
					fcmh_2_attrp(c->fcc_fcmh), fi);

		fidc_membh_dropref(c->fcc_fcmh);
		return;

	} else
		/* Now we've established a local placeholder for this create.
		 *  any other creates to this pathame will block in 
		 *  fidc_child_wait_locked() until we release 'c'.
		 */
		c = fidc_child_add(p, NULL, name);

	freelock(&p->fcmh_lock);

	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	     SRMT_CREATE, rq, mq, mp)) != 0)
		goto out;
	
        slash2fuse_getcred(req, &mq->creds);
	slash2fuse_transflags((fi->flags | O_CREAT), 
			      &mq->flags, &mq->mode);
	mq->mode = mode;
	mq->pino = parent;
	mq->len = strlen(name);
	strncpy(mq->name, name, mq->len);

	rc = rsx_waitrep(rq, sizeof(*mp), &mp);

	if (mp->rc == EEXIST) {
		psc_info("fid %"_P_U64"d already existed on mds", 
			 mp->fg.fg_fid);
		/*  Handle the network side of O_EXCL.
		 */
		if (fi->flags & O_EXCL) {
			rc = EEXIST;
			goto out;
		}

	} else if (rc || (mp->rc && mp->rc != EEXIST)) {
		rc = rc ? rc : mp->rc;
		goto out;
	}
	
	n = slash2fuse_fidc_putget(&mp->fg, &mp->attr, NULL, NULL, &mq->creds,
				   (FIDC_LOOKUP_EXCL | FIDC_LOOKUP_FCOOSTART));
	psc_assert(n);
	psc_assert(n->fcmh_fcoo && (n->fcmh_state & FCMH_FCOO_STARTING));
	n->fcmh_fcoo->fcoo_cfd = mp->cfd;

	fi->fh = (uint64_t)n;
	fi->keep_cache = 1;
	/* Inc ref the fcoo.  The rpc has already taken place.
	 */
	slash2fuse_openref_update(n, fi->flags, &flags);

	fidc_fcoo_startdone(n);

 out:
	if (rc) {
                fuse_reply_err(req, rc);		
		fidc_child_fail(c);

	} else {
		fidc_child_add_fcmh(c, n);
		slash2fuse_reply_create(req, &mp->fg, &mp->attr, fi);
		/* slash2fuse_fidc_putget() leaves the fcmh ref'd.
		 */
		fidc_membh_dropref(n);		
	}
	pscrpc_req_finished(rq);
	fidc_membh_dropref(p);
}

static void 
slash2fuse_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
	int rc=0;
	struct fidc_membh *c;

	msfsthr_ensure();

	psc_trace("FID %"_P_U64"d", (slfid_t)ino);

	c = fidc_lookup_inode((slfid_t)ino);
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

	fi->fh = (uint64_t) c;
	fi->keep_cache = 1;
	rc = slash2fuse_fcoo_start(req, ino, fi);
 out:
	if (rc)
                fuse_reply_err(req, rc);
	else {
		fuse_reply_open(req, fi);
		fidc_membh_dropref(c);
	}
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

	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION, SRMT_GETATTR, 
			     rq, mq, mp)) != 0)
		return (rc);
	
	memcpy(&mq->creds, creds, sizeof(*creds));
	mq->ino = fcmh_2_fid(fcmh);

	rc = rsx_waitrep(rq, sizeof(*mp), &mp);
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
	struct srm_link_req *mq;
	struct srm_link_rep *mp;
	struct fidc_membh *p, *c;
	struct fuse_entry_param e;
	struct slash_creds creds;
	int rc=0;

	memset(&e, 0, sizeof(e));

	msfsthr_ensure();

	slash2fuse_getcred(req, &creds);
	/* Check the newparent inode.
	 */
	p = fidc_lookup_load_inode((slfid_t)newparent, &creds);
	if (!p) {
		rc = ENOMEM;
		goto err;
	}
	
	if (!fcmh_2_isdir(p)) {
		rc = ENOTDIR;
		goto err;
	}
	/* Check the child inode.
	 */
	c = fidc_lookup_load_inode((slfid_t)ino, &creds);
	if (!c) {
		rc = ENOMEM;
		goto err;
	} else
		psc_assert(fcmh_2_fid(p) == ino);

	if (fcmh_2_isdir(p)) {
		rc = EISDIR;
		goto err;
	}
	/* Create and initialize the link rpc.
	 */
	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
			     SRMT_LINK, rq, mq, mp)) != 0)
		goto err;

	mq->len = strlen(newname);
	if (mq->len >= NAME_MAX) {
		rc = ENAMETOOLONG;
		goto err;
	} else
		strncpy(mq->name, newname, mq->len);

	memcpy(&mq->creds, &creds, sizeof(mq->creds));
	mq->pino = fcmh_2_fid(p);
	mq->ino = ino;

	rc = rsx_waitrep(rq, sizeof(*mp), &mp);
	if (rc || mp->rc) {
	err:		
		rc = rc ? rc : mp->rc;	
		fuse_reply_err(req, rc);

	} else {
		slash2fuse_reply_entry(req, &mp->fg, &mp->attr);
		//slash2fuse_fidc_put(&mp->fg, &mp->attr, name, p, &mq->creds);
		fidc_fcm_setattr(c, &mp->attr);
	}	
	fidc_membh_dropref(c);
	fidc_membh_dropref(p);
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
	struct fidc_child *c;
	struct fuse_entry_param e;
	int rc;

	memset(&e, 0, sizeof(e));
	msfsthr_ensure();

	if (strlen(name) >= NAME_MAX) {
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

	else if ((c = fidc_child_get(p, name, strlen(name)))) {
		fidc_membh_dropref(c->fcc_fcmh);
		rc = EEXIST;
                goto out;
	}
	/* Create and initialize the link rpc.
	 */
	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	     SRMT_MKDIR, rq, mq, mp)) != 0)
		goto out;

	mq->len = strlen(name);
	strncpy(mq->name, name, mq->len);

	slash2fuse_getcred(req, &mq->creds);

	psc_assert(fcmh_2_fid(p) == parent);
	mq->pino = parent;
	mq->mode = mode;

	rc = rsx_waitrep(rq, sizeof(*mp), &mp);

	psc_info("pino=%"_P_U64"x mode=0%o name='%s' rc=%d mp->rc=%d", 
		 mq->pino, mq->mode, mq->name, rc, mp->rc);

	if (rc || mp->rc) {
		rc = rc ? rc : mp->rc;	
		fuse_reply_err(req, rc);
	} else {
		slash2fuse_reply_entry(req, &mp->fg, &mp->attr);
		slash2fuse_fidc_put(&mp->fg, &mp->attr, name, p, &mq->creds, 0);
	}
 out:
	fidc_membh_dropref(p);
	if (rq)
		pscrpc_req_finished(rq);
}


static int 
slash2fuse_unlink(fuse_req_t req, fuse_ino_t parent, const char *name, 
		  int ford)
{
	struct pscrpc_request *rq;
	struct srm_unlink_req *mq;
	struct srm_unlink_rep *mp;
	struct fidc_membh *p;
	struct fidc_child *fcc;
	struct fuse_entry_param e;
	int rc;

	memset(&e, 0, sizeof(e));
	msfsthr_ensure();

        if(strlen(name) >= NAME_MAX)
                return (ENAMETOOLONG);

	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
 	     (ford ? SRMT_UNLINK : SRMT_RMDIR), rq, mq, mp)) != 0)
		return (rc);

	slash2fuse_getcred(req, &mq->creds);
	/* Check the parent inode.
	 */
	p = fidc_lookup_load_inode((slfid_t)parent, &mq->creds);
	if (!p) {
		pscrpc_req_finished(rq);
		return (EINVAL);
	}

	if (!fcmh_2_isdir(p)) {
                rc = ENOTDIR;
		goto out;
	}
	mq->len = strlen(name);
	strncpy(mq->name, name, mq->len);
	mq->pino = parent;

	rc = rsx_waitrep(rq, sizeof(*mp), &mp);
	if (rc || mp->rc) {
		rc = rc ? rc : mp->rc;
		goto out;
	}

	fcc = fidc_child_get(p, name, mq->len);
	if (fcc) {
		struct fidc_membh *c=fcc->fcc_fcmh;
		fidc_child_free(p, fcc);
		fidc_membh_dropref(c);
	}
 out:
	fidc_membh_dropref(p);
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

	fuse_reply_err(req, EOPNOTSUPP);
}

#define OBD_TIMEOUT 15

int
slashrpc_timeout(__unusedx void *arg)
{
	return (0);
}


static int 
slash2fuse_readdir(fuse_req_t req, __unusedx fuse_ino_t ino, size_t size, 
		   off_t off, struct fuse_file_info *fi)
{
	struct pscrpc_bulk_desc *desc;
	struct pscrpc_request *rq;
	struct srm_readdir_req *mq;
	struct srm_readdir_rep *mp;
	struct fidc_membh *d;
	struct iovec iov[2];
	int rc;
	u64 cfd;

	msfsthr_ensure();

	iov[0].iov_base = NULL;
	iov[1].iov_base = NULL;

	/* Don't allow writes on directory inodes.
	 */
	if ((fi->flags & O_WRONLY) || (fi->flags & O_RDWR))
		return (EINVAL);

	d = (struct fidc_membh *)fi->fh;
	psc_assert(d);

	/* Ensure that the fcmh is still valid, we can't rely 
	 *  only on the inode number, the generation # number
	 *  must be taken into account.
	 * NOTE: 'd' must be decref'd.
	 */
	if (fidc_lookup_fg(fcmh_2_fgp(d)) != d)
		return (EBADF);

	if ((fidc_fcmh2cfd(d, &cfd) < 0) || cfd == FID_ANY) {
		fidc_membh_dropref(d);
		return (EBADF);
	}
	
	if (!fcmh_2_isdir(d)) {
		fidc_membh_dropref(d);
		return (ENOTDIR);
	}

	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_READDIR, rq, mq, mp)) != 0) {
		fidc_membh_dropref(d);
		return (rc);
	}

	slash2fuse_getcred(req, &mq->creds);
	mq->cfd = cfd;
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

	rc = rsx_waitrep(rq, sizeof(*mp), &mp);
	if (rc || mp->rc) {
		rc = rc ? rc : mp->rc;
		goto out;
	}

	if (mq->nstbpref) {
		int i;
		struct fidc_memb fcm;
		struct fidc_membh *fcmh;
		struct srm_getattr_rep *attr = (struct srm_getattr_rep *)iov[1].iov_base;

		for (i=0; i < mq->nstbpref; i++, attr++) {
			if (attr->rc || !attr->attr.st_ino)
				continue;
			memset(&fcm, 0, sizeof(struct fidc_memb));
			memcpy(&fcm.fcm_stb, &attr->attr, sizeof(struct stat));
			fcm.fcm_fg.fg_fid = attr->attr.st_ino;
			fcm.fcm_fg.fg_gen = attr->gen;
			
			psc_trace("adding i+g:%"_P_U64"d+%"_P_U64"d rc=%d", 
				  fcm.fcm_fg.fg_fid, fcm.fcm_fg.fg_gen, attr->rc);  

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
		return (EINVAL);

	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION, SRMT_LOOKUP, 
			     rq, mq, mp)) != 0)
		return (rc);

	slash2fuse_getcred(req, &mq->creds);
	strncpy(mq->name, name, strlen(name));
	mq->pino = fcmh_2_fid(p);
	mq->len = strlen(name);

	rc = rsx_waitrep(rq, sizeof(*mp), &mp);
	if (rc || mp->rc) 
		rc = rc ? rc : mp->rc;	

	else {
		/* Add the inode to the cache first, otherwise fuse may 
		 *  come to us with another request for the inode it won't
		 *  yet be visible in the cache.
		 */
		slash2fuse_fidc_put(&mp->fg, &mp->attr, name, p, &mq->creds, 0);
		slash2fuse_reply_entry(req, &mp->fg, &mp->attr);
	}

	pscrpc_req_finished(rq);
	return (rc);
}


static void 
slash2fuse_lookup_helper(fuse_req_t req, fuse_ino_t parent, const char *name)
{
        int error;
	struct fidc_membh *p;
	struct fidc_child *c;
	struct slash_creds creds;

	slash2fuse_getcred(req, &creds);

	psc_infos(PSS_OTHER, "name %s inode %"_P_U64"d", 
		  name, parent);

	p = fidc_lookup_load_inode((slfid_t)parent, &creds);
	if (!p) {
		/* Parent inode must exist in the cache.
		 */
		psc_warnx("name %s - failed to load inode %"_P_U64"x", 
			  name, parent);
		error = EINVAL;
		goto out;

	} else if (!fcmh_2_isdir(p)) {
		fidc_membh_dropref(p);
		error = ENOTDIR;
		goto out;

	} else if ((c = fidc_child_get(p, name, strlen(name)))) {
		/* The component is already cached.
		 */
		struct fuse_entry_param e;
		struct slash_creds creds;
		struct stat stb;
		struct fidc_membh *t=c->fcc_fcmh;
		
		memset(&e, 0, sizeof(e));
		memset(&stb, 0, sizeof(stb));

		slash2fuse_getcred(req, &creds);
		error = slash2fuse_stat(c->fcc_fcmh, &creds);
		
		e.ino = fcmh_2_fid(c->fcc_fcmh);
		e.generation = fcmh_2_gen(c->fcc_fcmh);

		DEBUG_FCMH(PLL_INFO, c->fcc_fcmh, "lookup debug");

		if (!memcmp(fcmh_2_stb(c->fcc_fcmh), &stb, 
			    sizeof(struct stat))) {
			DEBUG_FCMH(PLL_ERROR, c->fcc_fcmh, 
				   "attrs should not be zero here!");
			abort();
		}			

		memcpy(&e.attr, fcmh_2_stb(c->fcc_fcmh), sizeof(e.attr));
		fuse_reply_entry(req, &e);
		fidc_membh_dropref(c->fcc_fcmh);
		error = 0;
		
		if (t != c->fcc_fcmh || 
		    strncmp(name, c->fcc_name, c->fcc_len) || 
		    fcmh_2_fid(c->fcc_fcmh) != fcmh_2_fid(t)) {
			/*  Try to catch fcmh state before hitting bug #14.
			 */
			DEBUG_FCMH(PLL_ERROR, t, "fcc_fcmh was");
			DEBUG_FCMH(PLL_ERROR, c->fcc_fcmh, "fcc_fcmh is now");
			abort();
		}
			
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
	struct pscrpc_request *rq;
	struct srm_readlink_req *mq;
	struct srm_readlink_rep *mp;
	int rc;

	msfsthr_ensure();

	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION, SRMT_READLINK, 
			     rq, mq, mp)) != 0)
		goto out;

	slash2fuse_getcred(req, &mq->creds);
	mq->ino = ino;

	rc = rsx_waitrep(rq, sizeof(*mp), &mp);
	if (rc || mp->rc) 
		rc = rc ? rc : mp->rc;

 out:	
	if (!rc) {
		mp->buf[mp->len] = '\0';
		fuse_reply_readlink(req, mp->buf);
	} else 
		fuse_reply_err(req, rc);

	pscrpc_req_finished(rq);

}


static int
slash2fuse_releaserpc(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
	struct pscrpc_request *rq;
	struct srm_release_req *mq;
	struct srm_generic_rep *mp;
	struct fidc_membh *h=(struct fidc_membh *)fi->fh;
	int rc=0;
	u32 mode;

	psc_assert(fi->fh);	
	psc_assert(ino == fcmh_2_fid(h));
	psc_assert(h->fcmh_fcoo);
	
	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
			     SRMT_RELEASE, rq, mq, mp)) != 0)
		return (rc);

        slash2fuse_getcred(req, &mq->creds);
	slash2fuse_transflags(fi->flags, &mq->flags, &mode);	
	mq->cfd = h->fcmh_fcoo->fcoo_cfd;
	
	if (!(rc = rsx_waitrep(rq, sizeof(*mp), &mp))) {
		if (mp->rc)
			rc = mp->rc;
	}
	pscrpc_req_finished(rq);

	return (rc);
}

static void 
slash2fuse_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
	int rc=0, fdstate=0;
	struct fidc_membh *c=(struct fidc_membh *)fi->fh;

	msfsthr_ensure();

	psc_assert(c->fcmh_fcoo);
	
	slash2fuse_openref_update(c, fi->flags, &fdstate);	
	
	DEBUG_FCMH(PLL_INFO, c, "slash2fuse_release");

	if ((c->fcmh_state & FCMH_FCOO_CLOSING) && fdstate) {
		DEBUG_FCMH(PLL_INFO, c, "calling releaserpc");
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
	struct pscrpc_request *rq;
	struct srm_generic_rep *mp;
	struct srm_rename_req *mq;
	struct iovec iov[2];
	int rc;

	msfsthr_ensure();

	if (strlen(name) >= NAME_MAX || strlen(newname) >= NAME_MAX)
		return (ENAMETOOLONG);

	(int)fidc_child_rename(parent, name, newparent, newname);

	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_RENAME, rq, mq, mp)) != 0)
		return (rc);

	mq->opino = parent;
	mq->npino = newparent;
	mq->fromlen = strlen(name);
	mq->tolen = strlen(newname);

	iov[0].iov_base = (void *)name;
	iov[0].iov_len = strlen(name);
	iov[1].iov_base = (void *)newname;
	iov[1].iov_len = strlen(newname);

	rsx_bulkclient(rq, &desc, BULK_GET_SOURCE, SRMC_BULK_PORTAL, iov, 2);

	rc = rsx_waitrep(rq, sizeof(*mp), &mp);
	if (rc || mp->rc) 
		rc = rc ? rc : mp->rc;

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
slash2fuse_statfs(fuse_req_t req)
{
	struct pscrpc_request *rq;
	struct srm_statfs_req *mq;
	struct srm_statfs_rep *mp;
	int rc;

	msfsthr_ensure();

	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_STATFS, rq, mq, mp)) != 0) {}
	else 
		rc = rsx_waitrep(rq, sizeof(*mp), &mp);

	if (rc || mp->rc) {
		rc = rc ? rc : mp->rc;
		fuse_reply_err(req, rc);
	} else
		fuse_reply_statfs(req, &mp->stbv);

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
	struct iovec iov;
	struct fidc_membh *p;
	int rc;

	msfsthr_ensure();

	iov.iov_base = NULL;

	if (strlen(name) >= NAME_MAX)
		return (ENAMETOOLONG);

	p = fidc_lookup_inode((slfid_t)parent);
	if (!p)
		return (ENOMEM);

	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_SYMLINK, rq, mq, mp)) != 0) {
		fidc_membh_dropref(p);
		return (rc);
	}

	slash2fuse_getcred(req, &mq->creds);
	mq->pino = parent;
	mq->linklen = strlen(link);
	if (mq->linklen >= PATH_MAX) {
		rc = ENAMETOOLONG;
		goto out;
	}

	mq->namelen = strlen(name);
	if (mq->namelen >= NAME_MAX) {
		rc = ENAMETOOLONG;
		goto out;
	}

	iov.iov_base = (void *)PSCALLOC(mq->linklen + 1);
	iov.iov_len = mq->linklen;
	
	strncpy((char *)iov.iov_base, name, mq->linklen);
	strncpy(mq->name, name, mq->linklen);
	
	rsx_bulkclient(rq, &desc, BULK_GET_SOURCE, SRMC_BULK_PORTAL, &iov, 1);

	rc = rsx_waitrep(rq, sizeof(*mp), &mp);
	if (rc || mp->rc) {
		rc = rc ? rc : mp->rc;	
		goto out;
	} else {
		slash2fuse_reply_entry(req, &mp->fg, &mp->attr);
		slash2fuse_fidc_put(&mp->fg, &mp->attr, name, p, &mq->creds, 0);
	}
 out:
	fidc_membh_dropref(p);
	pscrpc_req_finished(rq);
	free(iov.iov_base);
	return (rc);
}

static void 
slash2fuse_symlink_helper(fuse_req_t req, const char *link, fuse_ino_t parent, 
			  const char *name)
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
	struct fidc_membh *c;
	int rc;

	ENTRY;
	
	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION, SRMT_SETATTR, 
			     rq, mq, mp)) != 0)
		goto cleanup;

	c = fidc_lookup_inode((slfid_t)ino);
	if (!c) {
		rc = EINVAL;
		goto err;
	}
		
	spinlock(&c->fcmh_lock);
	/* We're obtaining the attributes now.
	 */
	if (!((c->fcmh_state & FCMH_GETTING_ATTRS) ||
	      (c->fcmh_state & FCMH_HAVE_ATTRS)))
		c->fcmh_state |= FCMH_GETTING_ATTRS;
	freelock(&c->fcmh_lock);

	if (fi && fi->fh)
		psc_assert(c == (struct fidc_membh *)fi->fh);
	
	if (fidc_fcmh2cfd(c, &mq->cfd) < 0)
		psc_assert(mq->cfd == FID_ANY);
	
	slash2fuse_getcred(req, &mq->creds);
	mq->ino = ino;
	mq->to_set = to_set;
	memcpy(&mq->attr, attr, sizeof(*attr));	

	rc = rsx_waitrep(rq, sizeof(*mp), &mp);
 err:
	if (rc || mp->rc) {
		rc = rc ? rc : mp->rc;
		fuse_reply_err(req, rc);
	} else {
		/* It's possible that the inode wasn't cached.
		 */ 
		if (c) {
			fidc_fcm_setattr(c, &mp->attr);
			fidc_membh_dropref(c);
		}
                fuse_reply_attr(req, &mp->attr, 0.0);
	}
 cleanup:
	pscrpc_req_finished(rq);
}


//XXX convert me
static int 
slash2fuse_fsync(__unusedx fuse_req_t req, __unusedx fuse_ino_t ino, 
		 __unusedx int datasync, __unusedx struct fuse_file_info *fi)
{
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

// XXX convertme
static int 
slash2fuse_write(__unusedx fuse_req_t req, 
		 __unusedx fuse_ino_t ino, 
		 __unusedx const char *buf, 
		 __unusedx size_t size, 
		 __unusedx off_t off, 
		 __unusedx struct fuse_file_info *fi)
{
	return (EOPNOTSUPP);
}


static void 
slash2fuse_write_helper(__unusedx fuse_req_t req, 
			__unusedx fuse_ino_t ino, 
			__unusedx const char *buf, 
			__unusedx size_t size, 
			__unusedx off_t off, 
			__unusedx struct fuse_file_info *fi)
{
        int error = slash2fuse_write(req, ino, buf, size, off, fi);
        if(error)
                fuse_reply_err(req, error);
}


// XXX convert me
static int 
slash2fuse_read(fuse_req_t req, 
		__unusedx fuse_ino_t ino, 
		size_t size, 
		off_t off, 
		struct fuse_file_info *fi) 
{
	struct pscrpc_bulk_desc *desc;
	struct pscrpc_request *rq;
	struct srm_io_rep *mp;
	struct srm_io_req *mq;
	struct iovec iov;
	struct fhent *fh;
	char *buf;
	int rc;

	msfsthr_ensure();

	/* First get the fhentry from the cache.
	 */
	fh = fh_lookup(fi->fh);
	if (!fh)
		return (EBADF);

//	rc = msl_read(fh, buf, size, off);

	if ((rc = RSX_NEWREQ(ion_get()->csvc_import,
	    SRIC_VERSION, SRMT_READ, rq, mq, mp)) != 0)
		return (rc);
	
	buf = PSCALLOC(size);

	mq->cfd = fi->fh;
	mq->size = size;
	mq->offset = off;
	mq->op = SRMIO_WR;
	if ((rc = rsx_waitrep(rq, sizeof(*mp), &mp)) == 0) {
		if (mp->rc)
			rc = mp->rc;
		else {
			iov.iov_base = buf;
			iov.iov_len = size;
			rc = rsx_bulkclient(rq, &desc, BULK_GET_SOURCE,
			    SRIC_BULK_PORTAL, &iov, 1);
			
			if (!rc)
				fuse_reply_buf(req, buf, mp->size);

			if (desc)
				pscrpc_free_bulk(desc);
		}
	}	
	PSCFREE(buf);
	pscrpc_req_finished(rq);
	return (rc);
}

static void 
slash2fuse_read_helper(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, 
		    struct fuse_file_info *fi)
{
        int error = slash2fuse_read(req, ino, size, off, fi);
        if(error)
                fuse_reply_err(req, error);
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


struct fuse_lowlevel_ops zfs_operations =
{
	.open       = slash2fuse_open,
	.read       = slash2fuse_read_helper,
	.write      = slash2fuse_write_helper,
	.release    = slash2fuse_release,
	.opendir    = slash2fuse_opendir,
	.readdir    = slash2fuse_readdir_helper,
	.releasedir = slash2fuse_release,
	.lookup     = slash2fuse_lookup_helper,
	.getattr    = slash2fuse_getattr,
	.readlink   = slash2fuse_readlink,
	.mkdir      = slash2fuse_mkdir,
	.rmdir      = slash2fuse_rmdir_helper,
	.create     = slash2fuse_create,
	.unlink     = slash2fuse_unlink_helper,
	.mknod      = slash2fuse_mknod_helper,
	.symlink    = slash2fuse_symlink_helper,
	.link       = slash2fuse_link,
	.rename     = slash2fuse_rename_helper,
	.setattr    = slash2fuse_setattr,
	.fsync      = slash2fuse_fsync_helper, //
	.fsyncdir   = slash2fuse_fsync_helper, //
	.access     = slash2fuse_access,
	.statfs     = slash2fuse_statfs,
	.destroy    = slash2fuse_destroy, //
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

enum {
	MS_OPT_CTLSOCK,
	MS_OPT_USAGE
};

struct fuse_opt msopts[] = {
	FUSE_OPT_KEY("-S ", MS_OPT_CTLSOCK),
	FUSE_OPT_KEY("-? ", MS_OPT_USAGE),
	FUSE_OPT_END
};

__dead void
usage(void)
{
	//char *argv[] = { (char *)progname, "-ho", NULL };

	fprintf(stderr,
	    "usage: %s [options] node\n"
	    "\n"
	    "Slash options:\n"
	    "    -S ctlsock             specify alternate control socket\n\n",
	    progname);
	//fuse_main(2, argv, &slashops, NULL);
	exit(1);
}

int
proc_opt(__unusedx void *data, const char *arg, int c,
    __unusedx struct fuse_args *outargs)
{
	switch (c) {
	case FUSE_OPT_KEY_OPT:
	case FUSE_OPT_KEY_NONOPT:
		return (1);
	case MS_OPT_CTLSOCK:
		strlcpy(ctlsockfn, arg + 2, sizeof(ctlsockfn));
		break;
	default:
		usage();
	}
	return (0);
}


static int 
msl_fuse_lowlevel_mount(const char *mp)
{
	struct fuse_session *se;
	struct fuse_chan *ch;
        struct fuse_args args = FUSE_ARGS_INIT(0, NULL);
	char *fuse_opts;
	int fd;

	slash2fuse_listener_init();

        if(asprintf(&fuse_opts, FUSE_OPTIONS, mp) == -1) {
                return ENOMEM;
        }

        if(fuse_opt_add_arg(&args, "") == -1 ||
           fuse_opt_add_arg(&args, "-o") == -1 ||
           fuse_opt_add_arg(&args, fuse_opts) == -1) {
                fuse_opt_free_args(&args);
                free(fuse_opts);
                return ENOMEM;
        }
        free(fuse_opts);

        fd = fuse_mount(mp, &args);
	if (fd < 0)
		return ENOMEM;

        se = fuse_lowlevel_new(&args, &zfs_operations, sizeof(zfs_operations),
			       NULL);
        fuse_opt_free_args(&args);

        if(se == NULL) {
                close(fd);
                fuse_unmount(mp);
                return EIO;
        }

        ch = fuse_kern_chan_new(fd);
        if(ch == NULL) {
                fuse_session_destroy(se);
                close(fd);
                fuse_unmount(mp);
                return EIO;
        }

        fuse_session_add_chan(se, ch);

	if(slash2fuse_newfs(mp, ch) != 0) {
                fuse_session_destroy(se);
                close(fd);
                fuse_unmount(mp);
                return EIO;
        }

	return (0);
}

int
main(__unusedx int argc, char *argv[])
{
	int rc;
	char c, *mp="/slashfs_client";

	progname = argv[0];

	while ((c = getopt(argc, argv, "m:")) != -1)
                switch (c) {
                case 'm':
                        mp = optarg;
                        break;
                default:
                        usage();
                }


	pfl_init();

	pscthr_init(MSTHRT_FUSE, 0, NULL, NULL, 0, "msfusethr");

	slash_init(NULL);

	if (msl_fuse_lowlevel_mount(mp))
		return (-1);

#if 0	
	if(set_signal_handler(SIGHUP, exit_handler) != 0 ||
           set_signal_handler(SIGINT, exit_handler) != 0 ||
           set_signal_handler(SIGTERM, exit_handler) != 0 ||
           set_signal_handler(SIGPIPE, SIG_IGN) != 0) {
                return 2;
        }
#endif	
	rc = slash2fuse_listener_start();

	return (0);
}
