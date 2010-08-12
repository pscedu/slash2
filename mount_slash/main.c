/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
 *
 * Permission to use, copy, and modify this software and its documentation
 * without fee for personal use or non-commercial use within your organization
 * is hereby granted, provided that the above copyright notice is preserved in
 * all copies and that the copyright and this permission notice appear in
 * supporting documentation.  Permission to redistribute this software to other
 * organizations or individuals is not permitted without the written permission
 * of the Pittsburgh Supercomputing Center.  PSC makes no representations about
 * the suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

#include <sys/param.h>
#include <sys/stat.h>
#include <sys/time.h>

#include <dirent.h>
#include <err.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <gcrypt.h>

#include "pfl/cdefs.h"
#include "pfl/pfl.h"
#include "pfl/stat.h"
#include "pfl/str.h"
#include "psc_ds/vbitmap.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_util/ctlsvr.h"
#include "psc_util/eqpollthr.h"
#include "psc_util/log.h"
#include "psc_util/pool.h"
#include "psc_util/thread.h"
#include "psc_util/time.h"
#include "psc_util/usklndthr.h"

#include "bmap_cli.h"
#include "buffer.h"
#include "ctl_cli.h"
#include "fidc_cli.h"
#include "fidcache.h"
#include "fuse_listener.h"
#include "mount_slash.h"
#include "pathnames.h"
#include "rpc_cli.h"
#include "slashrpc.h"
#include "slerr.h"
#include "slutil.h"

#define ffi_setmfh(fi, mfh)	((fi)->fh = (uint64_t)(unsigned long)(mfh))
#define ffi_getmfh(fi)		((void *)(unsigned long)(fi)->fh)
#define mfh_getfid(mfh)		fcmh_2_fid((mfh)->mfh_fcmh)
#define mfh_getfg(mfh)		(mfh)->mfh_fcmh->fcmh_fg

sl_ios_id_t			 prefIOS = IOS_ID_ANY;
const char			*progname;
char				 ctlsockfn[PATH_MAX] = SL_PATH_MSCTLSOCK;
char				 mountpoint[PATH_MAX];
int				 allow_root_uid = 1;

struct sl_resm			*slc_rmc_resm;

struct psc_vbitmap		 msfsthr_uniqidmap = VBITMAP_INIT_AUTO;
psc_spinlock_t			 msfsthr_uniqidmap_lock = LOCK_INITIALIZER;

struct slash_creds		 rootcreds = { 0, 0 };

/* number of attribute prefetch in readdir() */
int				nstbpref = DEF_READDIR_NENTS;

GCRY_THREAD_OPTION_PTHREAD_IMPL;

static int msl_lookup_fidcache(const struct slash_creds *, fuse_ino_t,
    const char *, struct slash_fidgen *, struct srt_stat *);

/**
 * translate_pathname - convert an absolute file system path name into
 *	the relative location from the root of the mount point.
 * @fn: absolute file path.
 * @buf: value-result of the translated pathname.
 * Returns Boolean true on success or errno code on failure.
 *
 * XXX this should be rewritten to solely use msl_lookup_fidcache().
 */
int
translate_pathname(const char *fn, char buf[PATH_MAX])
{
	size_t len;

	if (fn[0] != '/')
		return (EINVAL);	/* require absolute paths */
	if (realpath(fn, buf) == NULL)
		return (errno);
	len = strlen(mountpoint);
	if (strncmp(buf, mountpoint, len))
		return (EINVAL);	/* outside residual slashfs root */
	if (buf[len] != '/' && buf[len] != '\0')
		return (EINVAL);
	memmove(buf, buf + len, strlen(buf) - len);
	buf[strlen(buf) - len] = '\0';
	return (0);
}

__static void
slash2fuse_getcred(fuse_req_t req, struct slash_creds *cred)
{
	const struct fuse_ctx *ctx = fuse_req_ctx(req);

	cred->uid = ctx->uid;
	cred->gid = ctx->gid;
}

static mode_t
slash2fuse_getumask(__unusedx fuse_req_t req)
{
	mode_t mode;
#if FUSE_VERSION > FUSE_MAKE_VERSION(2,7)
	const struct fuse_ctx *ctx = fuse_req_ctx(req);

	mode = ctx->umask;
#else
	mode = 0644;
#endif
	return (mode);
}

/**
 * lookup_pathname_fg - Get the fid+gen pair for a full pathname.  This
 *	routine is only really invoked via the control subsystem as the
 *	usual determinent for discovering this information is done by
 *	each file name component via LOOKUP.
 * @ofn: file path to lookup.
 * @crp: credentials of lookup.
 * @fgp: value-result fid+gen pair.
 * @sstb: optional value-result srt_stat buffer for file.
 */
int
lookup_pathname_fg(const char *ofn, struct slash_creds *crp,
    struct slash_fidgen *fgp, struct srt_stat *sstb)
{
	char *cpn, *next, fn[PATH_MAX];
	int rc;

	rc = translate_pathname(ofn, fn);
	if (rc)
		return (rc);

	fgp->fg_fid = SLFID_ROOT;
	for (cpn = fn + 1; cpn; cpn = next) {
		if ((next = strchr(cpn, '/')) != NULL)
			*next++ = '\0';
		rc = msl_lookup_fidcache(crp, fgp->fg_fid,
		    cpn, fgp, next ? NULL : sstb);
		if (rc)
			return (rc);
	}
	return (0);
}

__static void
msfsthr_teardown(void *arg)
{
	struct msfs_thread *mft = arg;

	spinlock(&msfsthr_uniqidmap_lock);
	psc_vbitmap_unset(&msfsthr_uniqidmap, mft->mft_uniqid);
	psc_vbitmap_setnextpos(&msfsthr_uniqidmap, 0);
	freelock(&msfsthr_uniqidmap_lock);
}

__static void
msfsthr_ensure(void)
{
	struct msfs_thread *mft;
	struct psc_thread *thr;
	size_t id;

	thr = pscthr_get_canfail();
	if (thr == NULL) {
		spinlock(&msfsthr_uniqidmap_lock);
		if (psc_vbitmap_next(&msfsthr_uniqidmap, &id) != 1)
			psc_fatal("psc_vbitmap_next");
		freelock(&msfsthr_uniqidmap_lock);

		thr = pscthr_init(MSTHRT_FS, PTF_FREE, NULL,
		    msfsthr_teardown, sizeof(*mft), "msfsthr%02zu", id);
		mft = thr->pscthr_private;
		mft->mft_uniqid = id;
		pscthr_setready(thr);
	}
	psc_assert(thr->pscthr_type == MSTHRT_FS);
}

__static void
slash2fuse_fill_entry(struct fuse_entry_param *e,
      const struct slash_fidgen *fgp, const struct stat *stb)
{
	e->entry_timeout = MSL_FUSE_ENTRY_TIMEO;
	e->ino = fgp->fg_fid;
	if (e->ino) {
		e->attr_timeout = MSL_FUSE_ATTR_TIMEO;
		memcpy(&e->attr, stb, sizeof(e->attr));
		e->generation = fgp->fg_gen;
	}

	psc_trace("inode:%lu generation:%lu", e->ino, e->generation);
	DEBUG_STATBUF(PLL_DEBUG, &e->attr, "");
}

__static void
slash2fuse_reply_create(fuse_req_t req, const struct slash_fidgen *fgp,
    const struct srt_stat *sstb, const struct fuse_file_info *fi)
{
	struct fuse_entry_param e;
	struct stat stb;

	sl_internalize_stat(sstb, &stb);
	slash2fuse_fill_entry(&e, fgp, &stb);
	fuse_reply_create(req, &e, fi);
}

__static void
slash2fuse_reply_entry(fuse_req_t req, const struct slash_fidgen *fgp,
    const struct srt_stat *sstb)
{
	struct fuse_entry_param e;
	struct stat stb;

	sl_internalize_stat(sstb, &stb);
	slash2fuse_fill_entry(&e, fgp, &stb);
	fuse_reply_entry(req, &e);
}

#define slc_fcmh_get(fgp, sstb, safl, fcmhp)				\
	_slc_fcmh_get((fgp), (sstb), (safl), (fcmhp),			\
	    __FILE__, __func__, __LINE__)

/**
 * slc_fcmh_get - Create/update a FID cache member handle
 *	based on the statbuf provided.
 * @fgp: file's fid+gen pair.
 * @sstb: file stat info.
 * @setattrflags: flags to fcmh_setattr().
 * @name: base name of file.
 * @lookupflags: fid cache lookup flags.
 * @fchmp: value-result fcmh.
 */
__static int
_slc_fcmh_get(const struct slash_fidgen *fgp, const struct srt_stat *sstb,
      int setattrflags, struct fidc_membh **fcmhp,
      const char *file, const char *func, int line)
{
	return (_fidc_lookup(fgp, FIDC_LOOKUP_CREATE, sstb,
	    setattrflags, fcmhp, file, func, line));
}

__static void
slash2fuse_access(fuse_req_t req, fuse_ino_t ino, int mask)
{
	struct slash_creds creds;
	struct fidc_membh *c;
	int rc;

	msfsthr_ensure();

	slash2fuse_getcred(req, &creds);
	rc = fidc_lookup_load_inode(ino, &c);
	if (rc)
		goto out;

	rc = checkcreds(&c->fcmh_sstb, &creds, mask);
 out:
	fuse_reply_err(req, rc);
	if (c)
		fcmh_op_done_type(c, FCMH_OPCNT_LOOKUP_FIDC);
}

__static void
slash2fuse_create(fuse_req_t req, fuse_ino_t parent, const char *name,
	  mode_t mode, struct fuse_file_info *fi)
{
	struct fidc_membh *m = NULL;
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct bmap_cli_info *msbd;
	struct fcmh_cli_info *fci;
	struct srm_create_req *mq;
	struct srm_create_rep *mp;
	struct bmapc_memb *bcm;
	struct msl_fhent *mfh;
	int rc = 0;

	msfsthr_ensure();

	psc_assert(fi->flags & O_CREAT);

	if (strlen(name) > NAME_MAX) {
		rc = ENAMETOOLONG;
		goto out;
	}

	/*
	 * Now we've established a local placeholder for this create.
	 *  any other creates to this pathame will block in
	 *  fidc_child_wait_locked() until we release the fcmh.
	 */
	rc = slc_rmc_getimp(&csvc);
	if (rc)
		goto out;
	rc = SL_RSX_NEWREQ(csvc->csvc_import, SRMC_VERSION,
	    SRMT_CREATE, rq, mq, mp);
	if (rc)
		goto out;

	mq->mode = (!(mode & 0777)) ? (0666 & ~(slash2fuse_getumask(req))) : mode;
	mq->pfg.fg_fid = parent;
	mq->pfg.fg_gen = 0;
	mq->prefios = prefIOS;
	slash2fuse_getcred(req, &mq->creds);
	strlcpy(mq->name, name, sizeof(mq->name));

	rc = SL_RSX_WAITREP(rq, mp);
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		goto out;

	rc = slc_fcmh_get(&mp->fg, &mp->attr, FCMH_SETATTRF_NONE, &m);
	if (rc)
		goto out;

	mfh = msl_fhent_new(m);
	ffi_setmfh(fi, mfh);
	fi->keep_cache = 0;
	fi->direct_io = 1;

	if (fi->flags & O_APPEND) {
		FCMH_LOCK(m);
		m->fcmh_state |= FCMH_CLI_APPENDWR;
		FCMH_ULOCK(m);
	}
	if (fi->flags & O_SYNC) {
		/* XXX write me */
	}

	FCMH_LOCK(m);
	fci = fcmh_2_fci(m);

	fci->fci_reptbl[0].bs_id = mp->sbd.sbd_ios_id;
	fci->fci_nrepls = 1;
	m->fcmh_state |= FCMH_CLI_HAVEREPLTBL;
	FCMH_ULOCK(m);

	rc = bmap_getf(m, 0, SL_WRITE, BMAPGETF_LOAD | BMAPGETF_NORETRIEVE,
	       &bcm);
	if (rc)
		goto out;

	msl_bmap_reap_init(bcm, &mp->sbd);

	msbd = bcm->bcm_pri;
	SL_REPL_SET_BMAP_IOS_STAT(msbd->msbd_msbcr.msbcr_repls,
	    0, BREPLST_VALID);

	bmap_op_done_type(bcm, BMAP_OPCNT_LOOKUP);
 out:
	if (m)
		fcmh_op_done_type(m, FCMH_OPCNT_LOOKUP_FIDC);

	if (rc)
		fuse_reply_err(req, rc);
	else
		slash2fuse_reply_create(req, &mp->fg, &mp->attr, fi);

	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

__static void
slash2fuse_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
	struct slash_creds creds;
	struct msl_fhent *mfh;
	struct fidc_membh *c;
	int rc=0;

	msfsthr_ensure();

	slash2fuse_getcred(req, &creds);

	psc_trace("inum %lu dir=%s", ino, (fi->flags & O_DIRECTORY) ?
		  "yes" : "no");

	rc = fidc_lookup_load_inode(ino, &c);
	if (rc)
		goto out;

	if ((fi->flags & O_ACCMODE) != O_WRONLY) {
		rc = checkcreds(&c->fcmh_sstb, &creds, R_OK);
		if (rc)
			goto out;
	}
	if (fi->flags & (O_WRONLY | O_RDWR)) {
		rc = checkcreds(&c->fcmh_sstb, &creds, W_OK);
		if (rc)
			goto out;
	}

	/* Directory sanity. */
	if (fcmh_isdir(c)) {
		/* fuse shouldn't ever pass us WR with a dir */
		psc_assert((fi->flags & (O_WRONLY | O_RDWR)) == 0);
		if (!(fi->flags & O_DIRECTORY)) {
			rc = EISDIR;
			goto out;
		}
		/* sfop_ctor() can be called prior to having attrs.  This
		 *   means that dir fcmh's can't be initialized fully until
		 *   here.
		 */
		if (!DIRCACHE_INITIALIZED(c)) {
			FCMH_LOCK(c);
			DIRCACHE_INIT(c, &dircacheMgr);
			FCMH_ULOCK(c);
		}
	} else {
		if (fi->flags & O_DIRECTORY) {
			rc = ENOTDIR;
			goto out;
		}
	}

	mfh = msl_fhent_new(c);

	DEBUG_FCMH(PLL_DEBUG, c, "new mfh=%p dir=%s", mfh,
		   (fi->flags & O_DIRECTORY) ? "yes" : "no");

	ffi_setmfh(fi, mfh);

	fi->keep_cache = (fi->flags & O_DIRECTORY) ? 1 : 0;
	/*
	 * FUSE direct_io does not work with mmap(), which is what the
	 * kernel uses under the hood when running executables, so
	 * disable it for this case.
	 */
	if ((c->fcmh_sstb.sst_mode &
	    (S_IXUSR | S_IXGRP | S_IXOTH)) == 0)
		fi->direct_io = 1;

	fuse_reply_open(req, fi);

	if (fi->flags & O_APPEND) {
		FCMH_LOCK(c);
		c->fcmh_state |= FCMH_CLI_APPENDWR;
		FCMH_ULOCK(c);
	}
	if (fi->flags & O_TRUNC) {
		/* XXX write me */
	}
	if (fi->flags & O_SYNC) {
		/* XXX write me */
	}

 out:
	if (c)
		fcmh_op_done_type(c, FCMH_OPCNT_LOOKUP_FIDC);

	if (rc)
		fuse_reply_err(req, rc);
}

__static void
slash2fuse_opendir(fuse_req_t req, fuse_ino_t ino,
    struct fuse_file_info *fi)
{
	fi->flags |= O_DIRECTORY;
	slash2fuse_open(req, ino, fi);
}

int
slash2fuse_stat(struct fidc_membh *fcmh, const struct slash_creds *creds)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_getattr_req *mq;
	struct srm_getattr_rep *mp;
	struct timeval now;
	int rc = 0;

 restart:
	FCMH_LOCK(fcmh);
	if (fcmh->fcmh_state & FCMH_HAVE_ATTRS) {
		PFL_GETTIMEVAL(&now);
		if (timercmp(&now, &fcmh_2_fci(fcmh)->fci_age, <)) {
			DEBUG_FCMH(PLL_DEBUG, fcmh,
			    "attrs retrieved from local cache");
			goto check;
		}
		fcmh->fcmh_state &= ~FCMH_HAVE_ATTRS;
	}

	/* If someone is already fetching attributes, wait for it to
	 *   complete
	 */
	if (fcmh->fcmh_state & FCMH_GETTING_ATTRS) {
		psc_waitq_wait(&fcmh->fcmh_waitq, &fcmh->fcmh_lock);
		goto restart;
	}
	fcmh->fcmh_state |= FCMH_GETTING_ATTRS;
	FCMH_ULOCK(fcmh);

	rc = slc_rmc_getimp(&csvc);
	if (rc)
		goto out;
	rc = SL_RSX_NEWREQ(csvc->csvc_import, SRMC_VERSION,
	    SRMT_GETATTR, rq, mq, mp);
	if (rc)
		goto out;

	mq->fg = fcmh->fcmh_fg;

	rc = SL_RSX_WAITREP(rq, mp);
	if (rc == 0)
		rc = mp->rc;
 out:
	FCMH_LOCK(fcmh);
	if (!rc)
		fcmh_setattr(fcmh, &mp->attr,
		    FCMH_SETATTRF_SAVELOCAL | FCMH_SETATTRF_HAVELOCK);

	fcmh->fcmh_state &= ~FCMH_GETTING_ATTRS;
	psc_waitq_wakeall(&fcmh->fcmh_waitq);

	if (rq)
		pscrpc_req_finished(rq);

	DEBUG_FCMH(PLL_DEBUG, fcmh, "attrs retrieved via rpc rc=%d", rc);

 check:
	FCMH_ULOCK(fcmh);
	if (!rc) {
		rc = checkcreds(&fcmh->fcmh_sstb, creds, R_OK);
		if (rc)
			psc_info("fcmh=%p, mode=%x, checkcreds rc=%d",
			    fcmh, fcmh->fcmh_sstb.sst_mode, rc);
	}
	if (csvc)
		sl_csvc_decref(csvc);
	return (rc);
}

__static void
slash2fuse_getattr(fuse_req_t req, fuse_ino_t ino,
    __unusedx struct fuse_file_info *fi)
{
	struct slash_creds creds;
	struct fidc_membh *f;
	struct stat stb;
	int rc;

	msfsthr_ensure();

	slash2fuse_getcred(req, &creds);
	/* Lookup and possibly create a new fidcache handle for ino.
	 *  If the fid does not exist in the cache then a placeholder will
	 *  be allocated.  slash2fuse_stat() will detect incomplete attrs via
	 *  FCMH_GETTING_ATTRS flag and RPC for them.
	 */
	rc = fidc_lookup_load_inode(ino, &f);
	if (rc)
		goto out;

	rc = slash2fuse_stat(f, &creds);
	if (rc)
		goto out;

	if (!fcmh_isdir(f))
		f->fcmh_sstb.sst_blksize = 32768;

	sl_internalize_stat(&f->fcmh_sstb, &stb);
	DEBUG_STATBUF(PLL_DEBUG, &stb, "getattr");
	fuse_reply_attr(req, &stb, MSL_FUSE_ATTR_TIMEO);

 out:
	if (f)
		fcmh_op_done_type(f, FCMH_OPCNT_LOOKUP_FIDC);
	if (rc)
		fuse_reply_err(req, rc);
}

__static void
slash2fuse_link(fuse_req_t req, fuse_ino_t c_inum,
    fuse_ino_t p_inum, const char *newname)
{
	struct fidc_membh *p = NULL, *c = NULL;
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct slash_creds creds;
	struct srm_link_req *mq;
	struct srm_link_rep *mp;
	int rc=0;

	msfsthr_ensure();

	rc = ENOTSUP;
	goto out;

	if (strlen(newname) > NAME_MAX) {
		rc = ENAMETOOLONG;
		goto out;
	}

	slash2fuse_getcred(req, &creds);

	/* Check the newparent inode. */
	rc = fidc_lookup_load_inode(p_inum, &p);
	if (rc)
		goto out;

	if (!fcmh_isdir(p)) {
		rc = ENOTDIR;
		goto out;
	}

	rc = checkcreds(&p->fcmh_sstb, &creds, W_OK);
	if (rc)
		goto out;

	/* Check the child inode. */
	rc = fidc_lookup_load_inode(c_inum, &c);
	if (rc)
		goto out;

	if (fcmh_isdir(c)) {
		rc = EISDIR;
		goto out;
	}

	/* Create, initialize, and send RPC. */
	rc = slc_rmc_getimp(&csvc);
	if (rc)
		goto out;
	rc = SL_RSX_NEWREQ(csvc->csvc_import, SRMC_VERSION,
	    SRMT_LINK, rq, mq, mp);
	if (rc)
		goto out;

	mq->creds = creds;
	mq->pfg = p->fcmh_fg;
	mq->fg = c->fcmh_fg;
	strlcpy(mq->name, newname, sizeof(mq->name));

	rc = SL_RSX_WAITREP(rq, mp);
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		goto out;
	slash2fuse_reply_entry(req, &mp->fg, &mp->attr);

	fcmh_setattr(c, &mp->attr, FCMH_SETATTRF_NONE);

 out:
	if (rc)
		fuse_reply_err(req, rc);
	if (c)
		fcmh_op_done_type(c, FCMH_OPCNT_LOOKUP_FIDC);
	if (p)
		fcmh_op_done_type(p, FCMH_OPCNT_LOOKUP_FIDC);

	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

__static void
slash2fuse_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name,
	 mode_t mode)
{
	struct fidc_membh *m = NULL;
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_mkdir_req *mq;
	struct srm_mkdir_rep *mp;
	struct slash_creds creds;
	int rc;

	msfsthr_ensure();

	if (strlen(name) > NAME_MAX) {
		rc = ENAMETOOLONG;
		goto out;
	}

	slash2fuse_getcred(req, &creds);

	rc = slc_rmc_getimp(&csvc);
	if (rc)
		goto out;
	rc = SL_RSX_NEWREQ(csvc->csvc_import, SRMC_VERSION,
		   SRMT_MKDIR, rq, mq, mp);
	if (rc)
		goto out;
	mq->creds = creds;
	mq->pfg.fg_fid = parent;
	mq->pfg.fg_gen = 0;
	mq->mode = mode;
	strlcpy(mq->name, name, sizeof(mq->name));

	rc = SL_RSX_WAITREP(rq, mp);
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		goto out;

	psc_info("pfid=%"PRIx64" mode=0%o name='%s' rc=%d mp->rc=%d",
		 mq->pfg.fg_fid, mq->mode, mq->name, rc, mp->rc);

	rc = slc_fcmh_get(&mp->fg, &mp->attr, FCMH_SETATTRF_NONE, &m);
	if (rc)
		goto out;
	slash2fuse_reply_entry(req, &mp->fg, &mp->attr);

 out:
	if (rc)
		fuse_reply_err(req, rc);
	if (m)
		fcmh_op_done_type(m, FCMH_OPCNT_LOOKUP_FIDC);
	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

__static int
slash2fuse_unlink(fuse_req_t req, fuse_ino_t parent, const char *name,
	  int isfile)
{
	struct slashrpc_cservice *csvc = NULL;
	struct fidc_membh *p;
	struct pscrpc_request *rq = NULL;
	struct srm_unlink_req *mq;
	struct srm_unlink_rep *mp;
	struct slash_creds cr;
	int rc;

	msfsthr_ensure();

	if (strlen(name) > NAME_MAX) {
		rc = ENAMETOOLONG;
		goto out;
	}

	rc = slc_rmc_getimp(&csvc);
	if (rc)
		goto out;

	rc = SL_RSX_NEWREQ(csvc->csvc_import, SRMC_VERSION,
	    isfile ? SRMT_UNLINK : SRMT_RMDIR, rq, mq, mp);
	if (rc)
		goto out;

	slash2fuse_getcred(req, &cr);

	mq->pfg.fg_fid = parent;
	mq->pfg.fg_gen = 0;

	strlcpy(mq->name, name, sizeof(mq->name));

	p = fidc_lookup_fid(parent);
	if (p) {
		if (!DIRCACHE_INITIALIZED(p)) {
			FCMH_LOCK(p);
			DIRCACHE_INIT(p, &dircacheMgr);
			FCMH_ULOCK(p);
		} else
			dircache_lookup(&fcmh_2_fci(p)->fci_dci,
				 name, DC_STALE);

		fcmh_op_done_type(p, FCMH_OPCNT_LOOKUP_FIDC);
	}

	rc = SL_RSX_WAITREP(rq, mp);
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		goto out;
 out:
	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
	return (rc);
}

__static void
slash2fuse_rmdir_helper(fuse_req_t req, fuse_ino_t parent, const char *name)
{
	int error = slash2fuse_unlink(req, parent, name, 0);

	/* rmdir events always reply_err
	 */
	fuse_reply_err(req, error);
}

__static void
slash2fuse_mknod_helper(fuse_req_t req, __unusedx fuse_ino_t parent,
    __unusedx const char *name, __unusedx mode_t mode, __unusedx dev_t rdev)
{
	msfsthr_ensure();

	fuse_reply_err(req, ENOTSUP);
}

__static int
slash2fuse_readdir(fuse_req_t req, __unusedx fuse_ino_t ino, size_t size,
	   off_t off, struct fuse_file_info *fi)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct pscrpc_bulk_desc *desc;
	struct fidc_membh *d = NULL;
	struct srm_readdir_req *mq;
	struct srm_readdir_rep *mp;
	struct dircache_ents *e;
	struct msl_fhent *mfh;
	struct iovec iov[2];
	int rc, niov = 0;

	iov[0].iov_base = NULL;
	iov[1].iov_base = NULL;

	msfsthr_ensure();

	/* Don't allow writes on directory inodes.
	 */
	if (fi->flags & (O_WRONLY | O_RDWR))
		return (EINVAL);

	mfh = ffi_getmfh(fi);
	d = mfh->mfh_fcmh;
	psc_assert(d);

	/* Ensure that the fcmh is still valid, we can't rely
	 *  only on the inode number, the generation # number
	 *  must be taken into account.
	 * NOTE: 'd' must be decref'd.
	 */
	if (fidc_lookup_fg(&d->fcmh_fg) != d)
		return (EBADF);

	if (!fcmh_isdir(d)) {
		rc = ENOTDIR;
		goto out;
	}

	rc = slc_rmc_getimp(&csvc);
	if (rc)
		goto out;
	rc = SL_RSX_NEWREQ(csvc->csvc_import, SRMC_VERSION,
	    SRMT_READDIR, rq, mq, mp);
	if (rc)
		goto out;

	mq->fg = d->fcmh_fg;
	mq->size = size;
	mq->offset = off;

	if (!DIRCACHE_INITIALIZED(d)) {
		FCMH_LOCK(d);
		DIRCACHE_INIT(d, &dircacheMgr);
		FCMH_ULOCK(d);
	}
	e = dircache_new_ents(&fcmh_2_fci(d)->fci_dci, size);

	iov[niov].iov_base = e->de_base;
	iov[niov].iov_len = size;
	niov++;

	mq->nstbpref = MIN(nstbpref, (int)howmany(LNET_MTU - size,
	    sizeof(struct srm_getattr_rep)));
	if (mq->nstbpref) {
		iov[niov].iov_len = mq->nstbpref *
			sizeof(struct srm_getattr_rep);
		iov[niov].iov_base = PSCALLOC(iov[1].iov_len);
		niov++;
	}

	rsx_bulkclient(rq, &desc, BULK_PUT_SINK, SRMC_BULK_PORTAL, iov, niov);
	rc = SL_RSX_WAITREP(rq, mp);
	if (!rc)
		rc = mp->rc;
	else {
		dircache_earlyrls_ents(e);
		goto out;
	}

	if (mq->nstbpref) {
		uint32_t i;
		struct slash_fidgen fg;
		struct fidc_membh *fcmh;
		struct srm_getattr_rep *attr = iov[1].iov_base;

		if (mp->num < mq->nstbpref)
			mq->nstbpref = mp->num;

		for (i = 0; i < mq->nstbpref; i++, attr++) {
			if (attr->rc || !attr->attr.sst_ino)
				continue;

			fg.fg_fid = attr->attr.sst_ino;
			fg.fg_gen = attr->attr.sst_gen;

			psc_info("adding i+g:%"PRId64"+%"PRId64" rc=%d",
			    fg.fg_fid, fg.fg_gen, attr->rc);

			attr->rc = fidc_lookup(&fg, FIDC_LOOKUP_CREATE,
			    &attr->attr, FCMH_SETATTRF_SAVELOCAL, &fcmh);

			if (fcmh)
				fcmh_op_done_type(fcmh,
				    FCMH_OPCNT_LOOKUP_FIDC);
		}
	}
	/* Establish these dirents in our cache.  Do this before replying
	 *   to fuse in order to prevent unnecessary lookup rpc's.
	 */
	if (mp->num)
		dircache_reg_ents(e, mp->num);
	/* Tell fuse of the good news!
	 */
	fuse_reply_buf(req, iov[0].iov_base, (size_t)mp->size);
	/* At this point the dirent cache is technically freeable.
	 */
	if (mp->num)
		dircache_setfreeable_ents(e);
	else
		dircache_earlyrls_ents(e);
 out:
	if (d)
		fcmh_op_done_type(d, FCMH_OPCNT_LOOKUP_FIDC);
	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);

	if (niov == 2)
		PSCFREE(iov[1].iov_base);
	return (rc);
}

__static void
slash2fuse_readdir_helper(fuse_req_t req, fuse_ino_t ino, size_t size,
	  off_t off, struct fuse_file_info *fi)
{
	int error = slash2fuse_readdir(req, ino, size, off, fi);

	if (error)
		fuse_reply_err(req, error);
}

__static int
slash_lookuprpc(const struct slash_creds *crp, fuse_ino_t parent,
	const char *name, struct slash_fidgen *fgp, struct srt_stat *sstb)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_lookup_req *mq;
	struct srm_lookup_rep *mp;
	struct fidc_membh *m = NULL;
	int rc;

	if (strlen(name) > NAME_MAX)
		return (ENAMETOOLONG);

	rc = slc_rmc_getimp(&csvc);
	if (rc)
		goto out;
	rc = SL_RSX_NEWREQ(csvc->csvc_import, SRMC_VERSION,
	    SRMT_LOOKUP, rq, mq, mp);
	if (rc)
		goto out;

	mq->pfg.fg_fid = parent;
	mq->pfg.fg_gen = 0;

	strlcpy(mq->name, name, sizeof(mq->name));

	rc = SL_RSX_WAITREP(rq, mp);
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		goto out;

	/*
	 * Add the inode to the cache first, otherwise fuse may
	 *  come to us with another request for the inode since it won't
	 *  yet be visible in the cache.
	 */
	rc = slc_fcmh_get(&mp->fg, &mp->attr, FCMH_SETATTRF_SAVELOCAL, &m);
	if (rc)
		goto out;

	rc = checkcreds(&mp->attr, crp, R_OK);
	if (rc)
		goto out;

	if (sstb)
		*sstb = mp->attr;

	*fgp = mp->fg;

 out:
	if (m)
		fcmh_op_done_type(m, FCMH_OPCNT_LOOKUP_FIDC);
	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
	return (rc);
}

static int
msl_lookup_fidcache(const struct slash_creds *cr, fuse_ino_t parent,
    const char *name, struct slash_fidgen *fgp, struct srt_stat *sstb)
{
	struct fidc_membh *p, *c;
	slfid_t child;
	int rc;

	psc_info("looking for file: %s under inode: %lu",
	    name, parent);

	p = fidc_lookup_fid(parent);
	if (!p)
		goto out;

	if (!DIRCACHE_INITIALIZED(p)) {
		FCMH_LOCK(p);
		DIRCACHE_INIT(p, &dircacheMgr);
		FCMH_ULOCK(p);
	}

	child = dircache_lookup(&fcmh_2_fci(p)->fci_dci, name, DC_LOOKUP);
	/* It's ok to unref the parent now.
	 */
	fcmh_op_done_type(p, FCMH_OPCNT_LOOKUP_FIDC);

	if (child == FID_ANY)
		goto out;

	c = fidc_lookup_fid(child);
	if (!c)
		goto out;

	/*
	 * We should do a lookup based on name here because a rename
	 * does not change the file ID and we would get a success
	 * in a stat RPC.  Note the app is looking based on a name
	 * here, not based on ID.
	 */
	rc = slash2fuse_stat(c, cr);
	if (!rc) {
		*fgp = c->fcmh_fg;
		if (sstb)
			*sstb = c->fcmh_sstb;
	}
	fcmh_op_done_type(c, FCMH_OPCNT_LOOKUP_FIDC);
	return (rc);
 out:
	return (slash_lookuprpc(cr, parent, name, fgp, sstb));
}

__static void
slash2fuse_lookup_helper(fuse_req_t req, fuse_ino_t parent, const char *name)
{
	struct slash_fidgen fg;
	struct slash_creds cr;
	struct srt_stat sstb;
	int rc;

	msfsthr_ensure();

	slash2fuse_getcred(req, &cr);
	rc = msl_lookup_fidcache(&cr, parent, name, &fg, &sstb);
	if (rc && rc != ENOENT)
		fuse_reply_err(req, rc);
	else {
		if (rc == ENOENT)
			fg.fg_fid = 0;

		slash2fuse_reply_entry(req, &fg, &sstb);
	}
}

__static void
slash2fuse_readlink(fuse_req_t req, fuse_ino_t ino)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct pscrpc_bulk_desc *desc;
	struct srm_readlink_req *mq;
	struct srm_readlink_rep *mp;
	struct fidc_membh *c = NULL;
	struct slash_creds creds;
	struct iovec iov;
	char buf[PATH_MAX];
	int rc;

	msfsthr_ensure();

	rc = slc_rmc_getimp(&csvc);
	if (rc)
		goto out;
	rc = SL_RSX_NEWREQ(csvc->csvc_import, SRMC_VERSION,
	    SRMT_READLINK, rq, mq, mp);
	if (rc)
		goto out;

	slash2fuse_getcred(req, &creds);

	rc = fidc_lookup_load_inode(ino, &c);
	if (rc)
		goto out;

	rc = checkcreds(&c->fcmh_sstb, &creds, R_OK);
	if (rc)
		goto out;

	mq->fg = c->fcmh_fg;

	iov.iov_base = buf;
	iov.iov_len = sizeof(buf);
	rsx_bulkclient(rq, &desc, BULK_PUT_SINK,
	    SRMC_BULK_PORTAL, &iov, 1);

	rc = SL_RSX_WAITREP(rq, mp);
	if (rc == 0)
		rc = mp->rc;

 out:
	if (c)
		fcmh_op_done_type(c, FCMH_OPCNT_LOOKUP_FIDC);
	if (rc)
		fuse_reply_err(req, rc);
	else {
		buf[sizeof(buf) - 1] = '\0';
		fuse_reply_readlink(req, buf);
	}
	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

/* Note that this function is called once for each open */
__static void
slash2fuse_flush_int_locked(struct msl_fhent *mfh)
{
	struct bmpc_ioreq *r;

	PLL_FOREACH(r, &mfh->mfh_biorqs) {
		spinlock(&r->biorq_lock);
		r->biorq_flags |= BIORQ_FORCE_EXPIRE;
		DEBUG_BIORQ(PLL_INFO, r, "force expire");
		freelock(&r->biorq_lock);
	}

	while (!pll_empty(&mfh->mfh_biorqs)) {
		psc_waitq_wait(&msl_fhent_flush_waitq, &mfh->mfh_lock);
		spinlock(&mfh->mfh_lock);
	}
}

__static void
slash2fuse_flush(fuse_req_t req, __unusedx fuse_ino_t ino,
		 struct fuse_file_info *fi)
{
	struct msl_fhent *mfh;

	mfh = ffi_getmfh(fi);

	DEBUG_FCMH(PLL_INFO, mfh->mfh_fcmh, "flushing");

	spinlock(&mfh->mfh_lock);
	slash2fuse_flush_int_locked(mfh);
	freelock(&mfh->mfh_lock);

	fuse_reply_err(req, 0);
}

__static void
slash2fuse_release(fuse_req_t req, __unusedx fuse_ino_t ino,
    struct fuse_file_info *fi)
{
	struct msl_fhent *mfh;
	struct fidc_membh *c;

	msfsthr_ensure();

	mfh = ffi_getmfh(fi);
	c = mfh->mfh_fcmh;

	spinlock(&mfh->mfh_lock);
#if FHENT_EARLY_RELEASE
	PLL_FOREACH(r, &mfh->mfh_biorqs) {
		spinlock(&r->biorq_lock);
		r->biorq_flags |= BIORQ_NOFHENT;
		freelock(&r->biorq_lock);
	}
#else
	slash2fuse_flush_int_locked(mfh);
	psc_assert(pll_empty(&mfh->mfh_biorqs));
#endif
	freelock(&mfh->mfh_lock);

	fcmh_op_done_type(c, FCMH_OPCNT_OPEN);

	PSCFREE(mfh);
	fuse_reply_err(req, 0);
}

__static int
slash2fuse_rename(__unusedx fuse_req_t req, fuse_ino_t parent,
    const char *name, fuse_ino_t newparent, const char *newname)
{
	struct slashrpc_cservice *csvc = NULL;
	struct fidc_membh *p;
	struct pscrpc_request *rq = NULL;
	struct pscrpc_bulk_desc *desc;
	struct srm_generic_rep *mp;
	struct srm_rename_req *mq;
	struct iovec iov[2];
	int rc;

	msfsthr_ensure();

	if (strlen(name) > NAME_MAX ||
	    strlen(newname) > NAME_MAX)
		return (ENAMETOOLONG);

	rc = slc_rmc_getimp(&csvc);
	if (rc)
		goto out;
	rc = SL_RSX_NEWREQ(csvc->csvc_import, SRMC_VERSION,
	    SRMT_RENAME, rq, mq, mp);
	if (rc)
		goto out;

	mq->opfg.fg_fid = parent;
	mq->npfg.fg_fid = newparent;
	mq->opfg.fg_gen = mq->npfg.fg_gen = 0;
	mq->fromlen = strlen(name) + 1;
	mq->tolen = strlen(newname) + 1;

	iov[0].iov_base = (char *)name;
	iov[0].iov_len = mq->fromlen;
	iov[1].iov_base = (char *)newname;
	iov[1].iov_len = mq->tolen;

	rsx_bulkclient(rq, &desc, BULK_GET_SOURCE, SRMC_BULK_PORTAL, iov, 2);

	p = fidc_lookup_fid(parent);
	if (p) {
		if (!DIRCACHE_INITIALIZED(p)) {
			FCMH_LOCK(p);
			DIRCACHE_INIT(p, &dircacheMgr);
			FCMH_ULOCK(p);
		} else
			dircache_lookup(&fcmh_2_fci(p)->fci_dci,
				 name, DC_STALE);

		fcmh_op_done_type(p, FCMH_OPCNT_LOOKUP_FIDC);
	}

	rc = SL_RSX_WAITREP(rq, mp);
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		goto out;
 out:
	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
	return (rc);
}

__static void
slash2fuse_rename_helper(fuse_req_t req, fuse_ino_t parent,
    const char *name, fuse_ino_t newparent, const char *newname)
{
	int error = slash2fuse_rename(req, parent, name, newparent, newname);

	/* rename events always reply_err */
	fuse_reply_err(req, error);
}

__static void
slash2fuse_statfs(fuse_req_t req, __unusedx fuse_ino_t ino)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_statfs_req *mq;
	struct srm_statfs_rep *mp;
	struct statvfs sfb;
	int rc;

	msfsthr_ensure();

	rc = slc_rmc_getimp(&csvc);
	if (rc)
		goto out;
	rc = SL_RSX_NEWREQ(csvc->csvc_import, SRMC_VERSION,
	    SRMT_STATFS, rq, mq, mp);
	if (rc)
		goto out;
	rc = SL_RSX_WAITREP(rq, mp);
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		goto out;

	sl_internalize_statfs(&mp->ssfb, &sfb);
	fuse_reply_statfs(req, &sfb);

 out:
	if (rc)
		fuse_reply_err(req, rc);
	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

__static int
slash2fuse_symlink(fuse_req_t req, const char *buf, fuse_ino_t parent,
    const char *name)
{
	struct fidc_membh *m = NULL;
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct pscrpc_bulk_desc *desc;
	struct srm_symlink_req *mq;
	struct srm_symlink_rep *mp;
	struct slash_creds creds;
	struct iovec iov;
	int rc;

	msfsthr_ensure();

	if (strlen(buf) >= PATH_MAX ||
	    strlen(name) > NAME_MAX)
		return (ENAMETOOLONG);

	slash2fuse_getcred(req, &creds);

	rc = slc_rmc_getimp(&csvc);
	if (rc)
		goto out;

	rc = SL_RSX_NEWREQ(csvc->csvc_import, SRMC_VERSION,
	    SRMT_SYMLINK, rq, mq, mp);
	if (rc)
		goto out;

	mq->creds = creds;
	mq->pfg.fg_fid = parent;
	mq->pfg.fg_gen = 0;

	mq->linklen = strlen(buf) + 1;
	strlcpy(mq->name, name, sizeof(mq->name));

	iov.iov_base = (char *)buf;
	iov.iov_len = mq->linklen;

	rsx_bulkclient(rq, &desc, BULK_GET_SOURCE,
	    SRMC_BULK_PORTAL, &iov, 1);

	rc = SL_RSX_WAITREP(rq, mp);
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		goto out;

	rc = slc_fcmh_get(&mp->fg, &mp->attr, FCMH_SETATTRF_NONE, &m);
	if (rc)
		goto out;
	slash2fuse_reply_entry(req, &mp->fg, &mp->attr);

 out:
	if (m)
		fcmh_op_done_type(m, FCMH_OPCNT_LOOKUP_FIDC);
	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
	return (rc);
}

__static void
slash2fuse_symlink_helper(fuse_req_t req, const char *buf,
    fuse_ino_t parent, const char *name)
{
	int error = slash2fuse_symlink(req, buf, parent, name);

	if (error)
		fuse_reply_err(req, error);
}

__static void
slash2fuse_unlink_helper(fuse_req_t req, fuse_ino_t parent, const char *name)
{
	int error = slash2fuse_unlink(req, parent, name, 1);

	/* unlink events always reply_err */
	fuse_reply_err(req, error);
}

int
slash2fuse_translate_setattr_flags(int in)
{
	int out = 0;

	if (in & FUSE_SET_ATTR_MODE)
		out |= SETATTR_MASKF_MODE;
	if (in & FUSE_SET_ATTR_UID)
		out |= SETATTR_MASKF_UID;
	if (in & FUSE_SET_ATTR_GID)
		out |= SETATTR_MASKF_GID;
	if (in & FUSE_SET_ATTR_SIZE)
		out |= SETATTR_MASKF_SIZE;
	if (in & FUSE_SET_ATTR_ATIME)
		out |= SETATTR_MASKF_ATIME;
	if (in & FUSE_SET_ATTR_MTIME)
		out |= SETATTR_MASKF_MTIME;
	return (out);
}

__static void
slash2fuse_setattr(fuse_req_t req, fuse_ino_t ino,
    struct stat *stb, int to_set, struct fuse_file_info *fi)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct fidc_membh *c = NULL;
	struct srm_setattr_req *mq;
	struct srm_setattr_rep *mp;
	struct slash_creds cr;
	struct msl_fhent *mfh;
	int rc, getting = 0;

	msfsthr_ensure();

	rc = slc_rmc_getimp(&csvc);
	if (rc)
		goto out;
	rc = SL_RSX_NEWREQ(csvc->csvc_import, SRMC_VERSION,
	    SRMT_SETATTR, rq, mq, mp);
	if (rc)
		goto out;

	slash2fuse_getcred(req, &cr);

	rc = fidc_lookup_load_inode(ino, &c);
	if (rc)
		goto out;


	rc = checkcreds(&c->fcmh_sstb, &cr, W_OK);
	if (rc)
		goto out;

	spinlock(&c->fcmh_lock);
	/* We're obtaining the attributes now.
	 */
	if ((c->fcmh_state & (FCMH_GETTING_ATTRS | FCMH_HAVE_ATTRS)) == 0) {
		getting = 1;
		c->fcmh_state |= FCMH_GETTING_ATTRS;
	}
	freelock(&c->fcmh_lock);

	if (fi && fi->fh) {
		mfh = ffi_getmfh(fi);
		psc_assert(c == mfh->mfh_fcmh);
	}

	mq->fg = c->fcmh_fg;
	mq->to_set = slash2fuse_translate_setattr_flags(to_set);
	sl_externalize_stat(stb, &mq->attr);
	mq->attr.sst_ptruncgen = fcmh_2_ptruncgen(c);
	mq->attr.sst_gen = fcmh_2_gen(c);
//	mq->attr.sst_mask = ;

	if (mq->to_set & SETATTR_MASKF_SIZE)
		DEBUG_FCMH(PLL_NOTIFY, c, "truncate (sz=%"PRId64")",
			   stb->st_size);
	/*
	 * Even though we know our FID, we expect the server to fill it
	 * along with the rest of the new attributes (mp->attr).
	 */
	rc = SL_RSX_WAITREP(rq, mp);
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		goto out;

	FCMH_LOCK(c);
	if (mq->to_set & SETATTR_MASKF_MTIME) {
		c->fcmh_sstb.sst_mtime = mp->attr.sst_mtime;
		c->fcmh_sstb.sst_mtime_ns = mp->attr.sst_mtime_ns;
	}
	if (mq->to_set & SETATTR_MASKF_SIZE)
		c->fcmh_sstb.sst_size = mp->attr.sst_size;
	fcmh_setattr(c, &mp->attr, FCMH_SETATTRF_SAVELOCAL |
		     FCMH_SETATTRF_HAVELOCK);
	FCMH_ULOCK(c);

	sl_internalize_stat(&c->fcmh_sstb, stb);
	fuse_reply_attr(req, stb, MSL_FUSE_ATTR_TIMEO);

	if (mq->to_set & SETATTR_MASKF_SIZE)
		DEBUG_FCMH(PLL_NOTIFY, c, "post truncate (sz=%"PRId64")",
			   stb->st_size);
 out:
	if (rc) {
		fuse_reply_err(req, rc);
		if (getting) {
			spinlock(&c->fcmh_lock);
			c->fcmh_state &= ~FCMH_GETTING_ATTRS;
			psc_waitq_wakeall(&c->fcmh_waitq);
			freelock(&c->fcmh_lock);
		}
	}
	if (c)
		fcmh_op_done_type(c, FCMH_OPCNT_LOOKUP_FIDC);
	if (rq)
		pscrpc_req_finished(rq);
}

__static void
slash2fuse_fsync_helper(__unusedx fuse_req_t req, __unusedx fuse_ino_t ino,
	__unusedx int datasync, struct fuse_file_info *fi)
{
	struct msl_fhent *mfh;

	mfh = ffi_getmfh(fi);

	DEBUG_FCMH(PLL_INFO, mfh->mfh_fcmh, "fsyncing via flush");

	spinlock(&mfh->mfh_lock);
	slash2fuse_flush_int_locked(mfh);
	freelock(&mfh->mfh_lock);

	fuse_reply_err(req, 0);
}

__static void
slash2fuse_destroy(__unusedx void *userdata)
{
	//fuse_reply_err(req, ENOTSUP);
}

void
slash2fuse_write(fuse_req_t req, __unusedx fuse_ino_t ino,
    const char *buf, size_t size, off_t off,
    struct fuse_file_info *fi)
{
	struct msl_fhent *mfh;
	int rc;

	msfsthr_ensure();

	mfh = ffi_getmfh(fi);
	if (fidc_lookup_fg(&mfh->mfh_fcmh->fcmh_fg) != mfh->mfh_fcmh) {
		rc = EBADF;
		goto out;
	}
	/* XXX EBADF if fd is not open for writing */
	if (fcmh_isdir(mfh->mfh_fcmh)) {
		fcmh_op_done_type(mfh->mfh_fcmh, FCMH_OPCNT_LOOKUP_FIDC);
		rc = EISDIR;
		goto out;
	}

	rc = msl_write(mfh, (char *)buf, size, off);

	DEBUG_FCMH(PLL_NOTIFY, mfh->mfh_fcmh,
		   "buf=%p rc=%d sz=%zu off=%"PSCPRIdOFF, buf, rc, size, off);

	FCMH_LOCK(mfh->mfh_fcmh);
	mfh->mfh_fcmh->fcmh_sstb.sst_mtime = time(NULL);
	fcmh_op_done_type(mfh->mfh_fcmh, FCMH_OPCNT_LOOKUP_FIDC);

	if (rc < 0)
		rc = -rc;
	else {
		fuse_reply_write(req, size);
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

	mfh = ffi_getmfh(fi);
	if (fidc_lookup_fg(&mfh->mfh_fcmh->fcmh_fg) != mfh->mfh_fcmh) {
		rc = EBADF;
		goto out;
	}

	if (fcmh_isdir(mfh->mfh_fcmh)) {
		fcmh_op_done_type(mfh->mfh_fcmh, FCMH_OPCNT_LOOKUP_FIDC);
		rc = EISDIR;
		goto out;
	}

	buf = PSCALLOC(size);
	rc = msl_read(mfh, buf, size, off);

	fcmh_op_done_type(mfh->mfh_fcmh, FCMH_OPCNT_LOOKUP_FIDC);
	if (rc < 0)
		rc = -rc;
	else {
		fuse_reply_buf(req, buf, rc);
		rc = 0;
	}
	PSCFREE(buf);
 out:
	if (rc)
		fuse_reply_err(req, rc);
}

void
unmount(const char *mp)
{
	char buf[BUFSIZ];
	int rc;

	rc = snprintf(buf, sizeof(buf), "umount %s", mp);
	if (rc == -1)
		psc_fatal("snprintf: umount %s", mp);
	if (rc >= (int)sizeof(buf))
		psc_fatalx("snprintf: umount %s: too long", mp);
	if (system(buf) == -1)
		psc_warn("system(%s)", buf);
}

void
unmount_mp(void)
{
	unmount(mountpoint);
}

void *
msl_init(__unusedx struct fuse_conn_info *conn)
{
	char *name;
	int rc;

	authbuf_checkkeyfile();
	authbuf_readkeyfile();

	libsl_init(PSCNET_CLIENT, 0);
	fidc_init(sizeof(struct fcmh_cli_info), FIDC_CLI_DEFSZ,
	    FIDC_CLI_MAXSZ, NULL);
	bmpc_global_init();
	bmap_cache_init(sizeof(struct bmap_cli_info));
	dircache_init(&dircacheMgr, "dircache", 262144);

	slc_rpc_initsvc();

	/* Start up service threads. */
	psc_eqpollthr_spawn(MSTHRT_EQPOLL, "mseqpollthr");
	msctlthr_spawn();
	mstimerthr_spawn();
	msbmapflushthr_spawn();

	if ((name = getenv("SLASH_MDS_NID")) == NULL)
		psc_fatalx("please export SLASH_MDS_NID");

	rc = slc_rmc_setmds(name);
	if (rc)
		psc_fatalx("invalid MDS %s: %s", name, slstrerror(rc));

	name = getenv("SLASH2_PIOS_ID");
	if (name) {
		prefIOS = libsl_str2id(name);
		if (prefIOS == IOS_ID_ANY)
			psc_warnx("SLASH2_PIOS_ID (%s) does not resolve to "
			    "a valid IOS, defaulting to IOS_ID_ANY", name);
	}
	atexit(unmount_mp);
	return (NULL);
}

struct fuse_lowlevel_ops zfs_operations = {
	.access		= slash2fuse_access,
	.create		= slash2fuse_create,
	.destroy	= slash2fuse_destroy,
	.flush          = slash2fuse_flush,
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
	if (strstr(namefmt, "lnacthr"))
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

void
msl_fuse_addarg(struct fuse_args *av, const char *arg)
{
	if (fuse_opt_add_arg(av, arg) == -1)
		psc_fatal("fuse_opt_add_arg");
}

void
msl_fuse_mount(const char *mp, struct fuse_args *args)
{
	struct fuse_session *se;
	struct fuse_chan *ch;
	char nameopt[BUFSIZ];
	int rc;

	slash2fuse_listener_init();

	rc = snprintf(nameopt, sizeof(nameopt), "fsname=%s", mp);
	if (rc == -1)
		psc_fatal("snprintf: fsname=%s", mp);
	if (rc >= (int)sizeof(nameopt))
		psc_fatalx("snprintf: fsname=%s: too long", mp);

	msl_fuse_addarg(args, "-o");
	msl_fuse_addarg(args, nameopt);

	ch = fuse_mount(mp, args);
	if (ch == NULL)
		psc_fatal("fuse_mount");

	se = fuse_lowlevel_new(args, &zfs_operations,
	    sizeof(zfs_operations), NULL);

	if (se == NULL) {
		fuse_unmount(mp, ch);
		psc_fatal("fuse_lowlevel_new");
	}

	fuse_session_add_chan(se, ch);

	if (slash2fuse_newfs(mp, ch) != 0) {
		fuse_session_destroy(se);
		fuse_unmount(mp, ch);
		psc_fatal("fuse_session_add_chan");
	}

	psc_info("FUSE version %d.%d", FUSE_MAJOR_VERSION,
	    FUSE_MINOR_VERSION);
}

__dead void
usage(void)
{
	fprintf(stderr,
	    "usage: %s [-dUX] [-f conf] [-o fuseopt] [-S socket] [-p prefetch] node\n",
	    progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	char c, *p, *noncanon_mp = NULL, *cfg = SL_PATH_CONF;
	struct fuse_args args = FUSE_ARGS_INIT(0, NULL);
	int unmount_first = 0;
	long l;

	/* gcrypt must be initialized very early on */
	gcry_control(GCRYCTL_SET_THREAD_CBS, &gcry_threads_pthread);
	if (!gcry_check_version(GCRYPT_VERSION))
		errx(1, "libgcrypt version mismatch");

	pfl_init();

	msl_fuse_addarg(&args, "");		/* progname/argv[0] */
	msl_fuse_addarg(&args, "-o");
	msl_fuse_addarg(&args, FUSE_OPTIONS);

	progname = argv[0];
	while ((c = getopt(argc, argv, "df:o:p:S:UX")) != -1)
		switch (c) {
		case 'd':
			msl_fuse_addarg(&args, "-odebug");
			break;
		case 'f':
			cfg = optarg;
			break;
		case 'o':
			msl_fuse_addarg(&args, "-o");
			msl_fuse_addarg(&args, optarg);
			break;
		case 'p':
			l = strtol(optarg, &p, 10);
			if (p == optarg || *p != '\0' ||
			    l < 0 || l > MAX_READDIR_NENTS)
				errx(1, "invalid readdir stat "
				    "prefetch (max %d): %s",
				    MAX_READDIR_NENTS, optarg);
			nstbpref = (int)l;
			break;
		case 'S':
			if (strlcpy(ctlsockfn, optarg,
			    sizeof(ctlsockfn)) >= sizeof(ctlsockfn))
				psc_fatalx("%s: too long", optarg);
			break;
		case 'U':
			unmount_first = 1;
			break;
		case 'X':
			allow_root_uid = 1;
			break;
		default:
			usage();
		}
	argc -= optind;
	argv += optind;
	if (argc == 1)
		noncanon_mp = argv[0];
	else if (argc || noncanon_mp == NULL)
		usage();

	pscthr_init(MSTHRT_FUSE, 0, NULL, NULL, 0, "msfusethr");

	if (unmount_first)
		unmount(noncanon_mp);

	/* canonicalize mount path */
	if (realpath(noncanon_mp, mountpoint) == NULL)
		psc_fatal("realpath %s", noncanon_mp);

	msl_fuse_mount(mountpoint, &args);
	fuse_opt_free_args(&args);

	sl_drop_privs(allow_root_uid);

	slcfg_parse(cfg);
	msl_init(NULL);

	exit(slash2fuse_listener_start());
}
