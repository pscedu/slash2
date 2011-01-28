/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2007-2011, Pittsburgh Supercomputing Center (PSC).
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
#include <sys/statvfs.h>
#include <sys/time.h>

#include <dirent.h>
#include <err.h>
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <inttypes.h>
#include <pwd.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <gcrypt.h>

#include "pfl/cdefs.h"
#include "pfl/fs.h"
#include "pfl/fsmod.h"
#include "pfl/pfl.h"
#include "pfl/stat.h"
#include "pfl/str.h"
#include "pfl/time.h"
#include "psc_ds/vbitmap.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_util/ctlsvr.h"
#include "psc_util/eqpollthr.h"
#include "psc_util/log.h"
#include "psc_util/thread.h"
#include "psc_util/usklndthr.h"

#include "bmap_cli.h"
#include "buffer.h"
#include "cache_params.h"
#include "ctl_cli.h"
#include "fidc_cli.h"
#include "fidcache.h"
#include "mount_slash.h"
#include "pathnames.h"
#include "rpc_cli.h"
#include "slashrpc.h"
#include "slerr.h"
#include "slsubsys.h"
#include "slutil.h"

GCRY_THREAD_OPTION_PTHREAD_IMPL;

#define STD_MOUNT_OPTIONS	"allow_other,max_write=134217728,big_writes"

#define mfh_getfid(mfh)		fcmh_2_fid((mfh)->mfh_fcmh)
#define mfh_getfg(mfh)		(mfh)->mfh_fcmh->fcmh_fg

sl_ios_id_t			 prefIOS = IOS_ID_ANY;
const char			*progname;
char				 ctlsockfn[PATH_MAX] = SL_PATH_MSCTLSOCK;
char				 mountpoint[PATH_MAX];
int				 allow_root_uid = 1;

struct sl_resm			*slc_rmc_resm;

struct psc_vbitmap		 msfsthr_uniqidmap = VBITMAP_INIT_AUTO;
psc_spinlock_t			 msfsthr_uniqidmap_lock = SPINLOCK_INIT;

struct slash_creds		 rootcreds = { 0, 0 };

/* number of attribute prefetch in readdir() */
int				 nstbpref = DEF_READDIR_NENTS;

extern struct psc_waitq		 bmapflushwaitq;

static int msl_lookup_fidcache(const struct slash_creds *, pscfs_inum_t,
    const char *, struct slash_fidgen *, struct srt_stat *);

/**
 * translate_pathname - Convert an absolute file system path name into
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

		thr = pscthr_init(MSTHRT_FS, 0, NULL,
		    msfsthr_teardown, sizeof(*mft), "msfsthr%02zu",
		    id);
		mft = thr->pscthr_private;
		mft->mft_uniqid = id;
		pscthr_setready(thr);
	}
	psc_assert(thr->pscthr_type == MSTHRT_FS);
}

#define slc_fcmh_get(sstb, safl, fcmhp)					\
	_slc_fcmh_get(PFL_CALLERINFOSS(SLSS_FCMH), (sstb), (safl), (fcmhp))

/**
 * slc_fcmh_get - Create/update a FID cache member handle based on the
 *	statbuf provided.
 * @sstb: file stat info.
 * @setattrflags: flags to fcmh_setattr().
 * @name: base name of file.
 * @lookupflags: fid cache lookup flags.
 * @fchmp: value-result fcmh.
 */
__static int
_slc_fcmh_get(const struct pfl_callerinfo *pci, struct srt_stat *sstb,
    int setattrflags, struct fidc_membh **fcmhp)
{
	return (_fidc_lookup(pci, &sstb->sst_fg, FIDC_LOOKUP_CREATE, sstb,
	    setattrflags, fcmhp));
}

void
mslfs_getcreds(struct pscfs_req *pfr, struct slash_creds *cr)
{
	struct pscfs_cred pfc;

	pscfs_getcreds(pfr, &pfc);
	cr->scr_uid = pfc.pfc_uid;
	cr->scr_gid = pfc.pfc_gid;
}

__static void
mslfsop_access(struct pscfs_req *pfr, pscfs_inum_t inum, int mask)
{
	struct slash_creds creds;
	struct fidc_membh *c;
	int rc;

	msfsthr_ensure();

	mslfs_getcreds(pfr, &creds);
	rc = fidc_lookup_load_inode(inum, &c);
	if (rc)
		goto out;

	rc = 0;
	FCMH_LOCK(c);
	if (creds.scr_uid == 0) {
		if ((mask & X_OK) && !S_ISDIR(c->fcmh_sstb.sst_mode) &&
		    (c->fcmh_sstb.sst_mode & (S_IXUSR | S_IXGRP | S_IXOTH)) == 0)
			rc = EACCES;
	} else
		rc = checkcreds(&c->fcmh_sstb, &creds, mask);
	FCMH_ULOCK(c);

 out:
	pscfs_reply_access(pfr, rc);
	if (c)
		fcmh_op_done_type(c, FCMH_OPCNT_LOOKUP_FIDC);
}

__static void
mslfsop_create(struct pscfs_req *pfr, pscfs_inum_t pinum,
    const char *name, int oflags, mode_t mode)
{
	struct fidc_membh *c = NULL, *p = NULL;
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_create_rep *mp = NULL;
	struct srm_create_req *mq;
	struct msl_fhent *mfh = NULL;
	struct fcmh_cli_info *fci;
	struct bmapc_memb *bcm;
	struct stat stb;
	int rc = 0;

	msfsthr_ensure();

	psc_assert(oflags & O_CREAT);

	if (strlen(name) == 0) {
		rc = ENOENT;
		goto out;
	}
	if (strlen(name) > SL_NAME_MAX) {
		rc = ENAMETOOLONG;
		goto out;
	}

	rc = fidc_lookup_load_inode(pinum, &p);
	if (rc)
		goto out;

	if (!fcmh_isdir(p)) {
		rc = ENOTDIR;
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
	rc = SL_RSX_NEWREQ(csvc, SRMT_CREATE, rq, mq, mp);
	if (rc)
		goto out;

	mq->mode = (!(mode & 0777)) ? (0666 & ~(pscfs_getumask(pfr))) : mode;
	mq->pfg.fg_fid = pinum;
	mq->pfg.fg_gen = 0;
	mq->prefios = prefIOS;
	mslfs_getcreds(pfr, &mq->creds);
	strlcpy(mq->name, name, sizeof(mq->name));

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		goto out;

	fcmh_setattr(p, &mp->pattr, FCMH_SETATTRF_NONE);

	rc = slc_fcmh_get(&mp->cattr, FCMH_SETATTRF_NONE, &c);
	if (rc)
		goto out;

	if (oflags & O_APPEND) {
		FCMH_LOCK(c);
		c->fcmh_flags |= FCMH_CLI_APPENDWR;
		FCMH_ULOCK(c);
	}
	if (oflags & O_SYNC) {
		/* XXX write me */
	}

	FCMH_LOCK(c);
	fci = fcmh_2_fci(c);

	fci->fci_reptbl[0].bs_id = mp->sbd.sbd_ios_id;
	fci->fci_nrepls = 1;
	c->fcmh_flags |= FCMH_CLI_HAVEREPLTBL;
	sl_internalize_stat(&c->fcmh_sstb, &stb);
	FCMH_ULOCK(c);

	rc = bmap_getf(c, 0, SL_WRITE, BMAPGETF_LOAD |
	    BMAPGETF_NORETRIEVE, &bcm);
	if (rc)
		goto out;

	msl_bmap_reap_init(bcm, &mp->sbd);

	DEBUG_BMAP(PLL_INFO, bcm, "nid=%"PRIx64" bmapnid=%"PRIx64,
	    mp->sbd.sbd_ion_nid, bmap_2_ion(bcm));

	SL_REPL_SET_BMAP_IOS_STAT(bcm->bcm_repls, 0, BREPLST_VALID);

	bmap_op_done_type(bcm, BMAP_OPCNT_LOOKUP);

	mfh = msl_fhent_new(c);

 out:
	if (c) {
		DEBUG_FCMH(PLL_INFO, c, "new mfh=%p rc=%d name=(%s)", mfh, rc, name);
		fcmh_op_done_type(c, FCMH_OPCNT_LOOKUP_FIDC);
	}

	if (p)
		fcmh_op_done_type(p, FCMH_OPCNT_LOOKUP_FIDC);

	pscfs_reply_create(pfr, mp ? mp->cattr.sst_fid : 0,
	    mp ? mp->cattr.sst_gen : 0, MSLFS_ENTRY_TIMEO, &stb,
	    MSLFS_ATTR_TIMEO, mfh, PSCFS_CREATEF_DIO, rc);

	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

__static int
msl_open(struct pscfs_req *pfr, pscfs_inum_t inum, int oflags,
    struct msl_fhent **mfhp, int *rflags)
{
	struct slash_creds creds;
	struct fidc_membh *c;
	int rc = 0;

	msfsthr_ensure();

	mslfs_getcreds(pfr, &creds);

	*mfhp = NULL;

	rc = fidc_lookup_load_inode(inum, &c);
	if (rc)
		goto out;

	if ((oflags & O_ACCMODE) != O_WRONLY) {
		FCMH_LOCK(c);
		rc = checkcreds(&c->fcmh_sstb, &creds, R_OK);
		FCMH_ULOCK(c);
		if (rc)
			goto out;
	}
	if (oflags & (O_WRONLY | O_RDWR)) {
		FCMH_LOCK(c);
		rc = checkcreds(&c->fcmh_sstb, &creds, W_OK);
		FCMH_ULOCK(c);
		if (rc)
			goto out;
	}

	/* Directory sanity. */
	if (fcmh_isdir(c)) {
		/* pscfs shouldn't ever pass us WR with a dir */
		psc_assert((oflags & (O_WRONLY | O_RDWR)) == 0);
		if (!(oflags & O_DIRECTORY)) {
			rc = EISDIR;
			goto out;
		}
		/* sfop_ctor() can be called prior to having attrs.  This
		 *   means that dir fcmh's can't be initialized fully until
		 *   here.
		 */
		slc_fcmh_initdci(c);
	} else {
		if (oflags & O_DIRECTORY) {
			rc = ENOTDIR;
			goto out;
		}
	}

	*mfhp = msl_fhent_new(c);
	(*mfhp)->mfh_oflags = oflags;

	if (oflags & O_DIRECTORY)
		*rflags |= PSCFS_OPENF_KEEPCACHE;

	/*
	 * PSCFS direct_io does not work with mmap(), which is what the
	 * kernel uses under the hood when running executables, so
	 * disable it for this case.
	 */
	if ((c->fcmh_sstb.sst_mode &
	    (S_IXUSR | S_IXGRP | S_IXOTH)) == 0)
		*rflags |= PSCFS_OPENF_DIO;

	if (oflags & O_APPEND) {
		FCMH_LOCK(c);
		c->fcmh_flags |= FCMH_CLI_APPENDWR;
		FCMH_ULOCK(c);
	}
	if (oflags & O_TRUNC) {
		/* XXX write me */
	}
	if (oflags & O_SYNC) {
		/* XXX write me */
	}

 out:
	DEBUG_FCMH(PLL_INFO, c, "new mfh=%p dir=%s rc=%d", *mfhp,
		   (oflags & O_DIRECTORY) ? "yes" : "no", rc);

	if (c)
		fcmh_op_done_type(c, FCMH_OPCNT_LOOKUP_FIDC);
	return (rc);
}

__static void
mslfsop_open(struct pscfs_req *pfr, pscfs_inum_t inum, int oflags)
{
	struct msl_fhent *mfh;
	int rflags, rc;

	rflags = 0;
	rc = msl_open(pfr, inum, oflags, &mfh, &rflags);
	pscfs_reply_open(pfr, mfh, rflags, rc);
}

__static void
mslfsop_opendir(struct pscfs_req *pfr, pscfs_inum_t inum, int oflags)
{
	struct msl_fhent *mfh;
	int rflags, rc;

	rflags = 0;
	rc = msl_open(pfr, inum, oflags | O_DIRECTORY, &mfh, &rflags);
	pscfs_reply_opendir(pfr, mfh, rflags, rc);
}

int
msl_stat(struct fidc_membh *fcmh, const struct slash_creds *creds)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_getattr_req *mq;
	struct srm_getattr_rep *mp;
	struct timeval now;
	int rc = 0;

 restart:
	FCMH_LOCK(fcmh);
	if (fcmh->fcmh_flags & FCMH_HAVE_ATTRS) {
		PFL_GETTIMEVAL(&now);
		if (timercmp(&now, &fcmh_2_fci(fcmh)->fci_age, <)) {
			DEBUG_FCMH(PLL_DEBUG, fcmh,
			    "attrs retrieved from local cache");
			goto check;
		}
		fcmh->fcmh_flags &= ~FCMH_HAVE_ATTRS;
	}

	/* If someone is already fetching attributes, wait for it to
	 *   complete
	 */
	if (fcmh->fcmh_flags & FCMH_GETTING_ATTRS) {
		psc_waitq_wait(&fcmh->fcmh_waitq, &fcmh->fcmh_lock);
		goto restart;
	}
	fcmh->fcmh_flags |= FCMH_GETTING_ATTRS;
	FCMH_ULOCK(fcmh);

	rc = slc_rmc_getimp(&csvc);
	if (rc)
		goto out;
	rc = SL_RSX_NEWREQ(csvc, SRMT_GETATTR, rq, mq, mp);
	if (rc)
		goto out;

	mq->fg = fcmh->fcmh_fg;

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc == 0)
		rc = mp->rc;
 out:
	FCMH_LOCK(fcmh);
	if (!rc && fcmh_2_fid(fcmh) != mp->attr.sst_fid)
		rc = EBADF;
	if (!rc)
		fcmh_setattr(fcmh, &mp->attr,
		    FCMH_SETATTRF_SAVELOCAL | FCMH_SETATTRF_HAVELOCK);

	fcmh->fcmh_flags &= ~FCMH_GETTING_ATTRS;
	fcmh_wake_locked(fcmh);

	if (rq)
		pscrpc_req_finished(rq);

	DEBUG_FCMH(PLL_DEBUG, fcmh, "attrs retrieved via rpc rc=%d", rc);

 check:
	FCMH_ULOCK(fcmh);
	if (csvc)
		sl_csvc_decref(csvc);
	return (rc);
}

__static void
mslfsop_getattr(struct pscfs_req *pfr, pscfs_inum_t inum)
{
	struct slash_creds creds;
	struct fidc_membh *f;
	struct stat stb;
	int rc;

	msfsthr_ensure();

	mslfs_getcreds(pfr, &creds);
	/*
	 * Lookup and possibly create a new fidcache handle for inum.
	 *  If the fid does not exist in the cache then a placeholder will
	 *  be allocated.  msl_stat() will detect incomplete attrs via
	 *  FCMH_GETTING_ATTRS flag and RPC for them.
	 */
	rc = fidc_lookup_load_inode(inum, &f);
	if (rc)
		goto out;

	rc = msl_stat(f, &creds);
	if (rc)
		goto out;

	if (!fcmh_isdir(f))
		f->fcmh_sstb.sst_blksize = 32768;

	FCMH_LOCK(f);
	sl_internalize_stat(&f->fcmh_sstb, &stb);
	FCMH_ULOCK(f);

	DEBUG_STATBUF(PLL_DEBUG, &stb, "getattr");

 out:
	if (f)
		fcmh_op_done_type(f, FCMH_OPCNT_LOOKUP_FIDC);
	pscfs_reply_getattr(pfr, &stb, MSLFS_ATTR_TIMEO, rc);
}

__static void
mslfsop_link(struct pscfs_req *pfr, pscfs_inum_t c_inum,
    pscfs_inum_t p_inum, const char *newname)
{
	struct fidc_membh *p = NULL, *c = NULL;
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_link_rep *mp = NULL;
	struct slash_creds creds;
	struct srm_link_req *mq;
	struct stat stb;
	int rc = 0;

	msfsthr_ensure();

	if (strlen(newname) == 0) {
		rc = ENOENT;
		goto out;
	}
	if (strlen(newname) > SL_NAME_MAX) {
		rc = ENAMETOOLONG;
		goto out;
	}

	mslfs_getcreds(pfr, &creds);

	/* Check the parent inode. */
	rc = fidc_lookup_load_inode(p_inum, &p);
	if (rc)
		goto out;

	if (!fcmh_isdir(p)) {
		rc = ENOTDIR;
		goto out;
	}

	FCMH_LOCK(p);
	rc = checkcreds(&p->fcmh_sstb, &creds, W_OK);
	FCMH_ULOCK(p);
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
	rc = SL_RSX_NEWREQ(csvc, SRMT_LINK, rq, mq, mp);
	if (rc)
		goto out;

	mq->pfg = p->fcmh_fg;
	mq->fg = c->fcmh_fg;
	strlcpy(mq->name, newname, sizeof(mq->name));

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		goto out;

	fcmh_setattr(p, &mp->pattr, FCMH_SETATTRF_NONE);
	FCMH_LOCK(c);
	fcmh_setattr(c, &mp->cattr, FCMH_SETATTRF_SAVELOCAL |
	    FCMH_SETATTRF_HAVELOCK);
	sl_internalize_stat(&c->fcmh_sstb, &stb);

 out:
	if (c)
		fcmh_op_done_type(c, FCMH_OPCNT_LOOKUP_FIDC);
	if (p)
		fcmh_op_done_type(p, FCMH_OPCNT_LOOKUP_FIDC);

	pscfs_reply_link(pfr, mp ? mp->cattr.sst_fid : 0,
	    mp ? mp->cattr.sst_gen : 0, MSLFS_ENTRY_TIMEO, &stb,
	    MSLFS_ATTR_TIMEO, rc);

	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

__static void
mslfsop_mkdir(struct pscfs_req *pfr, pscfs_inum_t pinum,
    const char *name, mode_t mode)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_mkdir_rep *mp = NULL;
	struct srm_mkdir_req *mq;
	struct fidc_membh *c = NULL, *p = NULL;
	struct slash_creds creds;
	struct stat stb;
	int rc;

	msfsthr_ensure();

	if (strlen(name) == 0) {
		rc = ENOENT;
		goto out;
	}
	if (strlen(name) > SL_NAME_MAX) {
		rc = ENAMETOOLONG;
		goto out;
	}

	rc = fidc_lookup_load_inode(pinum, &p);
	if (rc)
		goto out;

	if (!fcmh_isdir(p)) {
		rc = ENOTDIR;
		goto out;
	}

	mslfs_getcreds(pfr, &creds);

	rc = slc_rmc_getimp(&csvc);
	if (rc)
		goto out;
	rc = SL_RSX_NEWREQ(csvc, SRMT_MKDIR, rq, mq, mp);
	if (rc)
		goto out;

	mq->creds = creds;
	mq->pfg.fg_fid = pinum;
	mq->pfg.fg_gen = 0;
	mq->mode = mode;
	strlcpy(mq->name, name, sizeof(mq->name));

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		goto out;

	fcmh_setattr(p, &mp->pattr, FCMH_SETATTRF_NONE);

	psclog_info("pfid=%"PRIx64" mode=0%o name='%s' rc=%d mp->rc=%d",
	    mq->pfg.fg_fid, mq->mode, mq->name, rc, mp->rc);

	rc = slc_fcmh_get(&mp->cattr, FCMH_SETATTRF_NONE, &c);
	sl_internalize_stat(&mp->cattr, &stb);

 out:
	if (c)
		fcmh_op_done_type(c, FCMH_OPCNT_LOOKUP_FIDC);
	if (p)
		fcmh_op_done_type(p, FCMH_OPCNT_LOOKUP_FIDC);

	pscfs_reply_mkdir(pfr, mp ? mp->cattr.sst_fid : 0,
	    mp ? mp->cattr.sst_gen : 0, MSLFS_ENTRY_TIMEO, &stb,
	    MSLFS_ATTR_TIMEO, rc);

	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

__static int
msl_delete(struct pscfs_req *pfr, pscfs_inum_t pinum,
    const char *name, int isfile)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct fidc_membh *p = NULL;
	struct srm_unlink_req *mq;
	struct srm_unlink_rep *mp;
	struct slash_creds cr;
	int rc;

	msfsthr_ensure();

	if (strlen(name) == 0) {
		rc = ENOENT;
		goto out;
	}
	if (strlen(name) > SL_NAME_MAX) {
		rc = ENAMETOOLONG;
		goto out;
	}

	rc = fidc_lookup_load_inode(pinum, &p);
	if (rc)
		goto out;

	mslfs_getcreds(pfr, &cr);

	FCMH_LOCK(p);
	if ((p->fcmh_sstb.sst_mode & S_ISVTX) && cr.scr_uid) {
		if (p->fcmh_sstb.sst_uid != cr.scr_uid) {
			struct srt_stat sstb;

			FCMH_ULOCK(p);

			rc = msl_lookup_fidcache(&cr,
			    pinum, name, NULL, &sstb);
			if (rc)
				goto out;

			if (sstb.sst_uid != cr.scr_uid)
				rc = EPERM;
		} else
			FCMH_ULOCK(p);
	} else {
		rc = checkcreds(&p->fcmh_sstb, &cr, W_OK);
		FCMH_ULOCK(p);
	}
	if (rc)
		goto out;

	rc = slc_rmc_getimp(&csvc);
	if (rc)
		goto out;

	rc = SL_RSX_NEWREQ(csvc, isfile ? SRMT_UNLINK : SRMT_RMDIR, rq,
	    mq, mp);
	if (rc)
		goto out;

	mq->pfid = pinum;

	strlcpy(mq->name, name, sizeof(mq->name));

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc == 0)
		rc = mp->rc;

	/* Lock to be released in fcmh_op_done_type()
	 */
	FCMH_LOCK(p);
	if (!rc)
		fcmh_setattr(p, &mp->attr, FCMH_SETATTRF_HAVELOCK);
	if (DIRCACHE_INITIALIZED(p))
		dircache_lookup(&fcmh_2_fci(p)->fci_dci,
		    name, DC_STALE);
	else
		slc_fcmh_initdci(p);

	/* XXX if fcmh being unlinked has refcnt 0, purge it from fidcache */

 out:
	if (p)
		fcmh_op_done_type(p, FCMH_OPCNT_LOOKUP_FIDC);
	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
	return (rc);
}

__static void
mslfsop_unlink(struct pscfs_req *pfr, pscfs_inum_t pinum,
    const char *name)
{
	pscfs_reply_unlink(pfr, msl_delete(pfr, pinum, name, 1));
}

__static void
mslfsop_rmdir(struct pscfs_req *pfr, pscfs_inum_t pinum,
    const char *name)
{
	pscfs_reply_unlink(pfr, msl_delete(pfr, pinum, name, 0));
}

__static void
mslfsop_mknod(struct pscfs_req *pfr, pscfs_inum_t pinum,
    const char *name, mode_t mode, dev_t rdev)
{
	struct fidc_membh *p = NULL, *c = NULL;
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_mknod_rep *mp = NULL;
	struct srm_mknod_req *mq = NULL;
	struct slash_creds creds;
	struct stat stb;
	int rc;

	msfsthr_ensure();

	if (!S_ISFIFO(mode)) {
		rc = EOPNOTSUPP;
		goto out;
	}
	if (strlen(name) == 0) {
		rc = ENOENT;
		goto out;
	}
	if (strlen(name) > SL_NAME_MAX) {
		rc = ENAMETOOLONG;
		goto out;
	}

	rc = fidc_lookup_load_inode(pinum, &p);
	if (rc)
		goto out;

	if (!fcmh_isdir(p)) {
		rc = ENOTDIR;
		goto out;
	}
	mslfs_getcreds(pfr, &creds);

	FCMH_LOCK(p);
	rc = checkcreds(&p->fcmh_sstb, &creds, W_OK);
	FCMH_ULOCK(p);
	if (rc)
		goto out;

	rc = slc_rmc_getimp(&csvc);
	if (rc)
		goto out;
	rc = SL_RSX_NEWREQ(csvc, SRMT_MKNOD, rq, mq, mp);
	if (rc)
		goto out;

	mq->creds = creds;
	mq->pfg.fg_fid = pinum;
	mq->pfg.fg_gen = 0;
	mq->mode = mode;
	strlcpy(mq->name, name, sizeof(mq->name));

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		goto out;

	fcmh_setattr(p, &mp->pattr, FCMH_SETATTRF_NONE);

	psclog_info("pfid=%"PRIx64" mode=0%o name='%s' rc=%d mp->rc=%d",
	    mq->pfg.fg_fid, mq->mode, mq->name, rc, mp->rc);

	rc = slc_fcmh_get(&mp->cattr, FCMH_SETATTRF_NONE, &c);

	sl_internalize_stat(&mp->cattr, &stb);

 out:
	pscfs_reply_mknod(pfr, mp ? mp->cattr.sst_fid : 0,
	    mp ? mp->cattr.sst_gen : 0, MSLFS_ENTRY_TIMEO, &stb,
	    MSLFS_ATTR_TIMEO, rc);
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
mslfsop_readdir(struct pscfs_req *pfr, size_t size, off_t off, void *data)
{
	struct slashrpc_cservice *csvc = NULL;
	struct fidc_membh *dtmp, *d = NULL;
	struct srm_readdir_rep *mp = NULL;
	struct pscrpc_request *rq = NULL;
	struct dircache_ents *e = NULL;
	struct pscrpc_bulk_desc *desc;
	struct msl_fhent *mfh = data;
	struct srm_readdir_req *mq;
	struct iovec iov[2];
	int rc = 0, niov = 0;

	iov[0].iov_base = NULL;
	iov[1].iov_base = NULL;

	msfsthr_ensure();

	d = mfh->mfh_fcmh;
	psc_assert(d);

	/*
	 * Ensure that the fcmh is still valid, we can't rely
	 *  only on the inode number, the generation # number
	 *  must be taken into account.
	 */
	dtmp = fidc_lookup_fg(&d->fcmh_fg);
	if (dtmp != d)
		rc = EBADF;
	if (dtmp)
		fcmh_op_done_type(dtmp, FCMH_OPCNT_LOOKUP_FIDC);
	if (rc)
		goto out;

	if (!fcmh_isdir(d)) {
		rc = ENOTDIR;
		goto out;
	}

	rc = slc_rmc_getimp(&csvc);
	if (rc)
		goto out;
	rc = SL_RSX_NEWREQ(csvc, SRMT_READDIR, rq, mq, mp);
	if (rc)
		goto out;

	mq->fg = d->fcmh_fg;
	mq->size = size;
	mq->offset = off;

	FCMH_LOCK(d);
	if (!DIRCACHE_INITIALIZED(d))
		slc_fcmh_initdci(d);
	FCMH_ULOCK(d);

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
	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		goto out;

	if (mq->nstbpref) {
		struct srt_stat *attr = iov[1].iov_base;
		struct fidc_membh *fcmh;
		uint32_t i;

		if (mp->num < mq->nstbpref)
			mq->nstbpref = mp->num;

		for (i = 0; i < mq->nstbpref; i++, attr++) {
			if (attr->sst_fid == FID_ANY ||
			    attr->sst_fid == 0) {
				psc_warnx("invalid i+g:"SLPRI_FG", parent: "SLPRI_FID,
					   SLPRI_FG_ARGS(&attr->sst_fg), fcmh_2_fid(d));
				continue;
			}

			psclog_dbg("adding i+g:"SLPRI_FG,
			    SLPRI_FG_ARGS(&attr->sst_fg));

			fidc_lookup(&attr->sst_fg, FIDC_LOOKUP_CREATE,
			    attr, FCMH_SETATTRF_SAVELOCAL, &fcmh);

			if (fcmh)
				fcmh_op_done_type(fcmh,
				    FCMH_OPCNT_LOOKUP_FIDC);
		}
	}

	/* Establish these dirents in our cache.  Do this before replying
	 *   to pscfs in order to prevent unnecessary lookup RPC's.
	 */
	if (mp->num)
		dircache_reg_ents(e, mp->num);

 out:
	if (niov == 2)
		PSCFREE(iov[1].iov_base);

	pscfs_reply_readdir(pfr, iov[0].iov_base, mp ? mp->size : 0, rc);

	/* At this point the dirent cache is technically freeable.
	 */
	if (mp && mp->num)
		dircache_setfreeable_ents(e);
	else if (e)
		dircache_rls_ents(e, DCFREEF_EARLY);

	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

__static int
slash_lookuprpc(const struct slash_creds *crp, pscfs_inum_t pinum,
    const char *name, struct slash_fidgen *fgp, struct srt_stat *sstb)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct fidc_membh *m = NULL, *p;
	struct srm_lookup_req *mq;
	struct srm_lookup_rep *mp;
	int rc;

	if (strlen(name) == 0)
		return (ENOENT);
	if (strlen(name) > SL_NAME_MAX)
		return (ENAMETOOLONG);

	rc = fidc_lookup_load_inode(pinum, &p);
	if (rc)
		goto out;

	FCMH_LOCK(p);
	rc = checkcreds(&p->fcmh_sstb, crp, X_OK);
	FCMH_ULOCK(p);
	fcmh_op_done_type(p, FCMH_OPCNT_LOOKUP_FIDC);
	if (rc)
		goto out;

	rc = slc_rmc_getimp(&csvc);
	if (rc)
		goto out;
	rc = SL_RSX_NEWREQ(csvc, SRMT_LOOKUP, rq, mq, mp);
	if (rc)
		goto out;

	mq->pfg.fg_fid = pinum;
	mq->pfg.fg_gen = 0;

	strlcpy(mq->name, name, sizeof(mq->name));

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		goto out;

	/*
	 * Add the inode to the cache first, otherwise pscfs may
	 *  come to us with another request for the inode since it won't
	 *  yet be visible in the cache.
	 */
	rc = slc_fcmh_get(&mp->attr, FCMH_SETATTRF_SAVELOCAL, &m);
	if (rc)
		goto out;

	if (fgp)
		*fgp = mp->attr.sst_fg;

	if (sstb) {
		FCMH_LOCK(m);
		*sstb = m->fcmh_sstb;
	}

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
msl_lookup_fidcache(const struct slash_creds *cr, pscfs_inum_t pinum,
    const char *name, struct slash_fidgen *fgp, struct srt_stat *sstb)
{
	struct fidc_membh *p, *c;
	slfid_t child;
	int rc;

	psclog_info("looking for file: %s under inode: "SLPRI_FID,
	    name, pinum);

	p = fidc_lookup_fid(pinum);
	if (!p)
		goto out;

	FCMH_LOCK(p);
	if (!DIRCACHE_INITIALIZED(p))
		slc_fcmh_initdci(p);
	FCMH_ULOCK(p);

	child = dircache_lookup(&fcmh_2_fci(p)->fci_dci, name, DC_LOOKUP);
	/* It's OK to unref the parent now.
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
	rc = msl_stat(c, cr);
	if (!rc) {
		if (fgp)
			*fgp = c->fcmh_fg;
		if (sstb) {
			FCMH_LOCK(c);
			*sstb = c->fcmh_sstb;
			FCMH_ULOCK(c);
		}
	}
	fcmh_op_done_type(c, FCMH_OPCNT_LOOKUP_FIDC);
	return (rc);
 out:
	return (slash_lookuprpc(cr, pinum, name, fgp, sstb));
}

__static void
mslfsop_lookup(struct pscfs_req *pfr, pscfs_inum_t pinum,
    const char *name)
{
	struct slash_fidgen fg;
	struct slash_creds cr;
	struct srt_stat sstb;
	struct stat stb;
	int rc;

	msfsthr_ensure();

	mslfs_getcreds(pfr, &cr);
	rc = msl_lookup_fidcache(&cr, pinum, name, &fg, &sstb);
	if (rc == ENOENT)
		sstb.sst_fid = 0;
	sl_internalize_stat(&sstb, &stb);
	pscfs_reply_lookup(pfr, sstb.sst_fid, sstb.sst_gen,
	    MSLFS_ENTRY_TIMEO, &stb, MSLFS_ATTR_TIMEO, rc);
}

__static void
mslfsop_readlink(struct pscfs_req *pfr, pscfs_inum_t inum)
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
	rc = SL_RSX_NEWREQ(csvc, SRMT_READLINK, rq, mq, mp);
	if (rc)
		goto out;

	mslfs_getcreds(pfr, &creds);

	rc = fidc_lookup_load_inode(inum, &c);
	if (rc)
		goto out;

	FCMH_LOCK(c);
	rc = checkcreds(&c->fcmh_sstb, &creds, R_OK);
	FCMH_ULOCK(c);
	if (rc)
		goto out;

	mq->fg = c->fcmh_fg;

	iov.iov_base = buf;
	iov.iov_len = sizeof(buf);
	rsx_bulkclient(rq, &desc, BULK_PUT_SINK,
	    SRMC_BULK_PORTAL, &iov, 1);

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc == 0)
		rc = mp->rc;

	buf[sizeof(buf) - 1] = '\0';

 out:
	if (c)
		fcmh_op_done_type(c, FCMH_OPCNT_LOOKUP_FIDC);

	pscfs_reply_readlink(pfr, buf, rc);

	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

/* Note that this function is called once for each open */
__static void
msl_flush_int_locked(struct msl_fhent *mfh)
{
	struct bmpc_ioreq *r;

	PLL_FOREACH(r, &mfh->mfh_biorqs) {
		spinlock(&r->biorq_lock);
		r->biorq_flags |= BIORQ_FORCE_EXPIRE;
		DEBUG_BIORQ(PLL_INFO, r, "force expire");
		freelock(&r->biorq_lock);
		psc_waitq_wakeall(&bmapflushwaitq);
	}

	//psc_atomic32_inc(&bmapflushforceexpired);
	psc_waitq_wakeone(&bmapflushwaitq);

	while (!pll_empty(&mfh->mfh_biorqs)) {
		psc_waitq_wait(&msl_fhent_flush_waitq, &mfh->mfh_lock);
		spinlock(&mfh->mfh_lock);
	}
}

__static void
mslfsop_flush(struct pscfs_req *pfr, void *data)
{
	struct msl_fhent *mfh = data;

	DEBUG_FCMH(PLL_INFO, mfh->mfh_fcmh, "flushing (mfh=%p)", mfh);

	spinlock(&mfh->mfh_lock);
	msl_flush_int_locked(mfh);
	freelock(&mfh->mfh_lock);

	DEBUG_FCMH(PLL_INFO, mfh->mfh_fcmh, "done flushing (mfh=%p)", mfh);

	pscfs_reply_flush(pfr, 0);
}

__static void
mslfsop_close(struct pscfs_req *pfr, void *data)
{
	struct msl_fhent *mfh = data;
	struct fidc_membh *c;

	msfsthr_ensure();

	c = mfh->mfh_fcmh;

	spinlock(&mfh->mfh_lock);
#if FHENT_EARLY_RELEASE
	PLL_FOREACH(r, &mfh->mfh_biorqs) {
		spinlock(&r->biorq_lock);
		r->biorq_flags |= BIORQ_NOFHENT;
		freelock(&r->biorq_lock);
	}
#else
	msl_flush_int_locked(mfh);
	psc_assert(pll_empty(&mfh->mfh_biorqs));
#endif
	freelock(&mfh->mfh_lock);

	fcmh_op_done_type(c, FCMH_OPCNT_OPEN);

	DEBUG_FCMH(PLL_INFO, mfh->mfh_fcmh, "freeing mfh(%p)", mfh);
	PSCFREE(mfh);
//	if (process wants I/O guarentees)
//		rc = flush;
	pscfs_reply_close(pfr, 0);
}

__static void
mslfsop_rename(struct pscfs_req *pfr, pscfs_inum_t opinum,
    const char *oldname, pscfs_inum_t npinum, const char *newname)
{
	struct fidc_membh *np = NULL, *op = NULL;
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct pscrpc_bulk_desc *desc;
	struct srm_rename_req *mq;
	struct srm_rename_rep *mp;
	struct slash_creds cr;
	struct iovec iov[2];
	int rc;

	msfsthr_ensure();

	if (strlen(oldname) == 0 || strlen(newname) == 0) {
		rc = ENOENT;
		goto out;
	}
	if (strlen(oldname) > SL_NAME_MAX ||
	    strlen(newname) > SL_NAME_MAX) {
		rc = ENAMETOOLONG;
		goto out;
	}

	rc = fidc_lookup_load_inode(opinum, &op);
	if (rc)
		goto out;

	rc = fidc_lookup_load_inode(npinum, &np);
	if (rc)
		goto out;

	mslfs_getcreds(pfr, &cr);

	/*
	 * XXX permissions checks here are wrong;
	 * they should match CREATE and UNLINK.
	 */
	FCMH_LOCK(op);
	rc = checkcreds(&op->fcmh_sstb, &cr, W_OK);
	FCMH_ULOCK(op);
	if (rc)
		goto out;

	FCMH_LOCK(np);
	rc = checkcreds(&np->fcmh_sstb, &cr, W_OK);
	FCMH_ULOCK(np);
	if (rc)
		goto out;

	rc = slc_rmc_getimp(&csvc);
	if (rc)
		goto out;
	rc = SL_RSX_NEWREQ(csvc, SRMT_RENAME, rq, mq, mp);
	if (rc)
		goto out;

	mq->opfg.fg_fid = opinum;
	mq->npfg.fg_fid = npinum;
	mq->opfg.fg_gen = mq->npfg.fg_gen = 0;
	mq->fromlen = strlen(oldname);
	mq->tolen = strlen(newname);

	iov[0].iov_base = (char *)oldname;
	iov[0].iov_len = mq->fromlen;
	iov[1].iov_base = (char *)newname;
	iov[1].iov_len = mq->tolen;

	rsx_bulkclient(rq, &desc, BULK_GET_SOURCE, SRMC_BULK_PORTAL,
	    iov, 2);

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		goto out;

	FCMH_LOCK(op);
	if (DIRCACHE_INITIALIZED(op))
		/* we could move the dircache_ent to newparent here */
		dircache_lookup(&fcmh_2_fci(op)->fci_dci,
		    oldname, DC_STALE);
	else
		slc_fcmh_initdci(op);

	fcmh_setattr(op, &mp->srr_opattr, FCMH_SETATTRF_NONE |
	    FCMH_SETATTRF_HAVELOCK);
	if (np != np)
		fcmh_setattr(np, &mp->srr_npattr, FCMH_SETATTRF_NONE);

 out:
	if (np)
		fcmh_op_done_type(np, FCMH_OPCNT_LOOKUP_FIDC);
	if (op)
		fcmh_op_done_type(op, FCMH_OPCNT_LOOKUP_FIDC);

	pscfs_reply_rename(pfr, rc);

	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

__static void
mslfsop_statfs(struct pscfs_req *pfr)
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
	rc = SL_RSX_NEWREQ(csvc, SRMT_STATFS, rq, mq, mp);
	if (rc)
		goto out;
	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		goto out;

	sl_internalize_statfs(&mp->ssfb, &sfb);

 out:
	pscfs_reply_statfs(pfr, &sfb, rc);

	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

__static void
mslfsop_symlink(struct pscfs_req *pfr, const char *buf,
    pscfs_inum_t pinum, const char *name)
{
	struct fidc_membh *c = NULL, *p = NULL;
	struct slashrpc_cservice *csvc = NULL;
	struct srm_symlink_rep *mp = NULL;
	struct pscrpc_request *rq = NULL;
	struct pscrpc_bulk_desc *desc;
	struct srm_symlink_req *mq;
	struct slash_creds creds;
	struct iovec iov;
	struct stat stb;
	int rc;

	msfsthr_ensure();

	if (strlen(buf) == 0 || strlen(name) == 0) {
		rc = ENOENT;
		goto out;
	}
	if (strlen(buf) >= SL_PATH_MAX ||
	    strlen(name) > SL_NAME_MAX) {
		rc = ENAMETOOLONG;
		goto out;
	}

	mslfs_getcreds(pfr, &creds);

	rc = fidc_lookup_load_inode(pinum, &p);
	if (rc)
		goto out;

	if (!fcmh_isdir(p)) {
		rc = ENOTDIR;
		goto out;
	}

	rc = slc_rmc_getimp(&csvc);
	if (rc)
		goto out;

	rc = SL_RSX_NEWREQ(csvc, SRMT_SYMLINK, rq, mq, mp);
	if (rc)
		goto out;

	mq->creds = creds;
	mq->pfg.fg_fid = pinum;
	mq->pfg.fg_gen = 0;

	mq->linklen = strlen(buf);
	strlcpy(mq->name, name, sizeof(mq->name));

	iov.iov_base = (char *)buf;
	iov.iov_len = mq->linklen;

	rsx_bulkclient(rq, &desc, BULK_GET_SOURCE, SRMC_BULK_PORTAL,
	    &iov, 1);

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		goto out;

	fcmh_setattr(p, &mp->pattr, FCMH_SETATTRF_NONE);

	rc = slc_fcmh_get(&mp->cattr, FCMH_SETATTRF_NONE, &c);

	sl_internalize_stat(&mp->cattr, &stb);

 out:
	if (c)
		fcmh_op_done_type(c, FCMH_OPCNT_LOOKUP_FIDC);
	if (p)
		fcmh_op_done_type(p, FCMH_OPCNT_LOOKUP_FIDC);

	pscfs_reply_symlink(pfr, mp ? mp->cattr.sst_fid : 0,
	    mp ? mp->cattr.sst_gen : 0, MSLFS_ENTRY_TIMEO, &stb,
	    MSLFS_ATTR_TIMEO, rc);

	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

__static void
mslfsop_setattr(struct pscfs_req *pfr, pscfs_inum_t inum,
    struct stat *stb, int to_set, void *data)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct msl_fhent *mfh = data;
	struct fidc_membh *c = NULL;
	struct srm_setattr_req *mq;
	struct srm_setattr_rep *mp;
	struct slash_creds cr;
	int rc = 0, getting = 0;

	msfsthr_ensure();

	mslfs_getcreds(pfr, &cr);

	rc = fidc_lookup_load_inode(inum, &c);
	if (rc)
		goto out;

	if (to_set == 0)
		goto out;

	FCMH_LOCK(c);
	if ((to_set & PSCFS_SETATTRF_MODE) && cr.scr_uid) {
		if (cr.scr_uid != c->fcmh_sstb.sst_uid) {
			rc = EPERM;
			goto out;
		}
		if (cr.scr_uid)
			stb->st_mode &= ~(S_ISUID | S_ISGID);
	}
	if (to_set & PSCFS_SETATTRF_DATASIZE) {
		rc = checkcreds(&c->fcmh_sstb, &cr, W_OK);
		if (rc)
			goto out;
	}
	if ((to_set & (PSCFS_SETATTRF_ATIME | PSCFS_SETATTRF_MTIME)) &&
	    cr.scr_uid && cr.scr_uid != c->fcmh_sstb.sst_uid) {
		rc = EPERM;
		goto out;
	}
	if (rc == 0 && (to_set & PSCFS_SETATTRF_UID) && cr.scr_uid) {
		rc = EPERM;
		goto out;
	}
	if (rc == 0 && (to_set & PSCFS_SETATTRF_GID) && cr.scr_uid) {
		struct passwd pw, *pwp;
		gid_t *grpv = NULL;
		char buf[LINE_MAX];
		int ngrp = 0;

		rc = getpwuid_r(cr.scr_uid, &pw, buf, sizeof(buf), &pwp);
		if (rc)
			goto out;

		getgrouplist(pw.pw_name, cr.scr_gid, NULL, &ngrp);
		grpv = psc_calloc(ngrp, sizeof(*grpv), 0);
		rc = getgrouplist(pw.pw_name, cr.scr_gid, grpv, &ngrp);
		if (rc < 1) {
			PSCFREE(grpv);
			rc = EIO;
			goto out;
		}

		rc = EPERM;
		for (; ngrp >= 0; ngrp--)
			if (stb->st_gid == grpv[ngrp - 1]) {
				rc = 0;
				break;
			}

		PSCFREE(grpv);
		if (rc)
			goto out;
	}

	rc = slc_rmc_getimp(&csvc);
	if (rc)
		goto out;

	rc = SL_RSX_NEWREQ(csvc, SRMT_SETATTR, rq, mq, mp);
	if (rc)
		goto out;

	/* We're obtaining the attributes now. */
	if ((c->fcmh_flags & (FCMH_GETTING_ATTRS | FCMH_HAVE_ATTRS)) == 0) {
		getting = 1;
		c->fcmh_flags |= FCMH_GETTING_ATTRS;
	}

	mq->attr.sst_fg = c->fcmh_fg;
	mq->to_set = to_set;
	sl_externalize_stat(stb, &mq->attr);

	if (mq->to_set & PSCFS_SETATTRF_DATASIZE) {
		struct bmapc_memb *b;
		struct psc_dynarray a = DYNARRAY_INIT;
		int j;

		/* Make all new I/O's, read and write, wait until this
		 *   setattr RPC has completed.
		 */
		c->fcmh_flags |= FCMH_CLI_TRUNC;

		if (!stb->st_size) {
			DEBUG_FCMH(PLL_NOTIFY, c,
			   "full truncate, orphan bmaps");

			SPLAY_FOREACH(b, bmap_cache, &c->fcmh_bmaptree)
				psc_dynarray_add(&a, b);

			FCMH_ULOCK(c);

			DYNARRAY_FOREACH(b, j, &a)
				bmap_orphan(b);

		} else {
			uint32_t x = stb->st_size / SLASH_BMAP_SIZE;

			/* Partial truncate.  Block and flush.
			 */
			SPLAY_FOREACH(b, bmap_cache, &c->fcmh_bmaptree) {
				if (b->bcm_bmapno < x)
					continue;
				/* Take a reference to ensure the bmap
				 *   is still valid.  bmap_biorq_waitempty()
				 *   shoudn't be called while holding the
				 *   fcmh lock.
				 */
				bmap_op_start_type(b, BMAP_OPCNT_TRUNCWAIT);
				psc_dynarray_add(&a, b);
			}
			FCMH_ULOCK(c);

			/* XXX some writes can be cancelled, but no api exists yet.
			 */
			DYNARRAY_FOREACH(b, j, &a)
				bmap_biorq_expire(b);

			DYNARRAY_FOREACH(b, j, &a) {
				bmap_biorq_waitempty(b);
				bmap_op_done_type(b, BMAP_OPCNT_TRUNCWAIT);
			}
		}
		psc_dynarray_free(&a);

	} else
		FCMH_ULOCK(c);

	if (mfh)
		psc_assert(c == mfh->mfh_fcmh);

	DEBUG_FCMH(PLL_DEBUG, c, "pre setattr");
	DEBUG_SSTB(PLL_DEBUG, &c->fcmh_sstb, "fcmh %p pre setattr", c);

	psclog_dbg("fcmh %p setattr%s%s%s%s%s%s%s", c,
	    mq->to_set & PSCFS_SETATTRF_MODE ? " mode" : "",
	    mq->to_set & PSCFS_SETATTRF_UID ? " uid" : "",
	    mq->to_set & PSCFS_SETATTRF_GID ? " gid" : "",
	    mq->to_set & PSCFS_SETATTRF_ATIME ? " atime" : "",
	    mq->to_set & PSCFS_SETATTRF_MTIME ? " mtime" : "",
	    mq->to_set & PSCFS_SETATTRF_CTIME ? " ctime" : "",
	    mq->to_set & PSCFS_SETATTRF_DATASIZE ? " datasize" : "");

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		goto out;

	FCMH_LOCK(c);
	/*
	 * If we are setting mtime or size, tell MDS what we want it
	 * to be then blindly accept what he returns us; otherwise, we
	 * SAVELOCAL any updates we have.
	 */
	if (mq->to_set & PSCFS_SETATTRF_MTIME ||
	    mq->to_set & PSCFS_SETATTRF_DATASIZE) {
		c->fcmh_sstb.sst_mtime = mp->attr.sst_mtime;
		c->fcmh_sstb.sst_mtime_ns = mp->attr.sst_mtime_ns;
	}

	if (mq->to_set & PSCFS_SETATTRF_DATASIZE) {
		c->fcmh_sstb.sst_size = mp->attr.sst_size;
		c->fcmh_sstb.sst_ctime = mp->attr.sst_ctime;
		c->fcmh_sstb.sst_ctime_ns = mp->attr.sst_ctime_ns;
	}

	fcmh_setattr(c, &mp->attr, FCMH_SETATTRF_SAVELOCAL |
	    FCMH_SETATTRF_HAVELOCK);
	sl_internalize_stat(&c->fcmh_sstb, stb);

	/* Issue wakeup after calling fcmh_setattr() to avoid needless
	 *   spinlock contention.
	 */
	if (mq->to_set & PSCFS_SETATTRF_DATASIZE) {
		DEBUG_FCMH(PLL_NOTIFY, c, "truncate complete");
		psc_assert(c->fcmh_flags & FCMH_CLI_TRUNC);
		c->fcmh_flags &= ~FCMH_CLI_TRUNC;
		fcmh_wake_locked(c);
	}
	DEBUG_SSTB(PLL_DEBUG, &c->fcmh_sstb, "fcmh %p post setattr", c);

 out:
	if (c) {
		FCMH_RLOCK(c);
		if (rc && getting) {
			c->fcmh_flags &= ~FCMH_GETTING_ATTRS;
			fcmh_wake_locked(c);
		}
		sl_internalize_stat(&c->fcmh_sstb, stb);
		fcmh_op_done_type(c, FCMH_OPCNT_LOOKUP_FIDC);
	}
	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);

	pscfs_reply_setattr(pfr, stb, MSLFS_ATTR_TIMEO, rc);
}

__static void
mslfsop_fsync(struct pscfs_req *pfr, __unusedx int datasync, void *data)
{
	struct msl_fhent *mfh;

	mfh = data;

	DEBUG_FCMH(PLL_INFO, mfh->mfh_fcmh, "fsyncing via flush");

	spinlock(&mfh->mfh_lock);
	msl_flush_int_locked(mfh);
	freelock(&mfh->mfh_lock);

	pscfs_reply_fsync(pfr, 0);
}

__static void
mslfsop_umount(void)
{
//	unmount_mp();
}

#if PFL_DEBUG > 0
#  define CHECK_FCMH(f)
#else
#  define CHECK_FCMH(f)							\
	do {								\
		if (fidc_lookup_fg(&(f)->fcmh_fg) != (f))		\
			psc_assert("shouldn't happen");			\
		fcmh_op_done_type((f), FCMH_OPCNT_LOOKUP_FIDC);		\
	} while (0)
#endif

void
mslfsop_write(struct pscfs_req *pfr, const void *buf, size_t size,
    off_t off, void *data)
{
	struct msl_fhent *mfh = data;
	struct fidc_membh *f, *ftmp;
	struct timespec ts;
	int rc = 0;

	msfsthr_ensure();

	f = mfh->mfh_fcmh;
	ftmp = fidc_lookup_fg(&f->fcmh_fg);
	if (ftmp != f)
		rc = EBADF;
	if (ftmp)
		fcmh_op_done_type(ftmp, FCMH_OPCNT_LOOKUP_FIDC);
	if (rc)
		goto out;

	/* XXX EBADF if fd is not open for writing */
	if (fcmh_isdir(f)) {
		rc = EISDIR;
		goto out;
	}

	rc = msl_write(mfh, buf, size, off);
	if (rc < 0) {
		rc = -rc;
		goto out;
	}
	rc = 0;

	FCMH_LOCK(f);
	PFL_GETTIMESPEC(&ts);
	f->fcmh_sstb.sst_mtime = ts.tv_sec;
	f->fcmh_sstb.sst_mtime_ns = ts.tv_nsec;
	FCMH_ULOCK(f);

 out:
	DEBUG_FCMH(PLL_INFO, f, "write: buf=%p rc=%d sz=%zu off=%"PSCPRIdOFFT,
	    buf, rc, size, off);
	pscfs_reply_write(pfr, size, rc);
}

void
mslfsop_read(struct pscfs_req *pfr, size_t size, off_t off, void *data)
{
	struct msl_fhent *mfh = data;
	struct fidc_membh *f, *ftmp;
	void *buf = NULL;
	ssize_t len = 0;
	int rc = 0;

	msfsthr_ensure();

	f = mfh->mfh_fcmh;
	ftmp = fidc_lookup_fg(&f->fcmh_fg);
	if (ftmp != f)
		rc = EBADF;
	if (ftmp)
		fcmh_op_done_type(ftmp, FCMH_OPCNT_LOOKUP_FIDC);
	if (rc)
		goto out;

	if (fcmh_isdir(f)) {
//		psc_fatalx("pscfs gave us a directory");
		rc = EISDIR;
		goto out;
	}

	buf = PSCALLOC(size);
	rc = msl_read(mfh, buf, size, off);
	if (rc < 0)
		rc = -rc;
	else {
		len = rc;
		rc = 0;
	}
 out:
	DEBUG_FCMH(PLL_INFO, f, "read: buf=%p rc=%d sz=%zu len=%zd "
		   "off=%"PSCPRIdOFFT, buf, rc, size, len, off);
	pscfs_reply_read(pfr, buf, len, rc);
	PSCFREE(buf);
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

void
msl_init(void)
{
	char *name;
	int rc;

	authbuf_checkkeyfile();
	authbuf_readkeyfile();

	libsl_init(PSCNET_CLIENT, 0);
	fidc_init(sizeof(struct fcmh_cli_info), FIDC_CLI_DEFSZ,
	    NULL);
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
		psc_fatalx("SLASH_MDS_NID not specified");

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
}

struct pscfs pscfs = {
		mslfsop_access,
		mslfsop_close,
/* closedir */	mslfsop_close,
		mslfsop_create,
		mslfsop_flush,
		mslfsop_fsync,
		mslfsop_fsync,
		mslfsop_getattr,
/* ioctl */	NULL,
		mslfsop_link,
		mslfsop_lookup,
		mslfsop_mkdir,
		mslfsop_mknod,
		mslfsop_open,
		mslfsop_opendir,
		mslfsop_read,
		mslfsop_readdir,
		mslfsop_readlink,
		mslfsop_rename,
		mslfsop_rmdir,
		mslfsop_setattr,
		mslfsop_statfs,
		mslfsop_symlink,
		mslfsop_unlink,
		mslfsop_umount,
		mslfsop_write
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

__dead void
usage(void)
{
	fprintf(stderr,
	    "usage: %s [-dUX] [-D datadir] [-f conf] [-I iosystem] [-M mds]\n"
	    "\t[-o mountopt] [-p #prefetch] [-S socket] node\n",
	    progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	struct pscfs_args args = PSCFS_ARGS_INIT(0, NULL);
	char c, *p, *noncanon_mp, *cfg = SL_PATH_CONF;
	int unmount_first = 0;
	long l;

	/* gcrypt must be initialized very early on */
	gcry_control(GCRYCTL_SET_THREAD_CBS, &gcry_threads_pthread);
	if (!gcry_check_version(GCRYPT_VERSION))
		errx(1, "libgcrypt version mismatch");

	pfl_init();
	sl_subsys_register();

	pscfs_addarg(&args, "");		/* progname/argv[0] */
	pscfs_addarg(&args, "-o");
	pscfs_addarg(&args, STD_MOUNT_OPTIONS);

	progname = argv[0];
	while ((c = getopt(argc, argv, "D:df:I:M:o:p:S:UX")) != -1)
		switch (c) {
		case 'D':
			sl_datadir = optarg;
		case 'd':
			pscfs_addarg(&args, "-odebug");
			break;
		case 'f':
			cfg = optarg;
			break;
		case 'I':
			setenv("SLASH2_PIOS_ID", optarg, 1);
			break;
		case 'M':
			setenv("SLASH_MDS_NID", optarg, 1);
			break;
		case 'o':
			pscfs_addarg(&args, "-o");
			pscfs_addarg(&args, optarg);
			break;
		case 'p':
			l = strtol(optarg, &p, 10);
			if (p == optarg || *p != '\0' ||
			    l < 0 || l > MAX_READDIR_NENTS)
				errx(1, "invalid readdir statbuf "
				    "#prefetch (max %d): %s",
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
	if (argc != 1)
		usage();

	pscthr_init(MSTHRT_FSMGR, 0, NULL, NULL, 0, "msfsmgrthr");

	noncanon_mp = argv[0];
	if (unmount_first)
		unmount(noncanon_mp);

	/* canonicalize mount path */
	if (realpath(noncanon_mp, mountpoint) == NULL)
		psc_fatal("realpath %s", noncanon_mp);

	pscfs_mount(mountpoint, &args);
	pscfs_freeargs(&args);

	sl_drop_privs(allow_root_uid);

	slcfg_parse(cfg);
	msl_init();

	exit(pscfs_main());
}
