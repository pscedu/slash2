/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2007-2012, Pittsburgh Supercomputing Center (PSC).
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

#include <ctype.h>
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
#include "pfl/sys.h"
#include "pfl/time.h"
#include "psc_ds/dynarray.h"
#include "psc_ds/vbitmap.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_util/ctlsvr.h"
#include "psc_util/eqpollthr.h"
#include "psc_util/iostats.h"
#include "psc_util/log.h"
#include "psc_util/random.h"
#include "psc_util/thread.h"
#include "psc_util/usklndthr.h"

#include "bmap_cli.h"
#include "buffer.h"
#include "cache_params.h"
#include "ctl_cli.h"
#include "fidc_cli.h"
#include "fidcache.h"
#include "mkfn.h"
#include "mount_slash.h"
#include "pathnames.h"
#include "rpc_cli.h"
#include "slashrpc.h"
#include "slerr.h"
#include "slsubsys.h"
#include "slutil.h"
#include "subsys_cli.h"

GCRY_THREAD_OPTION_PTHREAD_IMPL;

#ifdef HAVE_FUSE_BIG_WRITES
# define STD_MOUNT_OPTIONS	"allow_other,max_write=134217728,big_writes"
#else
# define STD_MOUNT_OPTIONS	"allow_other,max_write=134217728"
#endif

#define MSL_FS_BLKSIZ		(256 * 1024)

#define msl_load_fcmh(pfr, inum, fp)					\
	fidc_lookup_load_inode((inum), (fp), pscfs_getclientctx(pfr))

#define mfh_getfid(mfh)		fcmh_2_fid((mfh)->mfh_fcmh)
#define mfh_getfg(mfh)		(mfh)->mfh_fcmh->fcmh_fg

#define MSL_FLUSH_ATTR_TIMEOUT	8

struct psc_waitq		msl_flush_attrq = PSC_WAITQ_INIT;
psc_spinlock_t			msl_flush_attrqlock = SPINLOCK_INIT;

struct psc_listcache		attrTimeoutQ;
psc_spinlock_t			attrTimeoutQLock = SPINLOCK_INIT;

static int msl_lookup_fidcache(struct pscfs_req *,
    const struct slash_creds *, pscfs_inum_t, const char *,
    struct slash_fidgen *, struct srt_stat *, struct fidc_membh **);

static int mslfsop_flush_attr(struct fidc_membh *);

sl_ios_id_t			 prefIOS = IOS_ID_ANY;
const char			*progname;
char				 ctlsockfn[PATH_MAX] = SL_PATH_MSCTLSOCK;
char				 mountpoint[PATH_MAX];
int				 allow_root_uid = 1;
struct psc_dynarray		 allow_exe = DYNARRAY_INIT;

struct psc_vbitmap		 msfsthr_uniqidmap = VBITMAP_INIT_AUTO;
psc_spinlock_t			 msfsthr_uniqidmap_lock = SPINLOCK_INIT;

/* number of attribute prefetch in readdir() */
int				 nstb_prefetch = DEF_READDIR_NENTS;

struct psc_poolmaster		 slc_async_req_poolmaster;
struct psc_poolmgr		*slc_async_req_pool;

struct psc_poolmaster		 slc_biorq_poolmaster;
struct psc_poolmgr		*slc_biorq_pool;

uint32_t			 sys_upnonce;

__inline int
fcmh_checkcreds(struct fidc_membh *f, const struct slash_creds *crp,
    int accmode)
{
	int rc, locked;

	locked = FCMH_RLOCK(f);
	rc = checkcreds(&f->fcmh_sstb, crp, accmode);
	FCMH_URLOCK(f, locked);
	return (rc);
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
		psc_multiwait_init(&mft->mft_mw, "%s",
		    thr->pscthr_name);
		mft->mft_uniqid = id;
		pscthr_setready(thr);
	}
	psc_assert(thr->pscthr_type == MSTHRT_FS);
}

/**
 * msl_create_fcmh - Create a FID cache member handle based on the
 *	statbuf provided.
 * @sstb: file stat info.
 * @setattrflags: flags to fcmh_setattrf().
 * @name: base name of file.
 * @lookupflags: fid cache lookup flags.
 * @fchmp: value-result fcmh.
 */
#define msl_create_fcmh(pfr, sstb, safl, fcmhp)				\
	_msl_create_fcmh(PFL_CALLERINFOSS(SLSS_FCMH), (pfr), (sstb),	\
	    (safl), (fcmhp))

__static int
_msl_create_fcmh(const struct pfl_callerinfo *pci,
    struct pscfs_req *pfr, struct srt_stat *sstb,
    int setattrflags, struct fidc_membh **fcmhp)
{
	// XXX FIDC_LOOKUP_EXCL ?
	return (_fidc_lookup(pci, &sstb->sst_fg, FIDC_LOOKUP_CREATE,
	    sstb, setattrflags, fcmhp, pscfs_getclientctx(pfr)));
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
	rc = msl_load_fcmh(pfr, inum, &c);
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

#define msl_progallowed(r)						\
	(psc_dynarray_len(&allow_exe) == 0 || _msl_progallowed(r))

int
_msl_progallowed(struct pscfs_req *pfr)
{
	char fn[PATH_MAX], exe[PATH_MAX];
	const char *p;
	pid_t pid, ppid;
	FILE *fp;
	int n;

	ppid = pscfs_getclientctx(pfr)->pfcc_pid;
	do {
		pid = ppid;

		/* we made it to the root; disallow */
		if (pid == 0 || pid == 1)
			return (0);

		snprintf(fn, sizeof(fn), "/proc/%d/exe", pid);
		if (readlink(fn, exe, sizeof(exe)) == -1) {
			psclog_warn("unable to check access on %s", fn);
			return (0);
		}
		DYNARRAY_FOREACH(p, n, &allow_exe)
		    if (strcmp(exe, p) == 0)
			    return (1);

		snprintf(fn, sizeof(fn), "/proc/%d/stat", pid);
		fp = fopen(fn, "r");
		if (fp == NULL) {
			psclog_warn("unable to read parent PID from %s", fn);
			return (0);
		}
		n = fscanf(fp, "%*d %*s %*c %d ", &ppid);
		fclose(fp);
		if (n != 1) {
			psclog_warn("unable to read parent PID from %s", fn);
			return (0);
		}
	} while (pid != ppid);
	return (0);
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

	if (!msl_progallowed(pfr)) {
		rc = EPERM;
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

	rc = msl_load_fcmh(pfr, pinum, &p);
	if (rc)
		goto out;

	if (!fcmh_isdir(p)) {
		rc = ENOTDIR;
		goto out;
	}

#if 0
	rc = fcmh_checkcreds(p, &creds, W_OK);
	if (rc)
		goto out;
#endif

 retry:
	MSL_RMC_NEWREQ(pfr, p, csvc, SRMT_CREATE, rq, mq, mp, rc);
	if (rc)
		goto out;

	mq->mode = !(mode & 0777) ? (0666 & ~pscfs_getumask(pfr)) : mode;
	mq->pfg.fg_fid = pinum;
	mq->pfg.fg_gen = FGEN_ANY;
	mq->prefios[0] = prefIOS;
	mslfs_getcreds(pfr, &mq->creds);
	strlcpy(mq->name, name, sizeof(mq->name));

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc && slc_rmc_retry(pfr, &rc))
		goto retry;
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		goto out;

	psclog_info("pfid="SLPRI_FID" fid="SLPRI_FID" "
	    "mode=%#o name='%s' rc=%d", pinum,
	    mp->cattr.sst_fg.fg_fid, mode, name, rc);

	fcmh_setattr(p, &mp->pattr);

	rc = msl_create_fcmh(pfr, &mp->cattr, FCMH_SETATTRF_NONE, &c);
	if (rc)
		goto out;

#if 0
	if (oflags & O_APPEND) {
		FCMH_LOCK(c);
		c->fcmh_flags |= FCMH_CLI_APPENDWR;
		FCMH_ULOCK(c);
	}

	if (oflags & O_SYNC) {
		/* XXX do we need to do anything special for this? */
	}
	if (oflags & O_NONBLOCK) {
		/* XXX do we need to do anything special for this? */
	}
#endif

	mfh = msl_fhent_new(c);
	mfh->mfh_oflags = oflags;
	PFL_GETTIMESPEC(&mfh->mfh_open_time);
	memcpy(&mfh->mfh_open_atime, &c->fcmh_sstb.sst_atime,
	    sizeof(mfh->mfh_open_atime));

	FCMH_LOCK(c);
	sl_internalize_stat(&c->fcmh_sstb, &stb);

	if (mp->rc2)
		goto out;

	fci = fcmh_2_fci(c);
	fci->fci_reptbl[0].bs_id = mp->sbd.sbd_ios;
	fci->fci_nrepls = 1;
	c->fcmh_flags |= FCMH_CLI_HAVEREPLTBL;
	FCMH_ULOCK(c);

	mp->rc2 = bmap_getf(c, 0, SL_WRITE, BMAPGETF_LOAD |
	    BMAPGETF_NORETRIEVE, &bcm);
	if (mp->rc2)
		goto out;

	msl_bmap_reap_init(bcm, &mp->sbd);

	DEBUG_BMAP(PLL_INFO, bcm, "ios(%s) sbd_seq=%"PRId64,
	   libsl_ios2name(mp->sbd.sbd_ios), mp->sbd.sbd_seq);

	SL_REPL_SET_BMAP_IOS_STAT(bcm->bcm_repls, 0, BREPLST_VALID);

	bmap_op_done_type(bcm, BMAP_OPCNT_LOOKUP);

 out:
	if (mp && rc == 0 && mp->rc == 0 && mp->rc2)
		DEBUG_FCMH(PLL_WARN, c, "error loading bmap rc=%d",
		    mp->rc2);
	if (c) {
		DEBUG_FCMH(PLL_DEBUG, c, "new mfh=%p rc=%d name=(%s) "
		    "oflags=%#o", mfh, rc, name, oflags);
		fcmh_op_done_type(c, FCMH_OPCNT_LOOKUP_FIDC);
	}

	if (p)
		fcmh_op_done_type(p, FCMH_OPCNT_LOOKUP_FIDC);

	pscfs_reply_create(pfr, mp ? mp->cattr.sst_fid : 0,
	    mp ? mp->cattr.sst_gen : 0, pscfs_entry_timeout, &stb,
	    pscfs_attr_timeout, mfh, PSCFS_CREATEF_DIO, rc);

	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

__static int
msl_open(struct pscfs_req *pfr, pscfs_inum_t inum, int oflags,
    struct msl_fhent **mfhp, int *rflags)
{
	struct fidc_membh *c = NULL;
	struct slash_creds creds;
	int rc = 0;

	msfsthr_ensure();

	mslfs_getcreds(pfr, &creds);

	*mfhp = NULL;

	if (!msl_progallowed(pfr)) {
		rc = EPERM;
		goto out;
	}

	rc = msl_load_fcmh(pfr, inum, &c);
	if (rc)
		goto out;

	if ((oflags & O_ACCMODE) != O_WRONLY) {
		rc = fcmh_checkcreds(c, &creds, R_OK);
		if (rc)
			goto out;
	}
	if (oflags & (O_WRONLY | O_RDWR)) {
		rc = fcmh_checkcreds(c, &creds, W_OK);
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
		/*
		 * sfop_ctor() is called prior to having attrs.
		 * This means that dir fcmh's can't be initialized fully
		 * until here.
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
	PFL_GETTIMESPEC(&(*mfhp)->mfh_open_time);
	memcpy(&(*mfhp)->mfh_open_atime, &c->fcmh_sstb.sst_atime,
	    sizeof((*mfhp)->mfh_open_atime));

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

	if (oflags & O_TRUNC) {
		/*
		 * XXX write me for pscfs backends that do not separate
		 * SETATTR st_mode=0
		 */
	}
	if (oflags & O_SYNC) {
		/* XXX write me */
	}
	if (oflags & O_NONBLOCK) {
		/* XXX write me */
	}

 out:
	if (c) {
		DEBUG_FCMH(PLL_INFO, c, "new mfh=%p dir=%s rc=%d oflags=%#o",
		    *mfhp, (oflags & O_DIRECTORY) ? "yes" : "no", rc, oflags);
		fcmh_op_done_type(c, FCMH_OPCNT_LOOKUP_FIDC);
	}
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
msl_stat(struct fidc_membh *fcmh, void *arg)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscfs_clientctx *pfcc = arg;
	struct pscrpc_request *rq = NULL;
	struct srm_getattr_req *mq;
	struct srm_getattr_rep *mp;
	struct timeval now;
	int rc = 0;

	/*
	 * Special case to handle accesses to
	 * /$mountpoint/.slfidns/<fid>
	 */
	if (fcmh_2_fid(fcmh) == SLFID_NS) {
		fcmh->fcmh_sstb.sst_gen = 0;
		fcmh->fcmh_sstb.sst_mode = S_IFDIR | 0111;
		fcmh->fcmh_sstb.sst_nlink = 2;
		fcmh->fcmh_sstb.sst_uid = 0;
		fcmh->fcmh_sstb.sst_gid = 0;
		fcmh->fcmh_sstb.sst_dev = 0;
		fcmh->fcmh_sstb.sst_rdev = 0;
		fcmh->fcmh_sstb.sstd_freplpol = 0;
		fcmh->fcmh_sstb.sst_utimgen = 0;
		fcmh->fcmh_sstb.sst_size = 2;
		fcmh->fcmh_sstb.sst_blksize = MSL_FS_BLKSIZ;
		fcmh->fcmh_sstb.sst_blocks = 4;
		return (0);
	}

	FCMH_LOCK(fcmh);
	fcmh_wait_locked(fcmh, (fcmh->fcmh_flags & FCMH_GETTING_ATTRS));

	if (fcmh->fcmh_flags & FCMH_HAVE_ATTRS) {
		PFL_GETTIMEVAL(&now);
		if (timercmp(&now, &fcmh_2_fci(fcmh)->fci_age, <)) {
			DEBUG_FCMH(PLL_INFO, fcmh,
			    "attrs retrieved from local cache");
			FCMH_ULOCK(fcmh);
			return (0);
		}
	}
	/* Attrs have expired or do not exist.
	 */
	fcmh->fcmh_flags |= FCMH_GETTING_ATTRS;
	FCMH_ULOCK(fcmh);

	do {
		MSL_RMC_NEWREQ_PFCC(pfcc, fcmh, csvc, SRMT_GETATTR, rq,
		    mq, mp, rc);
		if (rc)
			break;

		mq->fg = fcmh->fcmh_fg;
		mq->iosid = prefIOS;

		rc = SL_RSX_WAITREP(csvc, rq, mp);
	} while (rc && slc_rmc_retry_pfcc(pfcc, &rc));

	if (rc == 0)
		rc = mp->rc;

	FCMH_LOCK(fcmh);
	if (!rc && fcmh_2_fid(fcmh) != mp->attr.sst_fid)
		rc = EBADF;
	if (!rc)
		fcmh_setattrf(fcmh, &mp->attr,
		    FCMH_SETATTRF_SAVELOCAL | FCMH_SETATTRF_HAVELOCK);

	fcmh->fcmh_flags &= ~FCMH_GETTING_ATTRS;
	fcmh_wake_locked(fcmh);

	if (rq)
		pscrpc_req_finished(rq);

	DEBUG_FCMH(PLL_DEBUG, fcmh, "attrs retrieved via rpc rc=%d", rc);

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
	 * If the fid does not exist in the cache then a placeholder
	 * will be allocated.  msl_stat() will detect incomplete attrs
	 * via FCMH_GETTING_ATTRS flag and RPC for them.
	 */
	rc = msl_load_fcmh(pfr, inum, &f);
	if (rc)
		goto out;

	rc = msl_stat(f, pfr);
	if (rc)
		goto out;

	if (!fcmh_isdir(f))
		f->fcmh_sstb.sst_blksize = MSL_FS_BLKSIZ;

	FCMH_LOCK(f);
	sl_internalize_stat(&f->fcmh_sstb, &stb);
	FCMH_ULOCK(f);

	DEBUG_STATBUF(PLL_DEBUG, &stb, "getattr");

 out:
	if (f)
		fcmh_op_done_type(f, FCMH_OPCNT_LOOKUP_FIDC);
	pscfs_reply_getattr(pfr, &stb, pscfs_attr_timeout, rc);
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
	rc = msl_load_fcmh(pfr, p_inum, &p);
	if (rc)
		goto out;

	if (!fcmh_isdir(p)) {
		rc = ENOTDIR;
		goto out;
	}

	rc = fcmh_checkcreds(p, &creds, W_OK);
	if (rc)
		goto out;

	/* Check the child inode. */
	rc = msl_load_fcmh(pfr, c_inum, &c);
	if (rc)
		goto out;

	if (fcmh_isdir(c)) {
		rc = EISDIR;
		goto out;
	}

 retry:
	MSL_RMC_NEWREQ(pfr, p, csvc, SRMT_LINK, rq, mq, mp, rc);
	if (rc)
		goto out;

	mq->pfg = p->fcmh_fg;
	mq->fg = c->fcmh_fg;
	strlcpy(mq->name, newname, sizeof(mq->name));

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc && slc_rmc_retry(pfr, &rc))
		goto retry;
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		goto out;

	fcmh_setattr(p, &mp->pattr);
	FCMH_LOCK(c);
	fcmh_setattrf(c, &mp->cattr, FCMH_SETATTRF_SAVELOCAL |
	    FCMH_SETATTRF_HAVELOCK);
	sl_internalize_stat(&c->fcmh_sstb, &stb);

 out:
	if (c)
		fcmh_op_done_type(c, FCMH_OPCNT_LOOKUP_FIDC);
	if (p)
		fcmh_op_done_type(p, FCMH_OPCNT_LOOKUP_FIDC);

	pscfs_reply_link(pfr, mp ? mp->cattr.sst_fid : 0,
	    mp ? mp->cattr.sst_gen : 0, pscfs_entry_timeout, &stb,
	    pscfs_attr_timeout, rc);

	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

__static void
mslfsop_mkdir(struct pscfs_req *pfr, pscfs_inum_t pinum,
    const char *name, mode_t mode)
{
	struct fidc_membh *c = NULL, *p = NULL;
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_mkdir_rep *mp = NULL;
	struct srm_mkdir_req *mq;
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

	rc = msl_load_fcmh(pfr, pinum, &p);
	if (rc)
		goto out;

	if (!fcmh_isdir(p)) {
		rc = ENOTDIR;
		goto out;
	}

	mslfs_getcreds(pfr, &creds);

 retry:
	MSL_RMC_NEWREQ(pfr, p, csvc, SRMT_MKDIR, rq, mq, mp, rc);
	if (rc)
		goto out;

	mq->pfg.fg_fid = pinum;
	mq->pfg.fg_gen = FGEN_ANY;
	mq->sstb.sst_uid = creds.scr_uid;
	mq->sstb.sst_gid = creds.scr_gid;
	mq->sstb.sst_mode = mode;
	mq->to_set = PSCFS_SETATTRF_MODE;
	strlcpy(mq->name, name, sizeof(mq->name));

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc && slc_rmc_retry(pfr, &rc))
		goto retry;
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		goto out;

	psclog_info("pfid="SLPRI_FID" mode=%#o name='%s' rc=%d mp->rc=%d",
	    mq->pfg.fg_fid, mode, name, rc, mp->rc);

	fcmh_setattr(p, &mp->pattr);

	rc = msl_create_fcmh(pfr, &mp->cattr, FCMH_SETATTRF_NONE, &c);
	if (rc)
		goto out;

	sl_internalize_stat(&mp->cattr, &stb);

 out:
	if (c)
		fcmh_op_done_type(c, FCMH_OPCNT_LOOKUP_FIDC);
	if (p)
		fcmh_op_done_type(p, FCMH_OPCNT_LOOKUP_FIDC);

	pscfs_reply_mkdir(pfr, mp ? mp->cattr.sst_fid : 0,
	    mp ? mp->cattr.sst_gen : 0, pscfs_entry_timeout, &stb,
	    pscfs_attr_timeout, -rc);

	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

__static int
msl_delete(struct pscfs_req *pfr, pscfs_inum_t pinum,
    const char *name, int isfile)
{
	struct fidc_membh *c = NULL, *p = NULL;
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
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

	rc = msl_load_fcmh(pfr, pinum, &p);
	if (rc)
		goto out;

	mslfs_getcreds(pfr, &cr);

	FCMH_LOCK(p);
	if ((p->fcmh_sstb.sst_mode & S_ISVTX) && cr.scr_uid) {
		if (p->fcmh_sstb.sst_uid != cr.scr_uid) {
			struct srt_stat sstb;

			FCMH_ULOCK(p);

			rc = msl_lookup_fidcache(pfr, &cr, pinum, name,
			    NULL, &sstb, NULL);
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

 retry:
	MSL_RMC_NEWREQ(pfr, p, csvc, isfile ? SRMT_UNLINK : SRMT_RMDIR,
	    rq, mq, mp, rc);
	if (rc)
		goto out;

	mq->pfid = pinum;
	strlcpy(mq->name, name, sizeof(mq->name));

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc && slc_rmc_retry(pfr, &rc))
		goto retry;
	if (rc)
		goto out;
	if (rc == 0)
		rc = mp->rc;

	FCMH_LOCK(p);
	if (!rc)
		fcmh_setattr_locked(p, &mp->pattr);
	if (DIRCACHE_INITIALIZED(p)) {
		if (rc == 0 || rc == ENOENT)
			dircache_lookup(fcmh_2_dci(p), name, DC_STALE);
	} else
		slc_fcmh_initdci(p);
	FCMH_ULOCK(p);

	if (rc == 0 && mp->cattr.sst_fid) {
		rc = msl_load_fcmh(pfr, mp->cattr.sst_fid, &c);
		if (!rc)
			fcmh_setattrf(c, &mp->cattr,
			    FCMH_SETATTRF_SAVELOCAL);
	}

 out:
	psclog_info("pfid="SLPRI_FID" name='%s', isfile=%d rc=%d",
	    pinum, name, isfile, rc);

	if (c)
		fcmh_op_done_type(c, FCMH_OPCNT_LOOKUP_FIDC);
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

	rc = msl_load_fcmh(pfr, pinum, &p);
	if (rc)
		goto out;

	if (!fcmh_isdir(p)) {
		rc = ENOTDIR;
		goto out;
	}
	mslfs_getcreds(pfr, &creds);

	rc = fcmh_checkcreds(p, &creds, W_OK);
	if (rc)
		goto out;

 retry:
	MSL_RMC_NEWREQ(pfr, p, csvc, SRMT_MKNOD, rq, mq, mp, rc);
	if (rc)
		goto out;

	mq->creds = creds;
	mq->pfg.fg_fid = pinum;
	mq->pfg.fg_gen = FGEN_ANY;
	mq->mode = mode;
	strlcpy(mq->name, name, sizeof(mq->name));

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc && slc_rmc_retry(pfr, &rc))
		goto retry;
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		goto out;

	psclog_info("pfid="SLPRI_FID" mode=%#o name='%s' rc=%d mp->rc=%d",
	    mq->pfg.fg_fid, mq->mode, mq->name, rc, mp->rc);

	fcmh_setattr(p, &mp->pattr);

	rc = msl_create_fcmh(pfr, &mp->cattr, FCMH_SETATTRF_NONE, &c);
	if (rc)
		goto out;

	sl_internalize_stat(&mp->cattr, &stb);

 out:
	pscfs_reply_mknod(pfr, mp ? mp->cattr.sst_fid : 0,
	    mp ? mp->cattr.sst_gen : 0, pscfs_entry_timeout, &stb,
	    pscfs_attr_timeout, rc);
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
mslfsop_readdir(struct pscfs_req *pfr, size_t size, off_t off,
    void *data)
{
	int nstbpref, rc = 0, niov = 0;
	struct slashrpc_cservice *csvc = NULL;
	struct srm_readdir_rep *mp = NULL;
	struct pscrpc_request *rq = NULL;
	struct dircache_ents *e = NULL;
	struct msl_fhent *mfh = data;
	struct fidc_membh *d = NULL;
	struct srm_readdir_req *mq;
	struct slash_creds cr;
	struct iovec iov[2];

	iov[0].iov_base = NULL;
	iov[1].iov_base = NULL;

	msfsthr_ensure();

	d = mfh->mfh_fcmh;
	psc_assert(d);

	if (!fcmh_isdir(d)) {
		DEBUG_FCMH(PLL_ERROR, d, "inconsistency: readdir on a non-dir");
		rc = ENOTDIR;
		goto out;
	}

	mslfs_getcreds(pfr, &cr);

	rc = fcmh_checkcreds(d, &cr, R_OK);
	if (rc)
		goto out;
	if (fcmh_2_fid(d) == SLFID_NS) {
		rc = EPERM;
		goto out;
	}

	FCMH_LOCK(d);
	if (!DIRCACHE_INITIALIZED(d))
		slc_fcmh_initdci(d);
	FCMH_ULOCK(d);

	e = dircache_new_ents(fcmh_2_dci(d), size);

	iov[niov].iov_base = e->de_base;
	iov[niov].iov_len = size;
	niov++;

	/* calculate the max # of attributes that can be prefetched */
	nstbpref = MIN(nstb_prefetch, (int)howmany(LNET_MTU - size,
	    sizeof(struct srt_stat)));
	if (nstbpref) {
		iov[niov].iov_len = nstbpref *
		    sizeof(struct srt_stat);
		iov[niov].iov_base = PSCALLOC(iov[1].iov_len);
		niov++;
	}

 retry:
	MSL_RMC_NEWREQ(pfr, d, csvc, SRMT_READDIR, rq, mq, mp, rc);
	if (rc)
		goto out;

	mq->fg = d->fcmh_fg;
	mq->size = size;
	mq->offset = off;
	mq->nstbpref = nstbpref;

	rsx_bulkclient(rq, BULK_PUT_SINK, SRMC_BULK_PORTAL, iov, niov);
	rq->rq_bulk_abortable = 1;
	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc && rc != -ENOTDIR && rc != -ENOENT && slc_rmc_retry(pfr, &rc))
		goto retry;
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		goto out;

	if (SRM_READDIR_BUFSZ(mp->size, mp->num, mq->nstbpref) <=
	    sizeof(mp->ents)) {
		size_t sz;

		sz = MIN(mp->num, mq->nstbpref) *
		    sizeof(struct srt_stat);
		memcpy(iov[1].iov_base, mp->ents, sz);
		memcpy(iov[0].iov_base, mp->ents + sz, mp->size);
	}

	if (mq->nstbpref) {
		struct srt_stat *attr = iov[1].iov_base;
		struct fidc_membh *fcmh;
		uint32_t i;

		if (mp->num < mq->nstbpref)
			mq->nstbpref = mp->num;

		for (i = 0; i < mq->nstbpref; i++, attr++) {
			if (attr->sst_fid == FID_ANY ||
			    attr->sst_fid == 0) {
				psclog_warnx("invalid f+g:"SLPRI_FG", "
				    "parent: "SLPRI_FID,
				    SLPRI_FG_ARGS(&attr->sst_fg),
				    fcmh_2_fid(d));
				continue;
			}

			psclog_dbg("adding f+g:"SLPRI_FG,
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
msl_lookuprpc(struct pscfs_req *pfr, pscfs_inum_t pinum,
    const char *name, struct slash_fidgen *fgp, struct srt_stat *sstb,
    struct fidc_membh **fp)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct fidc_membh *m = NULL;
	struct srm_lookup_req *mq;
	struct srm_lookup_rep *mp;
	int rc;

	if (strlen(name) == 0)
		return (ENOENT);
	if (strlen(name) > SL_NAME_MAX)
		return (ENAMETOOLONG);

 retry:
	MSL_RMC_NEWREQ(pfr, NULL, csvc, SRMT_LOOKUP, rq, mq, mp, rc);
	if (rc)
		goto out;

	mq->pfg.fg_fid = pinum;
	mq->pfg.fg_gen = FGEN_ANY;
	strlcpy(mq->name, name, sizeof(mq->name));

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc && slc_rmc_retry(pfr, &rc))
		goto retry;
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		goto out;

	/*
	 * Add the inode to the cache first, otherwise pscfs may
	 *  come to us with another request for the inode since it won't
	 *  yet be visible in the cache.
	 */
	rc = msl_create_fcmh(pfr, &mp->attr, FCMH_SETATTRF_SAVELOCAL, &m);
	if (rc)
		goto out;

	if (fgp)
		*fgp = mp->attr.sst_fg;

	if (sstb) {
		FCMH_LOCK(m);
		*sstb = m->fcmh_sstb;
		FCMH_ULOCK(m);
	}

 out:
	psclog_info("pfid="SLPRI_FID" name='%s', rc=%d", pinum, name, rc);
	if (rc == 0 && fp)
		*fp = m;
	else if (m)
		fcmh_op_done_type(m, FCMH_OPCNT_LOOKUP_FIDC);
	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
	return (rc);
}

static int
msl_lookup_fidcache(struct pscfs_req *pfr,
    const struct slash_creds *crp, pscfs_inum_t pinum, const char *name,
    struct slash_fidgen *fgp, struct srt_stat *sstb,
    struct fidc_membh **fp)
{
	struct fidc_membh *p, *c = NULL;
	slfid_t cfid;
	int rc;

	if (fp)
		*fp = NULL;

	rc = msl_load_fcmh(pfr, pinum, &p);
	if (rc)
		goto out;

	FCMH_LOCK(p);
	rc = checkcreds(&p->fcmh_sstb, crp, X_OK);
	if (rc) {
		fcmh_op_done_type(p, FCMH_OPCNT_LOOKUP_FIDC);
		goto out;
	}

	if (!DIRCACHE_INITIALIZED(p))
		slc_fcmh_initdci(p);
	FCMH_ULOCK(p);

	cfid = dircache_lookup(fcmh_2_dci(p), name, DC_LOOKUP);
	/* It's OK to unref the parent now.
	 */
	fcmh_op_done_type(p, FCMH_OPCNT_LOOKUP_FIDC);

	if (cfid == FID_ANY)
		goto remote;

	c = fidc_lookup_fid(cfid);
	if (!c)
		goto remote;

	/*
	 * We should do a lookup based on name here because a rename
	 * does not change the file ID and we would get a success in a
	 * stat RPC.  Note the call is looking based on a name here, not
	 * based on FID.
	 */
	rc = msl_stat(c, pfr);
	if (!rc) {
		FCMH_LOCK(c);
		if (fgp)
			*fgp = c->fcmh_fg;
		if (sstb)
			*sstb = c->fcmh_sstb;
		FCMH_ULOCK(c);
	}
	goto out;

 remote:
#define MSL_FIDNS_RPATH	".slfidns"
	if (pinum == SLFID_ROOT && strcmp(name, MSL_FIDNS_RPATH) == 0) {
		struct fidc_membh f;

		fcmh_2_fid(&f) = SLFID_NS;
		msl_stat(&f, NULL);
		if (fgp) {
			fgp->fg_fid = SLFID_NS;
			fgp->fg_gen = 0;
		}
		if (sstb)
			*sstb = f.fcmh_sstb;
		return (0);
	}
	if (pinum == SLFID_NS) {
		slfid_t fid;
		char *endp;

		fid = strtoll(name, &endp, 16);
		if (endp == name || *endp != '\0')
			return (ENOENT);
		rc = msl_load_fcmh(pfr, fid, &c);
		if (rc)
			return (rc);
		if (fgp)
			*fgp = c->fcmh_fg;
		if (sstb) {
			FCMH_LOCK(c);
			*sstb = c->fcmh_sstb;
		}
		goto out;
	}
	return (msl_lookuprpc(pfr, pinum, name, fgp, sstb, fp));

 out:
	if (rc == 0 && fp)
		*fp = c;
	else if (c)
		fcmh_op_done_type(c, FCMH_OPCNT_LOOKUP_FIDC);

	psclog_info("look for file: %s under inode: "SLPRI_FID ", rc=%d",
	    name, pinum, rc);

	return (rc);
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
	rc = msl_lookup_fidcache(pfr, &cr, pinum, name, &fg, &sstb,
	    NULL);
	if (rc == ENOENT)
		sstb.sst_fid = 0;
	sl_internalize_stat(&sstb, &stb);
	if (!S_ISDIR(stb.st_mode))
		stb.st_blksize = MSL_FS_BLKSIZ;
	pscfs_reply_lookup(pfr, sstb.sst_fid, sstb.sst_gen,
	    pscfs_entry_timeout, &stb, pscfs_attr_timeout, rc);
}

__static void
mslfsop_readlink(struct pscfs_req *pfr, pscfs_inum_t inum)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_readlink_req *mq;
	struct srm_readlink_rep *mp;
	struct fidc_membh *c = NULL;
	struct slash_creds creds;
	struct iovec iov;
	char buf[SL_PATH_MAX];
	int rc;

	msfsthr_ensure();

	rc = msl_load_fcmh(pfr, inum, &c);
	if (rc)
		goto out;

	mslfs_getcreds(pfr, &creds);

	rc = fcmh_checkcreds(c, &creds, R_OK);
	if (rc)
		goto out;

 retry:
	MSL_RMC_NEWREQ(pfr, c, csvc, SRMT_READLINK, rq, mq, mp, rc);
	if (rc)
		goto out;

	mq->fg = c->fcmh_fg;

	iov.iov_base = buf;
	iov.iov_len = sizeof(buf);
	rsx_bulkclient(rq, BULK_PUT_SINK, SRMC_BULK_PORTAL, &iov, 1);

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc && slc_rmc_retry(pfr, &rc))
		goto retry;
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

/**
 * msl_flush_int_locked - Perform main data flush operation.
 * @mfh: handle corresponding to process file descriptor.
 * Note that this function is called (at least) once for each open.
 */
__static int
msl_flush_int_locked(struct msl_fhent *mfh)
{
	struct bmpc_ioreq *r;

	if (mfh->mfh_flush_rc) {
		int rc;

		rc = mfh->mfh_flush_rc;
		mfh->mfh_flush_rc = 0;
		return (rc);

	} else if (pll_empty(&mfh->mfh_biorqs)) {
		mfh->mfh_flush_rc = 0;
		return (0);
	}

	PLL_FOREACH(r, &mfh->mfh_biorqs) {
		BIORQ_LOCK(r);
		r->biorq_flags |= BIORQ_FORCE_EXPIRE;
		DEBUG_BIORQ(PLL_INFO, r, "force expire");
		BIORQ_ULOCK(r);
	}
	bmap_flushq_wake(BMAPFLSH_EXPIRE, NULL);

	while (!pll_empty(&mfh->mfh_biorqs)) {
		psc_waitq_wait(&msl_fhent_flush_waitq, &mfh->mfh_lock);
		spinlock(&mfh->mfh_lock);
	}

	return (0);
}

__static void
mslfsop_flush(struct pscfs_req *pfr, void *data)
{
	struct msl_fhent *mfh = data;
	int rc;

	DEBUG_FCMH(PLL_INFO, mfh->mfh_fcmh, "flushing (mfh=%p)", mfh);

	spinlock(&mfh->mfh_lock);
	rc = msl_flush_int_locked(mfh);
	freelock(&mfh->mfh_lock);

	DEBUG_FCMH(PLL_INFO, mfh->mfh_fcmh, "done flushing (mfh=%p)", mfh);

	pscfs_reply_flush(pfr, rc);
}

__static void
mslfsop_close(struct pscfs_req *pfr, void *data)
{
	struct msl_fhent *mfh = data;
	struct fidc_membh *c;
	pid_t sid;
	int rc, flush_attrs = 0;
	struct fcmh_cli_info *fci;

	msfsthr_ensure();

	c = mfh->mfh_fcmh;

	MFH_LOCK(mfh);
	mfh->mfh_flags |= MSL_FHENT_CLOSING;
#if FHENT_EARLY_RELEASE
	PLL_FOREACH(r, &mfh->mfh_biorqs) {
		spinlock(&r->biorq_lock);
		r->biorq_flags |= BIORQ_NOFHENT;
		freelock(&r->biorq_lock);
	}
#else
	rc = msl_flush_int_locked(mfh);
	psc_assert(pll_empty(&mfh->mfh_biorqs));
#endif
	while (!pll_empty(&mfh->mfh_ra_bmpces) ||
	    (mfh->mfh_flags & MSL_FHENT_RASCHED)) {
		psc_waitq_wait(&c->fcmh_waitq, &mfh->mfh_lock);
		MFH_LOCK(mfh);
	}

	/*
	 * Perhaps this checking should only be done on the mfh, with
	 * which we have modified the attributes.
	 */
	FCMH_LOCK(c);
	fcmh_wait_locked(c, c->fcmh_flags & FCMH_IN_SETATTR);
	if (c->fcmh_flags & FCMH_CLI_DIRTY_ATTRS) {
		flush_attrs = 1;
		c->fcmh_flags |= FCMH_IN_SETATTR;
		c->fcmh_flags &= ~FCMH_CLI_DIRTY_ATTRS;
	}
	FCMH_ULOCK(c);

	fci = fcmh_2_fci(c);
	if (flush_attrs) {
		rc = mslfsop_flush_attr(c);
		FCMH_LOCK(c);
		c->fcmh_flags &= ~FCMH_IN_SETATTR;
		fcmh_wake_locked(c);
		if (rc) {
			c->fcmh_flags |= FCMH_CLI_DIRTY_ATTRS;
			FCMH_ULOCK(c);
		} else if (!(c->fcmh_flags & FCMH_CLI_DIRTY_ATTRS)) {
			psc_assert(c->fcmh_flags & FCMH_CLI_DIRTY_QUEUE);
			c->fcmh_flags &= ~FCMH_CLI_DIRTY_QUEUE;
			spinlock(&attrTimeoutQLock);
			lc_remove(&attrTimeoutQ, fci);
			freelock(&attrTimeoutQLock);
			fcmh_op_done_type(c, FCMH_OPCNT_DIRTY_QUEUE);
		} else
			FCMH_ULOCK(c);
	}

	sid = getsid(pscfs_getclientctx(pfr)->pfcc_pid);
	pscfs_reply_close(pfr, rc);

	if (!fcmh_isdir(c) && (mfh->mfh_nbytes_rd || mfh->mfh_nbytes_wr))
		psclogs(PLL_INFO, SLCSS_INFO,
		    "file closed fid="SLPRI_FID" "
		    "uid=%u gid=%u "
		    "fsize=%"PRId64" "
		    "oatime="SLPRI_TIMESPEC" "
		    "mtime="SLPRI_TIMESPEC" sessid=%d "
		    "otime="PSCPRI_TIMESPEC" "
		    "rd=%"PSCPRIdOFFT" wr=%"PSCPRIdOFFT,
		    fcmh_2_fid(c),
		    c->fcmh_sstb.sst_uid, c->fcmh_sstb.sst_gid,
		    c->fcmh_sstb.sst_size,
		    SLPRI_TIMESPEC_ARGS(&mfh->mfh_open_atime),
		    SLPRI_TIMESPEC_ARGS(&c->fcmh_sstb.sst_mtim), sid,
		    PSCPRI_TIMESPEC_ARGS(&mfh->mfh_open_time),
		    mfh->mfh_nbytes_rd, mfh->mfh_nbytes_wr);

	fcmh_op_done_type(c, FCMH_OPCNT_OPEN);
	PSCFREE(mfh);
}

__static void
mslfsop_rename(struct pscfs_req *pfr, pscfs_inum_t opinum,
    const char *oldname, pscfs_inum_t npinum, const char *newname)
{
	struct fidc_membh *child = NULL, *np = NULL, *op = NULL;
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srt_stat srcsstb, dstsstb;
	struct slash_fidgen srcfg, dstfg;
	struct srm_rename_req *mq;
	struct srm_rename_rep *mp;
	struct slash_creds cr;
	struct iovec iov[2];
	int sticky, rc;

	srcfg.fg_fid = FID_ANY;
	dstfg.fg_fid = FID_ANY;

	msfsthr_ensure();

#if 0
	if (strcmp(oldname, ".") == 0 ||
	    strcmp(oldname, "..") == 0) {
		rc = EINVAL;
		goto out;
	}
#endif

	if (strlen(oldname) == 0 || strlen(newname) == 0) {
		rc = ENOENT;
		goto out;
	}
	if (strlen(oldname) > SL_NAME_MAX ||
	    strlen(newname) > SL_NAME_MAX) {
		rc = ENAMETOOLONG;
		goto out;
	}

	mslfs_getcreds(pfr, &cr);

	rc = msl_load_fcmh(pfr, opinum, &op);
	if (rc)
		goto out;
	if (cr.scr_uid) {
		FCMH_LOCK(op);
		sticky = op->fcmh_sstb.sst_mode & S_ISVTX;
		if (sticky) {
			if (op->fcmh_sstb.sst_uid == cr.scr_uid)
				sticky = 0;
		} else
			rc = checkcreds(&op->fcmh_sstb, &cr, W_OK);
		FCMH_ULOCK(op);
		if (rc)
			goto out;

		if (sticky) {
			/* XXX race */
			rc = msl_lookup_fidcache(pfr, &cr, opinum,
			    oldname, &srcfg, &srcsstb, &child);
			if (rc)
				goto out;
			if (srcsstb.sst_uid != cr.scr_uid)
				rc = EPERM;
			if (rc)
				goto out;
		}
	}

	if (npinum == opinum) {
		np = op;
	} else {
		rc = msl_load_fcmh(pfr, npinum, &np);
		if (rc)
			goto out;
		if (cr.scr_uid) {
			FCMH_LOCK(np);
			sticky = np->fcmh_sstb.sst_mode & S_ISVTX;
			if (sticky) {
				if (np->fcmh_sstb.sst_uid == cr.scr_uid)
					sticky = 0;
			} else
				rc = checkcreds(&np->fcmh_sstb, &cr, W_OK);
			FCMH_ULOCK(np);
			if (rc)
				goto out;

			if (sticky) {
				/* XXX race */
				rc = msl_lookup_fidcache(pfr, &cr,
				    npinum, newname, &dstfg, &dstsstb,
				    NULL);
				if (rc == 0 &&
				    dstsstb.sst_uid != cr.scr_uid)
					rc = EPERM;
				else
					rc = 0;
				if (rc)
					goto out;
			}
		}
	}

	if (cr.scr_uid) {
		if (srcfg.fg_fid == FID_ANY) {
			/* XXX race */
			rc = msl_lookup_fidcache(pfr, &cr, opinum,
			    oldname, &srcfg, &srcsstb, &child);
			if (rc)
				goto out;
		}
		if (S_ISDIR(srcsstb.sst_mode))
			rc = checkcreds(&srcsstb, &cr, W_OK);
		if (rc)
			goto out;
	}

 retry:
	MSL_RMC_NEWREQ(pfr, np, csvc, SRMT_RENAME, rq, mq, mp, rc);
	if (rc)
		goto out;

	mq->opfg.fg_fid = opinum;
	mq->npfg.fg_fid = npinum;
	mq->opfg.fg_gen = mq->npfg.fg_gen = FGEN_ANY;
	mq->fromlen = strlen(oldname);
	mq->tolen = strlen(newname);

	iov[0].iov_base = (char *)oldname;
	iov[0].iov_len = mq->fromlen;
	iov[1].iov_base = (char *)newname;
	iov[1].iov_len = mq->tolen;

	rsx_bulkclient(rq, BULK_GET_SOURCE, SRMC_BULK_PORTAL, iov, 2);

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc && slc_rmc_retry(pfr, &rc))
		goto retry;
	if (rc == 0)
		rc = mp->rc;

	psclog_info("opfid="SLPRI_FID" npfid="SLPRI_FID" from='%s' "
	    "to='%s' rc=%d", opinum, npinum, oldname, newname, rc);

	if (rc)
		goto out;

	FCMH_LOCK(op);
	if (DIRCACHE_INITIALIZED(op))
		/* we could move the dircache_ent to newparent here */
		dircache_lookup(fcmh_2_dci(op), oldname, DC_STALE);
	else
		slc_fcmh_initdci(op);

	fcmh_setattr_locked(op, &mp->srr_opattr);
	FCMH_ULOCK(op);
	if (np != op)
		fcmh_setattr(np, &mp->srr_npattr);

	if (srcfg.fg_fid == FID_ANY) {
		/* XXX race */
		rc = msl_lookup_fidcache(pfr, &cr, npinum,
		    newname, &srcfg, &srcsstb, &child);
		if (rc)
			goto out;
	}
	if (mp->srr_cattr.sst_fid)
		fcmh_setattrf(child, &mp->srr_cattr,
		    FCMH_SETATTRF_SAVELOCAL);
	DEBUG_FCMH(PLL_INFO, child, "newname=%s, setattr=%s",
	    newname, mp->srr_cattr.sst_fid ? "yes" : "no");

 out:
	if (child)
		fcmh_op_done_type(child, FCMH_OPCNT_LOOKUP_FIDC);
	if (op)
		fcmh_op_done_type(op, FCMH_OPCNT_LOOKUP_FIDC);
	if (np && np != op)
		fcmh_op_done_type(np, FCMH_OPCNT_LOOKUP_FIDC);

	pscfs_reply_rename(pfr, rc);

	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

__static void
mslfsop_statfs(struct pscfs_req *pfr, pscfs_inum_t inum)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_statfs_req *mq;
	struct srm_statfs_rep *mp;
	struct statvfs sfb;
	int rc;

	msfsthr_ensure();

//	checkcreds

 retry:
	MSL_RMC_NEWREQ_PFCC(NULL, NULL, csvc, SRMT_STATFS, rq, mq, mp,
	    rc);
	if (rc)
		goto out;
	mq->fid = inum;
	mq->iosid = prefIOS;
	if (rc)
		goto out;
	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc && slc_rmc_retry(pfr, &rc))
		goto retry;
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		goto out;

	sl_internalize_statfs(&mp->ssfb, &sfb);
	sfb.f_blocks = sfb.f_blocks / MSL_FS_BLKSIZ * sfb.f_frsize;
	sfb.f_bfree = sfb.f_bfree / MSL_FS_BLKSIZ * sfb.f_frsize;
	sfb.f_bavail = sfb.f_bavail / MSL_FS_BLKSIZ * sfb.f_frsize;
	sfb.f_bsize = MSL_FS_BLKSIZ;
	sfb.f_fsid = SLASH_FSID;

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

	rc = msl_load_fcmh(pfr, pinum, &p);
	if (rc)
		goto out;

	if (!fcmh_isdir(p)) {
		rc = ENOTDIR;
		goto out;
	}

 retry:
	MSL_RMC_NEWREQ(pfr, p, csvc, SRMT_SYMLINK, rq, mq, mp, rc);
	if (rc)
		goto out;

	mq->sstb.sst_uid = creds.scr_uid;
	mq->sstb.sst_gid = creds.scr_gid;
	mq->pfg.fg_fid = pinum;
	mq->pfg.fg_gen = FGEN_ANY;
	mq->linklen = strlen(buf);
	strlcpy(mq->name, name, sizeof(mq->name));

	iov.iov_base = (char *)buf;
	iov.iov_len = mq->linklen;

	rsx_bulkclient(rq, BULK_GET_SOURCE, SRMC_BULK_PORTAL, &iov, 1);

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc && slc_rmc_retry(pfr, &rc))
		goto retry;
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		goto out;

	fcmh_setattr(p, &mp->pattr);

	rc = msl_create_fcmh(pfr, &mp->cattr, FCMH_SETATTRF_NONE, &c);

	sl_internalize_stat(&mp->cattr, &stb);

 out:
	if (c)
		fcmh_op_done_type(c, FCMH_OPCNT_LOOKUP_FIDC);
	if (p)
		fcmh_op_done_type(p, FCMH_OPCNT_LOOKUP_FIDC);

	pscfs_reply_symlink(pfr, mp ? mp->cattr.sst_fid : 0,
	    mp ? mp->cattr.sst_gen : 0, pscfs_entry_timeout, &stb,
	    pscfs_attr_timeout, rc);

	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

struct msl_dc_inv_entry_data {
	struct pscfs_req	*mdie_pfr;
	pscfs_inum_t		 mdie_pinum;
};

void
msl_dc_inv_entry(struct dircache_desc *d, void *arg)
{
	const struct msl_dc_inv_entry_data *mdie = arg;

	pscfs_notify_inval_entry(mdie->mdie_pfr,
	    mdie->mdie_pinum, d->dd_name, d->dd_namelen);
}

__static void
mslfsop_setattr(struct pscfs_req *pfr, pscfs_inum_t inum,
    struct stat *stb, int to_set, void *data)
{
	int rc = 0, unset_trunc = 0, getting_attrs = 0;
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct msl_fhent *mfh = data;
	struct fidc_membh *c = NULL;
	struct srm_setattr_req *mq;
	struct srm_setattr_rep *mp;
	struct fcmh_cli_info *fci;
	struct slash_creds cr;
	struct timespec ts;
	int flush_attrs = 0;

	msfsthr_ensure();

	rc = msl_load_fcmh(pfr, inum, &c);
	if (rc)
		goto out;

	if ((to_set & PSCFS_SETATTRF_UID) && stb->st_uid == (uid_t)-1)
		to_set &= ~PSCFS_SETATTRF_UID;
	if ((to_set & PSCFS_SETATTRF_GID) && stb->st_gid == (gid_t)-1)
		to_set &= ~PSCFS_SETATTRF_GID;

	if (to_set == 0)
		goto out;

	if (mfh)
		psc_assert(c == mfh->mfh_fcmh);

	mslfs_getcreds(pfr, &cr);

	FCMH_LOCK(c);
	fcmh_wait_locked(c, c->fcmh_flags & FCMH_IN_SETATTR);
	c->fcmh_flags |= FCMH_IN_SETATTR;

	if ((to_set & PSCFS_SETATTRF_MODE) && cr.scr_uid) {
#if 0
		if ((stb->st_mode & ALLPERMS) !=
		    (c->fcmh_sstb.sst_mode & ALLPERMS)) {
			rc = EINVAL;
			goto out;
		}
#endif
		if (cr.scr_uid != c->fcmh_sstb.sst_uid) {
			rc = EPERM;
			goto out;
		}
		if (cr.scr_gid != c->fcmh_sstb.sst_gid &&
		    !pscfs_inprocgrouplist(pfr, c->fcmh_sstb.sst_gid))
			stb->st_mode &= ~S_ISGID;
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
	if ((to_set & PSCFS_SETATTRF_UID) && cr.scr_uid) {
		if (cr.scr_uid != c->fcmh_sstb.sst_uid ||
		    cr.scr_uid != stb->st_uid) {
			rc = EPERM;
			goto out;
		}
	}
	if ((to_set & PSCFS_SETATTRF_GID) && cr.scr_uid) {
		if (cr.scr_uid != c->fcmh_sstb.sst_uid ||
		    !pscfs_inprocgrouplist(pfr, stb->st_gid)) {
			rc = EPERM;
			goto out;
		}
		if (c->fcmh_sstb.sst_mode & (S_ISGID | S_ISUID)) {
			to_set |= PSCFS_SETATTRF_MODE;
			stb->st_mode = c->fcmh_sstb.sst_mode &
			    ~(S_ISGID | S_ISUID);
		}
	}

 wait_trunc_res:
	if (to_set & PSCFS_SETATTRF_DATASIZE) {
		fcmh_wait_locked(c, c->fcmh_flags & FCMH_CLI_TRUNC);
		/*
		 * Mark as busy against I/O on this and higher bmaps and
		 * concurrent truncation requests util the MDS has
		 * received new CRCs for the freshly truncated region.
		 */
		c->fcmh_flags |= FCMH_CLI_TRUNC;
		unset_trunc = 1;
	}

	if (to_set & PSCFS_SETATTRF_DATASIZE) {
		struct bmapc_memb *b;
		struct psc_dynarray a = DYNARRAY_INIT;
		int j;

		if (!stb->st_size) {
			DEBUG_FCMH(PLL_NOTIFY, c,
			   "full truncate, orphan bmaps");

			bmap_orphan_all_locked(c);
			FCMH_ULOCK(c);

		} else if (stb->st_size == (ssize_t)fcmh_2_fsz(c)) {
			/* No-op.  Don't send truncate request if the
			 *    sizes match.
			 */
			goto out;

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
				DEBUG_BMAP(PLL_NOTIFY, b,
					   "BMAP_OPCNT_TRUNCWAIT");
				psc_dynarray_add(&a, b);
			}
			FCMH_ULOCK(c);

			/*
			 * XXX some writes can be cancelled, but no api
			 * exists yet.
			 */
			DYNARRAY_FOREACH(b, j, &a)
				bmap_biorq_expire(b);

			DYNARRAY_FOREACH(b, j, &a) {
				bmap_biorq_waitempty(b);
				psc_assert(atomic_read(&b->bcm_opcnt) > 1);
				bmap_op_done_type(b, BMAP_OPCNT_TRUNCWAIT);
			}
		}
		psc_dynarray_free(&a);

	}

	FCMH_RLOCK(c);
	/* We're obtaining the attributes now. */
	if ((c->fcmh_flags & (FCMH_GETTING_ATTRS | FCMH_HAVE_ATTRS)) == 0) {
		getting_attrs = 1;
		c->fcmh_flags |= FCMH_GETTING_ATTRS;
	}
	FCMH_ULOCK(c);
	/*
	 * Turn on mtime explicitly if we are going to change the size.
	 * We want our local time to be saved, not the time when the RPC
	 * arrives at the MDS.
	 */
	if ((to_set & PSCFS_SETATTRF_DATASIZE) &&
	    !(to_set & PSCFS_SETATTRF_MTIME)) {
		to_set |= PSCFS_SETATTRF_MTIME;
		PFL_GETTIMESPEC(&ts);
		PFL_STB_MTIME_SET(ts.tv_sec, ts.tv_nsec, stb);
	}

 retry:
	MSL_RMC_NEWREQ(pfr, c, csvc, SRMT_SETATTR, rq, mq, mp, rc);
	if (rc)
		goto out;

	FCMH_LOCK(c);
	/*
	 * No need to do this on retry.  To do: set
	 * PSCFS_SETATTRF_TRUNCATE if setattr truncates a cached size so
	 * that the MDS can act accordingly.
	 */
	if (c->fcmh_flags & FCMH_CLI_DIRTY_ATTRS) {
		flush_attrs = 1;
		to_set |= PSCFS_SETATTRF_FLUSH;
		if (!(to_set & PSCFS_SETATTRF_MTIME)) {
			to_set |= PSCFS_SETATTRF_MTIME;
			PFL_STB_MTIME_SET(c->fcmh_sstb.sst_mtime,
					  c->fcmh_sstb.sst_mtime_ns,
					  stb);
		}
		if (!(to_set & PSCFS_SETATTRF_DATASIZE)) {
			to_set |= PSCFS_SETATTRF_DATASIZE;
			stb->st_size = c->fcmh_sstb.sst_size;
		}
		c->fcmh_flags &= ~FCMH_CLI_DIRTY_ATTRS;
	}

	mq->attr.sst_fg = c->fcmh_fg;
	mq->to_set = to_set;
	sl_externalize_stat(stb, &mq->attr);

	DEBUG_FCMH(PLL_DEBUG, c, "pre setattr");
	DEBUG_SSTB(PLL_DEBUG, &c->fcmh_sstb, "fcmh %p pre setattr", c);

	psclog_dbg("fcmh %p setattr%s%s%s%s%s%s%s", c,
	    to_set & PSCFS_SETATTRF_MODE ? " mode" : "",
	    to_set & PSCFS_SETATTRF_UID ? " uid" : "",
	    to_set & PSCFS_SETATTRF_GID ? " gid" : "",
	    to_set & PSCFS_SETATTRF_ATIME ? " atime" : "",
	    to_set & PSCFS_SETATTRF_MTIME ? " mtime" : "",
	    to_set & PSCFS_SETATTRF_CTIME ? " ctime" : "",
	    to_set & PSCFS_SETATTRF_DATASIZE ? " datasize" : "");

	FCMH_ULOCK(c);

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc && slc_rmc_retry(pfr, &rc))
		goto retry;
	if (rc == 0) {
		if (mp->rc == SLERR_BMAP_IN_PTRUNC) {
			if (getting_attrs) {
				getting_attrs = 0;
				c->fcmh_flags &= ~FCMH_GETTING_ATTRS;
			}
			goto wait_trunc_res;
		}
		rc = mp->rc;
		if (mp->rc == SLERR_BMAP_IN_PTRUNC)
			unset_trunc = 0;
	}
	if (rc)
		goto out;

	FCMH_LOCK(c);
	/*
	 * If we are setting mtime or size, we told the MDS what we
	 * wanted it to be and must now blindly accept what he returns
	 * to us; otherwise, we SAVELOCAL any updates we've made.
	 */
	if (to_set & (PSCFS_SETATTRF_MTIME | PSCFS_SETATTRF_DATASIZE)) {
		c->fcmh_sstb.sst_mtime = mp->attr.sst_mtime;
		c->fcmh_sstb.sst_mtime_ns = mp->attr.sst_mtime_ns;
	}

	if (to_set & PSCFS_SETATTRF_DATASIZE) {
		c->fcmh_sstb.sst_size = mp->attr.sst_size;
		c->fcmh_sstb.sst_ctime = mp->attr.sst_ctime;
		c->fcmh_sstb.sst_ctime_ns = mp->attr.sst_ctime_ns;
	}

	fcmh_setattrf(c, &mp->attr, FCMH_SETATTRF_SAVELOCAL |
	    FCMH_SETATTRF_HAVELOCK);

	DEBUG_SSTB(PLL_INFO, &c->fcmh_sstb, "fcmh %p post setattr", c);

#if 0
	if (fcmh_isdir(c) && DIRCACHE_INITIALIZED(c)) {
		struct msl_dc_inv_entry_data mdie;

		mdie.mdie_pfr = pfr;
		mdie.mdie_pinum = fcmh_2_fid(c);
		/* XXX this currently crashes fuse.ko but needs to happen */
		dircache_walk(fcmh_2_dci(c), msl_dc_inv_entry, &mdie);
	}
#endif

 out:
	if (c) {
		FCMH_RLOCK(c);
		if (unset_trunc) {
			c->fcmh_flags &= ~FCMH_CLI_TRUNC;
			fcmh_wake_locked(c);
		}
		c->fcmh_flags &= ~FCMH_IN_SETATTR;
		if (rc && getting_attrs)
			c->fcmh_flags &= ~FCMH_GETTING_ATTRS;
		fcmh_wake_locked(c);
		sl_internalize_stat(&c->fcmh_sstb, stb);

		if (rc && flush_attrs)
			c->fcmh_flags |= FCMH_CLI_DIRTY_ATTRS;
		if (!rc && flush_attrs &&
		    !(c->fcmh_flags & FCMH_CLI_DIRTY_ATTRS)) {
			psc_assert(c->fcmh_flags & FCMH_CLI_DIRTY_QUEUE);
			c->fcmh_flags &= ~FCMH_CLI_DIRTY_QUEUE;
			spinlock(&attrTimeoutQLock);
			fci = fcmh_2_fci(c);
			lc_remove(&attrTimeoutQ, fci);
			freelock(&attrTimeoutQLock);
			fcmh_op_done_type(c, FCMH_OPCNT_DIRTY_QUEUE);
		}
		fcmh_op_done_type(c, FCMH_OPCNT_LOOKUP_FIDC);
	}
	/* XXX if there is no fcmh, what do we do?? */
	pscfs_reply_setattr(pfr, stb, pscfs_attr_timeout, rc);
	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
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
//	exit();
}

void
mslfsop_write(struct pscfs_req *pfr, const void *buf, size_t size,
    off_t off, void *data)
{
	struct msl_fhent *mfh = data;
	struct fidc_membh *f, *ftmp;
	struct fcmh_cli_info *fci;
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

	msfsthr(pscthr_get())->mft_failcnt = 1;
	rc = msl_write(pfr, mfh, buf, size, off);
	if (rc < 0) {
		if (rc == -SLERR_AIOWAIT) {
			return;
		}
		rc = -rc;
		goto out;
	}
	rc = 0;

	FCMH_LOCK(f);
	PFL_GETTIMESPEC(&ts);
	f->fcmh_sstb.sst_mtime = ts.tv_sec;
	f->fcmh_sstb.sst_mtime_ns = ts.tv_nsec;
	if (off + size > fcmh_2_fsz(f)) {
		psclog_info("fid: "SLPRI_FID", "
		    "size from %"PRId64" to %"PRId64,
		    fcmh_2_fid(f), fcmh_2_fsz(f), off+size);
		fcmh_2_fsz(f) = off + size;
	}
	if (!(f->fcmh_flags & FCMH_CLI_DIRTY_ATTRS)) {
		f->fcmh_flags |= FCMH_CLI_DIRTY_ATTRS;
		fci = fcmh_2_fci(f);
		fci->fci_etime.tv_sec = ts.tv_sec;
		fci->fci_etime.tv_nsec = ts.tv_nsec;
		if (!(f->fcmh_flags & FCMH_CLI_DIRTY_QUEUE)) {
			f->fcmh_flags |= FCMH_CLI_DIRTY_QUEUE;
			spinlock(&attrTimeoutQLock);
			lc_addtail(&attrTimeoutQ, fci);
			freelock(&attrTimeoutQLock);
			fcmh_op_start_type(f, FCMH_OPCNT_DIRTY_QUEUE);
		}
	}
	FCMH_ULOCK(f);

	MFH_LOCK(mfh);
	mfh->mfh_nbytes_wr += size;
	MFH_ULOCK(mfh);

 out:
	DEBUG_FCMH(PLL_INFO, f, "write: buf=%p rc=%d sz=%zu "
	    "off=%"PSCPRIdOFFT, buf, rc, size, off);
	pscfs_reply_write(pfr, size, rc);
}

void
mslfsop_read(struct pscfs_req *pfr, size_t size, off_t off, void *data)
{
	struct msl_fhent *mfh = data;
	struct fidc_membh *f, *ftmp;
	void *buf = pfr->pfr_buf;
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

	DEBUG_FCMH(PLL_INFO, f, "read (start): buf=%p rc=%d sz=%zu "
	    "len=%zd off=%"PSCPRIdOFFT, buf, rc, size, len, off);

	if (fcmh_isdir(f)) {
//		psc_fatalx("pscfs gave us a directory");
		rc = EISDIR;
		goto out;
	}

	msfsthr(pscthr_get())->mft_failcnt = 1;

	len = msl_read(pfr, mfh, buf, size, off);

	/*
	 * If this request will be finished asynchronously, e.g. in the
	 * case of a archival storage system, do not tie up this fs
	 * worker thread.
	 */
	if (len == -SLERR_AIOWAIT)
		return;

	if (len < 0)
		rc = -len;
	else {
		MFH_LOCK(mfh);
		mfh->mfh_nbytes_rd += len;
		MFH_ULOCK(mfh);
	}

 out:
	pscfs_reply_read(pfr, buf, len, rc);
	DEBUG_FCMH(PLL_INFO, f, "read (end): buf=%p rc=%d sz=%zu "
	    "len=%zd off=%"PSCPRIdOFFT, buf, rc, size, len, off);
}

static int
mslfsop_flush_attr(struct fidc_membh *fcmh)
{
	struct slashrpc_cservice *csvc;
	struct srm_setattr_req *mq;
	struct srm_setattr_rep *mp;
	struct pscrpc_request *rq;
	int rc;

	rq = NULL;
	csvc = NULL;
	MSL_RMC_NEWREQ_PFCC(NULL, fcmh, csvc, SRMT_SETATTR, rq, mq, mp, rc);
	if (rc)
		return (rc);

	FCMH_LOCK(fcmh);
	mq->attr.sst_fg = fcmh->fcmh_fg;
	mq->to_set = PSCFS_SETATTRF_FLUSH;
	mq->to_set |= PSCFS_SETATTRF_MTIME | PSCFS_SETATTRF_DATASIZE;
	mq->attr.sst_size = fcmh->fcmh_sstb.sst_size;
	mq->attr.sst_mtim = fcmh->fcmh_sstb.sst_mtim;

	psclog_info("fcmh %p setattr%s%s%s%s%s%s%s", fcmh,
	    mq->to_set & PSCFS_SETATTRF_MODE ? " mode" : "",
	    mq->to_set & PSCFS_SETATTRF_UID ? " uid" : "",
	    mq->to_set & PSCFS_SETATTRF_GID ? " gid" : "",
	    mq->to_set & PSCFS_SETATTRF_ATIME ? " atime" : "",
	    mq->to_set & PSCFS_SETATTRF_MTIME ? " mtime" : "",
	    mq->to_set & PSCFS_SETATTRF_CTIME ? " ctime" : "",
	    mq->to_set & PSCFS_SETATTRF_DATASIZE ? " datasize" : "");

	FCMH_ULOCK(fcmh);

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc == 0)
		rc = mp->rc;
	pscrpc_req_finished(rq);
	sl_csvc_decref(csvc);
	return (rc);
}

void
msattrflushthr_main(__unusedx struct psc_thread *thr)
{
	int rc, did_work;
	struct fidc_membh *fcmh;
	struct timespec ts, nexttimeo;
	struct fcmh_cli_info *fci, *tmp_fci;

	while (pscthr_run()) {

		did_work = 0;
		PFL_GETTIMESPEC(&ts);
		nexttimeo.tv_sec = FCMH_ATTR_TIMEO;
		nexttimeo.tv_nsec = 0;

		spinlock(&attrTimeoutQLock);
		LIST_CACHE_FOREACH_SAFE(fci, tmp_fci, &attrTimeoutQ) {

			fcmh = fci_2_fcmh(fci);
			if (!FCMH_TRYLOCK(fcmh))
				continue;
			if (fcmh->fcmh_flags & FCMH_IN_SETATTR) {
				FCMH_ULOCK(fcmh);
				continue;
			}
			psc_assert(fcmh->fcmh_flags & FCMH_CLI_DIRTY_ATTRS);

			if (fci->fci_etime.tv_sec > ts.tv_sec ||
			   (fci->fci_etime.tv_sec == ts.tv_sec &&
			    fci->fci_etime.tv_nsec > ts.tv_nsec)) {
				timespecsub(&fci->fci_etime, &ts, &nexttimeo);
				FCMH_ULOCK(fcmh);
				break;
			}
			fcmh->fcmh_flags |= FCMH_IN_SETATTR;
			fcmh->fcmh_flags &= ~FCMH_CLI_DIRTY_ATTRS;
			FCMH_ULOCK(fcmh);

			freelock(&attrTimeoutQLock);

			rc = mslfsop_flush_attr(fcmh);

			FCMH_LOCK(fcmh);
			fcmh->fcmh_flags &= ~FCMH_IN_SETATTR;
			fcmh_wake_locked(fcmh);
			if (rc) {
				fcmh->fcmh_flags |= FCMH_CLI_DIRTY_ATTRS;
				FCMH_ULOCK(fcmh);
			} else if (!(fcmh->fcmh_flags & FCMH_CLI_DIRTY_ATTRS)) {
				psc_assert(fcmh->fcmh_flags & FCMH_CLI_DIRTY_QUEUE);
				fcmh->fcmh_flags &= ~FCMH_CLI_DIRTY_QUEUE;
				lc_remove(&attrTimeoutQ, fci);
				fcmh_op_done_type(fcmh, FCMH_OPCNT_DIRTY_QUEUE);
			} else
				FCMH_ULOCK(fcmh);

			did_work = 1;
			break;
		}
		if (did_work)
			continue;
		else
			freelock(&attrTimeoutQLock);

		spinlock(&msl_flush_attrqlock);
		psc_waitq_waitrel(&msl_flush_attrq,
		    &msl_flush_attrqlock, &nexttimeo);
	}
}

void
msattrflushthr_spawn(void)
{
	struct msattrfl_thread *mattrft;
	struct psc_thread *thr;

	lc_reginit(&attrTimeoutQ, struct fcmh_cli_info,
	    fci_lentry, "attrTimeout");

	thr = pscthr_init(MSTHRT_ATTRFLSH, 0,
	    msattrflushthr_main, NULL,
	    sizeof(struct msattrfl_thread), "msattrflushthr");
	mattrft = msattrflthr(thr);
	psc_multiwait_init(&mattrft->maft_mw, "%s",
	    thr->pscthr_name);
	pscthr_setready(thr);
}

void
unmount(const char *mp)
{
	char buf[BUFSIZ];
	int rc;

	/* XXX do not let this hang */
	rc = snprintf(buf, sizeof(buf),
	    "umount '%s' || umount -f '%s' || umount -l '%s'",
	    mp, mp, mp);
	if (rc == -1)
		psc_fatal("snprintf: umount %s", mp);
	if (rc >= (int)sizeof(buf))
		psc_fatalx("snprintf: umount %s: too long", mp);
	if (system(buf) == -1)
		psclog_warn("system(%s)", buf);
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

	libsl_init(4096);
	fidc_init(sizeof(struct fcmh_cli_info), FIDC_CLI_DEFSZ);
	bmpc_global_init();
	bmap_cache_init(sizeof(struct bmap_cli_info));
	dircache_init(&dircacheMgr, "dircache", 256 * 1024);

	psc_poolmaster_init(&slc_async_req_poolmaster,
	    struct slc_async_req, car_lentry, PPMF_AUTO, 64, 64, 0,
	    NULL, NULL, NULL, "asyncrq");
	slc_async_req_pool = psc_poolmaster_getmgr(&slc_async_req_poolmaster);

	psc_poolmaster_init(&slc_biorq_poolmaster,
	    struct bmpc_ioreq, biorq_lentry, PPMF_AUTO, 64, 64, 0, NULL,
	    NULL, NULL, "biorq");
	slc_biorq_pool = psc_poolmaster_getmgr(&slc_biorq_poolmaster);

	pndgReadaReqs = pscrpc_nbreqset_init(NULL, NULL);

	slc_rpc_initsvc();

	/* Start up service threads. */
	psc_eqpollthr_spawn(MSTHRT_EQPOLL, "mseqpollthr");
	msctlthr_spawn();
	mstimerthr_spawn();

	psc_iostats_init(&msl_diord_stat,   "dio-rd");
	psc_iostats_init(&msl_diowr_stat,   "dio-wr");
	psc_iostats_init(&msl_rdcache_stat, "rd-cache-hit");
	psc_iostats_init(&msl_racache_stat, "ra-cache-hit");

	psc_iostats_init(&msl_io_1b_stat,   "iosz:0-1k");
	psc_iostats_init(&msl_io_1k_stat,   "iosz:1k-3k");
	psc_iostats_init(&msl_io_4k_stat,   "iosz:4k-15k");
	psc_iostats_init(&msl_io_16k_stat,  "iosz:16k-63k");
	psc_iostats_init(&msl_io_64k_stat,  "iosz:64k-127k");
	psc_iostats_init(&msl_io_128k_stat, "iosz:128k-511k");
	psc_iostats_init(&msl_io_512k_stat, "iosz:512k-1m");
	psc_iostats_init(&msl_io_1m_stat,   "iosz:1m-");

	sl_nbrqset = pscrpc_nbreqset_init(NULL, NULL);
	pscrpc_nbreapthr_spawn(sl_nbrqset, MSTHRT_NBRQ, "msnbrqthr");

	msattrflushthr_spawn();
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
			psclog_warnx("SLASH2_PIOS_ID (%s) does not resolve to "
			    "a valid IOS, defaulting to IOS_ID_ANY", name);
	}
	atexit(unmount_mp);
}

struct pscfs pscfs = {
	mslfsop_access,
	mslfsop_close,
	mslfsop_close,		/* closedir */
	mslfsop_create,
	mslfsop_flush,
	mslfsop_fsync,
	mslfsop_fsync,
	mslfsop_getattr,
	NULL,			/* ioctl */
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

void
parse_allowexe(void)
{
	char *p, *s, *t;
	struct stat stb;

	s = globalConfig.gconf_allowexe;
	while (s) {
		p = s;
		while (isspace(*p))
			p++;
		s = strchr(p, ':');
		if (s)
			*s++ = '\0';
		if (strlen(p)) {
			t = p + strlen(p) - 1;
			if (isspace(*t)) {
				while (isspace(*t))
					t--;
				t[1] = '\0';
			}
		}
		if (*p == '\0')
			continue;
		if (stat(p, &stb) == -1) {
			warn("%s", p);
			continue;
		}

		psc_dynarray_add(&allow_exe, p);
		psclog_notice("restricting open(2) access to %s", p);
	}
}

__dead void
usage(void)
{
	fprintf(stderr,
	    "usage: %s [-dUVX] [-D datadir] [-f conf] [-I iosystem] [-M mds]\n"
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
	psc_subsys_register(SLCSS_INFO, "info");

	pscfs_addarg(&args, "");		/* progname/argv[0] */
	pscfs_addarg(&args, "-o");
	pscfs_addarg(&args, STD_MOUNT_OPTIONS);

	cfg = SL_PATH_CONF;
	p = getenv("CONFIG_FILE");
	if (p)
		cfg = p;

	progname = argv[0];
	while ((c = getopt(argc, argv, "D:df:I:M:o:p:S:UXV")) != -1)
		switch (c) {
		case 'D':
			sl_datadir = optarg;
			break;
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
			nstb_prefetch = (int)l;
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
		case 'V':
			errx(0, "revision is %d", SL_STK_VERSION);
			break;
		default:
			usage();
		}
	argc -= optind;
	argv += optind;
	if (argc != 1)
		usage();

	pscthr_init(MSTHRT_FSMGR, 0, NULL, NULL, 0, "msfsmgrthr");

	sys_upnonce = psc_random32();

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
	parse_allowexe();
	msl_init();

	pscfs_entry_timeout = 8.;
	pscfs_attr_timeout = 8.;

	exit(pscfs_main());
}
