/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2007-2014, Pittsburgh Supercomputing Center (PSC).
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or (at
 * your option) any later version.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License contained in the file
 * `COPYING-GPL' at the top of this distribution or at
 * https://www.gnu.org/licenses/gpl-2.0.html for more details.
 *
 * Pittsburgh Supercomputing Center	phone: 412.268.4960  fax: 412.268.5832
 * 300 S. Craig Street			e-mail: remarks@psc.edu
 * Pittsburgh, PA 15213			web: http://www.psc.edu/
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
#include "pfl/completion.h"
#include "pfl/ctlsvr.h"
#include "pfl/dynarray.h"
#include "pfl/eqpollthr.h"
#include "pfl/fault.h"
#include "pfl/fs.h"
#include "pfl/fsmod.h"
#include "pfl/iostats.h"
#include "pfl/log.h"
#include "pfl/pfl.h"
#include "pfl/random.h"
#include "pfl/rpc.h"
#include "pfl/rpclog.h"
#include "pfl/rsx.h"
#include "pfl/stat.h"
#include "pfl/str.h"
#include "pfl/sys.h"
#include "pfl/thread.h"
#include "pfl/time.h"
#include "pfl/usklndthr.h"
#include "pfl/vbitmap.h"
#include "pfl/workthr.h"

#include "bmap_cli.h"
#include "cache_params.h"
#include "creds.h"
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
	fidc_lookup_load((inum), (fp), pscfs_getclientctx(pfr))

#define msl_peek_fcmh(pfr, inum, fp)					\
	fidc_lookup_peek((inum), (fp), pscfs_getclientctx(pfr))

#define mfh_getfid(mfh)		fcmh_2_fid((mfh)->mfh_fcmh)
#define mfh_getfg(mfh)		(mfh)->mfh_fcmh->fcmh_fg

#define MSL_FLUSH_ATTR_TIMEOUT	8

#define fcmh_reserved(f)	(fcmh_2_fid(f) == SLFID_NS ? EPERM : 0)

struct uid_mapping {
	/* these are 64-bit as limitation of hash API */
	uint64_t		um_key;
	uint64_t		um_val;
	struct psc_hashent	um_hentry;
};

struct psc_waitq		 msl_flush_attrq = PSC_WAITQ_INIT;

struct psc_listcache		 slc_attrtimeoutq;
struct psc_listcache		 slc_readaheadq;

sl_ios_id_t			 prefIOS = IOS_ID_ANY;
const char			*progname;
const char			*ctlsockfn = SL_PATH_MSCTLSOCK;
char				 mountpoint[PATH_MAX];
int				 use_mapfile;
struct psc_dynarray		 allow_exe = DYNARRAY_INIT;

struct psc_vbitmap		 msfsthr_uniqidmap = VBITMAP_INIT_AUTO;
psc_spinlock_t			 msfsthr_uniqidmap_lock = SPINLOCK_INIT;

struct psc_poolmaster		 slc_async_req_poolmaster;
struct psc_poolmgr		*slc_async_req_pool;

struct psc_poolmaster		 slc_biorq_poolmaster;
struct psc_poolmgr		*slc_biorq_pool;

struct psc_poolmaster		 slc_mfh_poolmaster;
struct psc_poolmgr		*slc_mfh_pool;

struct psc_poolmaster		 mfsrq_poolmaster;
struct psc_poolmgr		*mfsrq_pool;

uint32_t			 sl_sys_upnonce;

struct psc_hashtbl		 slc_uidmap_ext;
struct psc_hashtbl		 slc_uidmap_int;

int				 slc_posix_mkgrps;

int
uidmap_ext_cred(struct srt_creds *cr)
{
	struct uid_mapping *um, q;

	if (!use_mapfile)
		return (0);

	q.um_key = cr->scr_uid;
	um = psc_hashtbl_search(&slc_uidmap_ext, NULL, NULL, &q.um_key);
	if (um == NULL)
		return (0);
	cr->scr_uid = um->um_val;
	return (0);
}

int
uidmap_ext_stat(struct srt_stat *sstb)
{
	struct uid_mapping *um, q;

	if (!use_mapfile)
		return (0);

	q.um_key = sstb->sst_uid;
	um = psc_hashtbl_search(&slc_uidmap_ext, NULL, NULL, &q.um_key);
	if (um == NULL)
		return (0);
	sstb->sst_uid = um->um_val;
	return (0);
}

int
uidmap_int_stat(struct srt_stat *sstb)
{
	struct uid_mapping *um, q;

	if (!use_mapfile)
		return (0);

	q.um_key = sstb->sst_uid;
	um = psc_hashtbl_search(&slc_uidmap_int, NULL, NULL, &q.um_key);
	if (um == NULL)
		return (0);
	sstb->sst_uid = um->um_val;
	return (0);
}

int
fcmh_checkcreds_ctx(struct fidc_membh *f,
    const struct pscfs_clientctx *pfcc, const struct pscfs_creds *pcrp,
    int accmode)
{
	int rc, locked;

#ifdef SLOPT_POSIX_ACLS
	rc = sl_fcmh_checkacls(f, pfcc, pcrp, accmode);
	(void)locked;
#else
	locked = FCMH_RLOCK(f);
	rc = checkcreds(&f->fcmh_sstb, pcrp, accmode);
	FCMH_URLOCK(f, locked);
	(void)pfcc;
#endif
	return (rc);
}

int
fcmh_checkcreds(struct fidc_membh *f, struct pscfs_req *pfr,
    const struct pscfs_creds *pcrp, int accmode)
{
	struct pscfs_clientctx *pfcc;

#ifdef SLOPT_POSIX_ACLS
	pfcc = pscfs_getclientctx(pfr);
#else
	pfcc = NULL;
	(void)pfr;
#endif
	return (fcmh_checkcreds_ctx(f, pfcc, pcrp, accmode));
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
msfsthr_ensure(struct pscfs_req *pfr)
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

		thr = pscthr_init(MSTHRT_FS, NULL, msfsthr_teardown,
		    sizeof(*mft), "msfsthr%02zu", id);
		mft = thr->pscthr_private;
		psc_multiwait_init(&mft->mft_mw, "%s",
		    thr->pscthr_name);
		mft->mft_uniqid = id;
		pscthr_setready(thr);
	}
	psc_assert(thr->pscthr_type == MSTHRT_FS);

	mft = thr->pscthr_private;
	mft->mft_pfr = pfr;
}

/**
 * msl_create_fcmh - Create a FID cache member handle based on the
 *	statbuf provided.
 * @pfr: pscfs request.
 * @fg: FID + generation of file.
 * @fp: value-result fcmh.
 */
#define msl_create_fcmh(pfr, fg, fp)					\
	_fidc_lookup(PFL_CALLERINFOSS(SLSS_FCMH), fg,			\
	    FIDC_LOOKUP_CREATE, (fp), pscfs_getclientctx(pfr))

void
mslfsop_access(struct pscfs_req *pfr, pscfs_inum_t inum, int accmode)
{
	struct pscfs_creds pcr;
	struct fidc_membh *c;
	int rc;

	msfsthr_ensure(pfr);

	OPSTAT_INCR(SLC_OPST_ACCESS);

	rc = msl_load_fcmh(pfr, inum, &c);
	if (rc)
		PFL_GOTOERR(out, rc);

	pscfs_getcreds(pfr, &pcr);

	rc = 0;
	FCMH_LOCK(c);
	if (pcr.pcr_uid == 0) {
		if ((accmode & X_OK) && !S_ISDIR(c->fcmh_sstb.sst_mode) &&
		    (c->fcmh_sstb.sst_mode & _S_IXUGO) == 0)
			rc = EACCES;
	} else
		rc = fcmh_checkcreds(c, pfr, &pcr, accmode);
	FCMH_ULOCK(c);

 out:
	pscfs_reply_access(pfr, rc);
	if (c)
		fcmh_op_done(c);
}

#define msl_progallowed(r)						\
	(psc_dynarray_len(&allow_exe) == 0 || _msl_progallowed(r))

int
_msl_progallowed(struct pscfs_req *pfr)
{
	char fn[PATH_MAX], exe[PATH_MAX];
	pid_t pid, ppid;
	const char *p;
	int n, rc;
	FILE *fp;

	ppid = pscfs_getclientctx(pfr)->pfcc_pid;
	do {
		pid = ppid;

		/* we made it to the root; disallow */
		if (pid == 0 || pid == 1)
			return (0);

		snprintf(fn, sizeof(fn), "/proc/%d/exe", pid);
		rc = readlink(fn, exe, sizeof(exe));
		if (rc == -1) {
			psclog_warn("unable to check access on %s", fn);
			return (0);
		}
		exe[rc] = '\0';
		DYNARRAY_FOREACH(p, n, &allow_exe)
		    if (strcmp(exe, p) == 0)
			    return (1);

		snprintf(fn, sizeof(fn), "/proc/%d/stat", pid);
		fp = fopen(fn, "r");
		if (fp == NULL) {
			psclog_warn("unable to read parent PID from %s",
			    fn);
			return (0);
		}
		n = fscanf(fp, "%*d %*s %*c %d ", &ppid);
		fclose(fp);
		if (n != 1) {
			psclog_warn("unable to read parent PID from %s",
			    fn);
			return (0);
		}
	} while (pid != ppid);
	return (0);
}

int
msl_io_convert_errno(int rc)
{
	switch (rc) {
	case -SLERR_ION_OFFLINE:
		rc = ETIMEDOUT;
		break;
	}
	return (rc);
}

void
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
	struct bmap_cli_info *bci;
	struct pscfs_creds pcr;
	struct bmapc_memb *b;
	struct stat stb;
	int rc = 0;

	msfsthr_ensure(pfr);

	OPSTAT_INCR(SLC_OPST_CREAT);

	psc_assert(oflags & O_CREAT);

	if (!msl_progallowed(pfr))
		PFL_GOTOERR(out, rc = EPERM);
	if (strlen(name) == 0)
		PFL_GOTOERR(out, rc = ENOENT);
	if (strlen(name) > SL_NAME_MAX)
		PFL_GOTOERR(out, rc = ENAMETOOLONG);

	rc = msl_load_fcmh(pfr, pinum, &p);
	if (rc)
		PFL_GOTOERR(out, rc);

	if (!fcmh_isdir(p))
		PFL_GOTOERR(out, rc = ENOTDIR);
	rc = fcmh_reserved(p);
	if (rc)
		PFL_GOTOERR(out, rc);

	pscfs_getcreds(pfr, &pcr);
	rc = fcmh_checkcreds(p, pfr, &pcr, W_OK);
	if (rc)
		PFL_GOTOERR(out, rc);

 retry:
	MSL_RMC_NEWREQ(pfr, p, csvc, SRMT_CREATE, rq, mq, mp, rc);
	if (rc)
		PFL_GOTOERR(out, rc);

	mq->mode = !(mode & 0777) ? (0666 & ~pscfs_getumask(pfr)) :
	    mode;
	mq->pfg.fg_fid = pinum;
	mq->pfg.fg_gen = FGEN_ANY;
	mq->prefios[0] = prefIOS;
	mq->creds.scr_uid = pcr.pcr_uid;
	mq->creds.scr_gid = slc_posix_mkgrps ?
	    p->fcmh_sstb.sst_gid : pcr.pcr_gid;
	rc = uidmap_ext_cred(&mq->creds);
	if (rc)
		PFL_GOTOERR(out, rc);
	strlcpy(mq->name, name, sizeof(mq->name));
	PFL_GETPTIMESPEC(&mq->time);

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc && slc_rmc_retry(pfr, &rc))
		goto retry;
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		PFL_GOTOERR(out, rc);

	psclog_diag("pfid="SLPRI_FID" fid="SLPRI_FID" "
	    "mode=%#o name='%s' rc=%d", pinum,
	    mp->cattr.sst_fg.fg_fid, mode, name, rc);

	uidmap_int_stat(&mp->pattr);
	slc_fcmh_setattr(p, &mp->pattr);

	uidmap_int_stat(&mp->cattr);
	rc = msl_create_fcmh(pfr, &mp->cattr.sst_fg, &c);
	if (rc)
		PFL_GOTOERR(out, rc);
	slc_fcmh_setattrf(c, &mp->cattr, FCMH_SETATTRF_NONE);

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

	mfh = msl_fhent_new(pfr, c);
	mfh->mfh_oflags = oflags;
	PFL_GETTIMESPEC(&mfh->mfh_open_time);
	memcpy(&mfh->mfh_open_atime, &c->fcmh_sstb.sst_atime,
	    sizeof(mfh->mfh_open_atime));

	FCMH_LOCK(c);
	sl_internalize_stat(&c->fcmh_sstb, &stb);

	rc = msl_io_convert_errno(mp->rc2);
	if (rc)
		PFL_GOTOERR(out, rc);

	fci = fcmh_2_fci(c);
	// XXX bug fci->fci_inode.reptbl inherited?
	fci->fci_inode.reptbl[0].bs_id = mp->sbd.sbd_ios;
	fci->fci_inode.nrepls = 1;
	// XXX bug fci->fci_inode.flags inherited?
	// XXX bug fci->fci_inode.newreplpol inherited?
	FCMH_ULOCK(c);

	/* XXX this load should be nonblocking so we can reply quickly */
	// no-op?
	rc = bmap_getf(c, 0, SL_WRITE, BMAPGETF_LOAD |
	    BMAPGETF_NORETRIEVE, &b);
	if (rc)
		PFL_GOTOERR(out, rc);

	msl_bmap_reap_init(b, &mp->sbd);

	DEBUG_BMAP(PLL_DIAG, b, "ios(%s) sbd_seq=%"PRId64,
	    libsl_ios2name(mp->sbd.sbd_ios), mp->sbd.sbd_seq);

	bci = bmap_2_bci(b);
	// XXX this is wrong if the fcmh inherited from a dir with a
	// repltbl!
	SL_REPL_SET_BMAP_IOS_STAT(bci->bci_repls, 0, BREPLST_VALID);

	bmap_op_done(b);

 out:
	psclog_diag("create: pfid="SLPRI_FID" name='%s' mode=%#x "
	    "flag=%#o rc=%d", pinum, name, mode, oflags, rc);

	if (c)
		fcmh_op_done(c);
	if (p)
		fcmh_op_done(p);

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
	struct pscfs_creds pcr;
	int rc = 0;

	msfsthr_ensure(pfr);

	OPSTAT_INCR(SLC_OPST_OPEN);

	pscfs_getcreds(pfr, &pcr);

	*mfhp = NULL;

	if (!msl_progallowed(pfr))
		PFL_GOTOERR(out, rc = EPERM);

	rc = msl_load_fcmh(pfr, inum, &c);
	if (rc)
		PFL_GOTOERR(out, rc);

	if ((oflags & O_ACCMODE) != O_WRONLY) {
		rc = fcmh_checkcreds(c, pfr, &pcr, R_OK);
		if (rc)
			PFL_GOTOERR(out, rc);
	}
	if (oflags & (O_WRONLY | O_RDWR)) {
		rc = fcmh_checkcreds(c, pfr, &pcr, W_OK);
		if (rc)
			PFL_GOTOERR(out, rc);
	}

	/* Perform rudimentary directory sanity checks. */
	if (fcmh_isdir(c)) {
		/* pscfs shouldn't ever pass us WR with a dir */
		psc_assert((oflags & (O_WRONLY | O_RDWR)) == 0);
		if (!(oflags & O_DIRECTORY))
			PFL_GOTOERR(out, rc = EISDIR);
	} else {
		if (oflags & O_DIRECTORY)
			PFL_GOTOERR(out, rc = ENOTDIR);
	}

	*mfhp = msl_fhent_new(pfr, c);
	(*mfhp)->mfh_oflags = oflags;
	PFL_GETTIMESPEC(&(*mfhp)->mfh_open_time);
	memcpy(&(*mfhp)->mfh_open_atime, &c->fcmh_sstb.sst_atime,
	    sizeof((*mfhp)->mfh_open_atime));

	if (oflags & O_DIRECTORY)
		*rflags |= PSCFS_OPENF_KEEPCACHE;

	/*
	 * PSCFS direct_io does not work with mmap(MAP_SHARED), which is
	 * what the kernel uses under the hood when running executables,
	 * so disable it for this case.
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
		DEBUG_FCMH(PLL_DIAG, c, "new mfh=%p dir=%s rc=%d oflags=%#o",
		    *mfhp, (oflags & O_DIRECTORY) ? "yes" : "no", rc, oflags);
		fcmh_op_done(c);
	}
	return (rc);
}

void
mslfsop_open(struct pscfs_req *pfr, pscfs_inum_t inum, int oflags)
{
	struct msl_fhent *mfh;
	int rflags, rc;

	rflags = 0;
	rc = msl_open(pfr, inum, oflags, &mfh, &rflags);
	pscfs_reply_open(pfr, mfh, rflags, rc);
}

void
mslfsop_opendir(struct pscfs_req *pfr, pscfs_inum_t inum, int oflags)
{
	struct msl_fhent *mfh;
	int rflags, rc;

	rflags = 0;
	rc = msl_open(pfr, inum, oflags | O_DIRECTORY, &mfh, &rflags);
	pscfs_reply_opendir(pfr, mfh, rflags, rc);
}

int
msl_stat(struct fidc_membh *f, void *arg)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscfs_clientctx *pfcc = arg;
	struct pscrpc_request *rq = NULL;
	struct srm_getattr_req *mq;
	struct srm_getattr_rep *mp;
	struct fcmh_cli_info *fci;
	struct timeval now;
	int rc = 0;

	/*
	 * Special case to handle accesses to
	 * /$mountpoint/.slfidns/<fid>
	 */
	if (fcmh_2_fid(f) == SLFID_NS) {
		struct srt_stat sstb;

		memset(&sstb, 0, sizeof(sstb));
		sstb.sst_fid = SLFID_NS;
		sstb.sst_mode = S_IFDIR | 0111;
		sstb.sst_nlink = 2;
		sstb.sst_size = 2;
		sstb.sst_blksize = MSL_FS_BLKSIZ;
		sstb.sst_blocks = 4;
		slc_fcmh_setattrf(f, &sstb, 0);
		return (0);
	}

	fci = fcmh_2_fci(f);

	FCMH_LOCK(f);
	fcmh_wait_locked(f, f->fcmh_flags & FCMH_GETTING_ATTRS);

	if (f->fcmh_flags & FCMH_HAVE_ATTRS) {
		PFL_GETTIMEVAL(&now);
		if (timercmp(&now, &fci->fci_age, <)) {
			DEBUG_FCMH(PLL_DIAG, f,
			    "attrs retrieved from local cache");
			FCMH_ULOCK(f);
			return (0);
		}
	}

	/* Attrs have expired or do not exist. */
	f->fcmh_flags |= FCMH_GETTING_ATTRS;
	FCMH_ULOCK(f);

	do {
		MSL_RMC_NEWREQ_PFCC(pfcc, f, csvc, SRMT_GETATTR, rq, mq,
		    mp, rc);
		if (rc)
			break;

		mq->fg = f->fcmh_fg;
		mq->iosid = prefIOS;

		rc = SL_RSX_WAITREP(csvc, rq, mp);
	} while (rc && slc_rmc_retry_pfcc(pfcc, &rc));

	if (rc == 0) {
		rc = mp->rc;
		uidmap_int_stat(&mp->attr);
	}

	FCMH_LOCK(f);
	if (!rc && fcmh_2_fid(f) != mp->attr.sst_fid)
		rc = EBADF;
	if (!rc) {
		slc_fcmh_setattrf(f, &mp->attr,
		    FCMH_SETATTRF_SAVELOCAL | FCMH_SETATTRF_HAVELOCK);
		fci->fci_xattrsize = mp->xattrsize;
	}

	f->fcmh_flags &= ~FCMH_GETTING_ATTRS;
	fcmh_wake_locked(f);

	if (rq)
		pscrpc_req_finished(rq);

	DEBUG_FCMH(PLL_DEBUG, f, "attrs retrieved via rpc rc=%d", rc);

	FCMH_ULOCK(f);
	if (csvc)
		sl_csvc_decref(csvc);
	return (rc);
}

void
mslfsop_getattr(struct pscfs_req *pfr, pscfs_inum_t inum)
{
	struct pscfs_creds pcr;
	struct fidc_membh *f;
	struct stat stb;
	int rc;

	msfsthr_ensure(pfr);

	OPSTAT_INCR(SLC_OPST_GETATTR);

	pscfs_getcreds(pfr, &pcr);

	/*
	 * Lookup and possibly create a new fidcache handle for inum.
	 * If the FID does not exist in the cache then a placeholder
	 * will be allocated.  msl_stat() will detect incomplete attrs
	 * via FCMH_GETTING_ATTRS flag and RPC for them.
	 */
	rc = msl_load_fcmh(pfr, inum, &f);
	if (rc)
		PFL_GOTOERR(out, rc);

	rc = msl_stat(f, pfr);
	if (rc)
		PFL_GOTOERR(out, rc);

	if (!fcmh_isdir(f))
		f->fcmh_sstb.sst_blksize = MSL_FS_BLKSIZ;

	FCMH_LOCK(f);
	sl_internalize_stat(&f->fcmh_sstb, &stb);

 out:
	if (f)
		fcmh_op_done(f);
	pscfs_reply_getattr(pfr, &stb, pscfs_attr_timeout, rc);
	DEBUG_STATBUF(rc ? PLL_INFO : PLL_DIAG, &stb, "getattr rc=%d",
	    rc);
}

void
mslfsop_link(struct pscfs_req *pfr, pscfs_inum_t c_inum,
    pscfs_inum_t p_inum, const char *newname)
{
	struct fidc_membh *p = NULL, *c = NULL;
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_link_rep *mp = NULL;
	struct srm_link_req *mq;
	struct pscfs_creds pcr;
	struct stat stb;
	int rc = 0;

	msfsthr_ensure(pfr);

	OPSTAT_INCR(SLC_OPST_LINK);

	if (strlen(newname) == 0)
		PFL_GOTOERR(out, rc = ENOENT);
	if (strlen(newname) > SL_NAME_MAX)
		PFL_GOTOERR(out, rc = ENAMETOOLONG);

	pscfs_getcreds(pfr, &pcr);

	/* Check the parent inode. */
	rc = msl_load_fcmh(pfr, p_inum, &p);
	if (rc)
		PFL_GOTOERR(out, rc);

	if (!fcmh_isdir(p))
		PFL_GOTOERR(out, rc = ENOTDIR);
	rc = fcmh_reserved(p);
	if (rc)
		PFL_GOTOERR(out, rc);

	/* XXX this is wrong, it needs to check sticky */
	rc = fcmh_checkcreds(p, pfr, &pcr, W_OK);
	if (rc)
		PFL_GOTOERR(out, rc);

	/* XXX missing checks/return conditions
	 *
	 * [EACCES]     The current process cannot access the existing file.
	 */

	/* Check the child inode. */
	rc = msl_load_fcmh(pfr, c_inum, &c);
	if (rc)
		PFL_GOTOERR(out, rc);

	if (fcmh_isdir(c))
		PFL_GOTOERR(out, rc = EISDIR);

 retry:
	MSL_RMC_NEWREQ(pfr, p, csvc, SRMT_LINK, rq, mq, mp, rc);
	if (rc)
		PFL_GOTOERR(out, rc);

	mq->pfg = p->fcmh_fg;
	mq->fg = c->fcmh_fg;
	strlcpy(mq->name, newname, sizeof(mq->name));

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc && slc_rmc_retry(pfr, &rc))
		goto retry;
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		PFL_GOTOERR(out, rc);

	uidmap_int_stat(&mp->pattr);
	slc_fcmh_setattr(p, &mp->pattr);

	FCMH_LOCK(c);
	uidmap_int_stat(&mp->cattr);
	slc_fcmh_setattrf(c, &mp->cattr, FCMH_SETATTRF_SAVELOCAL |
	    FCMH_SETATTRF_HAVELOCK);
	sl_internalize_stat(&c->fcmh_sstb, &stb);

 out:
	if (c)
		fcmh_op_done(c);
	if (p)
		fcmh_op_done(p);

	pscfs_reply_link(pfr, mp ? mp->cattr.sst_fid : 0,
	    mp ? mp->cattr.sst_gen : 0, pscfs_entry_timeout, &stb,
	    pscfs_attr_timeout, rc);

	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

void
mslfsop_mkdir(struct pscfs_req *pfr, pscfs_inum_t pinum,
    const char *name, mode_t mode)
{
	struct fidc_membh *c = NULL, *p = NULL;
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_mkdir_rep *mp = NULL;
	struct srm_mkdir_req *mq;
	struct pscfs_creds pcr;
	struct stat stb;
	int rc;

	msfsthr_ensure(pfr);

	OPSTAT_INCR(SLC_OPST_MKDIR);

	if (strlen(name) == 0)
		PFL_GOTOERR(out, rc = ENOENT);
	if (strlen(name) > SL_NAME_MAX)
		PFL_GOTOERR(out, rc = ENAMETOOLONG);

	rc = msl_load_fcmh(pfr, pinum, &p);
	if (rc)
		PFL_GOTOERR(out, rc);

	if (!fcmh_isdir(p))
		PFL_GOTOERR(out, rc = ENOTDIR);
	rc = fcmh_reserved(p);
	if (rc)
		PFL_GOTOERR(out, rc);

	pscfs_getcreds(pfr, &pcr);

 retry:
	MSL_RMC_NEWREQ(pfr, p, csvc, SRMT_MKDIR, rq, mq, mp, rc);
	if (rc)
		PFL_GOTOERR(out, rc);

	mq->pfg.fg_fid = pinum;
	mq->pfg.fg_gen = FGEN_ANY;
	mq->sstb.sst_uid = pcr.pcr_uid;
	mq->sstb.sst_gid = slc_posix_mkgrps ?
	    p->fcmh_sstb.sst_gid : pcr.pcr_gid;
	rc = uidmap_ext_stat(&mq->sstb);
	if (rc)
		PFL_GOTOERR(out, rc);
	mq->sstb.sst_mode = mode;
	mq->to_set = PSCFS_SETATTRF_MODE;
	strlcpy(mq->name, name, sizeof(mq->name));

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc && slc_rmc_retry(pfr, &rc))
		goto retry;
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		PFL_GOTOERR(out, rc);

	uidmap_int_stat(&mp->pattr);
	slc_fcmh_setattr(p, &mp->pattr);

	uidmap_int_stat(&mp->cattr);
	rc = msl_create_fcmh(pfr, &mp->cattr.sst_fg, &c);
	if (rc)
		PFL_GOTOERR(out, rc);
	slc_fcmh_setattrf(c, &mp->cattr, FCMH_SETATTRF_NONE);

	sl_internalize_stat(&mp->cattr, &stb);

 out:
	psclog_diag("mkdir: pfid="SLPRI_FID", cfid="SLPRI_FID", "
	    "mode=%#o, name='%s', rc=%d",
	    pinum, c ? c->fcmh_sstb.sst_fid : FID_ANY, mode, name, rc);

	if (c)
		fcmh_op_done(c);
	if (p)
		fcmh_op_done(p);

	pscfs_reply_mkdir(pfr, mp ? mp->cattr.sst_fid : 0,
	    mp ? mp->cattr.sst_gen : 0, pscfs_entry_timeout, &stb,
	    pscfs_attr_timeout, -rc);

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
	struct fidc_membh *f = NULL;
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
		PFL_GOTOERR(out, rc);

	mq->pfg.fg_fid = pinum;
	mq->pfg.fg_gen = FGEN_ANY;
	strlcpy(mq->name, name, sizeof(mq->name));

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc && slc_rmc_retry(pfr, &rc))
		goto retry;
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		PFL_GOTOERR(out, rc);

	/*
	 * Add the inode to the cache first, otherwise pscfs may come to
	 * us with another request for the inode since it won't yet be
	 * visible in the cache.
	 */
	uidmap_int_stat(&mp->attr);
	rc = msl_create_fcmh(pfr, &mp->attr.sst_fg, &f);
	if (rc)
		PFL_GOTOERR(out, rc);

	if (fgp)
		*fgp = mp->attr.sst_fg;

	FCMH_LOCK(f);
	slc_fcmh_setattrf(f, &mp->attr, FCMH_SETATTRF_SAVELOCAL |
	    FCMH_SETATTRF_HAVELOCK);
	fcmh_2_fci(f)->fci_xattrsize = mp->xattrsize;

	if (sstb)
		*sstb = f->fcmh_sstb;

	// XXX add to dircache

 out:
	psclog_diag("lookup: pfid="SLPRI_FID" name='%s' "
	    "cfid="SLPRI_FID" rc=%d",
	    pinum, name, f ? f->fcmh_sstb.sst_fid : FID_ANY, rc);

	if (rc == 0 && fp) {
		*fp = f;
		FCMH_ULOCK(f);
	} else if (f)
		fcmh_op_done(f);
	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
	return (rc);
}

int
msl_readdir_issue(struct pscfs_clientctx *, struct fidc_membh *, off_t,
    size_t, int);

int
slc_wk_issue_readdir(void *p)
{
	struct slc_wkdata_readdir *wk = p;

	msl_readdir_issue(NULL, wk->d, wk->off, wk->size, 0);
	FCMH_LOCK(wk->d);
	wk->pg->dcp_refcnt--;
	fcmh_op_done_type(wk->d, FCMH_OPCNT_WORKER);
	return (0);
}

/**
 * Register a 'miss' in the FID namespace lookup cache.
 * If we reach a threshold, we issue an asynchronous READDIR in hopes
 * that we will hit subsequent requests.
 */
void
lookup_cache_tally_miss(struct fidc_membh *p, off_t off)
{
	struct fcmh_cli_info *pi = fcmh_2_fci(p);
	struct slc_wkdata_readdir *wk = NULL;
	struct timeval ts, delta;
	struct dircache_page *np;
	int ra = 0;

	FCMH_LOCK(p);
	PFL_GETTIMEVAL(&ts);
	timersub(&ts, &pi->fcid_lookup_age, &delta);
	if (delta.tv_sec > 1) {
		pi->fcid_lookup_age = ts;
		pi->fcid_lookup_misses = pi->fcid_lookup_misses >>
		    delta.tv_sec;
	}
	pi->fcid_lookup_misses += DIR_LOOKUP_MISSES_INCR;
	if (pi->fcid_lookup_misses >= DIR_LOOKUP_MISSES_THRES)
		ra = 1;
	FCMH_ULOCK(p);

	if (!ra)
		return;

	np = dircache_new_page(p, off, 0);
	if (np == NULL)
		return;

	wk = pfl_workq_getitem(slc_wk_issue_readdir,
	    struct slc_wkdata_readdir);
	fcmh_op_start_type(p, FCMH_OPCNT_WORKER);
	wk->d = p;
	wk->pg = np;
	wk->off = off;
	wk->size = 32 * 1024;
	pfl_workq_putitem(wk);
}

__static int
msl_lookup_fidcache(struct pscfs_req *pfr,
    const struct pscfs_creds *pcrp, pscfs_inum_t pinum,
    const char *name, struct slash_fidgen *fgp, struct srt_stat *sstb,
    struct fidc_membh **fp)
{
	struct fidc_membh *p, *c = NULL;
	slfid_t cfid = FID_ANY;
	int remote = 0, rc;
	off_t nextoff;

	if (fp)
		*fp = NULL;

#define MSL_FIDNS_RPATH	".slfidns"
	if (pinum == SLFID_ROOT && strcmp(name, MSL_FIDNS_RPATH) == 0) {
		struct fidc_membh f;

		memset(&f, 0, sizeof(f));
		INIT_SPINLOCK(&f.fcmh_lock);
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

		if (pcrp->pcr_uid)
			return (EACCES);

		fid = strtoll(name, &endp, 16);
		if (endp == name || *endp != '\0')
			return (ENOENT);
		rc = msl_load_fcmh(pfr, fid, &c);
		if (rc)
			return (-rc);
		if (fgp)
			*fgp = c->fcmh_fg;
		if (sstb) {
			FCMH_LOCK(c);
			*sstb = c->fcmh_sstb;
		}
		PFL_GOTOERR(out, rc);
	}

	rc = msl_load_fcmh(pfr, pinum, &p);
	if (rc)
		PFL_GOTOERR(out, rc);

	rc = fcmh_checkcreds(p, pfr, pcrp, X_OK);
	if (rc) {
		fcmh_op_done(p);
		PFL_GOTOERR(out, rc);
	}

	FCMH_LOCK(p);
	cfid = dircache_lookup(p, name, &nextoff);
	FCMH_ULOCK(p);
	if (cfid == FID_ANY || fidc_lookup_fid(cfid, &c)) {
		OPSTAT_INCR(SLC_OPST_DIRCACHE_LOOKUP_MISS);
		lookup_cache_tally_miss(p, nextoff);
		remote = 1;
	}

	fcmh_op_done(p);

	if (remote)
		return (msl_lookuprpc(pfr, pinum, name, fgp, sstb, fp));

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

 out:
	if (rc == 0 && fp)
		*fp = c;
	else if (c)
		fcmh_op_done(c);

	psclog_diag("look for file: %s under inode: "SLPRI_FID" rc=%d",
	    name, pinum, rc);

	return (rc);
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
	struct pscfs_creds pcr;
	int rc;

	msfsthr_ensure(pfr);

	if (strlen(name) == 0)
		PFL_GOTOERR(out, rc = ENOENT);
	if (strlen(name) > SL_NAME_MAX)
		PFL_GOTOERR(out, rc = ENAMETOOLONG);

	rc = msl_load_fcmh(pfr, pinum, &p);
	if (rc)
		PFL_GOTOERR(out, rc);
	rc = fcmh_reserved(p);
	if (rc)
		PFL_GOTOERR(out, rc);

	pscfs_getcreds(pfr, &pcr);

#if 0

	/* XXX this code causes svn checkout fails with I/O errors */

	struct srt_stat sstb;

	rc = msl_lookup_fidcache(pfr, &pcr, pinum, name,
	    NULL, &sstb, NULL);
	if (rc)
		PFL_GOTOERR(out, rc);

	FCMH_LOCK(p);
	if ((p->fcmh_sstb.sst_mode & S_ISVTX) && pcr.pcr_uid) {
		if (p->fcmh_sstb.sst_uid != pcr.pcr_uid) {
			struct srt_stat sstb;

			FCMH_ULOCK(p);


			if (sstb.sst_uid != pcr.pcr_uid)
				rc = EPERM;
		} else
			FCMH_ULOCK(p);
	} else {
		rc = fcmh_checkcreds(p, pfr, &pcr, W_OK);
		FCMH_ULOCK(p);
	}
	if (rc)
		PFL_GOTOERR(out, rc);

#else

	FCMH_LOCK(p);
	if ((p->fcmh_sstb.sst_mode & S_ISVTX) && pcr.pcr_uid) {
		if (p->fcmh_sstb.sst_uid != pcr.pcr_uid) {
			struct srt_stat sstb;

			FCMH_ULOCK(p);

			rc = msl_lookup_fidcache(pfr, &pcr, pinum, name,
			    NULL, &sstb, NULL);
			if (rc)
				PFL_GOTOERR(out, rc);

			if (sstb.sst_uid != pcr.pcr_uid)
				rc = EPERM;
		} else
			FCMH_ULOCK(p);
	} else {
		rc = fcmh_checkcreds(p, pfr, &pcr, W_OK);
		FCMH_ULOCK(p);
	}
	if (rc)
		PFL_GOTOERR(out, rc);

#endif

 retry:
	MSL_RMC_NEWREQ(pfr, p, csvc, isfile ? SRMT_UNLINK : SRMT_RMDIR,
	    rq, mq, mp, rc);
	if (rc)
		PFL_GOTOERR(out, rc);

	mq->pfid = pinum;
	strlcpy(mq->name, name, sizeof(mq->name));

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc && slc_rmc_retry(pfr, &rc))
		goto retry;
	if (rc)
		PFL_GOTOERR(out, rc);
	if (rc == 0)
		rc = mp->rc;

	if (!rc) {
		int tmprc;

		FCMH_LOCK(p);
		uidmap_int_stat(&mp->pattr);
		slc_fcmh_setattr_locked(p, &mp->pattr);
		FCMH_ULOCK(p);

		tmprc = msl_peek_fcmh(pfr, mp->cattr.sst_fid, &c);
		if (!tmprc) {
			if (mp->valid) {
				uidmap_int_stat(&mp->cattr);
				slc_fcmh_setattrf(c, &mp->cattr,
				    FCMH_SETATTRF_SAVELOCAL);
			} else {
				FCMH_LOCK(c);
				c->fcmh_flags |= FCMH_DELETED;
				OPSTAT_INCR(SLC_OPST_DELETE_MARKED);
			}
		} else
			OPSTAT_INCR(SLC_OPST_DELETE_SKIPPED);
	}

	psclog_diag("delete: pinum="SLPRI_FID" fid="SLPRI_FG" valid=%d "
	    "name='%s' isfile=%d rc=%d",
	    pinum, SLPRI_FG_ARGS(&mp->cattr.sst_fg), mp->valid, name,
	    isfile, rc);

 out:
	if (c)
		fcmh_op_done(c);
	if (p)
		fcmh_op_done(p);
	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
	return (rc);
}

void
mslfsop_unlink(struct pscfs_req *pfr, pscfs_inum_t pinum,
    const char *name)
{
	OPSTAT_INCR(SLC_OPST_UNLINK);
	pscfs_reply_unlink(pfr, msl_delete(pfr, pinum, name, 1));
}

void
mslfsop_rmdir(struct pscfs_req *pfr, pscfs_inum_t pinum,
    const char *name)
{
	OPSTAT_INCR(SLC_OPST_RMDIR);
	pscfs_reply_unlink(pfr, msl_delete(pfr, pinum, name, 0));
}

void
mslfsop_mknod(struct pscfs_req *pfr, pscfs_inum_t pinum,
    const char *name, mode_t mode, dev_t rdev)
{
	struct fidc_membh *p = NULL, *c = NULL;
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_mknod_rep *mp = NULL;
	struct srm_mknod_req *mq = NULL;
	struct pscfs_creds pcr;
	struct stat stb;
	int rc;

	msfsthr_ensure(pfr);

	OPSTAT_INCR(SLC_OPST_MKNOD);

	if (!S_ISFIFO(mode))
		PFL_GOTOERR(out, rc = ENOTSUP);
	if (strlen(name) == 0)
		PFL_GOTOERR(out, rc = ENOENT);
	if (strlen(name) > SL_NAME_MAX)
		PFL_GOTOERR(out, rc = ENAMETOOLONG);

	rc = msl_load_fcmh(pfr, pinum, &p);
	if (rc)
		PFL_GOTOERR(out, rc);

	if (!fcmh_isdir(p))
		PFL_GOTOERR(out, rc = ENOTDIR);
	rc = fcmh_reserved(p);
	if (rc)
		PFL_GOTOERR(out, rc);

	pscfs_getcreds(pfr, &pcr);

	rc = fcmh_checkcreds(p, pfr, &pcr, W_OK);
	if (rc)
		PFL_GOTOERR(out, rc);

 retry:
	MSL_RMC_NEWREQ(pfr, p, csvc, SRMT_MKNOD, rq, mq, mp, rc);
	if (rc)
		PFL_GOTOERR(out, rc);

	mq->creds.scr_uid = pcr.pcr_uid;
	mq->creds.scr_gid = slc_posix_mkgrps ?
	    p->fcmh_sstb.sst_gid : pcr.pcr_gid;
	rc = uidmap_ext_cred(&mq->creds);
	if (rc)
		PFL_GOTOERR(out, rc);
	mq->pfg.fg_fid = pinum;
	mq->pfg.fg_gen = FGEN_ANY;
	mq->mode = mode;
	mq->rdev = rdev;
	strlcpy(mq->name, name, sizeof(mq->name));

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc && slc_rmc_retry(pfr, &rc))
		goto retry;
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		PFL_GOTOERR(out, rc);

	psclog_info("pfid="SLPRI_FID" mode=%#o name='%s' rc=%d mp->rc=%d",
	    mq->pfg.fg_fid, mq->mode, mq->name, rc, mp->rc);

	uidmap_int_stat(&mp->pattr);
	slc_fcmh_setattr(p, &mp->pattr);

	uidmap_int_stat(&mp->cattr);
	rc = msl_create_fcmh(pfr, &mp->cattr.sst_fg, &c);
	if (rc)
		PFL_GOTOERR(out, rc);

	slc_fcmh_setattrf(c, &mp->cattr, FCMH_SETATTRF_NONE);
	sl_internalize_stat(&mp->cattr, &stb);

 out:
	pscfs_reply_mknod(pfr, mp ? mp->cattr.sst_fid : 0,
	    mp ? mp->cattr.sst_gen : 0, pscfs_entry_timeout, &stb,
	    pscfs_attr_timeout, rc);
	if (c)
		fcmh_op_done(c);
	if (p)
		fcmh_op_done(p);
	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

void
msl_readdir_error(struct fidc_membh *d, struct dircache_page *p, int rc)
{
	FCMH_LOCK(d);
	p->dcp_refcnt--;
	PFLOG_DIRCACHEPG(PLL_DEBUG, p, "error rc=%d", rc);
	if (p->dcp_flags & DIRCACHEPGF_LOADING) {
		p->dcp_flags &= ~DIRCACHEPGF_LOADING;
		p->dcp_flags |= DIRCACHEPGF_LOADED;
		p->dcp_rc = rc;
		PFL_GETPTIMESPEC(&p->dcp_local_tm);
		p->dcp_remote_tm = d->fcmh_sstb.sst_mtim;
		fcmh_wake_locked(d);
	}
	FCMH_ULOCK(d);
}

void
msl_readdir_finish(struct fidc_membh *d, struct dircache_page *p,
    int eof, int nents, int size, struct iovec *iov)
{
	struct srt_readdir_ent *e;
	struct fidc_membh *f;
	int i;

	for (i = 0, e = iov[1].iov_base; i < nents; i++, e++) {
		if (e->sstb.sst_fid == FID_ANY ||
		    e->sstb.sst_fid == 0) {
			DEBUG_SSTB(PLL_WARN, &e->sstb,
			    "invalid readdir prefetch FID "
			    "parent@%p="SLPRI_FID, d, fcmh_2_fid(d));
			continue;
		}

		DEBUG_SSTB(PLL_DEBUG, &e->sstb, "prefetched");

		uidmap_int_stat(&e->sstb);

		fidc_lookup(&e->sstb.sst_fg, FIDC_LOOKUP_CREATE, &f);

		if (f) {
			FCMH_LOCK(f);
			slc_fcmh_setattrf(f, &e->sstb,
			    FCMH_SETATTRF_SAVELOCAL |
			    FCMH_SETATTRF_HAVELOCK);
			fcmh_2_fci(f)->fci_xattrsize =
			    e->xattrsize;
			fcmh_op_done(f);
		}
	}
	PFLOG_DIRCACHEPG(PLL_DEBUG, p, "registering");
	dircache_reg_ents(d, p, nents, iov[0].iov_base, size, eof);
}

int
msl_readdir_cb(struct pscrpc_request *rq, struct pscrpc_async_args *av)
{
	struct slashrpc_cservice *csvc = av->pointer_arg[MSL_READDIR_CBARG_CSVC];
	struct dircache_page *p = av->pointer_arg[MSL_READDIR_CBARG_PAGE];
	struct fidc_membh *d = av->pointer_arg[MSL_READDIR_CBARG_FCMH];
	struct srm_readdir_req *mq;
	struct srm_readdir_rep *mp;
	int rc;

	SL_GET_RQ_STATUS_TYPE(csvc, rq, struct srm_readdir_rep, rc);

	if (rc) {
		DEBUG_REQ(PLL_ERROR, rq, "rc=%d", rc);
		msl_readdir_error(d, p, rc);
	} else {
		slrpc_rep_in(csvc, rq);
		mq = pscrpc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq));
		mp = pscrpc_msg_buf(rq->rq_repmsg, 0, sizeof(*mp));
		if (SRM_READDIR_BUFSZ(mp->size, mp->num) <=
		    sizeof(mp->ents)) {
			struct iovec iov[2];

			iov[0].iov_base = PSCALLOC(mp->size);
			memcpy(iov[0].iov_base, mp->ents, mp->size);
			iov[1].iov_base = mp->ents + mp->size;
			msl_readdir_finish(d, p, mp->eof, mp->num,
			    mp->size, iov);
		} else {
			FCMH_LOCK(d);
			p->dcp_refcnt--;
			PFLOG_DIRCACHEPG(PLL_DEBUG, p, "decr");
		}
	}
	fcmh_op_done_type(d, FCMH_OPCNT_READDIR);
	sl_csvc_decref(csvc);
	return (0);
}

int
msl_readdir_issue(struct pscfs_clientctx *pfcc, struct fidc_membh *d,
    off_t off, size_t size, int wait)
{
	struct slashrpc_cservice *csvc = NULL;
	struct srm_readdir_req *mq = NULL;
	struct srm_readdir_rep *mp = NULL;
	struct pscrpc_request *rq = NULL;
	struct dircache_page *p;
	int rc;

	p = dircache_new_page(d, off, wait);
	if (p == NULL)
		return (-ESRCH);

	fcmh_op_start_type(d, FCMH_OPCNT_READDIR);

	MSL_RMC_NEWREQ_PFCC(pfcc, d, csvc, SRMT_READDIR, rq, mq, mp,
	    rc);
	if (rc)
		PFL_GOTOERR(out, rc);

	mq->fg = d->fcmh_fg;
	mq->size = size;
	mq->offset = off;

	rq->rq_interpret_reply = msl_readdir_cb;
	rq->rq_async_args.pointer_arg[MSL_READDIR_CBARG_CSVC] = csvc;
	rq->rq_async_args.pointer_arg[MSL_READDIR_CBARG_FCMH] = d;
	rq->rq_async_args.pointer_arg[MSL_READDIR_CBARG_PAGE] = p;
	PFLOG_DIRCACHEPG(PLL_DEBUG, p, "issuing");
	rc = SL_NBRQSET_ADD(csvc, rq);
	if (!rc)
		return (0);

	pscrpc_req_finished(rq);
	sl_csvc_decref(csvc);

 out:
	FCMH_LOCK(d);
	dircache_free_page(d, p);
	fcmh_op_done_type(d, FCMH_OPCNT_READDIR);
	return (rc);
}

void
mslfsop_readdir(struct pscfs_req *pfr, size_t size, off_t off,
    void *data)
{
	int hit = 1, j, nd, issue, rc;
	struct dircache_page *p, *np;
	struct pscfs_clientctx *pfcc;
	struct msl_fhent *mfh = data;
	struct dircache_expire dexp;
	struct fcmh_cli_info *fci;
	struct pscfs_dirent *pfd;
	struct pscfs_creds pcr;
	struct fidc_membh *d;

	msfsthr_ensure(pfr);

	OPSTAT_INCR(SLC_OPST_READDIR);

	if (off < 0 || size > 1024 * 1024)
		PFL_GOTOERR(out, rc = EINVAL);

	pfcc = pscfs_getclientctx(pfr);

	d = mfh->mfh_fcmh;
	psc_assert(d);

	if (!fcmh_isdir(d)) {
		DEBUG_FCMH(PLL_ERROR, d,
		    "inconsistency: readdir on a non-dir");
		PFL_GOTOERR(out, rc = ENOTDIR);
	}
	rc = fcmh_reserved(d);
	if (rc)
		PFL_GOTOERR(out, rc);

	pscfs_getcreds(pfr, &pcr);

	rc = fcmh_checkcreds(d, pfr, &pcr, R_OK);
	if (rc)
		PFL_GOTOERR(out, rc);

	FCMH_LOCK(d);
	fci = fcmh_2_fci(d);

 restart:
	DIRCACHEPG_INITEXP(&dexp);

	issue = 1;
	PLL_FOREACH_SAFE(p, np, &fci->fci_dc_pages) {

		if (p->dcp_flags & DIRCACHEPGF_LOADING) {
			/// XXX need to wake up if csvc fails
			OPSTAT_INCR(SLC_OPST_DIRCACHE_WAIT);
			fcmh_wait_nocond_locked(d);
			goto restart;
		}
		if (DIRCACHEPG_EXPIRED(d, p, &dexp)) {
			dircache_free_page(d, p);
			continue;
		}

		/* We found the last page; return EOF. */
		if (off == p->dcp_nextoff &&
		    p->dcp_flags & DIRCACHEPGF_EOF) {
			FCMH_ULOCK(d);
			OPSTAT_INCR(SLC_OPST_DIRCACHE_HIT_EOF);
			pscfs_reply_readdir(pfr, NULL, 0, rc);
			return;
		}

		if (dircache_hasoff(p, off)) {
			if (p->dcp_rc) {
				rc = p->dcp_rc;
				dircache_free_page(d, p);
				if (!slc_rmc_retry(pfr, &rc)) {
					FCMH_ULOCK(d);
					pscfs_reply_readdir(pfr, NULL,
					    0, rc);
					return;
				}
				break;
			} else {
				off_t poff, thisoff = p->dcp_off;
				size_t len, tlen;

				/* find starting entry */
				poff = 0;
				nd = psc_dynarray_len(p->dcp_dents_name);
				for (j = 0, pfd = p->dcp_base;
				    j < nd; j++) {
					if (off == thisoff)
						break;
					poff += PFL_DIRENT_SIZE(
					    pfd->pfd_namelen);
					thisoff = pfd->pfd_off;
					pfd = PSC_AGP(p->dcp_base,
					    poff);
				}

				/* determine size */
				for (len = 0; j < nd; j++)  {
					tlen = PFL_DIRENT_SIZE(
					    pfd->pfd_namelen);
					if (tlen + len > size)
						break;
					len += tlen;
					pfd = PSC_AGP(p->dcp_base,
					    poff + len);
				}

				// XXX I/O: remove from lock
				pscfs_reply_readdir(pfr,
				    p->dcp_base + poff, len, 0);
				p->dcp_flags |= DIRCACHEPGF_READ;
				if (hit)
					OPSTAT_INCR(SLC_OPST_DIRCACHE_HIT);

				issue = 0;
				break;
			}
		}
	}
	FCMH_ULOCK(d);

	if (issue) {
		hit = 0;
		rc = msl_readdir_issue(pfcc, d, off, size, 1);
		OPSTAT_INCR(SLC_OPST_DIRCACHE_ISSUE);
		if (rc && !slc_rmc_retry(pfr, &rc)) {
			pscfs_reply_readdir(pfr, NULL, 0, rc);
			return;
		}
		FCMH_LOCK(d);
		goto restart;
	}

	if (0)
 out:
		pscfs_reply_readdir(pfr, NULL, 0, rc);
}

void
mslfsop_lookup(struct pscfs_req *pfr, pscfs_inum_t pinum,
    const char *name)
{
	struct slash_fidgen fg;
	struct pscfs_creds pcr;
	struct srt_stat sstb;
	struct stat stb;
	int rc;

	msfsthr_ensure(pfr);

	OPSTAT_INCR(SLC_OPST_LOOKUP);

	memset(&sstb, 0, sizeof(sstb));

	pscfs_getcreds(pfr, &pcr);
	rc = msl_lookup_fidcache(pfr, &pcr, pinum, name, &fg, &sstb,
	    NULL);
	if (rc == ENOENT)
		sstb.sst_fid = 0;
	sl_internalize_stat(&sstb, &stb);
	if (!S_ISDIR(stb.st_mode))
		stb.st_blksize = MSL_FS_BLKSIZ;
	pscfs_reply_lookup(pfr, sstb.sst_fid, sstb.sst_gen,
	    pscfs_entry_timeout, &stb, pscfs_attr_timeout, rc);
}

void
mslfsop_readlink(struct pscfs_req *pfr, pscfs_inum_t inum)
{
	unsigned char buf[SL_PATH_MAX], *retbuf = buf;
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_readlink_req *mq;
	struct srm_readlink_rep *mp;
	struct fidc_membh *c = NULL;
	struct pscfs_creds pcr;
	struct iovec iov;
	int rc;

	msfsthr_ensure(pfr);

	OPSTAT_INCR(SLC_OPST_READLINK);

	rc = msl_load_fcmh(pfr, inum, &c);
	if (rc)
		PFL_GOTOERR(out, rc);

	pscfs_getcreds(pfr, &pcr);

	rc = fcmh_checkcreds(c, pfr, &pcr, R_OK);
	if (rc)
		PFL_GOTOERR(out, rc);

 retry:
	MSL_RMC_NEWREQ(pfr, c, csvc, SRMT_READLINK, rq, mq, mp, rc);
	if (rc)
		PFL_GOTOERR(out, rc);

	mq->fg = c->fcmh_fg;

	iov.iov_base = buf;
	iov.iov_len = sizeof(buf) - 1;
	rq->rq_bulk_abortable = 1;
	slrpc_bulkclient(rq, BULK_PUT_SINK, SRMC_BULK_PORTAL, &iov, 1);

	rc = SL_RSX_WAITREPF(csvc, rq, mp,
	    SRPCWAITF_DEFER_BULK_AUTHBUF_CHECK);
	if (rc && slc_rmc_retry(pfr, &rc))
		goto retry;
	if (!rc)
		rc = mp->rc;
	if (!rc) {
		if (mp->len > LNET_MTU) {
			rc = EINVAL;
		} else if (mp->len < sizeof(mp->buf)) {
			retbuf = mp->buf;
		} else {
			iov.iov_len = mp->len;
			rc = slrpc_bulk_checkmsg(rq, rq->rq_repmsg,
			    &iov, 1);
		}
	}
	if (!rc)
		retbuf[mp->len] = '\0';

 out:
	if (c)
		fcmh_op_done(c);

	pscfs_reply_readlink(pfr, retbuf, rc);

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
msl_flush_int_locked(struct msl_fhent *mfh, int wait)
{
	struct psc_dynarray a = DYNARRAY_INIT;
	struct fidc_membh *f;
	struct bmapc_memb *b;
	int i, rc = 0;

	f = mfh->mfh_fcmh;

	FCMH_LOCK(f);
	SPLAY_FOREACH(b, bmap_cache, &f->fcmh_bmaptree) {
		BMAP_LOCK(b);
		if (!(b->bcm_flags & BMAP_TOFREE)) {
			bmap_op_start_type(b, BMAP_OPCNT_FLUSH);
			psc_dynarray_add(&a, b);
		}
		BMAP_ULOCK(b);
	}
	FCMH_ULOCK(f);

	DYNARRAY_FOREACH(b, i, &a) {
		bmpc_biorqs_flush(b, wait);
		BMAP_LOCK(b);
		if (!rc)
			rc = bmap_2_bci(b)->bci_flush_rc;
		bmap_op_done_type(b, BMAP_OPCNT_FLUSH);
	}
	psc_dynarray_free(&a);

	return (rc);
}

int
msl_setattr(struct pscfs_clientctx *pfcc, struct fidc_membh *f,
    int32_t to_set, const struct srt_stat *sstb,
    const struct sl_fidgen *fgp, const struct stat *stb)
{
	int rc, ptrunc_started = 0, flags = 0;
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_setattr_req *mq;
	struct srm_setattr_rep *mp;

	MSL_RMC_NEWREQ_PFCC(pfcc, f, csvc, SRMT_SETATTR, rq, mq, mp,
	    rc);
	if (rc)
		return (rc);

	if (sstb)
		mq->attr = *sstb;
	else {
		sl_externalize_stat(stb, &mq->attr);
		mq->attr.sst_fg = *fgp;
	}
	mq->to_set = to_set;

	if (to_set & (PSCFS_SETATTRF_GID | PSCFS_SETATTRF_UID)) {
		rc = uidmap_ext_stat(&mq->attr);
		if (rc)
			PFL_GOTOERR(out, rc);
	}

	DEBUG_FCMH(PLL_DIAG, f, "before setattr RPC to_set=%#x", to_set);

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc == 0)
		rc = mp->rc;
	if (rc == -SLERR_BMAP_PTRUNC_STARTED) {
		ptrunc_started = 1;
		rc = 0;
	}
	DEBUG_SSTB(rc ? PLL_WARN : PLL_DIAG, &f->fcmh_sstb,
	    "attr flush; set=%x rc=%d", to_set, rc);
	if (rc)
		PFL_GOTOERR(out, rc);

	uidmap_int_stat(&mp->attr);

	if ((to_set & (PSCFS_SETATTRF_MTIME |
	    PSCFS_SETATTRF_DATASIZE)) == 0)
		flags |= FCMH_SETATTRF_SAVELOCAL;

	slc_fcmh_setattrf(f, &mp->attr, flags);

 out:
	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
	if (ptrunc_started)
		return (-SLERR_BMAP_PTRUNC_STARTED);
	return (rc);
}

int
msl_flush_ioattrs(struct pscfs_clientctx *pfcc, struct fidc_membh *f)
{
	int rc, waslocked, to_set = 0, flush_size = 0, flush_mtime = 0;
	struct srt_stat attr;

	waslocked = FCMH_RLOCK(f);
	fcmh_wait_locked(f, f->fcmh_flags & FCMH_BUSY);

	attr.sst_fg = f->fcmh_fg;

	/*
	 * Perhaps this checking should only be done on the mfh, with
	 * which we have modified the attributes.
	 */
	if (f->fcmh_flags & FCMH_CLI_DIRTY_DSIZE) {
		flush_size = 1;
		f->fcmh_flags &= ~FCMH_CLI_DIRTY_DSIZE;
		f->fcmh_flags |= FCMH_BUSY;
		to_set |= PSCFS_SETATTRF_DATASIZE;
		attr.sst_size = f->fcmh_sstb.sst_size;
	}
	if (f->fcmh_flags & FCMH_CLI_DIRTY_MTIME) {
		flush_mtime = 1;
		f->fcmh_flags &= ~FCMH_CLI_DIRTY_MTIME;
		f->fcmh_flags |= FCMH_BUSY;
		to_set |= PSCFS_SETATTRF_MTIME;
		attr.sst_mtim = f->fcmh_sstb.sst_mtim;
	}
	if (!to_set) {
		psc_assert((f->fcmh_flags & FCMH_CLI_DIRTY_QUEUE) == 0);
		FCMH_URLOCK(f, waslocked);
		return (0);
	}

	FCMH_ULOCK(f);

	OPSTAT_INCR(SLC_OPST_FLUSH_ATTR);

	rc = msl_setattr(pfcc, f, to_set, &attr, NULL, NULL);

	FCMH_LOCK(f);
	f->fcmh_flags &= ~FCMH_BUSY;
	if (rc && slc_rmc_retry_pfcc(NULL, &rc)) {
		if (flush_mtime)
			f->fcmh_flags |= FCMH_CLI_DIRTY_MTIME;
		if (flush_size)
			f->fcmh_flags |= FCMH_CLI_DIRTY_DSIZE;
		fcmh_wake_locked(f);
		FCMH_URLOCK(f, waslocked);
	} else if (!(f->fcmh_flags & FCMH_CLI_DIRTY_ATTRS)) {
		/*
		 * XXX: If an UNLINK occurs on an open file descriptor
		 * then it is receives WRITEs, we will try to SETATTR
		 * the FID which will result in ENOENT.
		 *
		 * The proper fix is to honor open file descriptors and
		 * only UNLINK the backend file after they are all
		 * closed.
		 */
		if (rc)
			DEBUG_FCMH(PLL_ERROR, f, "setattr: rc=%d", rc);

		psc_assert(f->fcmh_flags & FCMH_CLI_DIRTY_QUEUE);
		f->fcmh_flags &= ~FCMH_CLI_DIRTY_QUEUE;
		// XXX locking order violation
		lc_remove(&slc_attrtimeoutq, fcmh_2_fci(f));
		fcmh_op_done_type(f, FCMH_OPCNT_DIRTY_QUEUE);
		if (waslocked == PSLRV_WASLOCKED)
			FCMH_LOCK(f);
	} else
		FCMH_URLOCK(f, waslocked);

	return (rc);
}

/*
 * Note: this is not invoked from an application issued fsync(2).
 * This is called upon each close(2) of a file descriptor to this file.
 */
void
mslfsop_flush(struct pscfs_req *pfr, void *data)
{
	struct msl_fhent *mfh = data;
	struct pscfs_clientctx *pfcc;
	int rc, rc2;

	msfsthr_ensure(pfr);

	OPSTAT_INCR(SLC_OPST_FLUSH);

	DEBUG_FCMH(PLL_DIAG, mfh->mfh_fcmh, "flushing (mfh=%p)", mfh);

	pfcc = pscfs_getclientctx(pfr);

	spinlock(&mfh->mfh_lock);
	rc = msl_flush_int_locked(mfh, 1);
	rc2 = msl_flush_ioattrs(pfcc, mfh->mfh_fcmh);
	//if (rc && slc_rmc_retry(pfr, &rc))
	if (!rc)
		rc = rc2;

	DEBUG_FCMH(PLL_DIAG, mfh->mfh_fcmh,
	    "done flushing (mfh=%p, rc=%d)", mfh, rc);

	freelock(&mfh->mfh_lock);

	pscfs_reply_flush(pfr, rc);
}

void
mfh_incref(struct msl_fhent *mfh)
{
	int lk;

	lk = MFH_RLOCK(mfh);
	mfh->mfh_refcnt++;
	MFH_URLOCK(mfh, lk);
}

void
mfh_decref(struct msl_fhent *mfh)
{
	(void)MFH_RLOCK(mfh);
	psc_assert(mfh->mfh_refcnt > 0);
	if (--mfh->mfh_refcnt == 0) {
		fcmh_op_done_type(mfh->mfh_fcmh, FCMH_OPCNT_OPEN);
		psc_pool_return(slc_mfh_pool, mfh);
	} else
		MFH_ULOCK(mfh);
}

void
slc_getprog(pid_t pid, char *prog, size_t len)
{
	char *p, fn[128], buf[128];
	ssize_t sz;
	int rc, fd;

	rc = snprintf(fn, sizeof(fn), "/proc/%d/exe", pid);
	if (rc == -1)
		return;
	prog[0] = '\0';
	rc = readlink(fn, prog, len - 1);
	if (rc == -1)
		rc = 0;
	prog[rc] = '\0';

	/* no space to append script name */
	if ((size_t)rc >= len)
		return;

	if (strstr(prog, "/bash") == NULL &&
	    strstr(prog, "/python") == NULL &&
	    strstr(prog, "/perl") == NULL &&
	     strstr(prog, "/ksh") == NULL)
		return;

	snprintf(fn, sizeof(fn), "/proc/%d/cmdline", pid);
	fd = open(fn, O_RDONLY);
	if (fd == -1)
		return;

	sz = read(fd, buf, sizeof(buf) - 1);
	close(fd);

	if (sz == -1)
		return;

	buf[sz] = '\0';
	p = strchr(buf, '\0');
	if (p != buf + sz)
		snprintf(prog + strlen(prog), len - strlen(prog),
		    " %s", p + 1);
}

const char *
slc_log_get_fsctx_uprog(struct psc_thread *thr)
{
	struct psclog_data *pld;
	struct msfs_thread *mft;
	struct pscfs_req *pfr;

	pld = psclog_getdata();
// || mft->mft_lastp != p
	if (pld->pld_uprog)
		return (pld->pld_uprog);

	if (thr->pscthr_type != MSTHRT_FS)
		return "<n/a>";

	mft = msfsthr(thr);

	if (mft->mft_uprog[0] == '\0' && mft->mft_pfr) {
		pid_t pid;

		pfr = mft->mft_pfr; /* set by GETPFR() */
		if (pfr && pfr->pfr_fuse_fi) {
			struct msl_fhent *mfh;

			mfh = (void *)pfr->pfr_fuse_fi->fh; // XXX protocol violation
			pid = mfh->mfh_pid;
		} else
			pid = pscfs_getclientctx(pfr)->pfcc_pid;

		slc_getprog(pid, mft->mft_uprog, sizeof(mft->mft_uprog));
	}

	return (pld->pld_uprog = mft->mft_uprog);
}

pid_t
slc_log_get_fsctx_pid(struct psc_thread *thr)
{
	struct msfs_thread *mft;

	if (thr->pscthr_type != MSTHRT_FS)
		return (-1);

	mft = msfsthr(thr);
	if (mft->mft_pfr)
		return (pscfs_getclientctx(mft->mft_pfr)->pfcc_pid);
	return (-1);
}

uid_t
slc_log_get_fsctx_uid(struct psc_thread *thr)
{
	struct msfs_thread *mft;
	struct pscfs_creds pcr;

	if (thr->pscthr_type != MSTHRT_FS)
		return (-1);

	mft = msfsthr(thr);
	if (mft->mft_pfr) {
		pscfs_getcreds(mft->mft_pfr, &pcr);
		return (pcr.pcr_uid);
	}
	return (-1);
}

void
mslfsop_release(struct pscfs_req *pfr, void *data)
{
	struct msl_fhent *mfh = data;
	struct fcmh_cli_info *fci;
	struct fidc_membh *f;

	msfsthr_ensure(pfr);

	f = mfh->mfh_fcmh;
	fci = fcmh_2_fci(f);

	MFH_LOCK(mfh);
	mfh->mfh_flags |= MSL_FHENT_CLOSING;

	FCMH_LOCK(f);
	PFL_GETTIMESPEC(&fci->fci_etime);
	fci->fci_etime.tv_sec--;
	FCMH_ULOCK(f);
	psc_waitq_wakeall(&msl_flush_attrq);

	if (fcmh_isdir(f)) {
		OPSTAT_INCR(SLC_OPST_RELEASEDIR);
		pscfs_reply_releasedir(pfr, 0);
	} else {
		OPSTAT_INCR(SLC_OPST_RELEASE);
		pscfs_reply_release(pfr, 0);
	}

	if (!fcmh_isdir(f) &&
	    (mfh->mfh_nbytes_rd || mfh->mfh_nbytes_wr))
		psclogs(PLL_INFO, SLCSS_INFO,
		    "file closed fid="SLPRI_FID" "
		    "uid=%u gid=%u "
		    "fsize=%"PRId64" "
		    "oatime="PFLPRI_PTIMESPEC" "
		    "mtime="PFLPRI_PTIMESPEC" sessid=%d "
		    "otime="PSCPRI_TIMESPEC" "
		    "rd=%"PSCPRIdOFFT" wr=%"PSCPRIdOFFT" prog=%s",
		    fcmh_2_fid(f),
		    f->fcmh_sstb.sst_uid, f->fcmh_sstb.sst_gid,
		    f->fcmh_sstb.sst_size,
		    PFLPRI_PTIMESPEC_ARGS(&mfh->mfh_open_atime),
		    PFLPRI_PTIMESPEC_ARGS(&f->fcmh_sstb.sst_mtim),
		    mfh->mfh_sid,
		    PSCPRI_TIMESPEC_ARGS(&mfh->mfh_open_time),
		    mfh->mfh_nbytes_rd, mfh->mfh_nbytes_wr,
		    mfh->mfh_uprog);

	mfh_decref(mfh);
}

void
mslfsop_rename(struct pscfs_req *pfr, pscfs_inum_t opinum,
    const char *oldname, pscfs_inum_t npinum, const char *newname)
{
	struct fidc_membh *child = NULL, *np = NULL, *op = NULL, *ch;
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srt_stat srcsstb, dstsstb;
	struct slash_fidgen srcfg, dstfg;
	struct srm_rename_req *mq;
	struct srm_rename_rep *mp;
	struct pscfs_creds pcr;
	struct iovec iov[2];
	int sticky, rc;

	msfsthr_ensure(pfr);

	OPSTAT_INCR(SLC_OPST_RENAME);

	memset(&dstsstb, 0, sizeof(dstsstb));
	srcfg.fg_fid = FID_ANY;

#if 0
	if (strcmp(oldname, ".") == 0 ||
	    strcmp(oldname, "..") == 0) {
		rc = EINVAL;
		goto out;
	}
#endif

	if (strlen(oldname) == 0 ||
	    strlen(newname) == 0)
		PFL_GOTOERR(out, rc = ENOENT);
	if (strlen(oldname) > SL_NAME_MAX ||
	    strlen(newname) > SL_NAME_MAX)
		PFL_GOTOERR(out, rc = ENAMETOOLONG);

	pscfs_getcreds(pfr, &pcr);

	rc = msl_load_fcmh(pfr, opinum, &op);
	if (rc)
		PFL_GOTOERR(out, rc);

	rc = fcmh_reserved(op);
	if (rc)
		PFL_GOTOERR(out, rc);

	if (pcr.pcr_uid) {
		FCMH_LOCK(op);
		sticky = op->fcmh_sstb.sst_mode & S_ISVTX;
		if (sticky) {
			if (op->fcmh_sstb.sst_uid == pcr.pcr_uid)
				sticky = 0;
		} else
			rc = fcmh_checkcreds(op, pfr, &pcr, W_OK); // X_OK?
		FCMH_ULOCK(op);
		if (rc)
			PFL_GOTOERR(out, rc);

		if (sticky) {
			rc = msl_lookup_fidcache(pfr, &pcr, opinum,
			    oldname, &srcfg, &srcsstb, &child);
			if (rc)
				PFL_GOTOERR(out, rc);
			if (srcsstb.sst_uid != pcr.pcr_uid)
				PFL_GOTOERR(out, rc = EPERM);
		}
	}

	if (npinum == opinum) {
		np = op;
	} else {
		rc = msl_load_fcmh(pfr, npinum, &np);
		if (rc)
			PFL_GOTOERR(out, rc);

		rc = fcmh_reserved(np);
		if (rc)
			PFL_GOTOERR(out, rc);

		if (pcr.pcr_uid) {
			FCMH_LOCK(np);
			sticky = np->fcmh_sstb.sst_mode & S_ISVTX;
			if (sticky) {
				if (np->fcmh_sstb.sst_uid == pcr.pcr_uid)
					sticky = 0;
			} else
				rc = fcmh_checkcreds(np, pfr, &pcr, W_OK); // X_OK
			FCMH_ULOCK(np);
			if (rc)
				PFL_GOTOERR(out, rc);

			if (sticky) {
				/* XXX race */
				rc = msl_lookup_fidcache(pfr, &pcr,
				    npinum, newname, &dstfg, &dstsstb,
				    NULL);
				if (rc == 0 &&
				    dstsstb.sst_uid != pcr.pcr_uid)
					rc = EPERM;
				else
					rc = 0;
				if (rc)
					PFL_GOTOERR(out, rc);
			}
		}
	}

	if (pcr.pcr_uid) {
		if (srcfg.fg_fid == FID_ANY) {
			rc = msl_lookup_fidcache(pfr, &pcr, opinum,
			    oldname, &srcfg, &srcsstb, &child);
			if (rc)
				PFL_GOTOERR(out, rc);
		}
		if (fcmh_isdir(child))
			rc = fcmh_checkcreds(child, pfr, &pcr, W_OK);
		if (rc)
			PFL_GOTOERR(out, rc);
	}

 retry:
	MSL_RMC_NEWREQ(pfr, np, csvc, SRMT_RENAME, rq, mq, mp, rc);
	if (rc)
		PFL_GOTOERR(out, rc);

	mq->opfg.fg_fid = opinum;
	mq->npfg.fg_fid = npinum;
	mq->opfg.fg_gen = mq->npfg.fg_gen = FGEN_ANY;
	mq->fromlen = strlen(oldname);
	mq->tolen = strlen(newname);

	if (mq->fromlen + mq->tolen > SRM_RENAME_NAMEMAX) {
		iov[0].iov_base = (char *)oldname;
		iov[0].iov_len = mq->fromlen;
		iov[1].iov_base = (char *)newname;
		iov[1].iov_len = mq->tolen;

		slrpc_bulkclient(rq, BULK_GET_SOURCE, SRMC_BULK_PORTAL,
		    iov, 2);
	} else {
		memcpy(mq->buf, oldname, mq->fromlen);
		memcpy(mq->buf + mq->fromlen, newname, mq->tolen);
	}

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc && slc_rmc_retry(pfr, &rc))
		goto retry;
	if (rc == 0)
		rc = mp->rc;

	psclog_diag("rename: opfid="SLPRI_FID" npfid="SLPRI_FID" from='%s' "
	    "to='%s' rc=%d", opinum, npinum, oldname, newname, rc);

	if (rc)
		PFL_GOTOERR(out, rc);

	/* refresh old parent attributes */
	FCMH_LOCK(op);
	uidmap_int_stat(&mp->srr_opattr);
	slc_fcmh_setattr_locked(op, &mp->srr_opattr);
	FCMH_ULOCK(op);

	if (np != op) {
		/* refresh new parent attributes */
		FCMH_LOCK(np);
		uidmap_int_stat(&mp->srr_npattr);
		slc_fcmh_setattr_locked(np, &mp->srr_npattr);
		FCMH_ULOCK(np);
	}

	/* refresh moved file's attributes */
	if (mp->srr_cattr.sst_fid != FID_ANY &&
	    fidc_lookup_fg(&mp->srr_cattr.sst_fg, &ch) == 0) {
		uidmap_int_stat(&mp->srr_cattr);
		slc_fcmh_setattrf(ch, &mp->srr_cattr,
		    FCMH_SETATTRF_SAVELOCAL);
		fcmh_op_done(ch);
	}

	/*
	 * Refresh clobbered file's attributes.  This file might have
	 * additional links and may not be completely destroyed so don't
	 * evict.
	 */
	if (mp->srr_clattr.sst_fid != FID_ANY &&
	    fidc_lookup_fg(&mp->srr_clattr.sst_fg, &ch) == 0) {
		uidmap_int_stat(&mp->srr_clattr);
		slc_fcmh_setattrf(ch, &mp->srr_clattr,
		    FCMH_SETATTRF_SAVELOCAL);
		fcmh_op_done(ch);
	}

	/*
	 * XXX we do not update dstsstb in our cache if the dst was
	 * nlinks > 1 and the inode was not removed from the file system
	 * outright as a result of this rename op.
	 */

 out:
	if (child)
		fcmh_op_done(child);
	if (op)
		fcmh_op_done(op);
	if (np && np != op)
		fcmh_op_done(np);

	pscfs_reply_rename(pfr, rc);

	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

void
mslfsop_statfs(struct pscfs_req *pfr, pscfs_inum_t inum)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_statfs_req *mq;
	struct srm_statfs_rep *mp;
	struct statvfs sfb;
	int rc;

	msfsthr_ensure(pfr);

	OPSTAT_INCR(SLC_OPST_STATFS);

//	checkcreds

 retry:
	MSL_RMC_NEWREQ_PFCC(NULL, NULL, csvc, SRMT_STATFS, rq, mq, mp,
	    rc);
	if (rc)
		PFL_GOTOERR(out, rc);
	mq->fid = inum;
	mq->iosid = prefIOS;
	if (rc)
		PFL_GOTOERR(out, rc);
	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc && slc_rmc_retry(pfr, &rc))
		goto retry;
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		PFL_GOTOERR(out, rc);

	sl_internalize_statfs(&mp->ssfb, &sfb);
	sfb.f_blocks = sfb.f_blocks * 1. / MSL_FS_BLKSIZ * sfb.f_frsize;
	sfb.f_bfree = sfb.f_bfree * 1. / MSL_FS_BLKSIZ * sfb.f_frsize;
	sfb.f_bavail = sfb.f_bavail * 1. / MSL_FS_BLKSIZ * sfb.f_frsize;
	sfb.f_bsize = MSL_FS_BLKSIZ;
	sfb.f_fsid = SLASH_FSID;

 out:
	pscfs_reply_statfs(pfr, &sfb, rc);

	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

void
mslfsop_symlink(struct pscfs_req *pfr, const char *buf,
    pscfs_inum_t pinum, const char *name)
{
	struct fidc_membh *c = NULL, *p = NULL;
	struct slashrpc_cservice *csvc = NULL;
	struct srm_symlink_rep *mp = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_symlink_req *mq;
	struct pscfs_creds pcr;
	struct iovec iov;
	struct stat stb;
	int rc;

	msfsthr_ensure(pfr);

	OPSTAT_INCR(SLC_OPST_SYMLINK);

	if (strlen(buf) == 0 || strlen(name) == 0)
		PFL_GOTOERR(out, rc = ENOENT);
	if (strlen(buf) >= SL_PATH_MAX ||
	    strlen(name) > SL_NAME_MAX)
		PFL_GOTOERR(out, rc = ENAMETOOLONG);

	pscfs_getcreds(pfr, &pcr);

	rc = msl_load_fcmh(pfr, pinum, &p);
	if (rc)
		PFL_GOTOERR(out, rc);

	if (!fcmh_isdir(p))
		PFL_GOTOERR(out, rc = ENOTDIR);
	rc = fcmh_reserved(p);
	if (rc)
		PFL_GOTOERR(out, rc);

 retry:
	MSL_RMC_NEWREQ(pfr, p, csvc, SRMT_SYMLINK, rq, mq, mp, rc);
	if (rc)
		PFL_GOTOERR(out, rc);

	mq->sstb.sst_uid = pcr.pcr_uid;
	mq->sstb.sst_gid = pcr.pcr_gid;
	rc = uidmap_ext_stat(&mq->sstb);
	if (rc)
		PFL_GOTOERR(out, rc);
	mq->pfg.fg_fid = pinum;
	mq->pfg.fg_gen = FGEN_ANY;
	mq->linklen = strlen(buf);
	strlcpy(mq->name, name, sizeof(mq->name));

	iov.iov_base = (char *)buf;
	iov.iov_len = mq->linklen;

	slrpc_bulkclient(rq, BULK_GET_SOURCE, SRMC_BULK_PORTAL, &iov,
	    1);

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc && slc_rmc_retry(pfr, &rc))
		goto retry;
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		PFL_GOTOERR(out, rc);

	uidmap_int_stat(&mp->pattr);
	slc_fcmh_setattr(p, &mp->pattr);

	uidmap_int_stat(&mp->cattr);
	rc = msl_create_fcmh(pfr, &mp->cattr.sst_fg, &c);
	if (rc)
		PFL_GOTOERR(out, rc);

	slc_fcmh_setattrf(c, &mp->cattr, FCMH_SETATTRF_NONE);
	sl_internalize_stat(&mp->cattr, &stb);

 out:
	if (c)
		fcmh_op_done(c);
	if (p)
		fcmh_op_done(p);

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
msl_dc_inv_entry(struct dircache_page *p, struct dircache_ent *d,
    void *arg)
{
	const struct msl_dc_inv_entry_data *mdie = arg;

	if (p->dcp_flags & DIRCACHEPGF_LOADING)
		return;

	pscfs_notify_inval_entry(mdie->mdie_pfr, mdie->mdie_pinum,
	    d->dce_name, d->dce_namelen);
}

int
inprocgrouplist(gid_t key, struct pscfs_creds *pcr)
{
	int i;

	for (i = 0; i < pcr->pcr_ngid; i++)
		if (pcr->pcr_gidv[i] == key)
			return (1);
	return (0);
}

void
mslfsop_setattr(struct pscfs_req *pfr, pscfs_inum_t inum,
    struct stat *stb, int to_set, void *data)
{
	int i, rc = 0, unset_trunc = 0, getting_attrs = 0;
	int flush_mtime = 0, flush_size = 0;
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct pscfs_clientctx *pfcc;
	struct msl_fhent *mfh = data;
	struct fidc_membh *c = NULL;
	struct fcmh_cli_info *fci;
	struct pscfs_creds pcr;
	struct timespec ts;

	msfsthr_ensure(pfr);

	OPSTAT_INCR(SLC_OPST_SETATTR);

	if ((to_set & PSCFS_SETATTRF_UID) && stb->st_uid == (uid_t)-1)
		to_set &= ~PSCFS_SETATTRF_UID;
	if ((to_set & PSCFS_SETATTRF_GID) && stb->st_gid == (gid_t)-1)
		to_set &= ~PSCFS_SETATTRF_GID;

	rc = msl_load_fcmh(pfr, inum, &c);
	if (rc)
		PFL_GOTOERR(out, rc);

	if (mfh)
		psc_assert(c == mfh->mfh_fcmh);

	FCMH_WAIT_BUSY(c);

	if (to_set == 0)
		goto out;

	pscfs_getcreds(pfr, &pcr);

	if ((to_set & PSCFS_SETATTRF_MODE) && pcr.pcr_uid) {
#if 0
		if ((stb->st_mode & ALLPERMS) !=
		    (c->fcmh_sstb.sst_mode & ALLPERMS)) {
			rc = EINVAL;
			goto out;
		}
#endif
		if (pcr.pcr_uid != c->fcmh_sstb.sst_uid)
			PFL_GOTOERR(out, rc = EPERM);
		if (pcr.pcr_gid != c->fcmh_sstb.sst_gid &&
		    !inprocgrouplist(c->fcmh_sstb.sst_gid, &pcr))
			stb->st_mode &= ~S_ISGID;
	}
	if (to_set & PSCFS_SETATTRF_DATASIZE) {
		rc = fcmh_checkcreds(c, pfr, &pcr, W_OK);
		if (rc)
			PFL_GOTOERR(out, rc);
	}
	if ((to_set & (PSCFS_SETATTRF_ATIME | PSCFS_SETATTRF_MTIME)) &&
	    pcr.pcr_uid && pcr.pcr_uid != c->fcmh_sstb.sst_uid)
		PFL_GOTOERR(out, rc = EPERM);

	if (to_set & PSCFS_SETATTRF_ATIME_NOW)
		stb->st_pfl_ctim = stb->st_pfl_atim;
	else if (to_set & PSCFS_SETATTRF_MTIME_NOW)
		stb->st_pfl_ctim = stb->st_pfl_mtim;
	else
		PFL_GETPTIMESPEC(&stb->st_pfl_ctim);
	to_set |= PSCFS_SETATTRF_CTIME;

	if (to_set & PSCFS_SETATTRF_UID) {
		if (pcr.pcr_uid &&
		    (pcr.pcr_uid != c->fcmh_sstb.sst_uid ||
		     pcr.pcr_uid != stb->st_uid))
			PFL_GOTOERR(out, rc = EPERM);
		// XXX sysctl fs.posix.setuid
		if (c->fcmh_sstb.sst_mode & (S_ISGID | S_ISUID)) {
			to_set |= PSCFS_SETATTRF_MODE;
			stb->st_mode = c->fcmh_sstb.sst_mode &
			    ~(S_ISGID | S_ISUID);
		}
	}
	if (to_set & PSCFS_SETATTRF_GID) {
		if (pcr.pcr_uid &&
		    (pcr.pcr_uid != c->fcmh_sstb.sst_uid ||
		     !inprocgrouplist(stb->st_gid, &pcr)))
			PFL_GOTOERR(out, rc = EPERM);
		// XXX sysctl fs.posix.setuid
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

		if (!stb->st_size) {
			DEBUG_FCMH(PLL_DIAG, c,
			    "full truncate, free bmaps");

			OPSTAT_INCR(SLC_OPST_TRUNCATE_FULL);
			bmap_free_all_locked(c);
			FCMH_ULOCK(c);

		} else if (stb->st_size == (ssize_t)fcmh_2_fsz(c)) {
			/*
			 * No-op.  Don't send truncate request if the
			 * sizes match.
			 */
			goto out;

		} else {
			struct psc_dynarray a = DYNARRAY_INIT;
			uint32_t x = stb->st_size / SLASH_BMAP_SIZE;

			OPSTAT_INCR(SLC_OPST_TRUNCATE_PART);

			DEBUG_FCMH(PLL_DIAG, c, "partial truncate");

			/* Partial truncate.  Block and flush. */
			SPLAY_FOREACH(b, bmap_cache, &c->fcmh_bmaptree) {
				if (b->bcm_bmapno < x)
					continue;

				/*
				 * Take a reference to ensure the bmap
				 * is still valid.
				 * bmap_biorq_waitempty() shouldn't be
				 * called while holding the fcmh lock.
				 */
				bmap_op_start_type(b,
				    BMAP_OPCNT_TRUNCWAIT);
				DEBUG_BMAP(PLL_DIAG, b,
				    "BMAP_OPCNT_TRUNCWAIT");
				psc_dynarray_add(&a, b);
			}
			FCMH_ULOCK(c);

			/*
			 * XXX some writes can be cancelled, but no api
			 * exists yet.
			 */
			DYNARRAY_FOREACH(b, i, &a)
				bmap_biorq_expire(b);

			DYNARRAY_FOREACH(b, i, &a) {
				bmap_biorq_waitempty(b);
				bmap_op_done_type(b, BMAP_OPCNT_TRUNCWAIT);
			}
			psc_dynarray_free(&a);
		}
	}

	(void)FCMH_RLOCK(c);
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

	FCMH_LOCK(c);
	if (c->fcmh_flags & FCMH_CLI_DIRTY_MTIME) {
		flush_mtime = 1;
		if (!(to_set & PSCFS_SETATTRF_MTIME)) {
			to_set |= PSCFS_SETATTRF_MTIME;
			PFL_STB_MTIME_SET(c->fcmh_sstb.sst_mtime,
				  c->fcmh_sstb.sst_mtime_ns,
				  stb);
		}
	}
	if (c->fcmh_flags & FCMH_CLI_DIRTY_DSIZE) {
		flush_size = 1;
		if (!(to_set & PSCFS_SETATTRF_DATASIZE)) {
			to_set |= PSCFS_SETATTRF_DATASIZE;
			stb->st_size = c->fcmh_sstb.sst_size;
		}
	}
	c->fcmh_flags &= ~FCMH_CLI_DIRTY_ATTRS;
	FCMH_ULOCK(c);

	pfcc = pscfs_getclientctx(pfr);
 retry:
	rc = msl_setattr(pfcc, c, to_set, NULL, &c->fcmh_fg, stb);
	if (rc && slc_rmc_retry(pfr, &rc))
		goto retry;
	switch (rc) {
	case -SLERR_BMAP_IN_PTRUNC:
		if (getting_attrs) {
			getting_attrs = 0;
			FCMH_LOCK(c);
			c->fcmh_flags &= ~FCMH_GETTING_ATTRS;
		}
		rc = 0;
		goto wait_trunc_res;
	case -SLERR_BMAP_PTRUNC_STARTED:
		unset_trunc = 0;
		rc = 0;
		break;
	}
	if (rc)
		PFL_GOTOERR(out, rc);

	FCMH_LOCK(c);
	if (fcmh_isdir(c)) {
		struct msl_dc_inv_entry_data mdie;

		mdie.mdie_pfr = pfr;
		mdie.mdie_pinum = fcmh_2_fid(c);
		/* XXX this currently crashes fuse.ko but needs to happen */
		dircache_walk(c, msl_dc_inv_entry, &mdie);
	}

 out:
	if (c) {
		(void)FCMH_RLOCK(c);
		if (unset_trunc) {
			c->fcmh_flags &= ~FCMH_CLI_TRUNC;
			fcmh_wake_locked(c);
		}
		if (rc && getting_attrs)
			c->fcmh_flags &= ~FCMH_GETTING_ATTRS;
		sl_internalize_stat(&c->fcmh_sstb, stb);

		if (flush_mtime || flush_size) {
			if (!rc && !(c->fcmh_flags & FCMH_CLI_DIRTY_ATTRS)) {
				fci = fcmh_2_fci(c);
				psc_assert(c->fcmh_flags & FCMH_CLI_DIRTY_QUEUE);
				c->fcmh_flags &= ~FCMH_CLI_DIRTY_QUEUE;
				lc_remove(&slc_attrtimeoutq, fci);
				fcmh_op_done_type(c, FCMH_OPCNT_DIRTY_QUEUE);
			}
			if (rc) {
				if (flush_mtime)
					c->fcmh_flags |= FCMH_CLI_DIRTY_MTIME;
				if (flush_size)
					c->fcmh_flags |= FCMH_CLI_DIRTY_DSIZE;
			}
		}
		FCMH_UNBUSY(c);
		fcmh_op_done(c);
	}
	/* XXX if there is no fcmh, what do we do?? */
	pscfs_reply_setattr(pfr, stb, pscfs_attr_timeout, rc);
	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

void
mslfsop_fsync(struct pscfs_req *pfr, int datasync_only, void *data)
{
	struct pscfs_clientctx *pfcc;
	struct msl_fhent *mfh;
	struct fidc_membh *f;
	int rc = 0;

	msfsthr_ensure(pfr);

	mfh = data;
	f = mfh->mfh_fcmh;
	if (fcmh_isdir(f)) {
		OPSTAT_INCR(SLC_OPST_FSYNCDIR);
		// XXX flush all fcmh attrs under dir
	} else {
		OPSTAT_INCR(SLC_OPST_FSYNC);
		DEBUG_FCMH(PLL_DIAG, mfh->mfh_fcmh, "fsyncing");

		spinlock(&mfh->mfh_lock);
		rc = msl_flush_int_locked(mfh, 1);
		if (!datasync_only) {
			int rc2;

			pfcc = pscfs_getclientctx(pfr);
			rc2 = msl_flush_ioattrs(pfcc, mfh->mfh_fcmh);
			//if (rc && slc_rmc_retry(pfr, &rc))
			if (!rc)
				rc = rc2;
		}
		freelock(&mfh->mfh_lock);
	}

	pscfs_reply_fsync(pfr, rc);
}

void
mslfsop_destroy(void)
{
	pscthr_killall();
	/* XXX wait */
	pscrpc_exit_portals();
//	exit(0);
}

void
mslfsop_write(struct pscfs_req *pfr, const void *buf, size_t size,
    off_t off, void *data)
{
	struct msl_fhent *mfh = data;
	struct fidc_membh *f;
	int rc = 0;

	msfsthr_ensure(pfr);

	OPSTAT_INCR(SLC_OPST_WRITE);

	f = mfh->mfh_fcmh;

	/* XXX EBADF if fd is not open for writing */
	if (fcmh_isdir(f))
		PFL_GOTOERR(out, rc = EISDIR);

	rc = msl_write(pfr, mfh, (void *)buf, size, off);

 out:
	if (rc) {
		pscfs_reply_write(pfr, size, rc);
		OPSTAT_INCR(SLC_OPST_FSRQ_WRITE_ERR);
	}
	DEBUG_FCMH(PLL_DIAG, f, "write: buf=%p rc=%d sz=%zu "
	    "off=%"PSCPRIdOFFT, buf, rc, size, off);
}

void
mslfsop_read(struct pscfs_req *pfr, size_t size, off_t off, void *data)
{
	struct msl_fhent *mfh = data;
	struct fidc_membh *f;
	void *buf = pfr->pfr_buf;
	int rc = 0;

	msfsthr_ensure(pfr);

	OPSTAT_INCR(SLC_OPST_READ);

	f = mfh->mfh_fcmh;

	DEBUG_FCMH(PLL_DIAG, f, "read (start): buf=%p rc=%d sz=%zu "
	    "off=%"PSCPRIdOFFT, buf, rc, size, off);

	if (fcmh_isdir(f)) {
		OPSTAT_INCR(SLC_OPST_FSRQ_READ_NOREG);
		PFL_GOTOERR(out, rc = EISDIR);
	}

	rc = msl_read(pfr, mfh, buf, size, off);

 out:
	if (rc) {
		struct iovec iov;

		iov.iov_base = buf;
		pscfs_reply_read(pfr, &iov, 1, rc);
		OPSTAT_INCR(SLC_OPST_FSRQ_READ_ERR);
	}

	DEBUG_FCMH(PLL_DIAG, f, "read (end): buf=%p rc=%d sz=%zu "
	    "off=%"PSCPRIdOFFT, buf, rc, size, off);
}

void
mslfsop_listxattr(struct pscfs_req *pfr, size_t size, pscfs_inum_t inum)
{
	struct pscrpc_request *rq = NULL;
	struct slashrpc_cservice *csvc = NULL;
	struct srm_listxattr_rep *mp = NULL;
	struct srm_listxattr_req *mq;
	struct fidc_membh *f = NULL;
	struct pscfs_creds pcr;
	struct iovec iov;
	char *buf = NULL;
	int rc;

	msfsthr_ensure(pfr);

	OPSTAT_INCR(SLC_OPST_LISTXATTR);

	if (size > LNET_MTU)
		PFL_GOTOERR(out, rc = EINVAL);

	rc = msl_load_fcmh(pfr, inum, &f);
	if (rc)
		PFL_GOTOERR(out, rc);

	pscfs_getcreds(pfr, &pcr);

	rc = fcmh_checkcreds(f, pfr, &pcr, R_OK);
	if (rc)
		PFL_GOTOERR(out, rc);

	if (size)
		buf = PSCALLOC(size);

 retry:
	MSL_RMC_NEWREQ(pfr, f, csvc, SRMT_LISTXATTR, rq, mq, mp, rc);
	if (rc)
		PFL_GOTOERR(out, rc);

	mq->fg.fg_fid = inum;
	mq->fg.fg_gen = FGEN_ANY;

	if (size) {
		mq->size = size - 1;
		iov.iov_base = buf;
		iov.iov_len = size - 1;
		rq->rq_bulk_abortable = 1;
		slrpc_bulkclient(rq, BULK_PUT_SINK, SRMC_BULK_PORTAL,
		    &iov, 1);
	}

	OPSTAT_INCR(SLC_OPST_LISTXATTR_RPC);
	rc = SL_RSX_WAITREPF(csvc, rq, mp,
	    SRPCWAITF_DEFER_BULK_AUTHBUF_CHECK);
	if (rc && slc_rmc_retry(pfr, &rc))
		goto retry;
	if (!rc)
		rc = mp->rc;
	if (!rc && size) {
		// XXX sanity check mp->size
		iov.iov_len = mp->size;
		rc = slrpc_bulk_checkmsg(rq, rq->rq_repmsg, &iov, 1);
	}
	if (!rc) {
		FCMH_LOCK(f);
		fcmh_2_fci(f)->fci_xattrsize = mp->size;
	}

 out:
	if (f)
		fcmh_op_done(f);

	pscfs_reply_listxattr(pfr, buf, mp ? mp->size : 0, rc);

	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
	PSCFREE(buf);
}

void
mslfsop_setxattr(struct pscfs_req *pfr, const char *name,
    const void *value, size_t size, pscfs_inum_t inum)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_setxattr_rep *mp = NULL;
	struct srm_setxattr_req *mq;
	struct fidc_membh *f = NULL;
	struct pscfs_creds pcr;
	struct iovec iov;
	int rc;

	msfsthr_ensure(pfr);

	OPSTAT_INCR(SLC_OPST_SETXATTR);

	if (strlen(name) >= sizeof(mq->name))
		PFL_GOTOERR(out, rc = EINVAL);

	rc = msl_load_fcmh(pfr, inum, &f);
	if (rc)
		PFL_GOTOERR(out, rc);

	pscfs_getcreds(pfr, &pcr);

	rc = fcmh_checkcreds(f, pfr, &pcr, R_OK);
	if (rc)
		PFL_GOTOERR(out, rc);

 retry:
	MSL_RMC_NEWREQ(pfr, f, csvc, SRMT_SETXATTR, rq, mq, mp, rc);
	if (rc)
		PFL_GOTOERR(out, rc);

	mq->fg.fg_fid = inum;
	mq->fg.fg_gen = FGEN_ANY;
	mq->valuelen = size;
	strlcpy(mq->name, name, sizeof(mq->name));

	iov.iov_base = (char *)value;
	iov.iov_len = size;

	slrpc_bulkclient(rq, BULK_GET_SOURCE, SRMC_BULK_PORTAL, &iov,
	    1);

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc && slc_rmc_retry(pfr, &rc))
		goto retry;
	if (!rc)
		rc = mp->rc;
	if (!rc) {
		FCMH_LOCK(f);
		// XXX we should just load the new attr in
		fcmh_2_fci(f)->fci_xattrsize = 1;
	}

 out:
	if (f)
		fcmh_op_done(f);

	pscfs_reply_setxattr(pfr, rc);

	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

ssize_t
slc_getxattr(const struct pscfs_clientctx *pfcc,
    const struct pscfs_creds *pcrp, const char *name, void *buf,
    size_t size, struct fidc_membh *f, size_t *retsz)
{
	int rc = 0, locked = 0;
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_getxattr_rep *mp;
	struct srm_getxattr_req *mq;
	struct fcmh_cli_info *fci;
	struct iovec iov;

	if (strlen(name) >= sizeof(mq->name))
		PFL_GOTOERR(out, rc = EINVAL);

//	rc = fcmh_checkcreds(c, pfr, &pcr, R_OK);
//	if (rc)
//		PFL_GOTOERR(out, rc);

	if (f->fcmh_flags & FCMH_HAVE_ATTRS) {
		struct timeval now;

		PFL_GETTIMEVAL(&now);
		fci = fcmh_2_fci(f);
		locked = FCMH_RLOCK(f);
		if (timercmp(&now, &fci->fci_age, <) &&
		    fci->fci_xattrsize == 0)
			rc = ENODATA; // ENOATTR
		FCMH_URLOCK(f, locked);
		locked = 0;
		if (rc)
			PFL_GOTOERR(out, rc);
	}

	locked = FCMH_HAS_LOCK(f);
	if (locked)
		FCMH_ULOCK(f);

 retry:
	MSL_RMC_NEWREQ_PFCC(pfcc, f, csvc, SRMT_GETXATTR, rq, mq, mp,
	    rc);
	if (rc)
		PFL_GOTOERR(out, rc = -rc);

	mq->fg = f->fcmh_fg;
	mq->size = size;
	strlcpy(mq->name, name, sizeof(mq->name));

	if (size) {
		iov.iov_base = buf;
		iov.iov_len = size;
		slrpc_bulkclient(rq, BULK_PUT_SINK, SRMC_BULK_PORTAL,
		    &iov, 1);
		rq->rq_bulk_abortable = 1;
	}

	OPSTAT_INCR(SLC_OPST_GETXATTR_RPC);
	rc = SL_RSX_WAITREPF(csvc, rq, mp,
	    SRPCWAITF_DEFER_BULK_AUTHBUF_CHECK);
	if (rc && slc_rmc_retry_pfcc(pfcc, &rc))
		goto retry;
	if (!rc)
		rc = mp->rc;
	if (!rc && size) {
		iov.iov_len = mp->valuelen;
		rc = slrpc_bulk_checkmsg(rq, rq->rq_repmsg, &iov, 1);
	}
	if (!rc)
		*retsz = mp->valuelen;
	rc = -rc;

 out:
	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
	if (locked)
		FCMH_LOCK(f);
	return (rc);
}

void
mslfsop_getxattr(struct pscfs_req *pfr, const char *name, size_t size,
    pscfs_inum_t inum)
{
	struct pscfs_clientctx *pfcc;
	struct fidc_membh *f = NULL;
	struct pscfs_creds pcr;
	size_t retsz = 0;
	void *buf = NULL;
	int rc;

	msfsthr_ensure(pfr);

	OPSTAT_INCR(SLC_OPST_GETXATTR);

	if (size > LNET_MTU)
		PFL_GOTOERR(out, rc = EINVAL);

	rc = msl_load_fcmh(pfr, inum, &f);
	if (rc)
		PFL_GOTOERR(out, rc);

	if (size)
		buf = PSCALLOC(size);

	pfcc = pscfs_getclientctx(pfr);
	pscfs_getcreds(pfr, &pcr);

	rc = slc_getxattr(pfcc, &pcr, name, buf, size, f, &retsz);

 out:
	if (f)
		fcmh_op_done(f);
	pscfs_reply_getxattr(pfr, buf, retsz, rc);
	PSCFREE(buf);
}

void
mslfsop_removexattr(struct pscfs_req *pfr, const char *name,
    pscfs_inum_t inum)
{
	struct slashrpc_cservice *csvc = NULL;
	struct srm_removexattr_rep *mp = NULL;
	struct srm_removexattr_req *mq;
	struct pscrpc_request *rq = NULL;
	struct fidc_membh *f = NULL;
	struct pscfs_creds pcr;
	int rc;

	msfsthr_ensure(pfr);

	OPSTAT_INCR(SLC_OPST_REMOVEXATTR);

	if (strlen(name) >= sizeof(mq->name))
		PFL_GOTOERR(out, rc = EINVAL);

	rc = msl_load_fcmh(pfr, inum, &f);
	if (rc)
		PFL_GOTOERR(out, rc);

	pscfs_getcreds(pfr, &pcr);

	rc = fcmh_checkcreds(f, pfr, &pcr, R_OK);
	if (rc)
		PFL_GOTOERR(out, rc);

 retry:
	MSL_RMC_NEWREQ(pfr, f, csvc, SRMT_REMOVEXATTR, rq, mq, mp, rc);
	if (rc)
		PFL_GOTOERR(out, rc);

	mq->fg.fg_fid = inum;
	mq->fg.fg_gen = FGEN_ANY;
	strlcpy(mq->name, name, sizeof(mq->name));

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc && slc_rmc_retry(pfr, &rc))
		goto retry;

	if (rc == 0)
		rc = mp->rc;

 out:
	if (f)
		fcmh_op_done(f);

	pscfs_reply_removexattr(pfr, rc);

	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

void
msreadaheadthr_main(struct psc_thread *thr)
{
	int i, rc, did_work;
	struct bmap *b;
	struct fidc_membh *f;
	struct bmpc_ioreq *r;
	struct bmap_pagecache *bmpc;
	struct bmap_pagecache_entry *e;
	struct fcmh_cli_info *fci, *tmp_fci;

	while (pscthr_run(thr)) {
		did_work = 0;
		LIST_CACHE_LOCK(&slc_readaheadq);
		lc_peekheadwait(&slc_readaheadq);
		LIST_CACHE_FOREACH_SAFE(fci, tmp_fci, &slc_readaheadq) {

			f = fci_2_fcmh(fci);
			if (!FCMH_TRYLOCK(f))
				continue;

			if (!(f->fcmh_flags & FCMH_CLI_READA_QUEUE)) {
				FCMH_ULOCK(f);
				continue;
			}
			lc_remove(&slc_readaheadq, fci);
			f->fcmh_flags &= ~FCMH_CLI_READA_QUEUE;
			FCMH_ULOCK(f);

			LIST_CACHE_ULOCK(&slc_readaheadq);
			did_work = 1;

			rc = bmap_get(f, fci->fci_bmapno, SL_READ, &b);
			if (rc) {
				fcmh_op_done_type(f,
				    FCMH_OPCNT_READAHEAD);
				break;
			}
			r = psc_pool_get(slc_biorq_pool);
			memset(r, 0, sizeof(*r));

			INIT_PSC_LISTENTRY(&r->biorq_lentry);
			INIT_PSC_LISTENTRY(&r->biorq_png_lentry);
			INIT_SPINLOCK(&r->biorq_lock);

			r->biorq_ref = 1;
			r->biorq_bmap = b;
			r->biorq_npages = 0;
			r->biorq_flags = BIORQ_READ;
			r->biorq_last_sliod = IOS_ID_ANY;

			bmpc = bmap_2_bmpc(b);
			pll_add(&bmpc->bmpc_pndg_biorqs, r);
			bmap_op_start_type(b, BMAP_OPCNT_BIORQ);

			bmap_op_done(b);

			for (i = 0; i < fci->fci_rapages; i++) {
				BMAP_LOCK(b);
				e = bmpce_lookup_locked(b,
				    fci->fci_raoff + i * BMPC_BUFSZ,
				    &r->biorq_bmap->bcm_fcmh->fcmh_waitq);
				BMAP_ULOCK(b);

				psc_dynarray_add(&r->biorq_pages, e);
			}
			OPSTAT_INCR(SLC_OPST_READAHEAD_FETCH);
			msl_pages_fetch(r);
			msl_biorq_destroy(r);

			fcmh_op_done_type(f, FCMH_OPCNT_READAHEAD);
			break;
		}
		if (!did_work)
			LIST_CACHE_ULOCK(&slc_readaheadq);
	}
}

void
msattrflushthr_main(struct psc_thread *thr)
{
	struct timespec ts, nexttimeo;
	struct fcmh_cli_info *fci;
	struct fidc_membh *f;

	while (pscthr_run(thr)) {
		nexttimeo.tv_sec = FCMH_ATTR_TIMEO;
		nexttimeo.tv_nsec = 0;

		LIST_CACHE_LOCK(&slc_attrtimeoutq);
		lc_peekheadwait(&slc_attrtimeoutq);
		PFL_GETTIMESPEC(&ts);
		LIST_CACHE_FOREACH(fci, &slc_attrtimeoutq) {
			f = fci_2_fcmh(fci);
			if (!FCMH_TRYLOCK(f))
				continue;

			if (f->fcmh_flags & FCMH_BUSY) {
				FCMH_ULOCK(f);
				continue;
			}

			if (timespeccmp(&fci->fci_etime, &ts, >)) {
				timespecsub(&fci->fci_etime, &ts,
				    &nexttimeo);
				FCMH_ULOCK(f);
				continue;
			}

			LIST_CACHE_ULOCK(&slc_attrtimeoutq);

			msl_flush_ioattrs(NULL, f);
			FCMH_ULOCK(f);
			break;
		}
		if (fci == NULL) {
			OPSTAT_INCR(SLC_OPST_FLUSH_ATTR_WAIT);
			psc_waitq_waitrel(&msl_flush_attrq,
			    &slc_attrtimeoutq.plc_lock, &nexttimeo);
		}
	}
}

void
msreadaheadthr_spawn(void)
{
	struct msreadahead_thread *mrat;
	struct psc_thread *thr;
	int i;

	lc_reginit(&slc_readaheadq, struct fcmh_cli_info, fci_readahead,
	    "readahead");

	for (i = 0; i < NUM_READAHEAD_THREADS; i++) {
		thr = pscthr_init(MSTHRT_READAHEAD, msreadaheadthr_main,
		    NULL, sizeof(*mrat), "msreadaheadthr%d", i);
		mrat = msreadaheadthr(thr);
		psc_multiwait_init(&mrat->mrat_mw, "%s",
		    thr->pscthr_name);
		pscthr_setready(thr);
	}
}

void
msattrflushthr_spawn(void)
{
	struct msattrflush_thread *maft;
	struct psc_thread *thr;
	int i;

	lc_reginit(&slc_attrtimeoutq, struct fcmh_cli_info, fci_lentry,
	    "attrtimeout");

	for (i = 0; i < NUM_ATTR_FLUSH_THREADS; i++) {
		thr = pscthr_init(MSTHRT_ATTR_FLUSH,
		    msattrflushthr_main, NULL, sizeof(*maft),
		    "msattrflushthr%d", i);
		maft = msattrflushthr(thr);
		psc_multiwait_init(&maft->maft_mw, "%s",
		    thr->pscthr_name);
		pscthr_setready(thr);
	}
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
slc_setprefios(sl_ios_id_t id)
{
	struct sl_resource *r, *ri;
	int j;

//	CONF_LOCK();
	r = libsl_id2res(prefIOS);
	if (r) {
		r->res_flags &= ~RESF_PREFIOS;
		if (RES_ISCLUSTER(r))
			DYNARRAY_FOREACH(ri, j, &r->res_peers)
				ri->res_flags &= ~RESF_PREFIOS;
	}
	prefIOS = id;
	r = libsl_id2res(id);
	r->res_flags |= RESF_PREFIOS;
	if (RES_ISCLUSTER(r))
		DYNARRAY_FOREACH(ri, j, &r->res_peers)
			ri->res_flags &= ~RESF_PREFIOS;
//	CONF_ULOCK();
}

void
msl_init(void)
{
	char *name;
	int rc;

	authbuf_checkkeyfile();
	authbuf_readkeyfile();

	libsl_init(4096);//2 * (SRCI_NBUFS + SRCM_NBUFS));
	fidc_init(sizeof(struct fcmh_cli_info));
	bmpc_global_init();
	bmap_cache_init(sizeof(struct bmap_cli_info));
	dircache_mgr_init();

	psc_poolmaster_init(&slc_async_req_poolmaster,
	    struct slc_async_req, car_lentry, PPMF_AUTO, 64, 64, 0,
	    NULL, NULL, NULL, "asyncrq");
	slc_async_req_pool = psc_poolmaster_getmgr(&slc_async_req_poolmaster);

	psc_poolmaster_init(&slc_biorq_poolmaster,
	    struct bmpc_ioreq, biorq_lentry, PPMF_AUTO, 64, 64, 0, NULL,
	    NULL, NULL, "biorq");
	slc_biorq_pool = psc_poolmaster_getmgr(&slc_biorq_poolmaster);

	psc_poolmaster_init(&slc_mfh_poolmaster,
	    struct msl_fhent, mfh_lentry, PPMF_AUTO, 64, 64, 0, NULL,
	    NULL, NULL, "mfh");
	slc_mfh_pool = psc_poolmaster_getmgr(&slc_mfh_poolmaster);

	pfl_workq_init(128);
	pfl_wkthr_spawn(MSTHRT_WORKER, 4, "mswkthr%d");

	slrpc_initcli();
	slc_rpc_initsvc();

	/* Start up service threads. */
	psc_eqpollthr_spawn(MSTHRT_EQPOLL, "mseqpollthr");
	msctlthr_spawn();
	mstimerthr_spawn();

	psc_iostats_init(&msl_diord_stat, "dio-rd");
	psc_iostats_init(&msl_diowr_stat, "dio-wr");
	psc_iostats_init(&msl_rdcache_stat, "rd-cache-hit");
	psc_iostats_init(&msl_racache_stat, "ra-cache-hit");

	psc_iostats_initf(&msl_io_1b_stat, PISTF_BASE10, "iosz:0-1k");
	psc_iostats_initf(&msl_io_1k_stat, PISTF_BASE10, "iosz:1k-3k");
	psc_iostats_initf(&msl_io_4k_stat, PISTF_BASE10, "iosz:4k-15k");
	psc_iostats_initf(&msl_io_16k_stat, PISTF_BASE10, "iosz:16k-63k");
	psc_iostats_initf(&msl_io_64k_stat, PISTF_BASE10, "iosz:64k-127k");
	psc_iostats_initf(&msl_io_128k_stat, PISTF_BASE10, "iosz:128k-511k");
	psc_iostats_initf(&msl_io_512k_stat, PISTF_BASE10, "iosz:512k-1m");
	psc_iostats_initf(&msl_io_1m_stat, PISTF_BASE10, "iosz:1m-");

	sl_nbrqset = pscrpc_nbreqset_init(NULL);
	pscrpc_nbreapthr_spawn(sl_nbrqset, MSTHRT_NBRQ, 1,
	    "msnbrqthr%d");

	msbmapthr_spawn();
	sl_freapthr_spawn(MSTHRT_FREAP, "msfreapthr");
	msattrflushthr_spawn();
	msreadaheadthr_spawn();

	name = getenv("MDS");
	if (name == NULL)
		psc_fatalx("environment variable MDS not specified");

	rc = slc_rmc_setmds(name);
	if (rc)
		psc_fatalx("invalid MDS %s: %s", name, slstrerror(rc));

	name = getenv("PREF_IOS");
	if (name) {
		struct sl_resource *r;

		r = libsl_str2res(name);
		if (r == NULL)
			psclog_warnx("PREF_IOS (%s) does not resolve to "
			    "a valid IOS; defaulting to IOS_ID_ANY", name);
		else
			slc_setprefios(r->res_id);
	}
}

struct pscfs pscfs = {
	mslfsop_access,
	mslfsop_release,
	mslfsop_release,	/* releasedir */
	mslfsop_create,
	mslfsop_flush,
	mslfsop_fsync,
	mslfsop_fsync,		/* fsyncdir */
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
	mslfsop_destroy,
	mslfsop_write,
	mslfsop_listxattr,
	mslfsop_getxattr,
	mslfsop_setxattr,
	mslfsop_removexattr
};

int
psc_usklndthr_get_type(const char *namefmt)
{
	if (strstr(namefmt, "lnacthr"))
		psc_fatalx("invalid name");
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

	s = slcfg_local->cfg_allowexe;
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
		psclog_info("restricting open(2) access to %s", p);
	}
}

void
parse_mapfile(void)
{
	char fn[PATH_MAX], buf[LINE_MAX], *endp, *p, *t;
	struct uid_mapping *um;
	uid_t to, from;
	FILE *fp;
	long l;
	int ln;

	xmkfn(fn, "%s/%s", sl_datadir, SL_FN_MAPFILE);

	fp = fopen(fn, "r");
	if (fp == NULL)
		err(1, "%s", fn);
	ln = 0;
	while (fgets(buf, sizeof(buf), fp)) {
		ln++;

		for (p = t = buf; isdigit(*t); t++)
			;
		if (!isspace(*t))
			goto malformed;
		*t++ = '\0';
		while (isspace(*t))
			t++;

		l = strtol(p, &endp, 10);
		if (l < 0 || l >= INT_MAX ||
		    endp == p || *endp)
			goto malformed;
		from = l;

		for (p = t; isdigit(*t); t++)
			;
		*t++ = '\0';
		while (isspace(*t))
			t++;
		if (*t)
			goto malformed;

		l = strtol(p, &endp, 10);
		if (l < 0 || l >= INT_MAX ||
		    endp == p || *endp)
			goto malformed;
		to = l;

		um = PSCALLOC(sizeof(*um));
		psc_hashent_init(&slc_uidmap_ext, um);
		um->um_key = from;
		um->um_val = to;
		psc_hashtbl_add_item(&slc_uidmap_ext, um);

		um = PSCALLOC(sizeof(*um));
		psc_hashent_init(&slc_uidmap_int, um);
		um->um_key = to;
		um->um_val = from;
		psc_hashtbl_add_item(&slc_uidmap_int, um);

		continue;

 malformed:
		warn("%s: %d: malformed line", fn, ln);
	}
	if (ferror(fp))
		warn("%s", fn);
	fclose(fp);
}

int
opt_lookup(const char *opt)
{
	struct {
		const char	*name;
		int		*var;
	} *io, opts[] = {
		{ "mapfile",	&use_mapfile },
		{ NULL,		NULL }
	};

	for (io = opts; io->name; io++)
		if (strcmp(opt, io->name) == 0) {
			*io->var = 1;
			return (1);
		}
	return (0);
}

__dead void
usage(void)
{
	fprintf(stderr,
	    "usage: %s [-dQUVX] [-D datadir] [-f conf] [-I iosystem] [-M mds]\n"
	    "\t[-o mountopt] [-S socket] node\n",
	    progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	struct pscfs_args args = PSCFS_ARGS_INIT(0, NULL);
	char c, *p, *noncanon_mp, *cfg = SL_PATH_CONF;
	int unmount_first = 0;

	/* gcrypt must be initialized very early on */
	gcry_control(GCRYCTL_SET_THREAD_CBS, &gcry_threads_pthread);
	if (!gcry_check_version(GCRYPT_VERSION))
		errx(1, "libgcrypt version mismatch");

	progname = argv[0];
	pfl_init();
	sl_subsys_register();
	psc_subsys_register(SLCSS_INFO, "info");

	psc_fault_register(SLC_FAULT_READAHEAD_CB_EIO);
	psc_fault_register(SLC_FAULT_READRPC_OFFLINE);
	psc_fault_register(SLC_FAULT_READ_CB_EIO);
	psc_fault_register(SLC_FAULT_REQUEST_TIMEOUT);

	pscfs_addarg(&args, "");		/* progname/argv[0] */
	pscfs_addarg(&args, "-o");
	pscfs_addarg(&args, STD_MOUNT_OPTIONS);

	p = getenv("CTL_SOCK_FILE");
	if (p)
		ctlsockfn = p;

	cfg = SL_PATH_CONF;
	p = getenv("CONFIG_FILE");
	if (p)
		cfg = p;

	while ((c = getopt(argc, argv, "D:df:I:M:o:QS:UV")) != -1)
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
			setenv("PREF_IOS", optarg, 1);
			break;
		case 'M':
			setenv("MDS", optarg, 1);
			break;
		case 'o':
			if (!opt_lookup(optarg)) {
				pscfs_addarg(&args, "-o");
				pscfs_addarg(&args, optarg);
			}
			break;
		case 'Q':
			slcfg_local->cfg_root_squash = 1;
			break;
		case 'S':
			ctlsockfn = optarg;
			break;
		case 'U':
			unmount_first = 1;
			break;
		case 'V':
			errx(0, "revision is %d", SL_STK_VERSION);
		default:
			usage();
		}
	argc -= optind;
	argv += optind;
	if (argc != 1)
		usage();

	pscthr_init(MSTHRT_FSMGR, NULL, NULL, 0, "msfsmgrthr");

	sl_sys_upnonce = psc_random32();

	noncanon_mp = argv[0];
	if (unmount_first)
		unmount(noncanon_mp);

	/* canonicalize mount path */
	if (realpath(noncanon_mp, mountpoint) == NULL)
		psc_fatal("realpath %s", noncanon_mp);

	pflog_get_fsctx_uprog = slc_log_get_fsctx_uprog;
	pflog_get_fsctx_uid = slc_log_get_fsctx_uid;
	pflog_get_fsctx_pid = slc_log_get_fsctx_pid;

	pscfs_mount(mountpoint, &args);
	pscfs_freeargs(&args);

	sl_drop_privs(1);

	slcfg_local->cfg_fidcachesz = 1024;
	slcfg_parse(cfg);
	parse_allowexe();
	if (use_mapfile) {
		psc_hashtbl_init(&slc_uidmap_ext, 0, struct uid_mapping,
		    um_key, um_hentry, 191, NULL, "uidmapext");
		psc_hashtbl_init(&slc_uidmap_int, 0, struct uid_mapping,
		    um_key, um_hentry, 191, NULL, "uidmapint");
		parse_mapfile();
	}
	msl_init();

	pscfs_entry_timeout = 8.;
	pscfs_attr_timeout = 8.;

	OPSTAT_ASSIGN(SLC_OPST_VERSION, SL_STK_VERSION);
	exit(pscfs_main(sizeof(struct msl_fsrqinfo)));
}
