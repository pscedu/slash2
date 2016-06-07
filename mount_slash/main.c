/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright 2007-2016, Pittsburgh Supercomputing Center
 * All rights reserved.
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
 * ---------------------------------------------------------------------
 * %END_LICENSE%
 */

/*
 * Main SLASH2 client (mount_slash) logic: file system handling
 * routines, daemon initialization, etc.
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
#include "pfl/fault.h"
#include "pfl/fmt.h"
#include "pfl/fs.h"
#include "pfl/fsmod.h"
#include "pfl/log.h"
#include "pfl/opstats.h"
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
#include "pfl/timerthr.h"
#include "pfl/usklndthr.h"
#include "pfl/vbitmap.h"

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


/*
 * Cap max_write at 1048576, which is the same as LNET_MTU, because our flush
 * mechanism assumes that each request can be flushed within ONE RPC.
 */
#ifdef HAVE_FUSE_BIG_WRITES
# define STD_MOUNT_OPTIONS	"allow_other,max_write=1048576,big_writes"
#else
# define STD_MOUNT_OPTIONS	"allow_other,max_write=1048576"
#endif

#define MSL_FS_BLKSIZ		(256 * 1024)

#define msl_load_fcmh(pfr, fid, fp)					\
	msl_fcmh_load_fid((fid), (fp), (pfr))

#define msl_fcmh_get_fg(pfr, fg, fp)					\
	sl_fcmh_lookup((fg)->fg_fid, (fg)->fg_gen, FIDC_LOOKUP_CREATE,	\
	    (fp), (pfr))

#define mfh_getfid(mfh)		fcmh_2_fid((mfh)->mfh_fcmh)
#define mfh_getfg(mfh)		(mfh)->mfh_fcmh->fcmh_fg

#define MSL_FLUSH_ATTR_TIMEOUT	8

#define fcmh_super_root(f)	(fcmh_2_fid(f) == SLFID_ROOT && \
				fcmh_2_gen(f) == FGEN_ANY - 1  ? EPERM : 0)

#define fcmh_reserved(f)	(FID_GET_INUM(fcmh_2_fid(f)) == SLFID_NS ? EPERM : 0)

struct psc_hashtbl		 msl_namecache_hashtbl;
struct psc_waitq		 msl_flush_attrq = PSC_WAITQ_INIT("flush-attr");

struct psc_listcache		 msl_attrtimeoutq;

sl_ios_id_t			 msl_pref_ios = IOS_ID_ANY;

const char			*msl_ctlsockfn = SL_PATH_MSCTLSOCK;

char				 mountpoint[PATH_MAX];
int				 msl_use_mapfile;
struct psc_dynarray		 allow_exe = DYNARRAY_INIT;
char				*msl_cfgfn = SL_PATH_CONF;

struct psc_poolmaster		 msl_async_req_poolmaster;
struct psc_poolmgr		*msl_async_req_pool;

struct psc_poolmaster		 msl_biorq_poolmaster;
struct psc_poolmgr		*msl_biorq_pool;

struct psc_poolmaster		 msl_retry_req_poolmaster;
struct psc_poolmgr		*msl_retry_req_pool;

struct psc_poolmaster		 msl_mfh_poolmaster;
struct psc_poolmgr		*msl_mfh_pool;

struct psc_poolmaster		 mfsrq_poolmaster;
struct psc_poolmgr		*mfsrq_pool;

struct psc_poolmaster		 msl_iorq_poolmaster;
struct psc_poolmgr		*msl_iorq_pool;

uint32_t			 sl_sys_upnonce;

struct psc_hashtbl		 msl_uidmap_ext;
struct psc_hashtbl		 msl_uidmap_int;
struct psc_hashtbl		 msl_gidmap_int;

/*
 * This allows io_submit(2) to work before Linux kernel version 4.1.
 * Before that, O_DIRECT and the FUSE direct_io path were not fully
 * integrated.
 */
int				 msl_acl;
int				 msl_force_dio;
int				 msl_fuse_direct_io = 1;
int				 msl_root_squash;
int				 msl_max_retries = 5;
uint64_t			 msl_pagecache_maxsize;
int				 msl_statfs_pref_ios_only;
struct resprof_cli_info		 msl_statfs_aggr_rpci;

int				 msl_ios_max_inflight_rpcs = RESM_MAX_IOS_OUTSTANDING_RPCS;
int				 msl_mds_max_inflight_rpcs = RESM_MAX_MDS_OUTSTANDING_RPCS;

int				 msl_newent_inherit_groups = 1;

struct psc_thread		*slcconnthr;

/*
 * I/O requests that have failed due to timeouts are placed here for
 * retry.
 */
struct psc_lockedlist		 slc_retry_req_list;

int64_t slc_io_grad_sizes[] = {
		0,
	     1024,
	 4 * 1024,
	16 * 1024,
	64 * 1024,
       128 * 1024,
       512 * 1024,
      1024 * 1024,
};

struct sl_resource *
msl_get_pref_ios(void)
{
	return (libsl_id2res(msl_pref_ios));
}

void
sl_resource_put(__unusedx struct sl_resource *res)
{
}

/*
 * Perform an access check on a file against the specified credentials.
 */
int
fcmh_checkcreds(struct fidc_membh *f,
    struct pscfs_req *pfr, const struct pscfs_creds *pcrp,
    int accmode)
{
	int rc, locked;

	if (msl_root_squash && pcrp->pcr_uid == 0)
		return (EACCES);

#ifdef SLOPT_POSIX_ACLS
	if (msl_acl)
		rc = sl_fcmh_checkacls(f, pfr, pcrp, accmode);
	else
#endif
	{
		(void)pfr;
		locked = FCMH_RLOCK(f);
		rc = checkcreds(&f->fcmh_sstb, pcrp, accmode);
		FCMH_URLOCK(f, locked);
	}
	return (rc);
}

/*
 * Select the group for a new entity (file or directory) that is being
 * created.
 */
gid_t
newent_select_group(struct fidc_membh *p, struct pscfs_creds *pcr)
{
//	if (p->fcmh_sstb.sst_mode & S_ISVTX)
//		return (pcr.pcr_gid);
	if (p->fcmh_sstb.sst_mode & S_ISGID)
		return (p->fcmh_sstb.sst_gid);
	if (msl_newent_inherit_groups)
		return (p->fcmh_sstb.sst_gid);
	return (pcr->pcr_gid);
}

struct pscfs_creds *
slc_getfscreds(struct pscfs_req *pfr, struct pscfs_creds *pcr)
{
	pscfs_getcreds(pfr, pcr);
	gidmap_int_cred(pcr);
	return (pcr);
}

__static void
msfsthr_destroy(void *arg)
{
	struct msfs_thread *mft = arg;

	pfl_multiwait_free(&mft->mft_mw);
	PSCFREE(mft);
}

__static void *
msfsthr_init(struct psc_thread *thr)
{
	struct msfs_thread *mft;

	mft = PSCALLOC(sizeof(*mft));
	pfl_multiwait_init(&mft->mft_mw, "%s", thr->pscthr_name);
	return (mft);
}

void
mslfsop_access(struct pscfs_req *pfr, pscfs_inum_t inum, int accmode)
{
	struct pscfs_creds pcr;
	struct fidc_membh *c;
	int rc;

	rc = msl_load_fcmh(pfr, inum, &c);
	if (rc)
		PFL_GOTOERR(out, rc);

	slc_getfscreds(pfr, &pcr);

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

/*
 * For ProgACLs, determine if the user process that is accessing a file
 * is on the allowed program list.
 */
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

void
mslfsop_create(struct pscfs_req *pfr, pscfs_inum_t pinum,
    const char *name, int oflags, mode_t mode)
{
	int rc = 0, rc2, rflags = 0;
	struct dircache_ent_update dcu = DCE_UPD_INIT;
	struct fidc_membh *c = NULL, *p = NULL;
	struct slrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_create_rep *mp = NULL;
	struct msl_fhent *mfh = NULL;
	struct srm_create_req *mq;
	struct fcmh_cli_info *fci;
	struct bmap_cli_info *bci;
	struct pscfs_creds pcr;
	struct stat stb;
	struct bmap *b;

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
	rc = fcmh_super_root(p);
	if (rc)
		PFL_GOTOERR(out, rc);
	rc = fcmh_reserved(p);
	if (rc)
		PFL_GOTOERR(out, rc);

	slc_getfscreds(pfr, &pcr);
	rc = fcmh_checkcreds(p, pfr, &pcr, W_OK | X_OK);
	if (rc)
		PFL_GOTOERR(out, rc);

 retry1:
	MSL_RMC_NEWREQ(p, csvc, SRMT_CREATE, rq, mq, mp, rc);
	if (rc)
		goto retry2;

	mq->mode = !(mode & 0777) ? (0666 & ~pscfs_getumask(pfr)) :
	    mode;
	mq->pfg.fg_fid = pinum;
	mq->pfg.fg_gen = FGEN_ANY;
	mq->prefios[0] = msl_pref_ios;
	mq->owner.scr_uid = pcr.pcr_uid;
	mq->owner.scr_gid = newent_select_group(p, &pcr);
	rc = uidmap_ext_cred(&mq->owner);
	if (rc)
		PFL_GOTOERR(out, rc);
	strlcpy(mq->name, name, sizeof(mq->name));
	PFL_GETPTIMESPEC(&mq->time);

	namecache_hold_entry(&dcu, p, name);
	rc = SL_RSX_WAITREP(csvc, rq, mp);

 retry2:
	if (rc && slc_rpc_should_retry(pfr, &rc)) {
		namecache_fail(&dcu);
		goto retry1;
	}
	rc = abs(rc);
	if (rc == 0)
		rc = -mp->rc;
	if (rc)
		PFL_GOTOERR(out, rc);

	slc_fcmh_setattr(p, &mp->pattr);

	rc = msl_fcmh_get_fg(pfr, &mp->cattr.sst_fg, &c);
	if (rc)
		PFL_GOTOERR(out, rc);

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
	slc_fcmh_setattrf(c, &mp->cattr, FCMH_SETATTRF_HAVELOCK |
	    FCMH_SETATTRF_CLOBBER);
	sl_internalize_stat(&c->fcmh_sstb, &stb);

	fci = fcmh_2_fci(c);

	// fci_inode should be read from
	// msl_fcmh_save_inode(c, &mp->ino);

	// XXX bug fci->fci_inode.reptbl inherited?

	fci->fci_inode.reptbl[0].bs_id = mp->sbd.sbd_ios;
	fci->fci_inode.nrepls = 1;

	// XXX bug fci->fci_inode.flags inherited?
	// XXX bug fci->fci_inode.newreplpol inherited?

	fcmh_op_start_type(c, FCMH_OPCNT_OPEN);

	if ((c->fcmh_sstb.sst_mode & _S_IXUGO) == 0 && msl_fuse_direct_io)
		rflags |= PSCFS_CREATEF_DIO;

	FCMH_ULOCK(c);

	/*
	 * Any errors encountered here cannot affect the return status
	 * of the open(2).  If they were to, the namei cache in the
	 * kernel would get confused thinking there the error here was a
	 * failure of the creation itself.
	 *
	 * Instead, wait for the application to perform some actual I/O
	 * to retrieve bmap lease on-demand.
	 */
	if (mp->rc2)
		PFL_GOTOERR(out, mp->rc2);

	/*
	 * Instantiate a bmap and load it with the piggybacked lease
	 * from the above create RPC.
	 */
	rc2 = bmap_getf(c, 0, SL_WRITE, BMAPGETF_CREATE |
	    BMAPGETF_NORETRIEVE, &b);
	if (rc2)
		PFL_GOTOERR(out, rc2);

	b->bcm_flags |= BMAPF_LOADED;
	msl_bmap_stash_lease(b, &mp->sbd, "preload");
	msl_bmap_reap_init(b);

	DEBUG_BMAP(PLL_DIAG, b, "ios(%s) sbd_seq=%"PRId64,
	    libsl_ios2name(mp->sbd.sbd_ios), mp->sbd.sbd_seq);

	bci = bmap_2_bci(b);
	// XXX this is wrong if the fcmh inherited from a dir with a
	// reptbl!
	SL_REPL_SET_BMAP_IOS_STAT(bci->bci_repls, 0, BREPLST_VALID);

	bmap_op_done(b);

 out:
	pscfs_reply_create(pfr, mp ? mp->cattr.sst_fid : 0,
	    mp ? mp->cattr.sst_gen : 0, pscfs_entry_timeout, &stb,
	    pscfs_attr_timeout, mfh, rflags, rc);
	namecache_update(&dcu, mp->cattr.sst_fid, rc);

	psclogs(rc ? PLL_INFO : PLL_DIAG, SLCSS_FSOP, "CREATE: pfid="SLPRI_FID" "
	    "cfid="SLPRI_FID" name='%s' mode=%#o oflags=%#o rc=%d",
	    pinum, mp ? mp->cattr.sst_fid : FID_ANY, name, mode, oflags,
	    rc);

	if (c)
		fcmh_op_done(c);
	if (p)
		fcmh_op_done(p);
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

	slc_getfscreds(pfr, &pcr);

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
	 * so don't enable DIO on executable files so they can be
	 * executed.
	 */
	if ((c->fcmh_sstb.sst_mode & _S_IXUGO) == 0 && msl_fuse_direct_io)
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
	FCMH_LOCK(c);
	fcmh_op_start_type(c, FCMH_OPCNT_OPEN);

 out:
	if (c) 
		fcmh_op_done(c);
	psclogs(rc ? PLL_INFO : PLL_DIAG, SLCSS_FSOP, ""
	    "OPEN: new mfh=%p dir=%s rc=%d oflags=%#o rflags=%#o",
	    *mfhp, (oflags & O_DIRECTORY) ? "yes" : "no", rc, oflags, *rflags);
	return (rc);
}

void
mslfsop_open(struct pscfs_req *pfr, pscfs_inum_t inum, int oflags)
{
	struct msl_fhent *mfh = NULL;
	int rflags, rc;

	rflags = 0;
	rc = msl_open(pfr, inum, oflags, &mfh, &rflags);
	pscfs_reply_open(pfr, mfh, rflags, rc);
}

void
mslfsop_opendir(struct pscfs_req *pfr, pscfs_inum_t inum, int oflags)
{
	struct msl_fhent *mfh = NULL;
	int rflags, rc;

	rflags = 0;
	rc = msl_open(pfr, inum, oflags | O_DIRECTORY, &mfh, &rflags);
	pscfs_reply_opendir(pfr, mfh, rflags, rc);
}

int
msl_stat(struct fidc_membh *f, void *arg)
{
	struct slrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct pscfs_req *pfr = arg;
	struct srm_getattr_req *mq;
	struct srm_getattr_rep *mp;
	struct fcmh_cli_info *fci;
	struct timeval now;
	int rc = 0;

	/*
	 * Special case to handle accesses to
	 * /$mountpoint/.slfidns/<fid>
	 */
	if (FID_GET_INUM(fcmh_2_fid(f)) == SLFID_NS) {
		f->fcmh_sstb.sst_mode = S_IFDIR | 0111;
		f->fcmh_sstb.sst_nlink = 2;
		f->fcmh_sstb.sst_size = 2;
		f->fcmh_sstb.sst_blksize = MSL_FS_BLKSIZ;
		f->fcmh_sstb.sst_blocks = 4;
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
		MSL_RMC_NEWREQ(f, csvc, SRMT_GETATTR, rq, mq, mp, rc);
		if (!rc) {
			mq->fg = f->fcmh_fg;
			mq->iosid = msl_pref_ios;

			rc = SL_RSX_WAITREP(csvc, rq, mp);
		}
	} while (rc && slc_rpc_should_retry(pfr, &rc));

	rc = abs(rc);
	if (rc == 0)
		rc = -mp->rc;

	FCMH_LOCK(f);
	if (!rc && fcmh_2_fid(f) != mp->attr.sst_fid)
		rc = EBADF;
	if (!rc) {
		slc_fcmh_setattr_locked(f, &mp->attr);
		msl_fcmh_stash_xattrsize(f, mp->xattrsize);
	}
	f->fcmh_flags &= ~FCMH_GETTING_ATTRS;
	fcmh_wake_locked(f);

	DEBUG_FCMH(PLL_DEBUG, f, "attrs retrieved via rpc rc=%d", rc);

	FCMH_ULOCK(f);
	pscrpc_req_finished(rq);
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

	slc_getfscreds(pfr, &pcr);

	/*
	 * Lookup and possibly create a new fidcache handle for inum.
	 * If the FID does not exist in the cache then a placeholder
	 * will be allocated.  msl_stat() will detect incomplete attrs
	 * via the FCMH_GETTING_ATTRS flag and RPC for them.
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
	struct dircache_ent_update dcu = DCE_UPD_INIT;
	struct fidc_membh *p = NULL, *c = NULL;
	struct slrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_link_rep *mp = NULL;
	struct srm_link_req *mq;
	struct pscfs_creds pcr;
	struct stat stb;
	int rc = 0;

	if (strlen(newname) == 0)
		PFL_GOTOERR(out, rc = ENOENT);
	if (strlen(newname) > SL_NAME_MAX)
		PFL_GOTOERR(out, rc = ENAMETOOLONG);

	slc_getfscreds(pfr, &pcr);

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

	/*
	 * XXX missing checks/return conditions
	 *
	 * [EACCES]     The current process cannot access the existing file.
	 */

	/* Check the child inode. */
	rc = msl_load_fcmh(pfr, c_inum, &c);
	if (rc)
		PFL_GOTOERR(out, rc);

	if (fcmh_isdir(c))
		PFL_GOTOERR(out, rc = EISDIR);

	if (FID_GET_SITEID(fcmh_2_fid(p)) !=
	    FID_GET_SITEID(fcmh_2_fid(c)))
		PFL_GOTOERR(out, rc = EXDEV);

 retry:
	MSL_RMC_NEWREQ(p, csvc, SRMT_LINK, rq, mq, mp, rc);
	if (!rc) {
		mq->pfg = p->fcmh_fg;
		mq->fg = c->fcmh_fg;
		strlcpy(mq->name, newname, sizeof(mq->name));

		namecache_hold_entry(&dcu, p, newname);
		rc = SL_RSX_WAITREP(csvc, rq, mp);
	}
	if (rc && slc_rpc_should_retry(pfr, &rc)) {
		namecache_fail(&dcu);
		goto retry;
	}
	rc = abs(rc);
	if (rc == 0)
		rc = -mp->rc;
	if (rc)
		PFL_GOTOERR(out, rc);

	slc_fcmh_setattr(p, &mp->pattr);

	FCMH_LOCK(c);
	slc_fcmh_setattr_locked(c, &mp->cattr);
	sl_internalize_stat(&c->fcmh_sstb, &stb);
	FCMH_ULOCK(c);

 out:
	pscfs_reply_link(pfr, mp ? mp->cattr.sst_fid : 0,
	    mp ? mp->cattr.sst_gen : 0, pscfs_entry_timeout, &stb,
	    pscfs_attr_timeout, rc);
	namecache_update(&dcu, fcmh_2_fid(c), rc);

	psclogs(rc ? PLL_INFO : PLL_DIAG, SLCSS_FSOP, "LINK: cfid="SLPRI_FID" "
	    "pfid="SLPRI_FID" name='%s' rc=%d",
	    c_inum, p_inum, newname, rc);

	if (c)
		fcmh_op_done(c);
	if (p)
		fcmh_op_done(p);
	pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

void
mslfsop_mkdir(struct pscfs_req *pfr, pscfs_inum_t pinum,
    const char *name, mode_t mode)
{
	struct dircache_ent_update dcu = DCE_UPD_INIT;
	struct fidc_membh *c = NULL, *p = NULL;
	struct slrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_mkdir_rep *mp = NULL;
	struct srm_mkdir_req *mq;
	struct pscfs_creds pcr;
	struct stat stb;
	int rc;

	if (strlen(name) == 0)
		PFL_GOTOERR(out, rc = ENOENT);
	if (strlen(name) > SL_NAME_MAX)
		PFL_GOTOERR(out, rc = ENAMETOOLONG);

	rc = msl_load_fcmh(pfr, pinum, &p);
	if (rc)
		PFL_GOTOERR(out, rc);

	if (!fcmh_isdir(p))
		PFL_GOTOERR(out, rc = ENOTDIR);
	rc = fcmh_super_root(p);
	if (rc)
		PFL_GOTOERR(out, rc);
	rc = fcmh_reserved(p);
	if (rc)
		PFL_GOTOERR(out, rc);

	slc_getfscreds(pfr, &pcr);

	rc = fcmh_checkcreds(p, pfr, &pcr, W_OK);
	if (rc)
		PFL_GOTOERR(out, rc);

	if (p->fcmh_sstb.sst_mode & S_ISGID)
		mode |= S_ISGID;

 retry1:
	MSL_RMC_NEWREQ(p, csvc, SRMT_MKDIR, rq, mq, mp, rc);
	if (rc)
		goto retry2;

	mq->pfg.fg_fid = pinum;
	mq->pfg.fg_gen = FGEN_ANY;
	mq->sstb.sst_uid = pcr.pcr_uid;
	mq->sstb.sst_gid = newent_select_group(p, &pcr);
	rc = uidmap_ext_stat(&mq->sstb);
	if (rc)
		PFL_GOTOERR(out, rc);
	mq->sstb.sst_mode = mode;
	mq->to_set = PSCFS_SETATTRF_MODE;
	strlcpy(mq->name, name, sizeof(mq->name));

	namecache_hold_entry(&dcu, p, name);
	rc = SL_RSX_WAITREP(csvc, rq, mp);

  retry2:
	if (rc && slc_rpc_should_retry(pfr, &rc)) {
		namecache_fail(&dcu);
		goto retry1;
	}
	rc = abs(rc);
	if (rc == 0)
		rc = -mp->rc;
	if (rc)
		PFL_GOTOERR(out, rc);

	slc_fcmh_setattr(p, &mp->pattr);

	rc = msl_fcmh_get_fg(pfr, &mp->cattr.sst_fg, &c);
	if (rc)
		PFL_GOTOERR(out, rc);

	FCMH_LOCK(c);
	slc_fcmh_setattr_locked(c, &mp->cattr);
	sl_internalize_stat(&mp->cattr, &stb);
	FCMH_ULOCK(c);

 out:
	pscfs_reply_mkdir(pfr, mp ? mp->cattr.sst_fid : 0,
	    mp ? mp->cattr.sst_gen : 0, pscfs_entry_timeout, &stb,
	    pscfs_attr_timeout, rc);
	namecache_update(&dcu, mp->cattr.sst_fid, rc);

	psclogs(rc ? PLL_INFO : PLL_DIAG, SLCSS_FSOP, "MKDIR: pfid="SLPRI_FID" "
	    "cfid="SLPRI_FID" mode=%#o name='%s' rc=%d",
	    pinum, c ? c->fcmh_sstb.sst_fid : FID_ANY, mode, name, rc);

	if (c)
		fcmh_op_done(c);
	if (p)
		fcmh_op_done(p);
	pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

__static int
msl_lookuprpc(struct pscfs_req *pfr, struct fidc_membh *p,
    const char *name, struct sl_fidgen *fgp, struct srt_stat *sstb,
    struct fidc_membh **fp)
{
	slfid_t pfid = fcmh_2_fid(p);
	struct slrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct fidc_membh *f = NULL;
	struct srm_lookup_req *mq;
	struct srm_lookup_rep *mp;
	int rc;

 retry:
	MSL_RMC_NEWREQ(p, csvc, SRMT_LOOKUP, rq, mq, mp, rc);
	if (!rc) {
		mq->pfg.fg_fid = pfid;
		mq->pfg.fg_gen = FGEN_ANY;
		strlcpy(mq->name, name, sizeof(mq->name));

		rc = SL_RSX_WAITREP(csvc, rq, mp);
	}
	if (rc && slc_rpc_should_retry(pfr, &rc))
		goto retry;

	rc = abs(rc);
	if (rc == 0)
		rc = -mp->rc;
	if (rc)
		PFL_GOTOERR(out, rc);

	/*
	 * Add the inode to the cache first, otherwise pscfs may come to
	 * us with another request for the inode since it won't yet be
	 * visible in the cache.
	 */
	rc = msl_fcmh_get_fg(pfr, &mp->attr.sst_fg, &f);
	if (rc)
		PFL_GOTOERR(out, rc);

	if (fgp)
		*fgp = mp->attr.sst_fg;

	FCMH_LOCK(f);
	slc_fcmh_setattr_locked(f, &mp->attr);
	msl_fcmh_stash_xattrsize(f, mp->xattrsize);

	if (sstb)
		*sstb = f->fcmh_sstb;

 out:
	psclogs_diag(SLCSS_FSOP, "LOOKUP: pfid="SLPRI_FID" name='%s' "
	    "cfid="SLPRI_FID" rc=%d",
	    pfid, name, f ? f->fcmh_sstb.sst_fid : FID_ANY, rc);

	if (rc == 0 && fp) {
		*fp = f;
		FCMH_ULOCK(f);
	} else if (f)
		fcmh_op_done(f);
	pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
	return (rc);
}


/* Remove the following #if when we actually implement dircache_tally_lookup_miss() */
#if 0
int
slc_wk_issue_readdir(void *p)
{
	struct slc_wkdata_readdir *wk = p;

	msl_readdir_issue(wk->d, wk->off, wk->size, 0);
	FCMH_LOCK(wk->d);
	wk->pg->dcp_refcnt--;
	fcmh_op_done_type(wk->d, FCMH_OPCNT_WORKER);
	return (0);
}
#endif

#define msl_lookup_fidcache(pfr, pcrp, pinum, name, fgp, sstb, fp)	\
	msl_lookup_fidcache_dcu((pfr), (pcrp), (pinum), (name), (fgp),	\
	    (sstb), (fp), NULL, NULL)

/*
 * Query the fidcache for a file system entity name.
 *
 * Special handling for pseudo files is made first.
 *
 * Then the local caches are queried.  If not present and fresh, remote
 * RPCs are made to perform classic UNIX-style "namei" resolution and
 * refresh file attribute metadata.
 */
__static int
msl_lookup_fidcache_dcu(struct pscfs_req *pfr,
    const struct pscfs_creds *pcrp, pscfs_inum_t pinum,
    const char *name, struct sl_fidgen *fgp, struct srt_stat *sstb,
    struct fidc_membh **fp, struct fidc_membh **parentp,
    struct dircache_ent_update *dcup)
{
	struct dircache_ent_update *orig_dcu = dcup, dcu = DCE_UPD_INIT;
	struct fidc_membh *p = NULL, *c = NULL;
	int rc;

	if (dcup == NULL)
		dcup = &dcu;

	if (fp)
		*fp = NULL;
	if (parentp)
		*parentp = NULL;

	/*
	 * The parent inode number is either the super root or the site
	 * root when the global mount is not enabled by the MDS.
	 */
	if (pinum == SLFID_ROOT && strcmp(name, MSL_FIDNS_RPATH) == 0) {

		slfid_t	fid;

		if (pcrp->pcr_uid)
			return (EACCES);

		fid = SLFID_NS;
		FID_SET_SITEID(fid, msl_rmc_resm->resm_siteid);
		if (fgp) {
			fgp->fg_fid = fid;
			fgp->fg_gen = 0;
		}
		sstb->sst_fid = fid;
		sstb->sst_gen = 0;
		sstb->sst_mode = S_IFDIR | 0111;
		sstb->sst_nlink = 2;
		sstb->sst_size = 2;
		sstb->sst_blksize = MSL_FS_BLKSIZ;
		return (0);
	}
	if (FID_GET_INUM(pinum) == SLFID_NS) {
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
	if (rc)
		PFL_GOTOERR(out, rc);

	namecache_hold_entry(dcup, p, name);
	if (dcup->dcu_dce->dce_pfd->pfd_ino == FID_ANY ||
	    sl_fcmh_lookup(dcup->dcu_dce->dce_pfd->pfd_ino, FGEN_ANY,
	    FIDC_LOOKUP_LOCK, &c, pfr)) {
		rc = msl_lookuprpc(pfr, p, name, fgp, sstb, &c);
		PFL_GOTOERR(out, rc);
	}
	/*
	 * Note that the name cache is actually disconnected from the
	 * fcmh cache.  So we must populate the name cache carefully.
	 */
	psc_assert((c->fcmh_flags & FCMH_DELETED) == 0);
	FCMH_ULOCK(c);

	/*
	 * We should do a lookup based on name here because a rename
	 * does not change the FID and we would get a success in a STAT
	 * RPC.  Note the call is looking based on a name here, not
	 * based on FID.
	 */
	rc = msl_stat(c, pfr);
	if (rc)
		PFL_GOTOERR(out, rc);

	FCMH_LOCK(c);
	if (fgp)
		*fgp = c->fcmh_fg;
	if (sstb)
		*sstb = c->fcmh_sstb;
	if (fp)
		FCMH_ULOCK(c);

 out:
	/* Caller wants parent; don't drop. */
	if (parentp && p) {
		*parentp = p;
		p = NULL;
		psc_assert(orig_dcu);
	}

	/*
	 * Use 'dcu' here instead of 'dcup' because we only want to
	 * release HOLD if we grabbed it; whereas a caller will want to
	 * release it if they grabbed HOLD.
	 */
	namecache_update(&dcu, fcmh_2_fid(c), rc);
	if (rc == 0 && fp) {
		*fp = c;
		c = NULL;
	}
	if (p)
		fcmh_op_done(p);
	if (c)
		fcmh_op_done(c);

	psclog_diag("lookup basename='%s' pfid="SLPRI_FID" rc=%d",
	    name, pinum, rc);

	return (rc);
}

__static void
msl_unlink(struct pscfs_req *pfr, pscfs_inum_t pinum, const char *name,
    int isfile)
{
	struct dircache_ent_update dcu = DCE_UPD_INIT;
	struct fidc_membh *c = NULL, *p = NULL;
	struct slrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_unlink_rep *mp = NULL;
	struct srm_unlink_req *mq;
	struct pscfs_creds pcr;
	int rc;

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
	if (pinum == SLFID_ROOT && strcmp(name, MSL_FIDNS_RPATH) == 0)
		PFL_GOTOERR(out, rc = EPERM);

	slc_getfscreds(pfr, &pcr);

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
				rc = EACCES;
		} else
			FCMH_ULOCK(p);
	} else {
		rc = fcmh_checkcreds(p, pfr, &pcr, W_OK);
		FCMH_ULOCK(p);
	}
	if (rc)
		PFL_GOTOERR(out, rc);

 retry:
	if (isfile)
		MSL_RMC_NEWREQ(p, csvc, SRMT_UNLINK, rq, mq, mp, rc);
	else
		MSL_RMC_NEWREQ(p, csvc,  SRMT_RMDIR, rq, mq, mp, rc);

	if (!rc) {
		mq->pfid = pinum;
		strlcpy(mq->name, name, sizeof(mq->name));
		namecache_hold_entry(&dcu, p, name);
		rc = SL_RSX_WAITREP(csvc, rq, mp);
	}
	if (rc && slc_rpc_should_retry(pfr, &rc)) {
		namecache_fail(&dcu);
		goto retry;
	}
	rc = abs(rc);
	if (rc == 0)
		rc = -mp->rc;
	if (rc)
		PFL_GOTOERR(out, rc);

	if (!rc) {
		slc_fcmh_setattr(p, &mp->pattr);

		if (sl_fcmh_lookup(mp->cattr.sst_fg.fg_fid, FGEN_ANY,
		    FIDC_LOOKUP_LOCK, &c, pfr))
			OPSTAT_INCR("msl.delete-skipped");
		else {
			if (mp->valid) {
				slc_fcmh_setattr_locked(c, &mp->cattr);
			} else {
				c->fcmh_flags |= FCMH_DELETED;
				OPSTAT_INCR("msl.delete-marked");
			}
		}
	}

 out:
	psclogs_diag(SLCSS_FSOP, "UNLINK: pinum="SLPRI_FID" "
	    "fid="SLPRI_FID" valid=%d name='%s' isfile=%d rc=%d",
	    pinum, mp ? mp->cattr.sst_fid : FID_ANY,
	    mp ? mp->valid : -1, name, isfile, rc);

	if (isfile)
		pscfs_reply_unlink(pfr, rc);
	else
		pscfs_reply_rmdir(pfr, rc);

	namecache_delete(&dcu, rc);

	if (c)
		fcmh_op_done(c);
	if (p)
		fcmh_op_done(p);

	pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

void
mslfsop_unlink(struct pscfs_req *pfr, pscfs_inum_t pinum,
    const char *name)
{
	msl_unlink(pfr, pinum, name, 1);
}

void
mslfsop_rmdir(struct pscfs_req *pfr, pscfs_inum_t pinum,
    const char *name)
{
	msl_unlink(pfr, pinum, name, 0);
}

void
mslfsop_mknod(struct pscfs_req *pfr, pscfs_inum_t pinum,
    const char *name, mode_t mode, dev_t rdev)
{
	struct dircache_ent_update dcu = DCE_UPD_INIT;
	struct fidc_membh *p = NULL, *c = NULL;
	struct slrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_mknod_rep *mp = NULL;
	struct srm_mknod_req *mq = NULL;
	struct pscfs_creds pcr;
	struct stat stb;
	int rc;

	if (!S_ISFIFO(mode) && !S_ISSOCK(mode))
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

	slc_getfscreds(pfr, &pcr);

	rc = fcmh_checkcreds(p, pfr, &pcr, W_OK);
	if (rc)
		PFL_GOTOERR(out, rc);

 retry1:
	MSL_RMC_NEWREQ(p, csvc, SRMT_MKNOD, rq, mq, mp, rc);
	if (rc)
		goto retry2;

	mq->creds.scr_uid = pcr.pcr_uid;
	mq->creds.scr_gid = newent_select_group(p, &pcr);
	rc = uidmap_ext_cred(&mq->creds);
	if (rc)
		PFL_GOTOERR(out, rc);
	mq->pfg.fg_fid = pinum;
	mq->pfg.fg_gen = FGEN_ANY;
	mq->mode = mode;
	mq->rdev = rdev;
	strlcpy(mq->name, name, sizeof(mq->name));

	namecache_hold_entry(&dcu, p, name);
	rc = SL_RSX_WAITREP(csvc, rq, mp);

 retry2:

	if (rc && slc_rpc_should_retry(pfr, &rc)) {
		namecache_fail(&dcu);
		goto retry1;
	}
	rc = abs(rc);
	if (rc == 0)
		rc = -mp->rc;
	if (rc)
		PFL_GOTOERR(out, rc);

	slc_fcmh_setattr(p, &mp->pattr);

	rc = msl_fcmh_get_fg(pfr, &mp->cattr.sst_fg, &c);
	if (rc)
		PFL_GOTOERR(out, rc);

	FCMH_LOCK(c);
	slc_fcmh_setattr_locked(c, &mp->cattr);
	sl_internalize_stat(&mp->cattr, &stb);
	FCMH_ULOCK(c);

 out:
	pscfs_reply_mknod(pfr, mp ? mp->cattr.sst_fid : 0,
	    mp ? mp->cattr.sst_gen : 0, pscfs_entry_timeout, &stb,
	    pscfs_attr_timeout, rc);
	namecache_update(&dcu, mp->cattr.sst_fid, rc);

	psclogs_diag(SLCSS_FSOP, "MKNOD: pfid="SLPRI_FID" "
	    "cfid="SLPRI_FID" mode=%#o name='%s' rc=%d",
	    pinum, c ? c->fcmh_sstb.sst_fid : FID_ANY, mode, name, rc);

	if (c)
		fcmh_op_done(c);
	if (p)
		fcmh_op_done(p);
	pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

void
msl_readdir_error(struct fidc_membh *d, struct dircache_page *p, int rc)
{
	DIRCACHE_WRLOCK(d);
	p->dcp_refcnt--;
	PFLOG_DIRCACHEPG(PLL_DEBUG, p, "error rc=%d", rc);
	if (p->dcp_flags & DIRCACHEPGF_LOADING) {
		p->dcp_flags &= ~DIRCACHEPGF_LOADING;
		p->dcp_rc = rc;
		OPSTAT_INCR("msl.dircache-load-error");
		PFL_GETPTIMESPEC(&p->dcp_local_tm);
		p->dcp_remote_tm = d->fcmh_sstb.sst_mtim;
		DIRCACHE_WAKE(d);
	}
	DIRCACHE_ULOCK(d);
}

/*
 * Perform successful READDIR RPC reception processing.
 */
int
msl_readdir_finish(struct fidc_membh *d, struct dircache_page *p,
    int eof, int nents, int size, void *base)
{
	struct srt_readdir_ent *e;
	struct sl_fidgen *fgp;
	struct fidc_membh *f;
	void *ebase;
	int rc, i;

	ebase = PSC_AGP(base, size);

	rc = dircache_reg_ents(d, p, &nents, base, size, eof);
	if (rc)
		return (rc);
	DIRCACHE_WAKE(d);
	for (i = 0, e = ebase; i < nents; i++, e++) {
		fgp = &e->sstb.sst_fg;
		if (fgp->fg_fid == FID_ANY ||
		    fgp->fg_fid == 0) {
			DEBUG_SSTB(PLL_WARN, &e->sstb,
			    "invalid readdir prefetch FID ent=%d "
			    "parent@%p="SLPRI_FID, i, d, fcmh_2_fid(d));
			continue;
		}

		DEBUG_SSTB(PLL_DEBUG, &e->sstb, "prefetched");

		if (!sl_fcmh_lookup(fgp->fg_fid, fgp->fg_gen,
		    FIDC_LOOKUP_CREATE | FIDC_LOOKUP_LOCK, &f, NULL)) {
			slc_fcmh_setattr_locked(f, &e->sstb);

			/*
			 * Race: entry was entered into namecache, file
			 * system unlink occurred, then we tried to
			 * refresh stat(2) attributes.  This is OK
			 * however, since namecache is synchronized with
			 * unlink, we just did extra work here.
			 */
//			psc_assert((f->fcmh_flags & FCMH_DELETED) == 0);

			msl_fcmh_stash_xattrsize(f, e->xattrsize);
			fcmh_op_done(f);
		}
	}
	DIRCACHE_WRLOCK(d);
	/*
	 * We could free unused space here but we would have to adjust
	 * the various pointers referenced by the dynarrays.
	 */
//	p->dcp_base = psc_realloc(p->dcp_base, size, 0);
	p->dcp_refcnt--;
	PFLOG_DIRCACHEPG(PLL_DEBUG, p, "decref");
	DIRCACHE_ULOCK(d);
	return (0);
}

/*
 * Callback triggered when READDIR RPC is received.
 */
int
msl_readdir_cb(struct pscrpc_request *rq, struct pscrpc_async_args *av)
{
	struct iovec iov;
	struct srm_readdir_req *mq;
	struct srm_readdir_rep *mp;
	struct slrpc_cservice *csvc = av->pointer_arg[MSL_READDIR_CBARG_CSVC];
	struct dircache_page *p = av->pointer_arg[MSL_READDIR_CBARG_PAGE];
	struct fidc_membh *d = av->pointer_arg[MSL_READDIR_CBARG_FCMH];
	void *dentbuf = av->pointer_arg[MSL_READDIR_CBARG_DENTBUF];
	char buf[PSCRPC_NIDSTR_SIZE];
	int rc;

	SL_GET_RQ_STATUSF(csvc, rq, mp,
	    SRPCWAITF_DEFER_BULK_AUTHBUF_CHECK, rc);

	if (rc) {
 error:
		DEBUG_REQ(PLL_ERROR, rq, buf, "rc=%d", rc);
		msl_readdir_error(d, p, rc);
	} else {
		size_t len;

		len = mp->size + mp->nents *
		    sizeof(struct srt_readdir_ent);

		mq = pscrpc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq));
		if (SRM_READDIR_BUFSZ(mp->size, mp->nents) <=
		    sizeof(mp->ents)) {
			OPSTAT_INCR("msl.readdir-piggyback");

			memcpy(dentbuf, mp->ents, len);
		} else {
			OPSTAT_INCR("msl.readdir-bulk-reply");

			iov.iov_base = dentbuf;
			iov.iov_len = len;
			rc = slrpc_bulk_checkmsg(rq, rq->rq_repmsg,
			    &iov, 1);
			if (rc)
				PFL_GOTOERR(error, rc);
		}
		rc = msl_readdir_finish(d, p, mp->eof, mp->nents,
		    mp->size, dentbuf);
		if (rc)
			goto error;
		dentbuf = NULL;
	}
	fcmh_op_done_type(d, FCMH_OPCNT_READDIR);
	sl_csvc_decref(csvc);

	PSCFREE(dentbuf);

	return (0);
}

/*
 * Send out an asynchronous READDIR RPC.
 */
int
msl_readdir_issue(struct fidc_membh *d, off_t off, size_t size,
    int block)
{
	void *dentbuf = NULL;
	struct slrpc_cservice *csvc = NULL;
	struct srm_readdir_req *mq = NULL;
	struct srm_readdir_rep *mp = NULL;
	struct pscrpc_request *rq = NULL;
	struct dircache_page *p;
	struct iovec iov;
	int rc, nents;

	p = dircache_new_page(d, off, block);
	if (p == NULL)
		return (-ESRCH);

	fcmh_op_start_type(d, FCMH_OPCNT_READDIR);

	MSL_RMC_NEWREQ(d, csvc, SRMT_READDIR, rq, mq, mp, rc);
	if (rc)
		PFL_GOTOERR(out2, rc);

	mq->fg = d->fcmh_fg;
	mq->size = size;
	mq->offset = off;

	/*
	 * Estimate the size of the statbufs.  Being wasteful most of
	 * the time by overestimating is probably better than often
	 * underestimating and missing some and imposing round trips for
	 * GETATTRs when the application is likely to request them.
	 */
	nents = size / PFL_DIRENT_SIZE(sizeof(int));
	iov.iov_len = size + nents * sizeof(struct srt_readdir_ent);
	iov.iov_base = dentbuf = PSCALLOC(iov.iov_len);

	/*
	 * Set up an abortable RPC if the server denotes that the
	 * contents can fit directly in the reply message.
	 */
	rq->rq_bulk_abortable = 1;
	rc = slrpc_bulkclient(rq, BULK_PUT_SINK, SRMC_BULK_PORTAL, &iov,
	    1);
	if (rc)
		PFL_GOTOERR(out1, rc);

	/*
 	 * XXX Use SL_RSX_WAITREP() API in the blocking case so that the 
 	 * callback only has to deal with read-ahead, making it easy to 
 	 * know when to retry.  In addition, ignore all errors for 
 	 * non-blocking case.
 	 */
	rq->rq_interpret_reply = msl_readdir_cb;
	rq->rq_async_args.pointer_arg[MSL_READDIR_CBARG_CSVC] = csvc;
	rq->rq_async_args.pointer_arg[MSL_READDIR_CBARG_FCMH] = d;
	rq->rq_async_args.pointer_arg[MSL_READDIR_CBARG_PAGE] = p;
	rq->rq_async_args.pointer_arg[MSL_READDIR_CBARG_DENTBUF] = dentbuf;
	PFLOG_DIRCACHEPG(PLL_DEBUG, p, "issuing");
	rc = SL_NBRQSET_ADD(csvc, rq);
	if (!rc)
		return (0);

 out1:
	PSCFREE(dentbuf);

	pscrpc_req_finished(rq);
	sl_csvc_decref(csvc);

 out2:
	DIRCACHE_WRLOCK(d);
	p->dcp_refcnt--;
	dircache_free_page(d, p);
	DIRCACHE_ULOCK(d);
	fcmh_op_done_type(d, FCMH_OPCNT_READDIR);
	return (rc);
}

void
mslfsop_readdir(struct pscfs_req *pfr, size_t size, off_t off,
    void *data)
{
	int hit = 1, j, nd, issue, rc;
	struct dircache_page *p, *np;
	struct msl_fhent *mfh = data;
	struct fidc_membh *d = NULL;
	struct dircache_expire dexp;
	struct fcmh_cli_info *fci;
	struct pscfs_dirent *pfd;
	struct pscfs_creds pcr;
	off_t raoff = 0;

	if (off < 0 || size > 1024 * 1024)
		PFL_GOTOERR(out, rc = EINVAL);

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

	slc_getfscreds(pfr, &pcr);

	rc = fcmh_checkcreds(d, pfr, &pcr, R_OK);
	if (rc)
		PFL_GOTOERR(out, rc);

	DIRCACHE_WRLOCK(d);
	fci = fcmh_2_fci(d);

 restart:
	DIRCACHEPG_INITEXP(&dexp);

	issue = 1;

	/*
	 * XXX Large directories will page in lots of buffers so this
	 * code should really be changed to create a stub and wait for
	 * it to be filled in instead of rescanning the entire list.
	 */
	PLL_FOREACH_SAFE(p, np, &fci->fci_dc_pages) {

		if (p->dcp_flags & DIRCACHEPGF_LOADING) {
			// XXX need to wake up if csvc fails
			OPSTAT_INCR("msl.dircache-wait");
			DIRCACHE_WAIT(d);
			goto restart;
		}
		if (DIRCACHEPG_EXPIRED(d, p, &dexp)) {
			dircache_free_page(d, p);
			continue;
		}

		/* We found the last page; return EOF. */
		if (off == p->dcp_nextoff &&
		    p->dcp_flags & DIRCACHEPGF_EOF) {
			DIRCACHE_ULOCK(d);
			OPSTAT_INCR("msl.dircache-hit-eof");
			PFL_GOTOERR(out, rc = 0);
		}

		if (dircache_hasoff(p, off)) {
			if (p->dcp_rc) {
				rc = p->dcp_rc;
				dircache_free_page(d, p);
				if (!slc_rpc_should_retry(pfr, &rc)) {
					DIRCACHE_ULOCK(d);
					PFL_GOTOERR(out, rc);
				}
				break;
			} else {
				off_t poff, thisoff = p->dcp_off;
				size_t len, tlen;

				/* find starting entry */
				poff = 0;
				nd = psc_dynarray_len(p->dcp_dents_off);
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
					OPSTAT_INCR("msl.dircache-hit");

				psclogs_diag(SLCSS_FSOP, "READDIR: "
				    "fid="SLPRI_FID" size=%zd "
				    "off=%"PSCPRIdOFFT" rc=%d",
				    fcmh_2_fid(d), size, off, rc);

				if ((p->dcp_flags &
				    DIRCACHEPGF_EOF) == 0) {
					/*
					 * The reply_readdir() up ahead
					 * may be followed by a RELEASE
					 * so take an extra reference to
					 * avoid use-after-free on the
					 * fcmh.
					 */
					fcmh_op_start_type(d,
					    FCMH_OPCNT_READAHEAD);
					raoff = p->dcp_nextoff;
					psc_assert(raoff);
				}

				issue = 0;
				break;
			}
		}
	}
	DIRCACHE_ULOCK(d);

	if (issue) {
		/*
		 * The dircache_page was not found, or it was found but
		 * had an error.  Issue a READDIR then wait for a reply.
		 */
		hit = 0;
		rc = msl_readdir_issue(d, off, size, 1);
		if (rc && !slc_rpc_should_retry(pfr, &rc))
			PFL_GOTOERR(out, rc);
		DIRCACHE_WRLOCK(d);
		goto restart;
	}

	if (0) {
 out:
		rc = abs(rc);
		psclogs_diag(SLCSS_FSOP, "READDIR: fid="SLPRI_FID" "
		    "size=%zd off=%"PSCPRIdOFFT" rc=%d",
		    fcmh_2_fid(d), size, off, rc);
		pscfs_reply_readdir(pfr, NULL, 0, rc);
		raoff = 0;
	}

	if (raoff) {
		msl_readdir_issue(d, raoff, size, 0);
		fcmh_op_done_type(d, FCMH_OPCNT_READAHEAD);
	}
}

void
mslfsop_lookup(struct pscfs_req *pfr, pscfs_inum_t pinum,
    const char *name)
{
	struct dircache_ent_update dcu = DCE_UPD_INIT;
	struct fidc_membh *p = NULL, *c = NULL;
	struct pscfs_creds pcr;
	struct srt_stat sstb;
	struct sl_fidgen fg;
	struct stat stb;
	int rc;

	memset(&sstb, 0, sizeof(sstb));

	slc_getfscreds(pfr, &pcr);
	if (strlen(name) == 0)
		PFL_GOTOERR(out, rc = ENOENT);
	if (strlen(name) > SL_NAME_MAX)
		PFL_GOTOERR(out, rc = ENAMETOOLONG);

	rc = msl_lookup_fidcache_dcu(pfr, &pcr, pinum, name, &fg, &sstb,
	    &c, &p, &dcu);
	if (rc == ENOENT)
		sstb.sst_fid = 0;
	sl_internalize_stat(&sstb, &stb);
	if (!S_ISDIR(stb.st_mode))
		stb.st_blksize = MSL_FS_BLKSIZ;

 out:
	pscfs_reply_lookup(pfr, sstb.sst_fid, sstb.sst_gen,
	    pscfs_entry_timeout, &stb, pscfs_attr_timeout, rc);
	namecache_update(&dcu, sstb.sst_fid, rc);
	if (c)
		fcmh_op_done(c);
	if (p)
		fcmh_op_done(p);
}

void
mslfsop_readlink(struct pscfs_req *pfr, pscfs_inum_t inum)
{
	unsigned char buf[SL_PATH_MAX], *retbuf = buf;
	struct slrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_readlink_req *mq;
	struct srm_readlink_rep *mp;
	struct fidc_membh *c = NULL;
	struct pscfs_creds pcr;
	struct iovec iov;
	int rc;

	rc = msl_load_fcmh(pfr, inum, &c);
	if (rc)
		PFL_GOTOERR(out, rc);

	slc_getfscreds(pfr, &pcr);

	rc = fcmh_checkcreds(c, pfr, &pcr, R_OK);
	if (rc)
		PFL_GOTOERR(out, rc);

 retry:
	MSL_RMC_NEWREQ(c, csvc, SRMT_READLINK, rq, mq, mp, rc);
	if (!rc) {

		mq->fg = c->fcmh_fg;
		iov.iov_base = buf;
		iov.iov_len = sizeof(buf) - 1;
		rq->rq_bulk_abortable = 1;
		slrpc_bulkclient(rq, BULK_PUT_SINK, SRMC_BULK_PORTAL, &iov, 1);

		rc = SL_RSX_WAITREPF(csvc, rq, mp,
		    SRPCWAITF_DEFER_BULK_AUTHBUF_CHECK);
	}
	if (rc && slc_rpc_should_retry(pfr, &rc))
		goto retry;
	rc = abs(rc);
	if (!rc)
		rc = -mp->rc;
	if (rc)
		PFL_GOTOERR(out, rc);

	if (mp->len > SL_PATH_MAX) {
		rc = EINVAL;
	} else if (!mp->flag) {
		retbuf = mp->buf;
	} else {
		iov.iov_len = mp->len;
		rc = slrpc_bulk_checkmsg(rq, rq->rq_repmsg, &iov, 1);
		if (rc == 0)
			OPSTAT_INCR("msl.readlink-bulk");
	}
	if (!rc)
		retbuf[mp->len] = '\0';

 out:
	pscfs_reply_readlink(pfr, retbuf, rc);

	if (c)
		fcmh_op_done(c);
	pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

/*
 * Perform main data flush operation.
 * @mfh: handle corresponding to process file descriptor.
 * Note that this function is called (at least) once for each open.
 */
__static int
msl_flush(struct msl_fhent *mfh)
{
	struct psc_dynarray a = DYNARRAY_INIT;
	struct fidc_membh *f;
	struct bmap *b;
	int i, rc = 0;

	f = mfh->mfh_fcmh;

  restart:
	DYNARRAY_FOREACH(b, i, &a)
		bmap_op_done_type(b, BMAP_OPCNT_FLUSH);
	psc_dynarray_reset(&a);

	pfl_rwlock_rdlock(&f->fcmh_rwlock);
	RB_FOREACH(b, bmaptree, &f->fcmh_bmaptree) {
		/*
		 * Avoid a deadlock with the bmap release thread who
		 * will grab locks on multiple bmaps simultaneously.
		 */
		if (!BMAP_TRYLOCK(b)) {
			pfl_rwlock_unlock(&f->fcmh_rwlock);
			OPSTAT_INCR("msl.flush-backout");
			goto restart;
		}
		if (!(b->bcm_flags & BMAPF_TOFREE)) {
			bmap_op_start_type(b, BMAP_OPCNT_FLUSH);
			psc_dynarray_add(&a, b);
		}
		BMAP_ULOCK(b);
	}
	pfl_rwlock_unlock(&f->fcmh_rwlock);

	DYNARRAY_FOREACH(b, i, &a) {
		BMAP_LOCK(b);
		bmpc_biorqs_flush(b);
		if (!rc)
			rc = -bmap_2_bci(b)->bci_flush_rc;
		bmap_2_bci(b)->bci_flush_rc = 0;
		bmap_op_done_type(b, BMAP_OPCNT_FLUSH);
	}
	psc_dynarray_free(&a);

	return (rc);
}

/*
 * Send a SETATTR RPC to the MDS.  @sstb is filled out according to the
 * new values any attributes should take on.  Upon reply, the fcmh is
 * updated according to how the MDS refreshes it, naturally handling
 * success and failure.
 */
int
msl_setattr(struct fidc_membh *f, int32_t to_set,
    const struct srt_stat *sstb, int setattrflags)
{
	struct slrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_setattr_req *mq;
	struct srm_setattr_rep *mp;
	int rc;

	FCMH_BUSY_ENSURE(f);

	MSL_RMC_NEWREQ(f, csvc, SRMT_SETATTR, rq, mq, mp, rc);
	if (rc)
		PFL_GOTOERR(out, rc);

	mq->attr = *sstb;
	mq->attr.sst_fg = f->fcmh_fg;
	mq->to_set = to_set;

	if (to_set & (PSCFS_SETATTRF_GID | PSCFS_SETATTRF_UID)) {
		rc = uidmap_ext_stat(&mq->attr);
		if (rc)
			PFL_GOTOERR(out, rc);
	}

	DEBUG_FCMH(PLL_DIAG, f, "before setattr RPC to_set=%#x",
	    to_set);

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc)
		PFL_GOTOERR(out, rc);

	rc = abs(rc);
	if (rc == 0)
		rc = -mp->rc;

	if (rc == SLERR_BMAP_IN_PTRUNC)
		rc = EAGAIN;
	else if (rc == SLERR_BMAP_PTRUNC_STARTED)
		rc = 0;

	DEBUG_SSTB(rc ? PLL_WARN : PLL_DIAG, &f->fcmh_sstb,
	    "attr flush; set=%#x rc=%d", to_set, rc);
	if (rc)
		PFL_GOTOERR(out, rc);

	slc_fcmh_setattrf(f, &mp->attr, setattrflags);

 out:
	pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
	return (rc);
}

/*
 * Send out a synchronous RPC to update file attribute metadata (e.g.
 * mtime) as a result of I/O operation.  These updates are sent out
 * periodically instead of continuously to reduce traffic.
 */
int
msl_flush_ioattrs(struct pscfs_req *pfr, struct fidc_membh *f)
{
	int dummy, flush_size = 0, flush_mtime = 0;
	int rc, waslocked, to_set = 0;
	struct srt_stat attr;

	memset(&attr, 0, sizeof(attr));

	waslocked = FCMH_RLOCK(f);
	fcmh_wait_locked(f, f->fcmh_flags & FCMH_BUSY);

	/*
	 * Perhaps this checking should only be done on the mfh, with
	 * which we have modified the attributes.
	 */
	if (f->fcmh_flags & FCMH_CLI_DIRTY_DSIZE) {
		flush_size = 1;
		f->fcmh_flags &= ~FCMH_CLI_DIRTY_DSIZE;
		FCMH_REQ_BUSY(f, &dummy);
		to_set |= PSCFS_SETATTRF_DATASIZE;
		attr.sst_size = f->fcmh_sstb.sst_size;
	}
	if (f->fcmh_flags & FCMH_CLI_DIRTY_MTIME) {
		flush_mtime = 1;
		f->fcmh_flags &= ~FCMH_CLI_DIRTY_MTIME;
		FCMH_REQ_BUSY(f, &dummy);
		to_set |= PSCFS_SETATTRF_MTIME;
		attr.sst_mtim = f->fcmh_sstb.sst_mtim;
	}
	if (!to_set) {
		psc_assert((f->fcmh_flags & FCMH_CLI_DIRTY_QUEUE) == 0);
		FCMH_URLOCK(f, waslocked);
		return (0);
	}

	FCMH_ULOCK(f);

	rc = msl_setattr(f, to_set, &attr, 0);

	FCMH_LOCK(f);
	FCMH_UREQ_BUSY(f, 0, PSLRV_WASLOCKED);
	if (rc && slc_rpc_should_retry(pfr, &rc)) {
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
		lc_remove(&msl_attrtimeoutq, fcmh_2_fci(f));
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
	int rc, rc2;

	DEBUG_FCMH(PLL_DIAG, mfh->mfh_fcmh, "flushing (mfh=%p)", mfh);

	rc = msl_flush(mfh);
	rc2 = msl_flush_ioattrs(pfr, mfh->mfh_fcmh);
	if (!rc)
		rc = rc2;

	DEBUG_FCMH(PLL_DIAG, mfh->mfh_fcmh,
	    "done flushing (mfh=%p, rc=%d)", mfh, rc);

	pscfs_reply_flush(pfr, rc);
}

void
mfh_incref(struct msl_fhent *mfh)
{
	MFH_LOCK(mfh);
	mfh->mfh_refcnt++;
	MFH_ULOCK(mfh);
}

void
mfh_decref(struct msl_fhent *mfh)
{
	MFH_LOCK_ENSURE(mfh);
	psc_assert(mfh->mfh_refcnt > 0);
	if (--mfh->mfh_refcnt == 0) {
		fcmh_op_done_type(mfh->mfh_fcmh, FCMH_OPCNT_OPEN);
		psc_pool_return(msl_mfh_pool, mfh);
	} else
		MFH_ULOCK(mfh);
}

/*
 * Scrape user program name (uprog).
 *
 * Interpreters get special treatment as the arguments get included so
 * parsers of these logs can differentiate entries by script name.
 *
 * Examples:
 *	/bin/sh - foo.sh
 *	/bin/sh foo.sh
 *	/bin/perl -W foo.pl
 */
void
slc_getuprog(pid_t pid, char *uprog, size_t maxlen)
{
	char *sp, *dp, fn[128], buf[128];
	ssize_t sz, dst_remaining;
	int n, fd;

	n = snprintf(fn, sizeof(fn), "/proc/%d/exe", pid);
	if (n == -1)
		return;
	n = readlink(fn, uprog, maxlen - 1);
	if (n == -1)
		n = 0;
	uprog[n] = '\0';

	/* no space to append script name */
	if (n == -1 || (size_t)n >= maxlen)
		return;

	if (strstr(uprog, "/bash") == NULL &&
	    strstr(uprog, "/python") == NULL &&
	    strstr(uprog, "/perl") == NULL &&
	    strstr(uprog, "/ksh") == NULL)
		return;

	snprintf(fn, sizeof(fn), "/proc/%d/cmdline", pid);
	fd = open(fn, O_RDONLY);
	if (fd == -1)
		return;

	sz = read(fd, buf, sizeof(buf) - 1);
	close(fd);

	if (sz == -1)
		return;

	/* Note: arguments in `buf' are separated by NUL bytes. */

	buf[sz] = '\0';

	dp = uprog + n;

	/*
	 * Skip first argument which is the executable name (e.g. `sh').
	 */
	for (sp = buf; *sp; sp++)
		;

	dst_remaining = maxlen - (dp - uprog) + 1;
	if (dst_remaining < sz)
		sz = dst_remaining;

	/*
	 * Copy as much of the command as we can fit, substituting
	 * newlines for spaces.
	 */
	for (; sp - buf < sz; sp++, dp++) {
		switch (*sp) {
		case '\0':
			/*
			 * Two sequential NULs denotes end of argument
			 * list.
			 */
			if (sp[1] == '\0') {
				dp--;
				goto out;
			}
			/* FALLTHRU */
		case '\n':
			*dp = ' ';
			break;
		default:
			*dp = *sp;
			break;
		}
	}
 out:
	if (dp >= uprog + n)
		*dp = '\0';
}

const char *
slc_log_get_fsctx_uprog(struct psc_thread *thr)
{
	struct psclog_data *pld;
	struct pfl_fsthr *pft;
	struct pscfs_req *pfr;

	pld = psclog_getdata();
// || mft->mft_lastp != p
	if (pld->pld_uprog)
		return (pld->pld_uprog);

	if (thr->pscthr_type != PFL_THRT_FS)
		return "<n/a>";

	pft = thr->pscthr_private;
	if (pft->pft_uprog[0] == '\0' && pft->pft_pfr) {
		struct msl_fhent *mfh;
		pid_t pid;

		pfr = pft->pft_pfr; /* set by GETPFR() */
		mfh = pfr ? pflfs_req_getfh(pfr) : NULL;
		if (mfh)
			pid = mfh->mfh_pid;
		else
			pid = pscfs_getclientctx(pfr)->pfcc_pid;

		slc_getuprog(pid, pft->pft_uprog,
		    sizeof(pft->pft_uprog));
	}

	return (pld->pld_uprog = pft->pft_uprog);
}

pid_t
slc_log_get_fsctx_pid(struct psc_thread *thr)
{
	struct pfl_fsthr *pft;

	if (thr->pscthr_type != PFL_THRT_FS)
		return (-1);

	pft = thr->pscthr_private;
	if (pft->pft_pfr)
		return (pscfs_getclientctx(pft->pft_pfr)->pfcc_pid);
	return (-1);
}

uid_t
slc_log_get_fsctx_uid(struct psc_thread *thr)
{
	struct pscfs_creds pcr;
	struct pfl_fsthr *pft;

	if (thr->pscthr_type != PFL_THRT_FS)
		return (-1);

	pft = thr->pscthr_private;
	if (pft->pft_pfr) {
		slc_getfscreds(pft->pft_pfr, &pcr);
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

	f = mfh->mfh_fcmh;
	fci = fcmh_2_fci(f);

	MFH_LOCK(mfh);
	/*
	 * Force expire to provoke immediate flush.
	 * We probably do not need this because we
	 * already did this at flush time.
	 */
	FCMH_LOCK(f);
	if (f->fcmh_flags & FCMH_CLI_DIRTY_QUEUE) {
		OPSTAT_INCR("msl.release_dirty_attrs");
		PFL_GETTIMESPEC(&fci->fci_etime);
		fci->fci_etime.tv_sec--;
		psc_waitq_wakeone(&msl_flush_attrq);
	}
	FCMH_ULOCK(f);

	if (fcmh_isdir(f)) {
		pscfs_reply_releasedir(pfr, 0);
	} else {
		pscfs_reply_release(pfr, 0);

		if (mfh->mfh_nbytes_rd || mfh->mfh_nbytes_wr)
			psclogs_info(SLCSS_INFO,
			    "file closed fid="SLPRI_FID" "
			    "euid=%u owner=%u fgrp=%u "
			    "fsize=%"PRId64" "
			    "oatime="PFLPRI_PTIMESPEC" "
			    "mtime="PFLPRI_PTIMESPEC" sessid=%d "
			    "otime="PSCPRI_TIMESPEC" "
			    "rd=%"PSCPRIdOFFT" wr=%"PSCPRIdOFFT" prog=%s",
			    fcmh_2_fid(f),
			    mfh->mfh_accessing_euid,
			    f->fcmh_sstb.sst_uid, f->fcmh_sstb.sst_gid,
			    f->fcmh_sstb.sst_size,
			    PFLPRI_PTIMESPEC_ARGS(&mfh->mfh_open_atime),
			    PFLPRI_PTIMESPEC_ARGS(&f->fcmh_sstb.sst_mtim),
			    mfh->mfh_sid,
			    PSCPRI_TIMESPEC_ARGS(&mfh->mfh_open_time),
			    mfh->mfh_nbytes_rd, mfh->mfh_nbytes_wr,
			    mfh->mfh_uprog);
	}

	mfh_decref(mfh);
}

void
mslfsop_rename(struct pscfs_req *pfr, pscfs_inum_t opinum,
    const char *oldname, pscfs_inum_t npinum, const char *newname)
{
	struct dircache_ent_update odcu = DCE_UPD_INIT, ndcu = DCE_UPD_INIT;
	struct fidc_membh *child = NULL, *np = NULL, *op = NULL, *ch;
	struct slrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srt_stat srcsstb, dstsstb;
	struct sl_fidgen srcfg, dstfg;
	struct srm_rename_req *mq;
	struct srm_rename_rep *mp = NULL;
	struct pscfs_creds pcr;
	struct iovec iov[2];
	int sticky, rc;

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

	if (FID_GET_SITEID(opinum) != FID_GET_SITEID(npinum))
		PFL_GOTOERR(out, rc = EXDEV);

	slc_getfscreds(pfr, &pcr);

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
				PFL_GOTOERR(out, rc = EACCES);
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
					rc = EACCES;
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

 retry1:
	MSL_RMC_NEWREQ(np, csvc, SRMT_RENAME, rq, mq, mp, rc);
	if (rc)
		goto retry2;

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

	namecache_get_entries(&odcu, op, oldname, &ndcu, np, newname);

	rc = SL_RSX_WAITREP(csvc, rq, mp);

  retry2:
	if (rc && slc_rpc_should_retry(pfr, &rc)) {
		namecache_fail(&odcu);
		namecache_fail(&ndcu);
		goto retry1;
	}
	rc = abs(rc);
	if (rc == 0)
		rc = -mp->rc;
	if (rc)
		PFL_GOTOERR(out, rc);

	/* refresh old parent attributes */
	slc_fcmh_setattr(op, &mp->srr_opattr);

	if (np != op)
		/* refresh new parent attributes */
		slc_fcmh_setattr(np, &mp->srr_npattr);

	/* refresh moved file's attributes */
	if (mp->srr_cattr.sst_fid != FID_ANY &&
	    msl_fcmh_load_fg(&mp->srr_cattr.sst_fg, &ch, pfr) == 0) {
		slc_fcmh_setattr(ch, &mp->srr_cattr);
		fcmh_op_done(ch);
	}

	/*
	 * The following logic was introduced by the following commit:
	 *
	 * commit ef7e931f959d2e1055ef845a22fc91874a565e00
	 * Author: Jared Yanovich <yanovich@psc.edu>
	 * Date:   Wed Jan 8 23:26:58 2014 +0000
	 *
	 * fix an fcmh leak of child and refresh RENAME clobbered file stat(2) attributes
	 *         
	 */
	/*
	 * Refresh clobbered file's attributes.  This file might have
	 * additional links and may not be completely destroyed so don't
	 * evict.
	 */
	if (mp->srr_clattr.sst_fid != FID_ANY &&
	    sl_fcmh_lookup(mp->srr_clattr.sst_fg.fg_fid, FGEN_ANY,
	    FIDC_LOOKUP_LOCK, &ch, pfr) == 0) {
		if (!mp->srr_clattr.sst_nlink) {
			ch->fcmh_flags |= FCMH_DELETED;
			OPSTAT_INCR("msl.clobber");
		}
		slc_fcmh_setattr_locked(ch, &mp->srr_clattr);
		fcmh_op_done(ch);
	}

	/*
	 * XXX we do not update dstsstb in our cache if the dst was
	 * nlinks > 1 and the inode was not removed from the file system
	 * outright as a result of this rename op.
	 */

 out:
	pscfs_reply_rename(pfr, rc);
	namecache_delete(&odcu, rc);
	namecache_update(&ndcu, mp->srr_cattr.sst_fid, rc);

	psclogs_diag(SLCSS_FSOP, "RENAME: opinum="SLPRI_FID" "
	    "npinum="SLPRI_FID" oldname='%s' newname='%s' rc=%d",
	    opinum, npinum, oldname, newname, rc);

	if (child)
		fcmh_op_done(child);
	if (op)
		fcmh_op_done(op);
	if (np && np != op)
		fcmh_op_done(np);
	pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

#define MSL_STATFS_EXPIRE_S	4
#define MSL_STATFS_AGGR_IOSID	0

void
mslfsop_statfs(struct pscfs_req *pfr, pscfs_inum_t inum)
{
	struct slrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct resprof_cli_info *rpci;
	struct sl_resource *pref_ios;
	struct srm_statfs_req *mq;
	struct srm_statfs_rep *mp;
	struct timespec expire;
	struct statvfs sfb;
	sl_ios_id_t iosid;
	int rc = 0;

	PFL_GETTIMESPEC(&expire);
	expire.tv_sec -= MSL_STATFS_EXPIRE_S;
	if (msl_statfs_pref_ios_only) {
		pref_ios = msl_get_pref_ios();
		iosid = pref_ios->res_id;
		rpci = res2rpci(pref_ios);
	} else {
		iosid = MSL_STATFS_AGGR_IOSID;
		rpci = &msl_statfs_aggr_rpci;
	}
	RPCI_LOCK(rpci);
	while (rpci->rpci_flags & RPCIF_STATFS_FETCHING) {
		RPCI_WAIT(rpci);
		RPCI_LOCK(rpci);
	}
	if (timespeccmp(&rpci->rpci_sfb_time, &expire, >)) {
		memcpy(&sfb, &rpci->rpci_sfb, sizeof(sfb));
		RPCI_ULOCK(rpci);
		PFL_GOTOERR(out, 0);
	}
	rpci->rpci_flags |= RPCIF_STATFS_FETCHING;
	RPCI_ULOCK(rpci);

 retry1:
	MSL_RMC_NEWREQ(NULL, csvc, SRMT_STATFS, rq, mq, mp, rc);
	if (rc)
		goto retry2;

	mq->fid = inum;
	mq->iosid = iosid;
	if (rc)
		PFL_GOTOERR(out, rc);
	rc = SL_RSX_WAITREP(csvc, rq, mp);
 retry2:
	if (rc && slc_rpc_should_retry(pfr, &rc))
		goto retry1;
	rc = abs(rc);
	if (rc == 0)
		rc = -mp->rc;
	if (rc)
		PFL_GOTOERR(out, rc);

	sl_internalize_statfs(&mp->ssfb, &sfb);

	PFL_GETTIMESPEC(&expire);
	memcpy(&rpci->rpci_sfb, &sfb, sizeof(sfb));
	rpci->rpci_sfb_time = expire;

 out:
	RPCI_LOCK(rpci);
	rpci->rpci_flags &= ~RPCIF_STATFS_FETCHING;
	RPCI_WAKE(rpci);
	RPCI_ULOCK(rpci);

	pscfs_reply_statfs(pfr, &sfb, rc);

	if (iosid != MSL_STATFS_AGGR_IOSID)
		sl_resource_put(pref_ios);
	pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

void
mslfsop_symlink(struct pscfs_req *pfr, const char *buf,
    pscfs_inum_t pinum, const char *name)
{
	struct dircache_ent_update dcu = DCE_UPD_INIT;
	struct fidc_membh *c = NULL, *p = NULL;
	struct slrpc_cservice *csvc = NULL;
	struct srm_symlink_rep *mp = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_symlink_req *mq;
	struct pscfs_creds pcr;
	struct iovec iov;
	struct stat stb;
	int rc;

	if (strlen(buf) == 0 || strlen(name) == 0)
		PFL_GOTOERR(out, rc = ENOENT);
	if (strlen(buf) >= SL_PATH_MAX ||
	    strlen(name) > SL_NAME_MAX)
		PFL_GOTOERR(out, rc = ENAMETOOLONG);

	slc_getfscreds(pfr, &pcr);

	rc = msl_load_fcmh(pfr, pinum, &p);
	if (rc)
		PFL_GOTOERR(out, rc);

	if (!fcmh_isdir(p))
		PFL_GOTOERR(out, rc = ENOTDIR);
	rc = fcmh_reserved(p);
	if (rc)
		PFL_GOTOERR(out, rc);

	rc = fcmh_checkcreds(p, pfr, &pcr, W_OK);
	if (rc)
		PFL_GOTOERR(out, rc);

 retry1:
	MSL_RMC_NEWREQ(p, csvc, SRMT_SYMLINK, rq, mq, mp, rc);
	if (rc)
		goto retry2;

	mq->sstb.sst_uid = pcr.pcr_uid;
	mq->sstb.sst_gid = newent_select_group(p, &pcr);
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

	namecache_hold_entry(&dcu, p, name);
	rc = SL_RSX_WAITREP(csvc, rq, mp);

 retry2:
	if (rc && slc_rpc_should_retry(pfr, &rc)) {
		namecache_fail(&dcu);
		goto retry1;
	}
	rc = abs(rc);
	if (rc == 0)
		rc = -mp->rc;
	if (rc)
		PFL_GOTOERR(out, rc);

	slc_fcmh_setattr(p, &mp->pattr);

	rc = msl_fcmh_get_fg(pfr, &mp->cattr.sst_fg, &c);
	if (rc)
		PFL_GOTOERR(out, rc);

	FCMH_LOCK(c);
	slc_fcmh_setattr_locked(c, &mp->cattr);
	sl_internalize_stat(&mp->cattr, &stb);
	FCMH_ULOCK(c);

 out:
	pscfs_reply_symlink(pfr, mp ? mp->cattr.sst_fid : 0,
	    mp ? mp->cattr.sst_gen : 0, pscfs_entry_timeout, &stb,
	    pscfs_attr_timeout, rc);
	namecache_update(&dcu, mp->cattr.sst_fid, rc);

	psclogs_diag(SLCSS_FSOP, "SYMLINK: pfid="SLPRI_FID" "
	    "cfid="SLPRI_FID" name='%s' rc=%d",
	    pinum, c ? c->fcmh_sstb.sst_fid : FID_ANY, name, rc);

	if (c)
		fcmh_op_done(c);
	if (p)
		fcmh_op_done(p);
	pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

/*
 * Used for sending asynchronous invalidation requests to PFLFS.
 */
struct msl_dc_inv_entry_data {
	/*
	 * Private data used by the PFLFS module to send the
	 * invalidation request.
	 */
	void			*mdie_pri;
	pscfs_inum_t		 mdie_pinum;
};

/*
 * Send an name entry cache invalidation notification to the kernel.
 */
void
msl_dircache_inval_entry(__unusedx struct dircache_page *p,
    struct dircache_ent *d, void *arg)
{
	const struct msl_dc_inv_entry_data *mdie = arg;

	pscfs_notify_inval_entry(mdie->mdie_pri, mdie->mdie_pinum,
	    d->dce_pfd->pfd_name, d->dce_pfd->pfd_namelen);
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
	int flush_mtime = 0, flush_size = 0, setattrflags = 0;
	int i, busied = 0, rc = 0, unset_trunc = 0, getting_attrs = 0;
	struct msl_dc_inv_entry_data mdie;
	struct msl_fhent *mfh = data;
	struct fidc_membh *c = NULL;
	struct fcmh_cli_info *fci;
	struct pscfs_creds pcr;
	struct srt_stat sstb;
	struct timespec ts;

	memset(&mdie, 0, sizeof(mdie));

	rc = msl_load_fcmh(pfr, inum, &c);
	if (rc)
		PFL_GOTOERR(out, rc);

	if (mfh)
		psc_assert(c == mfh->mfh_fcmh);

	/*
	 * pscfs_reply_setattr() needs a fresh statbuf to refresh the
	 * entry, so we have to defer the short circuit processing till
	 * after loading the fcmh to avoid sending garbage resulting in
	 * EIO.
	 */
	if ((to_set & PSCFS_SETATTRF_UID) && stb->st_uid == (uid_t)-1)
		to_set &= ~PSCFS_SETATTRF_UID;
	if ((to_set & PSCFS_SETATTRF_GID) && stb->st_gid == (gid_t)-1)
		to_set &= ~PSCFS_SETATTRF_GID;
	if (to_set == 0)
		goto out;

	busied = 1;
	FCMH_WAIT_BUSY(c);

	slc_getfscreds(pfr, &pcr);

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

	if ((to_set & PSCFS_SETATTRF_ATIME_NOW) &&
	    (to_set & PSCFS_SETATTRF_MTIME_NOW) &&
	    pcr.pcr_uid && pcr.pcr_uid != c->fcmh_sstb.sst_uid) {
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

	/*
	 * XXX: While the Linux kernel should synchronize a read with a
	 * truncate, we probably should synchronize with any read-ahead
	 * launched ourselves.
	 */
	if (to_set & PSCFS_SETATTRF_DATASIZE) {
		struct bmap *b;

		if (!stb->st_size) {
			DEBUG_FCMH(PLL_DIAG, c,
			    "full truncate, free bmaps");

			OPSTAT_INCR("msl.truncate-full");
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

			OPSTAT_INCR("msl.truncate-part");

			DEBUG_FCMH(PLL_DIAG, c, "partial truncate");

			FCMH_ULOCK(c);

			/* Partial truncate.  Block and flush. */
			pfl_rwlock_rdlock(&c->fcmh_rwlock);
			RB_FOREACH(b, bmaptree, &c->fcmh_bmaptree) {
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
			pfl_rwlock_unlock(&c->fcmh_rwlock);

			/*
			 * XXX some writes can be cancelled, but no API
			 * exists yet.
			 */
			DYNARRAY_FOREACH(b, i, &a) {
				struct bmap_pagecache *bmpc;

				bmpc = bmap_2_bmpc(b);
				BMAP_LOCK(b);
				bmpc_expire_biorqs(bmpc);
				BMAP_ULOCK(b);
			}

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
			    c->fcmh_sstb.sst_mtime_ns, stb);
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

	sl_externalize_stat(stb, &sstb);
	if (to_set & (PSCFS_SETATTRF_MTIME | PSCFS_SETATTRF_DATASIZE))
		setattrflags |= FCMH_SETATTRF_CLOBBER;

 retry:
	rc = msl_setattr(c, to_set, &sstb, setattrflags);
	if (rc && slc_rpc_should_retry(pfr, &rc))
		goto retry;

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
			if (rc) {
				if (flush_mtime)
					c->fcmh_flags |=
					    FCMH_CLI_DIRTY_MTIME;
				if (flush_size)
					c->fcmh_flags |=
					    FCMH_CLI_DIRTY_DSIZE;
			} else if (!(c->fcmh_flags &
			    FCMH_CLI_DIRTY_ATTRS)) {
				fci = fcmh_2_fci(c);
				psc_assert(c->fcmh_flags &
				    FCMH_CLI_DIRTY_QUEUE);
				c->fcmh_flags &= ~FCMH_CLI_DIRTY_QUEUE;
				lc_remove(&msl_attrtimeoutq, fci);
				fcmh_op_done_type(c,
				    FCMH_OPCNT_DIRTY_QUEUE);
			}
		}

		/* See note below. */
		if (!rc && (to_set & PSCFS_SETATTRF_MODE) &&
		    fcmh_isdir(c)) {
			mdie.mdie_pri = pflfs_inval_getprivate(pfr);
			mdie.mdie_pinum = fcmh_2_fid(c);
		}

	}

	pscfs_reply_setattr(pfr, stb, pscfs_attr_timeout, rc);

	if (c) {
		if (busied)
			FCMH_UNBUSY(c);

		/*
		 * If permissions changed for a directory, we need to
		 * specifically invalidate all entries under the dir
		 * from the cache in the kernel, otherwise there will be
		 * a window of access where the new permissions are
		 * ignored until the cache expires.
		 *
		 * Technically this invalidation should occur before
		 * returning to the syscall but FUSE will deadlock if we
		 * do it before.
		 *
		 * XXX Something may have been evicted from our cache
		 * but still held by the kernel.  Whenever we evict, we
		 * should clear the kernel cache too.
		 *
		 * XXX should we block access to the namecache until
		 * this operation completes?
		 */
		if (mdie.mdie_pri) {
			OPSTAT_INCR("msl.dircache-walk-begin");
			dircache_walk(c, msl_dircache_inval_entry,
			    &mdie);
			OPSTAT_INCR("msl.dircache-walk-end");
		}

		fcmh_op_done(c);
	}

	psclogs_diag(SLCSS_FSOP, "SETATTR: fid="SLPRI_FID" to_set=%#x "
	    "rc=%d", inum, to_set, rc);
}

void
mslfsop_fsync(struct pscfs_req *pfr, int datasync_only, void *data)
{
	struct msl_fhent *mfh;
	struct fidc_membh *f;
	int rc = 0;

	mfh = data;
	f = mfh->mfh_fcmh;
	if (fcmh_isdir(f)) {
		// XXX flush all fcmh attrs under dir
	} else {
		DEBUG_FCMH(PLL_DIAG, mfh->mfh_fcmh, "fsyncing");

		rc = msl_flush(mfh);
		if (!datasync_only) {
			int rc2;

			rc2 = msl_flush_ioattrs(pfr, mfh->mfh_fcmh);
			//if (rc && slc_rpc_should_retry(pfr, &rc))
			if (!rc)
				rc = rc2;
		}
	}

	pscfs_reply_fsync(pfr, rc);
}

void
mslfsop_destroy(__unusedx struct pscfs_req *pfr)
{
	lnet_process_id_t peer;
	struct psc_thread *thr, *thr_next;
	struct slrpc_cservice *csvc;
	struct pfl_opstat *opst;
	struct sl_resource *r;
	struct psc_poolmgr *p;
	struct pfl_fault *flt;
	struct sl_resm *m;
	struct sl_site *s;
	int i, j, remaining;

	/* mark listcaches as dead */
	lc_kill(&msl_bmapflushq);
	lc_kill(&msl_bmaptimeoutq);
	lc_kill(&msl_attrtimeoutq);
	lc_kill(&msl_readaheadq);

	/* notify of termination */
	LIST_CACHE_LOCK(&msl_bmaptimeoutq);
	psc_waitq_wakeall(&msl_bmaptimeoutq.plc_wq_empty);
	LIST_CACHE_ULOCK(&msl_bmaptimeoutq);

	psc_waitq_wakeall(&msl_flush_attrq);

	pscthr_setdead(sl_freapthr, 1);
	psc_waitq_wakeall(&sl_freap_waitq);

	/* wait for drain */
	LISTCACHE_WAITEMPTY_UNLOCKED(&msl_bmapflushq,
	    lc_nitems(&msl_bmapflushq));
	LISTCACHE_WAITEMPTY_UNLOCKED(&msl_bmaptimeoutq,
	    lc_nitems(&msl_bmaptimeoutq));
	LISTCACHE_WAITEMPTY_UNLOCKED(&msl_attrtimeoutq,
	    lc_nitems(&msl_attrtimeoutq));
	LISTCACHE_WAITEMPTY_UNLOCKED(&msl_readaheadq,
	    lc_nitems(&msl_readaheadq));

	/* XXX force flush */

	p = sl_fcmh_pool;
	for (;;) {
		POOL_LOCK(p);
		remaining = p->ppm_total - lc_nitems(&p->ppm_lc);
		POOL_ULOCK(p);

		if (!remaining)
			break;

		if (!fidc_reap(FCMH_MAX_REAP, SL_FIDC_REAPF_ROOT))
			usleep(10);
	}

	peer.nid = LNET_NID_ANY;
	pscrpc_drop_conns(&peer);

	CONF_FOREACH_RESM(s, r, i, m, j)
		if (!RES_ISCLUSTER(r) && m->resm_csvc) {
			csvc = m->resm_csvc;
			CSVC_LOCK(csvc);
			sl_csvc_incref(csvc);
			csvc->csvc_flags &= ~CSVCF_WATCH;
			sl_csvc_decref_locked(csvc);
			continue;
		}
	pscrpc_set_kill(sl_nbrqset);

	pscrpc_svh_destroy(msl_rci_svh);
	pscrpc_svh_destroy(msl_rcm_svh);

	pscrpc_exit_portals();

	/*
	 * Removal of the control socket is the last thing for
	 * observability reasons.
	 */
	pfl_ctl_destroy(psc_ctlthr(msl_ctlthr0)->pct_ctldata);

	PLL_LOCK(&psc_threads);
	PLL_FOREACH_SAFE(thr, thr_next, &psc_threads)
	    if (strncmp(thr->pscthr_name, "msfsthr",
		strlen("msfsthr")) == 0)
		    pscthr_destroy(thr);

	do {
		PLL_FOREACH(thr, &psc_threads)
			if (strncmp(thr->pscthr_name, "ms", 2) == 0)
				break;
		PLL_ULOCK(&psc_threads);
		if (thr) {
			usleep(10);
			PLL_LOCK(&psc_threads);
		}
	} while (thr);

	fidc_destroy();
	psc_hashtbl_destroy(&msl_namecache_hashtbl);
	slcfg_destroy();

	if (msl_use_mapfile) {
		psc_hashtbl_destroy(&msl_uidmap_ext);
		psc_hashtbl_destroy(&msl_uidmap_int);
		psc_hashtbl_destroy(&msl_gidmap_int);
	}

	spinlock(&pfl_faults_lock);
	DYNARRAY_FOREACH(flt, i, &pfl_faults)
		if (strncmp(flt->pflt_name, "slash2/",
		    strlen("slash2/")) == 0)
			pfl_fault_destroy(i--);
	freelock(&pfl_faults_lock);

	/* XXX wait for wkq to drain, or perhaps at the pflfs layer? */

	pfl_listcache_destroy_registered(&msl_attrtimeoutq);
	pfl_listcache_destroy_registered(&msl_bmapflushq);
	pfl_listcache_destroy_registered(&msl_bmaptimeoutq);
	pfl_listcache_destroy_registered(&msl_readaheadq);
	pfl_listcache_destroy_registered(&msl_idle_pages);
	pfl_listcache_destroy_registered(&msl_readahead_pages);

	pfl_opstats_grad_destroy(&slc_iosyscall_iostats_rd);
	pfl_opstats_grad_destroy(&slc_iosyscall_iostats_wr);
	pfl_opstats_grad_destroy(&slc_iorpc_iostats_rd);
	pfl_opstats_grad_destroy(&slc_iorpc_iostats_wr);

	bmap_pagecache_destroy();
	bmap_cache_destroy();

	pfl_poolmaster_destroy(&msl_async_req_poolmaster);
	pfl_poolmaster_destroy(&msl_retry_req_poolmaster);
	pfl_poolmaster_destroy(&msl_biorq_poolmaster);
	pfl_poolmaster_destroy(&msl_iorq_poolmaster);
	pfl_poolmaster_destroy(&msl_mfh_poolmaster);

	msl_readahead_svc_destroy();
	dircache_mgr_destroy();
	slrpc_destroy();

	spinlock(&pfl_opstats_lock);
	DYNARRAY_FOREACH(opst, i, &pfl_opstats)
		if (strncmp(opst->opst_name, "msl.",
		    strlen("msl.")) == 0 ||
		    strncmp(opst->opst_name, "rpc.",
		    strlen("rpc.")) == 0)
			pfl_opstat_destroy_pos(i--);
	freelock(&pfl_opstats_lock);

	pflog_get_fsctx_uprog = NULL;
	pflog_get_fsctx_uid = NULL;
	pflog_get_fsctx_pid = NULL;

	slc_destroy_rpci(&msl_statfs_aggr_rpci);

	pfl_subsys_unregister(SLCSS_FSOP);
	pfl_subsys_unregister(SLCSS_INFO);
	sl_subsys_unregister();
}

void
mslfsop_write(struct pscfs_req *pfr, const void *buf, size_t size,
    off_t off, void *data)
{
	struct msl_fhent *mfh = data;
	struct fidc_membh *f;

	f = mfh->mfh_fcmh;

	DEBUG_FCMH(PLL_DIAG, f, "write start: pfr=%p sz=%zu "
	    "off=%"PSCPRIdOFFT" buf=%p", pfr, size, off, buf);

	msl_write(pfr, mfh, (void *)buf, size, off);
}

void
mslfsop_read(struct pscfs_req *pfr, size_t size, off_t off, void *data)
{
	struct msl_fhent *mfh = data;
	struct fidc_membh *f;

	f = mfh->mfh_fcmh;

	DEBUG_FCMH(PLL_DIAG, f, "read start: pfr=%p sz=%zu "
	    "off=%"PSCPRIdOFFT, pfr, size, off);

	msl_read(pfr, mfh, NULL, size, off);
}

void
mslfsop_listxattr(struct pscfs_req *pfr, size_t size, pscfs_inum_t inum)
{
	struct pscrpc_request *rq = NULL;
	struct slrpc_cservice *csvc = NULL;
	struct srm_listxattr_rep tmp, *mp = NULL;
	struct srm_listxattr_req *mq;
	struct fidc_membh *f = NULL;
	struct pscfs_creds pcr;
	struct iovec iov;
	char *buf = NULL;
	int rc;

	if (size > LNET_MTU)
		PFL_GOTOERR(out, rc = EINVAL);

	rc = msl_load_fcmh(pfr, inum, &f);
	if (rc)
		PFL_GOTOERR(out, rc);

	slc_getfscreds(pfr, &pcr);

	rc = fcmh_checkcreds(f, pfr, &pcr, R_OK);
	if (rc)
		PFL_GOTOERR(out, rc);

	FCMH_LOCK(f);
	/* Check if information about xattr size is cached and useful. */
	if (f->fcmh_flags & FCMH_CLI_XATTR_INFO) {
		struct fcmh_cli_info *fci;
		struct timeval now;

		PFL_GETTIMEVAL(&now);
		fci = fcmh_2_fci(f);
		if (timercmp(&now, &fci->fci_age, >=)) {
			f->fcmh_flags &= ~FCMH_CLI_XATTR_INFO;
		} else if (size == 0 && fci->fci_xattrsize != (uint32_t)-1) {
			OPSTAT_INCR("msl.xattr-hit-size");
			FCMH_ULOCK(f);
			tmp.size = fci->fci_xattrsize;
			mp = &tmp;
			PFL_GOTOERR(out, rc = 0);
		} else if (size && fci->fci_xattrsize == 0) {
			OPSTAT_INCR("msl.xattr-hit-noattr");
			FCMH_ULOCK(f);
			tmp.size = 0;
			mp = &tmp;
			PFL_GOTOERR(out, rc = 0);
		} else if (size && fci->fci_xattrsize != (uint32_t)-1 &&
			   size < fci->fci_xattrsize) {
			OPSTAT_INCR("msl.xattr-hit-erange");
			FCMH_ULOCK(f);
			PFL_GOTOERR(out, rc = ERANGE);
		}
	}
	FCMH_ULOCK(f);

	if (size)
		buf = PSCALLOC(size);

 retry1:
	MSL_RMC_NEWREQ(f, csvc, SRMT_LISTXATTR, rq, mq, mp, rc);
	if (rc)
		goto retry2;

	mq->fg.fg_fid = inum;
	mq->fg.fg_gen = FGEN_ANY;

	if (size) {
		mq->size = size;
		iov.iov_base = buf;
		iov.iov_len = size;
		rq->rq_bulk_abortable = 1;
		slrpc_bulkclient(rq, BULK_PUT_SINK, SRMC_BULK_PORTAL,
		    &iov, 1);
	}

	rc = SL_RSX_WAITREPF(csvc, rq, mp,
	    SRPCWAITF_DEFER_BULK_AUTHBUF_CHECK);
 retry2:
	if (rc && slc_rpc_should_retry(pfr, &rc))
		goto retry1;
	rc = abs(rc);
	if (!rc)
		rc = -mp->rc;
	if (!rc && size) {
		// XXX sanity check mp->size
		iov.iov_len = mp->size;
		rc = slrpc_bulk_checkmsg(rq, rq->rq_repmsg, &iov, 1);
		if (rc == 0)
			OPSTAT_INCR("msl.listxattr-bulk");
	}
	if (!rc && !size) {
		FCMH_LOCK(f);
		msl_fcmh_stash_xattrsize(f, mp->size);
	}

 out:
	if (f)
		fcmh_op_done(f);

	pscfs_reply_listxattr(pfr, buf, mp ? mp->size : 0, rc);

	pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
	PSCFREE(buf);
}


/*
 * XATTR_REPLACE is not supported.
 */
void
mslfsop_setxattr(struct pscfs_req *pfr, const char *name,
    const void *value, size_t size, pscfs_inum_t inum)
{
	struct slrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_setxattr_rep *mp = NULL;
	struct srm_setxattr_req *mq;
	struct fidc_membh *f = NULL;
	struct pscfs_creds pcr;
	struct iovec iov;
	int rc;

	if (strlen(name) >= sizeof(mq->name))
		PFL_GOTOERR(out, rc = EINVAL);

	/*
	 * This prevents a crash in the RPC layer downwards.  So disable
	 * it for now.
	 */
	if (size == 0)
		PFL_GOTOERR(out, rc = EINVAL);

	rc = msl_load_fcmh(pfr, inum, &f);
	if (rc)
		PFL_GOTOERR(out, rc);

	slc_getfscreds(pfr, &pcr);

	rc = fcmh_checkcreds(f, pfr, &pcr, R_OK);
	if (rc)
		PFL_GOTOERR(out, rc);

 retry1:
	MSL_RMC_NEWREQ(f, csvc, SRMT_SETXATTR, rq, mq, mp, rc);
	if (rc)
		goto retry2;

	mq->fg.fg_fid = inum;
	mq->fg.fg_gen = FGEN_ANY;
	mq->valuelen = size;
	strlcpy(mq->name, name, sizeof(mq->name));

	iov.iov_base = (char *)value;
	iov.iov_len = size;

	slrpc_bulkclient(rq, BULK_GET_SOURCE, SRMC_BULK_PORTAL, &iov,
	    1);

	rc = SL_RSX_WAITREP(csvc, rq, mp);
 retry2:
	if (rc && slc_rpc_should_retry(pfr, &rc))
		goto retry1;
	rc = abs(rc);
	if (!rc)
		rc = -mp->rc;
	if (!rc) {
		/*
		 * Do not use xattr information until properly
		 * refreshed.
		 *
		 * XXX we could piggyback the new size in the reply
		 */
		FCMH_LOCK(f);
		f->fcmh_flags &= ~FCMH_CLI_XATTR_INFO;
		FCMH_ULOCK(f);
	}

 out:
	pscfs_reply_setxattr(pfr, rc);

	psclogs_diag(SLCSS_FSOP, "SETXATTR: fid="SLPRI_FID" "
	    "name='%s' rc=%d", inum, name, rc);

	if (f)
		fcmh_op_done(f);
	pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

ssize_t
slc_getxattr(struct pscfs_req *pfr,
    __unusedx const struct pscfs_creds *pcrp, const char *name, void *buf,
    size_t size, struct fidc_membh *f, size_t *retsz)
{
	int rc = 0, locked = 0;
	struct slrpc_cservice *csvc = NULL;
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

	if (f->fcmh_flags & FCMH_CLI_XATTR_INFO) {
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

 retry1:
	MSL_RMC_NEWREQ(f, csvc, SRMT_GETXATTR, rq, mq, mp, rc);
	if (rc)
		goto retry2;

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
	rc = SL_RSX_WAITREPF(csvc, rq, mp,
	    SRPCWAITF_DEFER_BULK_AUTHBUF_CHECK);
 retry2:
	if (rc && slc_rpc_should_retry(pfr, &rc))
		goto retry1;
	rc = abs(rc);
	if (!rc)
		rc = -mp->rc;
	if (!rc && size) {
		iov.iov_len = mp->valuelen;
		rc = slrpc_bulk_checkmsg(rq, rq->rq_repmsg, &iov, 1);
		if (rc == 0)
			OPSTAT_INCR("msl.getxattr-bulk");
	}
	if (!rc)
		*retsz = mp->valuelen;

 out:
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
	struct fidc_membh *f = NULL;
	struct pscfs_creds pcr;
	size_t retsz = 0;
	void *buf = NULL;
	int rc;

	if (size > LNET_MTU)
		PFL_GOTOERR(out, rc = EINVAL);

	rc = msl_load_fcmh(pfr, inum, &f);
	if (rc)
		PFL_GOTOERR(out, rc);

	/* XXX skip this allocation if xattrsize=0 */
	if (size)
		buf = PSCALLOC(size);

	slc_getfscreds(pfr, &pcr);

	rc = slc_getxattr(pfr, &pcr, name, buf, size, f, &retsz);

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
	struct slrpc_cservice *csvc = NULL;
	struct srm_removexattr_rep *mp = NULL;
	struct srm_removexattr_req *mq;
	struct pscrpc_request *rq = NULL;
	struct fidc_membh *f = NULL;
	struct pscfs_creds pcr;
	int rc;

	if (strlen(name) >= sizeof(mq->name))
		PFL_GOTOERR(out, rc = EINVAL);

	rc = msl_load_fcmh(pfr, inum, &f);
	if (rc)
		PFL_GOTOERR(out, rc);

	slc_getfscreds(pfr, &pcr);

	rc = fcmh_checkcreds(f, pfr, &pcr, R_OK);
	if (rc)
		PFL_GOTOERR(out, rc);

 retry1:
	MSL_RMC_NEWREQ(f, csvc, SRMT_REMOVEXATTR, rq, mq, mp, rc);
	if (rc)
		goto retry2;

	mq->fg.fg_fid = inum;
	mq->fg.fg_gen = FGEN_ANY;
	strlcpy(mq->name, name, sizeof(mq->name));

	rc = SL_RSX_WAITREP(csvc, rq, mp);

 retry2:
	if (rc && slc_rpc_should_retry(pfr, &rc))
		goto retry1;
	rc = abs(rc);
	if (rc == 0)
		rc = -mp->rc;
 out:
	if (f)
		fcmh_op_done(f);

	pscfs_reply_removexattr(pfr, rc);

	psclogs_diag(SLCSS_FSOP, "REMOVEXATTR: fid="SLPRI_FID" "
	    "name='%s' rc=%d", inum, name, rc);

	pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

/*
 * Attribute flush thread.  This thread takes files on a queue that are
 * getting write(2) activity which need to send st_size and st_mtime
 * updates to the MDS.
 *
 * XXX this logic should be async.
 */
void
msattrflushthr_main(struct psc_thread *thr)
{
	struct timespec ts, nexttimeo;
	struct fcmh_cli_info *fci;
	struct fidc_membh *f;

	while (pscthr_run(thr)) {
		nexttimeo.tv_sec = FCMH_ATTR_TIMEO;
		nexttimeo.tv_nsec = 0;

		LIST_CACHE_LOCK(&msl_attrtimeoutq);
		if (lc_peekheadwait(&msl_attrtimeoutq) == NULL) {
			LIST_CACHE_ULOCK(&msl_attrtimeoutq);
			break;
		}
		PFL_GETTIMESPEC(&ts);
		LIST_CACHE_FOREACH(fci, &msl_attrtimeoutq) {
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

			LIST_CACHE_ULOCK(&msl_attrtimeoutq);

			msl_flush_ioattrs(NULL, f);
			FCMH_ULOCK(f);
			break;
		}
		if (fci == NULL) {
			OPSTAT_INCR("msl.flush-attr-wait");
			psc_waitq_waitrel_ts(&msl_flush_attrq,
			    &msl_attrtimeoutq.plc_lock, &nexttimeo);
		}
	}
}

void
msreapthr_main(struct psc_thread *thr)
{
	int curr, last = 0;
	while (pscthr_run(thr)) {
		while (fidc_reap(0, SL_FIDC_REAPF_EXPIRED));

		POOL_LOCK(bmpce_pool);
		curr = bmpce_pool->ppm_nfree;
		POOL_ULOCK(bmpce_pool);

		POOL_LOCK(bmpce_pool);
		if (last && curr >= last)
			psc_pool_try_shrink(bmpce_pool, curr);
		last = bmpce_pool->ppm_nfree;
		POOL_ULOCK(bmpce_pool);

		msl_pgcache_reap();

		psc_waitq_waitrel_s(&sl_freap_waitq, NULL, 30);

	}
}

void
msreapthr_spawn(int thrtype, const char *name)
{
	pscthr_init(thrtype, msreapthr_main, 0, name);
}

void
msattrflushthr_spawn(void)
{
	struct msattrflush_thread *maft;
	struct psc_thread *thr;
	int i;

	lc_reginit(&msl_attrtimeoutq, struct fcmh_cli_info, fci_lentry,
	    "attr-timeout");

	for (i = 0; i < NUM_ATTR_FLUSH_THREADS; i++) {
		thr = pscthr_init(MSTHRT_ATTR_FLUSH,
		    msattrflushthr_main, sizeof(*maft),
		    "msattrflushthr%d", i);
		maft = msattrflushthr(thr);
		pfl_multiwait_init(&maft->maft_mw, "%s",
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

/*
 * Set preferred I/O system.
 */
void
slc_setprefios(sl_ios_id_t id)
{
	struct sl_resource *r, *ri;
	int j;

//	CONF_LOCK();
	r = libsl_id2res(msl_pref_ios);
	if (r) {
		r->res_flags &= ~RESF_PREFIOS;
		if (RES_ISCLUSTER(r))
			DYNARRAY_FOREACH(ri, j, &r->res_peers)
				ri->res_flags &= ~RESF_PREFIOS;
	}
	msl_pref_ios = id;
	r = libsl_id2res(id);
	r->res_flags |= RESF_PREFIOS;
	if (RES_ISCLUSTER(r))
		DYNARRAY_FOREACH(ri, j, &r->res_peers)
			ri->res_flags &= ~RESF_PREFIOS;
//	CONF_ULOCK();
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

int
msl_init(void)
{
	struct sl_resource *r;
	char *name;
	time_t now;
	int rc;

	gcry_control(GCRYCTL_SET_THREAD_CBS, &gcry_threads_pthread);
	if (!gcry_check_version(GCRYPT_VERSION)) {
		warnx("libgcrypt version mismatch");
		return (-1);
	}

	sl_subsys_register();
	pfl_subsys_register(SLCSS_INFO, "info");
	pfl_subsys_register(SLCSS_FSOP, "fsop");

	pflog_get_fsctx_uprog = slc_log_get_fsctx_uprog;
	pflog_get_fsctx_uid = slc_log_get_fsctx_uid;
	pflog_get_fsctx_pid = slc_log_get_fsctx_pid;

	sl_sys_upnonce = psc_random32();
	slc_init_rpci(&msl_statfs_aggr_rpci);

	slcfg_local->cfg_fidcachesz = MSL_FIDCACHE_SIZE;

	slcfg_parse(msl_cfgfn);
	if (slcfg_local->cfg_root_squash)
		msl_root_squash = 1;
	parse_allowexe();
	if (msl_use_mapfile) {
		psc_hashtbl_init(&msl_uidmap_ext, 0, struct uid_mapping,
		    um_key, um_hentry, 191, NULL, "uidmapext");
		psc_hashtbl_init(&msl_uidmap_int, 0, struct uid_mapping,
		    um_key, um_hentry, 191, NULL, "uidmapint");

		psc_hashtbl_init(&msl_gidmap_int, 0, struct gid_mapping,
		    gm_key, gm_hentry, 191, NULL, "gidmapint");

		parse_mapfile();
	}

	authbuf_checkkeyfile();
	authbuf_readkeyfile();

	libsl_init(4096);//2 * (SRCI_NBUFS + SRCM_NBUFS));
	fidc_init(sizeof(struct fcmh_cli_info));
	bmpc_global_init();
	bmap_cache_init(sizeof(struct bmap_cli_info), MSL_BMAP_COUNT);
	dircache_mgr_init();

	psc_hashtbl_init(&msl_namecache_hashtbl, 0, struct dircache_ent,
	    dce_key, dce_hentry, 3 * slcfg_local->cfg_fidcachesz - 1,
	    dircache_ent_cmp, "namecache");

	psc_poolmaster_init(&msl_async_req_poolmaster,
	    struct slc_async_req, car_lentry, PPMF_AUTO, 64, 64, 0,
	    NULL, "asyncrq");
	msl_async_req_pool = psc_poolmaster_getmgr(&msl_async_req_poolmaster);

	psc_poolmaster_init(&msl_biorq_poolmaster,
	    struct bmpc_ioreq, biorq_lentry, PPMF_AUTO, 512, 512, 0,
	    NULL, "biorq");
	msl_biorq_pool = psc_poolmaster_getmgr(&msl_biorq_poolmaster);

	psc_poolmaster_init(&msl_mfh_poolmaster,
	    struct msl_fhent, mfh_lentry, PPMF_AUTO, 64, 64, 0,
	    NULL, "mfh");
	msl_mfh_pool = psc_poolmaster_getmgr(&msl_mfh_poolmaster);

	psc_poolmaster_init(&msl_iorq_poolmaster, struct msl_fsrqinfo,
	    mfsrq_lentry, PPMF_AUTO, 64, 64, 0, NULL, "iorq");
	msl_iorq_pool = psc_poolmaster_getmgr(&msl_iorq_poolmaster);

	/* Start up service threads. */
	slrpc_initcli();
	slc_rpc_initsvc();

	sl_nbrqset = pscrpc_prep_set();
	pscrpc_nbreapthr_spawn(sl_nbrqset, MSTHRT_NBRQ,
	    NUM_NBRQ_THREADS, "msnbrqthr%d");

	msctlthr_spawn();

	pfl_opstats_grad_init(&slc_iosyscall_iostats_rd, 0,
	    slc_io_grad_sizes, nitems(slc_io_grad_sizes),
	    "msl.iosz-rd:%s");
	pfl_opstats_grad_init(&slc_iosyscall_iostats_wr, 0,
	    slc_io_grad_sizes, nitems(slc_io_grad_sizes),
	    "msl.iosz-wr:%s");
	pfl_opstats_grad_init(&slc_iorpc_iostats_rd, 0,
	    slc_io_grad_sizes, nitems(slc_io_grad_sizes),
	    "msl.iorpc-rd:%s");
	pfl_opstats_grad_init(&slc_iorpc_iostats_wr, 0,
	    slc_io_grad_sizes, nitems(slc_io_grad_sizes),
	    "msl.iorpc-wr:%s");

	msbmapthr_spawn();
	msreapthr_spawn(MSTHRT_REAP, "pool reapthr");
	msattrflushthr_spawn();
	msreadaheadthr_spawn();

	name = getenv("MDS");
	if (name == NULL)
		psc_fatalx("environment variable MDS not specified");

	slcconnthr = slconnthr_spawn(MSTHRT_CONN, "slc", NULL, NULL);

	rc = slc_rmc_setmds(name);
	if (rc)
		psc_fatalx("invalid MDS %s: %s", name, strerror(rc));

	name = getenv("PREF_IOS");
	if (name) {

		r = libsl_str2res(name);
		if (r == NULL)
			psclog_warnx("PREF_IOS (%s) does not resolve "
			    "to a valid IOS; defaulting to IOS_ID_ANY",
			    name);
		else
			slc_setprefios(r->res_id);
	}

	pscfs_entry_timeout = 8.;
	pscfs_attr_timeout = 8.;

	time(&now);
	psclogs_info(SLCSS_INFO, "SLASH2 client version %d "
	    "started at %s", sl_stk_version, ctime(&now));

	return (0);
}

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

enum {
	LOOKUP_TYPE_BOOL,
	LOOKUP_TYPE_STR,
	LOOKUP_TYPE_UINT64,
	LOOKUP_TYPE_INT,
};

int
msl_opt_lookup(const char *opt)
{
	struct {
		const char	*name;
		int		 type;
		void		*ptr;
	} *io, opts[] = {
		{ "acl",		LOOKUP_TYPE_BOOL,	&msl_acl },
		{ "ctlsock",		LOOKUP_TYPE_STR,	&msl_ctlsockfn },
		{ "datadir",		LOOKUP_TYPE_STR,	&sl_datadir },
		{ "mapfile",		LOOKUP_TYPE_BOOL,	&msl_use_mapfile },
		{ "pagecache_maxsize",	LOOKUP_TYPE_UINT64,	&msl_pagecache_maxsize },
		{ "predio_issue_maxpages",
					LOOKUP_TYPE_INT,	&msl_predio_issue_maxpages},
		{ "root_squash",	LOOKUP_TYPE_BOOL,	&msl_root_squash },
		{ "slcfg",		LOOKUP_TYPE_STR,	&msl_cfgfn },
		{ NULL,			0,			NULL }
	};
	const char *val;
	size_t optlen;
	char *endp;
	ssize_t sz;
	long l;

	val = strchr(opt, '=');
	if (val) {
		optlen = val - opt;
		val++;
	} else
		optlen = strlen(opt);

	for (io = opts; io->name; io++)
		if (strncmp(opt, io->name, optlen) == 0) {
			switch (io->type) {
			case LOOKUP_TYPE_BOOL:
				*(int *)io->ptr = 1;
				break;
			case LOOKUP_TYPE_STR:
				*(const char **)io->ptr = val;
				break;
			case LOOKUP_TYPE_UINT64:
				sz = pfl_humantonum(val);
				if (sz < 0)
					errx(1, "%s: %s: %s", io->name,
					    strerror(-sz), val);
				*(uint64_t *)io->ptr = sz;
				break;
			case LOOKUP_TYPE_INT:
				l = strtol(val, &endp, 10);
				if (l < 0 || l > INT_MAX ||
				    endp == val || *endp)
					errx(1, "%s: invalid format: "
					    "%s", io->name, val);
				*(int *)io->ptr = l;
				break;
			default:
				psc_fatalx("invalid type");
			}
			return (1);
		}
	return (0);
}

__dead void
usage(void)
{
	extern const char *__progname;

	fprintf(stderr, "usage: %s [-dUV] [-o mountopt] node\n",
	    __progname);
	exit(1);
}

struct msl_filehandle_frozen {
	struct msl_fhent	mff_mfh;
	struct sl_fidgen	mff_fg;
};

/*
 * This routine is called when pscfs fileinfo data is serialized i.e.
 * for purposes of reloading the entire SLASH2 file system module.
 */
void
msl_filehandle_freeze(struct pflfs_filehandle *pfh)
{
	struct msl_filehandle_frozen *mff;
	struct msl_fhent *mfh;
	struct fidc_membh *f;

	mfh = pfh->pfh_mod_data;
	f = mfh->mfh_fcmh;
	pfh->pfh_mod_data = mff = PSCALLOC(sizeof(*mff));
	mff->mff_mfh = *mfh;
	mff->mff_fg = f->fcmh_fg;
	fcmh_op_done_type(f, FCMH_OPCNT_OPEN);
	psc_pool_return(msl_mfh_pool, mfh);
}

/*
 * This routine is called when pscfs fileinfo data is deserialized.
 */
void
msl_filehandle_thaw(struct pflfs_filehandle *pfh)
{
	struct msl_filehandle_frozen *mff;
	struct msl_fhent *mfh;
	struct fidc_membh *f;

	mff = pfh->pfh_mod_data;
	pfh->pfh_mod_data = mfh = psc_pool_get(msl_mfh_pool);
	*mfh = mff->mff_mfh;
	INIT_SPINLOCK(&mfh->mfh_lock);
	INIT_PSC_LISTENTRY(&mfh->mfh_lentry);
	psc_assert(!sl_fcmh_load_fg(&mff->mff_fg, &f));
	mfh->mfh_fcmh = f;
	fcmh_op_start_type(f, FCMH_OPCNT_OPEN);
	fcmh_op_done(f);
	PSCFREE(mff);
}

void
msl_populate_module(struct pscfs *m)
{
	m->pf_name = "slash2";

	m->pf_handle_access		= mslfsop_access;
	m->pf_handle_release		= mslfsop_release;
	m->pf_handle_releasedir		= mslfsop_release;
	m->pf_handle_create		= mslfsop_create;
	m->pf_handle_flush		= mslfsop_flush;
	m->pf_handle_fsync		= mslfsop_fsync;
	m->pf_handle_fsyncdir		= mslfsop_fsync;
	m->pf_handle_getattr		= mslfsop_getattr;
	m->pf_handle_link		= mslfsop_link;
	m->pf_handle_lookup		= mslfsop_lookup;
	m->pf_handle_mkdir		= mslfsop_mkdir;
	m->pf_handle_mknod		= mslfsop_mknod;
	m->pf_handle_open		= mslfsop_open;
	m->pf_handle_opendir		= mslfsop_opendir;
	m->pf_handle_read		= mslfsop_read;
	m->pf_handle_readdir		= mslfsop_readdir;
	m->pf_handle_readlink		= mslfsop_readlink;
	m->pf_handle_rename		= mslfsop_rename;
	m->pf_handle_rmdir		= mslfsop_rmdir;
	m->pf_handle_setattr		= mslfsop_setattr;
	m->pf_handle_statfs		= mslfsop_statfs;
	m->pf_handle_symlink		= mslfsop_symlink;
	m->pf_handle_unlink		= mslfsop_unlink;
	m->pf_handle_destroy		= mslfsop_destroy;
	m->pf_handle_write		= mslfsop_write;
	m->pf_handle_listxattr		= mslfsop_listxattr;
	m->pf_handle_getxattr		= mslfsop_getxattr;
	m->pf_handle_setxattr		= mslfsop_setxattr;
	m->pf_handle_removexattr	= mslfsop_removexattr;

	m->pf_thr_init			= msfsthr_init;
	m->pf_thr_destroy		= msfsthr_destroy;
}

int
pscfs_module_load(struct pscfs *m)
{
	const char *opt;
	int i;

	msl_populate_module(m);

	m->pf_filehandle_freeze		= msl_filehandle_freeze;
	m->pf_filehandle_thaw		= msl_filehandle_thaw;

	DYNARRAY_FOREACH(opt, i, &m->pf_opts)
		if (!msl_opt_lookup(opt)) {
			warnx("invalid option: %s", opt);
			return (EINVAL);
		}

	return (msl_init());
}
