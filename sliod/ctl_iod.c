/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2006-2018, Pittsburgh Supercomputing Center
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

#include <sys/types.h>
#include <sys/statvfs.h>

/*
 * Interface for controlling live operation of a sliod instance.
 */

#include "pfl/cdefs.h"
#include "pfl/ctl.h"
#include "pfl/ctlsvr.h"
#include "pfl/lockedlist.h"
#include "pfl/rsx.h"
#include "pfl/stat.h"
#include "pfl/str.h"
#include "pfl/walk.h"

#include "bmap_iod.h"
#include "ctl.h"
#include "ctl_iod.h"
#include "ctlsvr.h"
#include "fidc_iod.h"
#include "pathnames.h"
#include "repl_iod.h"
#include "rpc_iod.h"
#include "sliod.h"
#include "slutil.h"
#include "slvr.h"

#define TRIMCHARS(str, ch)						\
	do {								\
		char *_p;						\
									\
		for (_p = (str) + strlen(str) - 1;			\
		    _p > (str) && *_p == (ch); _p--)			\
			*_p = '\0';					\
	} while (0)

int
sli_export(__unusedx FTSENT *f, __unusedx void *arg)
{
#if 0
	struct slictlmsg_fileop *sfop = arg;
	char fidfn[PATH_MAX], exfn[PATH_MAX];
	int rc;

	sli_fg_makepath(&fg, fidfn);
	snprintf("%s/%s", dst_base, dst);

	for (cpn) {
		rc = sli_rmi_lookup_fid(csvc, &fg, cpn, &tfg, &isdir);
		rc = mkdir();
	}
	rc = link(fidfn, exfn);
	return (rc);
#endif
	return (0);
}

int
slictlcmd_export(__unusedx int fd, __unusedx struct psc_ctlmsghdr *mh,
    void *m)
{
	struct slictlmsg_fileop *sfop = m;
	int fl = PFL_FILEWALKF_NOCHDIR;

	if (sfop->sfop_flags & SLI_CTL_FOPF_RECURSIVE)
		fl |= PFL_FILEWALKF_RECURSIVE;
if (fl & PFL_FILEWALKF_RECURSIVE)
 psc_fatalx("-R not supported");
	return (pfl_filewalk(sfop->sfop_fn, fl, NULL, sli_export, sfop));
}

struct sli_import_arg {
	struct psc_ctlmsghdr	*mh;
	struct slictlmsg_fileop	*sfop;
	int			 fd;
	int			 rc;
};

int
sli_rmi_issue_mkdir(struct slrpc_cservice *csvc,
    const struct sl_fidgen *pfg, const char *name,
    const struct stat *stb, char fidfn[PATH_MAX])
{
	struct pscrpc_request *rq;
	struct srm_mkdir_req *mq;
	struct srm_mkdir_rep *mp;
	int rc;

	rc = SL_RSX_NEWREQ(csvc, SRMT_MKDIR, rq, mq, mp);
	if (rc)
		return (rc);
	mq->pfg = *pfg;
	strlcpy(mq->name, name, sizeof(mq->name));
	sl_externalize_stat(stb, &mq->sstb);
	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc == 0) {
		rc = mp->rc;
		if (fidfn)
			sli_fg_makepath(&mp->cattr.sst_fg, fidfn);
	}
	pscrpc_req_finished(rq);
	return (rc);
}

/*
 * Import files resident on a sliod backfs into the SLASH2 namespace.
 * @fn: target file
 * @stb: file's attributes.
 * @arg: arg containing destination info, etc.
 *
 * Note the behavior of the each of the following:
 *
 *	# slictl import -R src-dir  non-exist-dst-dir/
 *
 *		Obviously, non-exist-dst-dir doesn't exist in the SLASH2
 *		namespace, so this will flag an error.
 *
 *	# slictl import -R src-dir  non-exist-dst-dir
 *	# slictl import -R src-dir/ non-exist-dst-dir
 *
 *		The contents of `src-dir' will be created directly under
 *		the newly created directory `non-exist-dst-dir' in the
 *		SLASH2 namespace.
 *
 *	# slictl import -R src-dir  exist-dst-dir
 *	# slictl import -R src-dir/ exist-dst-dir
 *
 *	# slictl import -R src-dir exist-dst-dir
 *
 * In the first case, the contents under 'src-dir' will be attached
 * directly inside 'non-exist-dst-dir' after it is mkdir'ed.  In the
 * second case, a subdir named 'src-dir' will be created under
 * 'exist-dst-dir'.  This is the same behavior when you mv directories
 * around.
 */
int
sli_import(FTSENT *f, void *arg)
{
	char *p, *np, fidfn[PATH_MAX], cpn[SL_NAME_MAX + 1];
	int rc = 0, isdir, dolink = 0;
	struct sli_import_arg *a = arg;
	struct slictlmsg_fileop *sfop = a->sfop;
	struct slrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct psc_ctlmsghdr *mh = a->mh;
	struct sl_fidgen tfg, fg;
	struct stat tstb, *stb;
	const char *str, *fn;

	stb = f->fts_statp;
	fn = f->fts_path;
	tstb = *stb;

	/*
	 * Start from the root of the SLASH2 namespace.  This means
	 * that if just a name is given as the destination, it will be
	 * treated as a child of the root.
	 */
	fg.fg_fid = SLFID_ROOT;
	fg.fg_gen = FGEN_ANY;

	psc_assert(strncmp(fn, sfop->sfop_fn,
	    strlen(sfop->sfop_fn)) == 0);

	rc = sli_rmi_getcsvc(&csvc);
	if (rc)
		PFL_GOTOERR(error, rc);

	if (sfop->sfop_pfid != FID_ANY) {
		strlcpy(cpn, sfop->sfop_fn2, sizeof(cpn));
		fg.fg_fid = sfop->sfop_pfid;
		goto gotpfid;
	}

	/* preserve hierarchy in the src tree via concatenation */
	snprintf(fidfn, sizeof(fidfn), "%s%s%s", sfop->sfop_fn2,
	    S_ISDIR(stb->st_mode) ? "/" : "",
	    fn + strlen(sfop->sfop_fn));

	/* trim trailing '/' chars */
	TRIMCHARS(fidfn, '/');
	for (p = fidfn; *p; p = np, cpn[0] = '\0') {
		/* skip leading '/' chars */
		while (*p == '/')
			p++;
		np = p + strcspn(p, "/");
		if (np == p)
			continue;
		if (np - p > (int)sizeof(cpn))
			PFL_GOTOERR(error, rc = ENAMETOOLONG);

		strlcpy(cpn, p, np - p + 1);

		/*
		 * Do not LOOKUP the last pathname component; we'll just
		 * blindly try to import there.
		 */
		if (*np == '\0')
			break;

		rc = sli_rmi_lookup_fid(csvc, &fg, cpn, &tfg, &isdir);

		/* Last component is intended destination; use directly. */
		if (rc == ENOENT && *np == '\0')
			break;

		if (rc)
			PFL_GOTOERR(error, rc);
		if (!isdir) {
			if (*np)
				PFL_GOTOERR(error, rc = ENOTDIR);
			break;
		}
		fg = tfg;
	}

 gotpfid:
	/*
	 * No destination name specified; preserve last component from
	 * src.
	 */
	if (cpn[0] == '\0') {
		if (S_ISREG(stb->st_mode)) {
			str = pfl_basename(fn);
			if (strlen(str) >= sizeof(cpn))
				PFL_GOTOERR(error, rc = ENAMETOOLONG);
			strlcpy(cpn, str, sizeof(cpn));
		} else {
			/*
			 * Directory is ambiguous: "should we reimport
			 * attrs or create a subdir with the same
			 * name?"  Instead, flag error.
			 */
			PFL_GOTOERR(error, rc = EEXIST);
		}
	}

	/* XXX perform user permission checks */

	if (S_ISDIR(stb->st_mode)) {
		rc = sli_rmi_issue_mkdir(csvc, &fg, cpn, &tstb, fidfn);
	} else if (S_ISBLK(stb->st_mode) || S_ISCHR(stb->st_mode) ||
	    S_ISFIFO(stb->st_mode) || S_ISSOCK(stb->st_mode)) {
		/* XXX: use mknod */
		rc = ENOTSUP;
	} else if (S_ISLNK(stb->st_mode)) {
		struct srm_symlink_req *mq;
		struct srm_symlink_rep *mp;
		char target[PATH_MAX];
		struct iovec iov;

		/*
		 * XXX should we check that st_nlink == 1 and refuse if
		 * this is not the case to protect against multiple
		 * imports?
		 *
		 * XXX we do not handle hardlinks correctly!  Need to be
		 * able to reference the same backing file as some other
		 * original already imported so a second hardlink
		 * doesn't get a separate SLASH2 FID.
		 */
#if 0
		if (stb->st_nlink > 1)
			PFL_GOTOERR(error, rc = EEXIST);
#endif

		if (readlink(fn, target, sizeof(target)) == -1)
			PFL_GOTOERR(error, rc = errno);

		rc = SL_RSX_NEWREQ(csvc, SRMT_SYMLINK, rq, mq, mp);
		if (rc)
			PFL_GOTOERR(error, rc);
		mq->pfg = fg;
		mq->linklen = strlen(target);
		strlcpy(mq->name, cpn, sizeof(mq->name));
		sl_externalize_stat(&tstb, &mq->sstb);

		iov.iov_base = target;
		iov.iov_len = mq->linklen;

		slrpc_bulkclient(rq, BULK_GET_SOURCE, SRMI_BULK_PORTAL,
		    &iov, 1);

		rc = SL_RSX_WAITREP(csvc, rq, mp);
		if (rc == 0)
			rc = mp->rc;
		if (rc == 0 || rc == -EEXIST) {
			sli_fg_makepath(&mp->cattr.sst_fg, fidfn);
			dolink = 1;
		}
	} else if (S_ISREG(stb->st_mode)) {
		struct srm_import_req *mq;
		struct srm_import_rep *mp;

		/*
		 * XXX should we check that st_nlink == 1 and refuse if
		 * this is not the case to protect against multiple
		 * imports?
		 *
		 * XXX we do not handle hardlinks correctly!  Need to be
		 * able to reference the same backing file as some other
		 * original already imported so a second hardlink
		 * doesn't get a separate SLASH2 FID.
		 */
#if 0
		if (stb->st_nlink > 1)
			PFL_GOTOERR(error, rc = EEXIST);
#endif

		rc = SL_RSX_NEWREQ(csvc, SRMT_IMPORT, rq, mq, mp);
		if (rc)
			PFL_GOTOERR(error, rc);
		mq->pfg = fg;
		strlcpy(mq->cpn, cpn, sizeof(mq->cpn));
		sl_externalize_stat(&tstb, &mq->sstb);
		if (sfop->sfop_flags & SLI_CTL_FOPF_XREPL)
			mq->flags = SRM_IMPORTF_XREPL;
		rc = SL_RSX_WAITREP(csvc, rq, mp);
		if (rc == 0)
			rc = mp->rc;
		if (rc == 0 || rc == -EEXIST) {
			sli_fg_makepath(&mp->fg, fidfn);
			dolink = 1;
		}
	} else
		rc = ENOTSUP;

	if (dolink) {
		/*
		 * XXX If we fail here, we should undo import above.
		 * However, with checks earlier, we probably won't fail
		 * for EXDEV here.
		 */
		if (link(fn, fidfn) == -1)
			rc = errno;
	}

 error:
	if (abs(rc) == EEXIST) {
		if (lstat(fn, &tstb) == -1) {
			rc = errno;
			a->rc = psc_ctlsenderr(a->fd, mh, NULL, "%s: %s", fn,
			    sl_strerror(rc));
		} else if (tstb.st_ino == stb->st_ino) {
			rc = 0;
			if (sfop->sfop_flags & SLI_CTL_FOPF_VERBOSE)
				a->rc = psc_ctlsenderr(a->fd, mh, NULL,
				    "reimporting %s%s", fn,
				    S_ISDIR(stb->st_mode) ?
				    "/" : "");
		} else {
			a->rc = psc_ctlsenderr(a->fd, mh, NULL,
			    "%s: another file has already been "
			    "imported at this namespace node",
			    fn);
		}
	} else if (rc)
		a->rc = psc_ctlsenderr(a->fd, mh, NULL, "%s: %s", fn,
		    sl_strerror(rc));
	else if (sfop->sfop_flags & SLI_CTL_FOPF_VERBOSE)
		a->rc = psc_ctlsenderr(a->fd, mh, NULL, "importing %s%s", fn,
		    S_ISDIR(stb->st_mode) ? "/" : "");

	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
	psclog_info("import file %s: fidfn=%s rc=%d",
	    fn, pfl_basename(fidfn), a->rc);
	return (rc || a->rc == 0);
}

int
slictlcmd_import(int fd, struct psc_ctlmsghdr *mh, void *m)
{
	int fl = PFL_FILEWALKF_NOCHDIR;
	struct slictlmsg_fileop *sfop = m;
	struct statvfs vfssb1, vfssb2;
	struct sli_import_arg a;
	struct stat sb1, sb2;
	char buf[SL_PATH_MAX];

	sfop->sfop_fn[sizeof(sfop->sfop_fn) - 1] = '\0';
	sfop->sfop_fn2[sizeof(sfop->sfop_fn2) - 1] = '\0';
	if (sfop->sfop_fn[0] == '\0')
		return (psc_ctlsenderr(fd, mh, NULL, "import source: %s",
		    sl_strerror(ENOENT)));
	if (sfop->sfop_fn2[0] == '\0')
		return (psc_ctlsenderr(fd, mh, NULL, "import destination: %s",
		    sl_strerror(ENOENT)));

	/*
	 * XXX: we should disallow recursive import of a parent
	 * directory to where the SLASH2 objdir resides, which would
	 * cause an infinite loop.
	 */

	if (stat(slcfg_local->cfg_fsroot, &sb1) == -1)
		return (psc_ctlsenderr(fd, mh, NULL, "%s: %s",
		    sfop->sfop_fn, sl_strerror(errno)));

	if (lstat(sfop->sfop_fn, &sb2) == -1)
		return (psc_ctlsenderr(fd, mh, NULL, "%s: %s",
		    sfop->sfop_fn, sl_strerror(errno)));

	if (sb1.st_dev != sb2.st_dev)
		return (psc_ctlsenderr(fd, mh, NULL, "%s: %s",
		    sfop->sfop_fn, sl_strerror(EXDEV)));

	/*
	 * The following checks are done to avoid EXDEV down the road.
	 * This is not bullet-proof but avoid false negatives.
	 */
	if (S_ISREG(sb2.st_mode)) {
		if (statvfs(slcfg_local->cfg_fsroot, &vfssb1) == -1)
			return (psc_ctlsenderr(fd, mh, NULL, "%s: %s",
			    sfop->sfop_fn, sl_strerror(errno)));

		if (statvfs(sfop->sfop_fn, &vfssb2) == -1)
			return (psc_ctlsenderr(fd, mh, NULL, "%s: %s",
			    sfop->sfop_fn, sl_strerror(errno)));

		if (vfssb1.f_fsid != vfssb2.f_fsid)
			return (psc_ctlsenderr(fd, mh, NULL, "%s: %s",
			    sfop->sfop_fn, sl_strerror(EXDEV)));
	}

	memset(&a, 0, sizeof(a));
	a.mh = mh;
	a.fd = fd;
	a.sfop = sfop;
	a.rc = 1;
	if (sfop->sfop_flags & SLI_CTL_FOPF_RECURSIVE)
		fl |= PFL_FILEWALKF_RECURSIVE;

	if (realpath(sfop->sfop_fn, buf) == NULL)
		return (psc_ctlsenderr(fd, mh, NULL, "%s: %s",
		    sfop->sfop_fn, sl_strerror(errno)));
	strlcpy(sfop->sfop_fn, buf, sizeof(sfop->sfop_fn));
	pfl_filewalk(sfop->sfop_fn, fl, NULL, sli_import, &a);
	return (a.rc);
}

int
slictlrep_getreplwkst(int fd, struct psc_ctlmsghdr *mh, void *m)
{
	struct slictlmsg_replwkst *srws = m;
	struct sli_repl_workrq *w;
	int rc;

	rc = 1;
	PLL_LOCK(&sli_replwkq_active);
	PLL_FOREACH(w, &sli_replwkq_active) {
		srws->srws_fg = w->srw_fg;
		srws->srws_bmapno = w->srw_bmapno;
		srws->srws_refcnt = psc_atomic32_read(&w->srw_refcnt);
		srws->srws_data_tot = SLASH_SLVR_SIZE * w->srw_nslvr_tot;
		srws->srws_data_cur = SLASH_SLVR_SIZE * w->srw_nslvr_cur;
		strlcpy(srws->srws_peer_addr, w->srw_src_res->res_name,
		    sizeof(srws->srws_peer_addr));

		rc = psc_ctlmsg_sendv(fd, mh, srws, NULL);
		if (!rc)
			break;
	}
	PLL_ULOCK(&sli_replwkq_active);
	return (rc);
}

int
slictlrep_getslvr(int fd, struct psc_ctlmsghdr *mh, void *m)
{
	struct slictlmsg_slvr *ss = m;
	struct slvr *s;
	int rc;

	rc = 1;
	LIST_CACHE_LOCK(&sli_lruslvrs);
	LIST_CACHE_FOREACH(s, &sli_lruslvrs) {
		memset(ss, 0, sizeof(*ss));
		ss->ss_fid = fcmh_2_fid(slvr_2_fcmh(s));
		ss->ss_bno = slvr_2_bmap(s)->bcm_bmapno;
		ss->ss_slvrno = s->slvr_num;
		ss->ss_flags = s->slvr_flags;
		ss->ss_refcnt = s->slvr_refcnt;
		ss->ss_err = s->slvr_err;
		ss->ss_ts.tv_sec = s->slvr_ts.tv_sec;
		ss->ss_ts.tv_nsec = s->slvr_ts.tv_nsec;

		rc = psc_ctlmsg_sendv(fd, mh, ss, NULL);
		if (!rc)
			break;
	}
	LIST_CACHE_ULOCK(&sli_lruslvrs);
	return (rc);
}

int
slctlmsg_bmap_send(int fd, struct psc_ctlmsghdr *mh,
    struct slctlmsg_bmap *scb, struct bmap *b)
{
	scb->scb_fg = b->bcm_fcmh->fcmh_fg;
	scb->scb_bno = b->bcm_bmapno;
	scb->scb_opcnt = psc_atomic32_read(&b->bcm_opcnt);
	scb->scb_flags = b->bcm_flags;
	return (psc_ctlmsg_sendv(fd, mh, scb, NULL));
}

int
slictl_resfield_connected(int fd, struct psc_ctlmsghdr *mh,
    struct psc_ctlmsg_param *pcp, char **levels, int nlevels, int set,
    struct sl_resource *r)
{
	struct slrpc_cservice *csvc;
	struct sl_resm *m;
	char nbuf[8];

	if (set && strcmp(pcp->pcp_value, "0") &&
	    strcmp(pcp->pcp_value, "1"))
		return (psc_ctlsenderr(fd, mh, NULL,
		    "connected: invalid value"));

	m = res_getmemb(r);
	if (set) {
		if (r->res_type == SLREST_MDS)
			csvc = sli_geticsvcf(m, CSVCF_NONBLOCK, 0);
		else
			csvc = sli_getmcsvcf(m, CSVCF_NONBLOCK, 0);
		if (strcmp(pcp->pcp_value, "0") == 0 && csvc)
			sl_csvc_disconnect(csvc);
		if (csvc)
			sl_csvc_decref(csvc);
		return (1);
	}
	if (r->res_type == SLREST_MDS)
		csvc = sli_getmcsvcf(m, CSVCF_NONBLOCK | CSVCF_NORECON, 0);
	else
		csvc = sli_geticsvcf(m, CSVCF_NONBLOCK | CSVCF_NORECON, 0);
	snprintf(nbuf, sizeof(nbuf), "%d", csvc ? 1 : 0);
	if (csvc)
		sl_csvc_decref(csvc);
	return (psc_ctlmsg_param_send(fd, mh, pcp, PCTHRNAME_EVERYONE,
	    levels, nlevels, nbuf));
}

const struct slctl_res_field slctl_resmds_fields[] = {
	{ "connected",		slictl_resfield_connected },
	{ NULL, NULL }
};

const struct slctl_res_field slctl_resios_fields[] = {
	{ "connected",		slictl_resfield_connected },
	{ NULL, NULL }
};

/*
 * Handle a STOP command to terminate execution.
 */
__dead int
slictlcmd_stop(__unusedx int fd, __unusedx struct psc_ctlmsghdr *mh,
    __unusedx void *m)
{
	/* XXX sync buffers on disk */
	pscthr_killall();
	pscrpc_exit_portals();
	/* XXX wait */
	exit(0);
}

struct psc_ctlop slictlops[] = {
	PSC_CTLDEFOPS,
	{ slictlrep_getreplwkst,	sizeof(struct slictlmsg_replwkst ) },
	{ slctlrep_getconn,		sizeof(struct slctlmsg_conn ) },
	{ slctlrep_getfcmh,		sizeof(struct slctlmsg_fcmh ) },
	{ slictlcmd_export,		sizeof(struct slictlmsg_fileop) },
	{ slictlcmd_import,		sizeof(struct slictlmsg_fileop) },
	{ slictlcmd_stop,		0 },
	{ slctlrep_getbmap,		sizeof(struct slctlmsg_bmap) },
	{ slictlrep_getslvr,		sizeof(struct slictlmsg_slvr) }
};

void
slictlthr_spawn(const char *fn)
{
	pflrpc_register_ctlops(slictlops);

	psc_ctlparam_register("faults", psc_ctlparam_faults);

#ifdef Linux
	psc_ctlparam_register("log.file", psc_ctlparam_log_file);
#endif
	psc_ctlparam_register("log.format", psc_ctlparam_log_format);
	psc_ctlparam_register("log.level", psc_ctlparam_log_level);
	psc_ctlparam_register("log.points", psc_ctlparam_log_points);
	psc_ctlparam_register("opstats", psc_ctlparam_opstats);
	psc_ctlparam_register("pause", psc_ctlparam_pause);
	psc_ctlparam_register("pool", psc_ctlparam_pool);
	psc_ctlparam_register("rlim", psc_ctlparam_rlim);
	psc_ctlparam_register("run", psc_ctlparam_run);
	psc_ctlparam_register("rusage", psc_ctlparam_rusage);

	psc_ctlparam_register_var("sys.nbrq_outstanding",
	    PFLCTL_PARAMT_INT, 0, &sl_nbrqset->set_remaining);
	psc_ctlparam_register("sys.resources", slctlparam_resources);

	psc_ctlparam_register_var("sys.rpc_timeout", PFLCTL_PARAMT_INT, 
	    PFLCTL_PARAMF_RDWR, &pfl_rpc_timeout);

	psc_ctlparam_register_simple("sys.uptime",
	    slctlparam_uptime_get, NULL);
	psc_ctlparam_register_simple("sys.version",
	    slctlparam_version_get, NULL);
	psc_ctlparam_register_var("sys.datadir", PFLCTL_PARAMT_STR, 0,
	    (char *)sl_datadir);

	psc_ctlparam_register_var("sys.bminseqno", PFLCTL_PARAMT_UINT64,
	    0, &sli_bminseq.bim_minseq);
	psc_ctlparam_register_var("sys.disable_write",
	    PFLCTL_PARAMT_INT, PFLCTL_PARAMF_RDWR, &sli_disable_write);

	psc_ctlparam_register_var("sys.reclaim_batchno",
	    PFLCTL_PARAMT_UINT64, 0, &sli_current_reclaim_batchno);
	psc_ctlparam_register_var("sys.reclaim_xid",
	    PFLCTL_PARAMT_UINT64, 0, &sli_current_reclaim_xid);

	psc_ctlparam_register_var("sys.self_test_enable",
	    PFLCTL_PARAMT_INT, PFLCTL_PARAMF_RDWR, &sli_selftest_enable);
	psc_ctlparam_register_var("sys.self_test_result", PFLCTL_PARAMT_INT,
	    0, &sli_selftest_result);

	psc_ctlparam_register_var("sys.min_space_reserve_gb",
	    PFLCTL_PARAMT_INT, PFLCTL_PARAMF_RDWR,
	    &sli_min_space_reserve_gb);
	psc_ctlparam_register_var("sys.min_space_reserve_pct",
	    PFLCTL_PARAMT_INT, PFLCTL_PARAMF_RDWR,
	    &sli_min_space_reserve_pct);

	psc_ctlparam_register_var("sys.pid", PFLCTL_PARAMT_INT, 0,
	    &pfl_pid);
#ifdef Linux
	psc_ctlparam_register("sys.rss", psc_ctlparam_get_rss);
#endif

	psc_ctlparam_register_var("sys.sync_max_writes",
	    PFLCTL_PARAMT_INT, PFLCTL_PARAMF_RDWR,
	    &sli_sync_max_writes);

	psc_ctlparam_register_var("sys.max_readahead",
	    PFLCTL_PARAMT_INT, PFLCTL_PARAMF_RDWR,
	    &sli_predio_max_slivers);

	psc_ctlthr_main(fn, slictlops, nitems(slictlops), 0, SLITHRT_CTLAC);
}
