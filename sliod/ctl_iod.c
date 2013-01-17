/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2013, Pittsburgh Supercomputing Center (PSC).
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

#include <sys/types.h>
#include <sys/statvfs.h>

/*
 * Interface for controlling live operation of a sliod instance.
 */

#include "pfl/cdefs.h"
#include "pfl/stat.h"
#include "pfl/str.h"
#include "pfl/walk.h"
#include "psc_ds/lockedlist.h"
#include "psc_rpc/rsx.h"
#include "psc_util/ctl.h"
#include "psc_util/ctlsvr.h"

#include "ctl.h"
#include "ctl_iod.h"
#include "ctlsvr.h"
#include "fidc_iod.h"
#include "repl_iod.h"
#include "rpc_iod.h"
#include "sliod.h"
#include "slutil.h"

#define TRIMCHARS(str, ch)						\
	do {								\
		char *_p;						\
									\
		for (_p = (str) + strlen(str) - 1;			\
		    _p > (str) && *_p == (ch); _p--)			\
			*_p = '\0';					\
	} while (0)

struct psc_lockedlist psc_odtables;

int
sli_export(__unusedx const char *fn, __unusedx const struct stat *stb,
    void *arg)
{
	struct slictlmsg_fileop *sfop = arg;
	int rc = 0;

	psclog_info("export: src=%s, dst=%s, flags=%d",
		sfop->sfop_fn, sfop->sfop_fn2, sfop->sfop_flags);
	return (rc);
}

int
slictlcmd_export(__unusedx int fd, __unusedx struct psc_ctlmsghdr *mh,
    void *m)
{
	struct slictlmsg_fileop *sfop = m;
	int fl = 0;

	if (sfop->sfop_flags & SLI_CTL_FOPF_RECURSIVE)
		fl |= PFL_FILEWALKF_RECURSIVE;
	return (pfl_filewalk(sfop->sfop_fn, fl, sli_export, sfop));
}

struct sli_import_arg {
	struct psc_ctlmsghdr	*mh;
	struct slictlmsg_fileop	*sfop;
	int			 fd;
	int			 rc;
};

int
sli_rmi_issue_mkdir(struct slashrpc_cservice *csvc,
    const struct slash_fidgen *pfg, const char *name,
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

/**
 * sli_import - Import files resident on a sliod backfs into the SLASH2
 *	namespace.
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
sli_import(const char *fn, const struct stat *stb, void *arg)
{
	char *p, *np, fidfn[PATH_MAX], cpn[SL_NAME_MAX + 1];
	int rc = 0, isdir, dolink = 0;
	struct sli_import_arg *a = arg;
	struct slictlmsg_fileop *sfop = a->sfop;
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct psc_ctlmsghdr *mh = a->mh;
	struct slash_fidgen tfg, fg;
	struct stat tstb;
	const char *str;

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

		rc = sli_fcmh_lookup_fid(csvc, &fg, cpn, &tfg, &isdir);

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
	/* No destination name specified; preserve last component from src. */
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
		rc = sli_rmi_issue_mkdir(csvc, &fg, cpn, stb, fidfn);
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
		sl_externalize_stat(stb, &mq->sstb);

		iov.iov_base = target;
		iov.iov_len = mq->linklen;

		rsx_bulkclient(rq, BULK_GET_SOURCE, SRMI_BULK_PORTAL,
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
		sl_externalize_stat(stb, &mq->sstb);
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
			a->rc = psc_ctlsenderr(a->fd, mh, "%s: %s", fn,
			    slstrerror(rc));
		} else if (tstb.st_ino == stb->st_ino) {
			rc = 0;
			if (sfop->sfop_flags & SLI_CTL_FOPF_VERBOSE)
				a->rc = psc_ctlsenderr(a->fd, mh,
				    "reimporting %s%s", fn,
				    S_ISDIR(stb->st_mode) ?
				    "/" : "");
		} else {
			a->rc = psc_ctlsenderr(a->fd, mh,
			    "%s: another file has already been "
			    "imported at this namespace node",
			    fn);
		}
	} else if (rc)
		a->rc = psc_ctlsenderr(a->fd, mh, "%s: %s", fn,
		    slstrerror(rc));
	else if (sfop->sfop_flags & SLI_CTL_FOPF_VERBOSE)
		a->rc = psc_ctlsenderr(a->fd, mh, "importing %s%s", fn,
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
	struct slictlmsg_fileop *sfop = m;
	struct statvfs vfssb1, vfssb2;
	struct sli_import_arg a;
	struct stat sb1, sb2;
	char buf[SL_PATH_MAX];
	int fl = 0;

	sfop->sfop_fn[sizeof(sfop->sfop_fn) - 1] = '\0';
	sfop->sfop_fn2[sizeof(sfop->sfop_fn2) - 1] = '\0';
	if (sfop->sfop_fn[0] == '\0')
		return (psc_ctlsenderr(fd, mh, "import source: %s",
		    slstrerror(ENOENT)));
	if (sfop->sfop_fn2[0] == '\0')
		return (psc_ctlsenderr(fd, mh, "import destination: %s",
		    slstrerror(ENOENT)));

	/*
	 * XXX: we should disallow recursive import of a parent
	 * directory to where the SLASH2 objdir resides, which would
	 * cause an infinite loop.
	 */

	if (stat(globalConfig.gconf_fsroot, &sb1) == -1)
		return (psc_ctlsenderr(fd, mh, "%s: %s",
		    sfop->sfop_fn, slstrerror(errno)));

	if (lstat(sfop->sfop_fn, &sb2) == -1)
		return (psc_ctlsenderr(fd, mh, "%s: %s",
		    sfop->sfop_fn, slstrerror(errno)));

	if (sb1.st_dev != sb2.st_dev)
		return (psc_ctlsenderr(fd, mh, "%s: %s",
		    sfop->sfop_fn, slstrerror(EXDEV)));

	/*
	 * The following checks are done to avoid EXDEV down the road.
	 * This is not bullet-proof but avoid false negatives.
	 */
	if (S_ISREG(sb2.st_mode)) {
		if (statvfs(globalConfig.gconf_fsroot, &vfssb1) == -1)
			return (psc_ctlsenderr(fd, mh, "%s: %s",
			    sfop->sfop_fn, slstrerror(errno)));

		if (statvfs(sfop->sfop_fn, &vfssb2) == -1)
			return (psc_ctlsenderr(fd, mh, "%s: %s",
			    sfop->sfop_fn, slstrerror(errno)));

		if (vfssb1.f_fsid != vfssb2.f_fsid)
			return (psc_ctlsenderr(fd, mh, "%s: %s",
			    sfop->sfop_fn, slstrerror(EXDEV)));
	}

	memset(&a, 0, sizeof(a));
	a.mh = mh;
	a.fd = fd;
	a.sfop = sfop;
	a.rc = 1;
	if (sfop->sfop_flags & SLI_CTL_FOPF_RECURSIVE)
		fl |= PFL_FILEWALKF_RECURSIVE;

	if (realpath(sfop->sfop_fn, buf) == NULL)
		return (psc_ctlsenderr(fd, mh, "%s: %s",
		    sfop->sfop_fn, slstrerror(errno)));
	strlcpy(sfop->sfop_fn, buf, sizeof(sfop->sfop_fn));
	pfl_filewalk(sfop->sfop_fn, fl, sli_import, &a);
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
		if (w->srw_op != SLI_REPLWKOP_REPL)
			continue;

		srws->srws_fg = w->srw_fg;
		srws->srws_bmapno = w->srw_bmapno;
		srws->srws_data_tot = SLASH_SLVR_SIZE * w->srw_nslvr_tot;
		srws->srws_data_cur = SLASH_SLVR_SIZE * w->srw_nslvr_cur;
		strlcpy(srws->srws_peer_addr, w->srw_src_res->res_name,
		    sizeof(srws->srws_peer_addr));

		rc = psc_ctlmsg_sendv(fd, mh, srws);
		if (!rc)
			break;
	}
	PLL_ULOCK(&sli_replwkq_active);
	return (rc);
}

/**
 * slictlcmd_stop - Handle a STOP command to terminate execution.
 */
__dead int
slictlcmd_stop(__unusedx int fd, __unusedx struct psc_ctlmsghdr *mh,
    __unusedx void *m)
{
	exit(0);
}

void
sliriithr_get(struct psc_thread *thr, struct psc_ctlmsg_thread *pcst)
{
	pcst->pcst_nread = sliriithr(thr)->sirit_st_nread;
}

struct psc_ctlop slictlops[] = {
	PSC_CTLDEFOPS,
	{ slictlrep_getreplwkst,	sizeof(struct slictlmsg_replwkst ) },
	{ slctlrep_getconn,		sizeof(struct slctlmsg_conn ) },
	{ slctlrep_getfcmh,		sizeof(struct slctlmsg_fcmh ) },
	{ slictlcmd_export,		sizeof(struct slictlmsg_fileop) },
	{ slictlcmd_import,		sizeof(struct slictlmsg_fileop) },
	{ slictlcmd_stop,		0 },
	{ slctlrep_getbmap,		sizeof(struct slctlmsg_bmap) }
};

psc_ctl_thrget_t psc_ctl_thrgets[] = {
/* ASYNC_IO	*/ NULL,
/* BMAPRLS	*/ NULL,
/* CONN		*/ NULL,
/* CTL		*/ psc_ctlthr_get,
/* CTLAC	*/ psc_ctlacthr_get,
/* LNETAC	*/ NULL,
/* NBRQ		*/ NULL,
/* REPLPND	*/ NULL,
/* RIC		*/ NULL,
/* RII		*/ sliriithr_get,
/* RIM		*/ NULL,
/* SLVR_CRC	*/ NULL,
/* STATFS	*/ NULL,
/* TIOS		*/ NULL,
/* USKLNDPL	*/ NULL
};

/* names must match enum list in file sliod.h */
struct pfl_opstat pflctl_opstats[] = {
	PFL_OPSTAT_INIT("crc_update"),
	PFL_OPSTAT_INIT("crc_update_backlog"),
	PFL_OPSTAT_INIT("crc_update_cb"),
	PFL_OPSTAT_INIT("crc_update_cb_failure"),
	PFL_OPSTAT_INIT("debug"),
	PFL_OPSTAT_INIT("fsio_read"),
	PFL_OPSTAT_INIT("fsio_read_fail"),
	PFL_OPSTAT_INIT("fsio_write"),
	PFL_OPSTAT_INIT("fsio_write_fail"),
	PFL_OPSTAT_INIT("handle_io"),
	PFL_OPSTAT_INIT("handle_replread"),
	PFL_OPSTAT_INIT("handle_replread_aio"),
	PFL_OPSTAT_INIT("iocb_get"),
	PFL_OPSTAT_INIT("iocb_free"),
	PFL_OPSTAT_INIT("io_prep_rmw"),
	PFL_OPSTAT_INIT("issue_replread"),
	PFL_OPSTAT_INIT("issue_replread_cb"),
	PFL_OPSTAT_INIT("issue_replread_cb_aio"),
	PFL_OPSTAT_INIT("issue_replread_error"),
	PFL_OPSTAT_INIT("open"),
	PFL_OPSTAT_INIT("open_fail"),
	PFL_OPSTAT_INIT("open_succeed"),
	PFL_OPSTAT_INIT("reopen"),
	PFL_OPSTAT_INIT("release_bmap"),
	PFL_OPSTAT_INIT("reclaim"),
	PFL_OPSTAT_INIT("reclaim_file"),
	PFL_OPSTAT_INIT("reclaim_file_fail"),
	PFL_OPSTAT_INIT("repl_readaio"),
	PFL_OPSTAT_INIT("slvr_aio_reply"),
	PFL_OPSTAT_INIT("srmt_release")
};

PFLCTL_SVR_DEFS;

void
slictlthr_main(const char *fn)
{

	psc_assert(nitems(pflctl_opstats) == SLI_OPST_MAX);

	psc_ctlparam_register("faults", psc_ctlparam_faults);
	psc_ctlparam_register("log.file", psc_ctlparam_log_file);
	psc_ctlparam_register("log.format", psc_ctlparam_log_format);
	psc_ctlparam_register("log.level", psc_ctlparam_log_level);
	psc_ctlparam_register("opstats", psc_ctlparam_opstats);
	psc_ctlparam_register("pause", psc_ctlparam_pause);
	psc_ctlparam_register("pool", psc_ctlparam_pool);
	psc_ctlparam_register("rlim", psc_ctlparam_rlim);
	psc_ctlparam_register("run", psc_ctlparam_run);

	psc_ctlparam_register_simple("version", slctlparam_version_get,
	    NULL);

	psc_ctlthr_main(fn, slictlops, nitems(slictlops), SLITHRT_CTLAC);
}
