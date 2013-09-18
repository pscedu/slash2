/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2013, Pittsburgh Supercomputing Center (PSC).
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

#include <sys/types.h>
#include <sys/statvfs.h>

/*
 * Interface for controlling live operation of a sliod instance.
 */

#include "pfl/cdefs.h"
#include "pfl/stat.h"
#include "pfl/str.h"
#include "pfl/walk.h"
#include "pfl/lockedlist.h"
#include "pfl/rsx.h"
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
sli_export(__unusedx const char *fn,
    __unusedx const struct pfl_stat *pst, __unusedx int info,
    __unusedx int level, void *arg)
{
	struct slictlmsg_fileop *sfop = arg;
	int rc = 0;

	psclog_info("export: src=%s dst=%s flags=%d",
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
sli_import(const char *fn, const struct pfl_stat *pst,
    __unusedx int info, __unusedx int level, void *arg)
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

	PFL_STAT_IMPORT(pst, &tstb);

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
	    S_ISDIR(pst->st_mode) ? "/" : "",
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
	/*
	 * No destination name specified; preserve last component from
	 * src.
	 */
	if (cpn[0] == '\0') {
		if (S_ISREG(pst->st_mode)) {
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

	if (S_ISDIR(pst->st_mode)) {
		rc = sli_rmi_issue_mkdir(csvc, &fg, cpn, &tstb, fidfn);
	} else if (S_ISBLK(pst->st_mode) || S_ISCHR(pst->st_mode) ||
	    S_ISFIFO(pst->st_mode) || S_ISSOCK(pst->st_mode)) {
		/* XXX: use mknod */
		rc = ENOTSUP;
	} else if (S_ISLNK(pst->st_mode)) {
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
		if (pst->st_nlink > 1)
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
	} else if (S_ISREG(pst->st_mode)) {
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
		if (pst->st_nlink > 1)
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
			a->rc = psc_ctlsenderr(a->fd, mh, "%s: %s", fn,
			    slstrerror(rc));
		} else if (tstb.st_ino == pst->st_ino) {
			rc = 0;
			if (sfop->sfop_flags & SLI_CTL_FOPF_VERBOSE)
				a->rc = psc_ctlsenderr(a->fd, mh,
				    "reimporting %s%s", fn,
				    S_ISDIR(pst->st_mode) ?
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
		    S_ISDIR(pst->st_mode) ? "/" : "");

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
		srws->srws_refcnt = psc_atomic32_read(&w->srw_refcnt);
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

int
slctlmsg_bmap_send(int fd, struct psc_ctlmsghdr *mh,
    struct slctlmsg_bmap *scb, struct bmap *b)
{
	scb->scb_fg = b->bcm_fcmh->fcmh_fg;
	scb->scb_bno = b->bcm_bmapno;
	scb->scb_opcnt = psc_atomic32_read(&b->bcm_opcnt);
	scb->scb_flags = b->bcm_flags;
	return (psc_ctlmsg_sendv(fd, mh, scb));
}

const struct slctl_res_field slctl_resmds_fields[] = { { NULL, NULL } };
const struct slctl_res_field slctl_resios_fields[] = { { NULL, NULL } };

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

void
slictlparam_reclaim_xid_get(char *val)
{
	snprintf(val, PCP_VALUE_MAX, "%"PRIu64, current_reclaim_xid);
}

void
slictlparam_reclaim_batchno_get(char *val)
{
	snprintf(val, PCP_VALUE_MAX, "%"PRIu64, current_reclaim_batchno);
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

PFLCTL_SVR_DEFS;

void
slictlthr_main(const char *fn)
{
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
	psc_ctlparam_register_simple("reclaim.xid",
	    slictlparam_reclaim_xid_get, NULL);
	psc_ctlparam_register_simple("reclaim.batchno",
	    slictlparam_reclaim_batchno_get, NULL);

	psc_ctlthr_main(fn, slictlops, nitems(slictlops), SLITHRT_CTLAC);
}
