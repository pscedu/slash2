/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2011, Pittsburgh Supercomputing Center (PSC).
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

/*
 * Interface for controlling live operation of a sliod instance.
 */

#include "pfl/cdefs.h"
#include "pfl/str.h"
#include "pfl/walk.h"
#include "psc_ds/lockedlist.h"
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

struct psc_lockedlist psc_mlists;
struct psc_lockedlist psc_odtables;

int
sli_export(__unusedx const char *fn, __unusedx const struct stat *stb,
    void *arg)
{
	struct slictlmsg_fileop *sfop = arg;
	int rc = 0;

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

int
sli_fcmh_lookup_fid(const struct slash_fidgen *pfg, const char *cpn,
    struct slash_fidgen *cfg)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_lookup_req *mq;
	struct srm_lookup_rep *mp;
	int rc = 0;

	rc = SL_RSX_NEWREQ(csvc, SRMT_LOOKUP, rq, mq, mp);
	if (rc)
		goto out;
	mq->pfg = *pfg;
	strlcpy(mq->name, cpn, sizeof(mq->name));
	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		goto out;

	*cfg = mp->attr.sst_fg;

 out:
	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
	return (rc);
}

struct sli_import_arg {
	struct psc_ctlmsghdr	*mh;
	struct slictlmsg_fileop	*sfop;
	int			 fd;
};

int
sli_import(const char *fn, const struct stat *stb, void *arg)
{
	char fidfn[PATH_MAX], cpn[SL_NAME_MAX + 1];
	struct sli_import_arg *a = arg;
	struct slictlmsg_fileop *sfop = a->sfop;
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct psc_ctlmsghdr *mh = a->mh;
	struct srm_import_req *mq;
	struct srm_import_rep *mp;
	struct slash_fidgen fg;
	char *p, *np;
	int rc;

	fg.fg_fid = FID_ANY;

	for (p = sfop->sfop_fn2; p; p = np) {
		np = strchr(p + 1, '/');
		if (np && np - p == 1)
			continue;
		if (np - p > SL_PATH_MAX) {
			rc = psc_ctlsenderr(a->fd, mh, "%s: %s",
			    fn, slstrerror(ENAMETOOLONG));
			goto out;
		}
		strlcpy(cpn, p, np - p);
		rc = sli_fcmh_lookup_fid(&fg, cpn, &fg);
		if (rc || fg.fg_fid == FID_ANY) {
			rc = psc_ctlsenderr(a->fd, mh, "%s: %s",
			    fn, slstrerror(rc));
			goto out;
		}
	}

	if (fg.fg_fid == FID_ANY) {
		rc = psc_ctlsenderr(a->fd, mh, "%s: %s",
		    fn, slstrerror(ENOENT));
		goto out;
	}

	rc = SL_RSX_NEWREQ(csvc, SRMT_IMPORT, rq, mq, mp);
	if (rc) {
		rc = psc_ctlsenderr(a->fd, mh, "%s: %s",
		    fn, slstrerror(rc));
		goto out;
	}
	mq->pfg = fg;
	strlcpy(mq->cpn, pfl_basename(fn), sizeof(mq->cpn));
	sl_externalize_stat(stb, &mq->sstb);
	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc == 0) {
		rc = mp->rc;
		if (rc == 0) {
			sli_fg_makepath(&mp->fg, fidfn);
			if (link(fn, fidfn) == -1)
				rc = errno;
		}
	}
	if (rc)
		rc = psc_ctlsenderr(a->fd, mh, "%s: %s",
		    fn, slstrerror(rc));

 out:
	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
	return (rc);
}

int
slictlcmd_import(int fd, struct psc_ctlmsghdr *mh, void *m)
{
	struct slictlmsg_fileop *sfop = m;
	struct sli_import_arg a;
	int fl = 0;

	a.mh = mh;
	a.fd = fd;
	a.sfop = sfop;
	if (sfop->sfop_flags & SLI_CTL_FOPF_RECURSIVE)
		fl |= PFL_FILEWALKF_RECURSIVE;
	return (pfl_filewalk(sfop->sfop_fn, fl, sli_import, &a));
}

int
slictlrep_getreplwkst(int fd, struct psc_ctlmsghdr *mh, void *m)
{
	struct slictlmsg_replwkst *srws = m;
	struct sli_repl_workrq *w;
	struct sl_resm *resm;
	int rc;

	rc = 1;
	PLL_LOCK(&sli_replwkq_active);
	PLL_FOREACH(w, &sli_replwkq_active) {
		if (w->srw_op != SLI_REPLWKOP_REPL)
			continue;

		resm = libsl_nid2resm(w->srw_nid);

		srws->srws_fg = w->srw_fg;
		srws->srws_bmapno = w->srw_bmapno;
		srws->srws_data_tot = SLASH_SLVR_SIZE * w->srw_nslvr_tot;
		srws->srws_data_cur = SLASH_SLVR_SIZE * w->srw_nslvr_cur;
		strlcpy(srws->srws_peer_addr, resm->resm_addrbuf,
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

struct psc_ctlop slictlops[] = {
	PSC_CTLDEFOPS,
	{ slictlrep_getreplwkst,	sizeof(struct slictlmsg_replwkst ) },
	{ slctlrep_getconns,		sizeof(struct slctlmsg_conn ) },
	{ slctlrep_getfcmhs,		sizeof(struct slctlmsg_fcmh ) },
	{ slictlcmd_export,		sizeof(struct slictlmsg_fileop) },
	{ slictlcmd_import,		sizeof(struct slictlmsg_fileop) },
	{ slictlcmd_stop,		0 }
};

psc_ctl_thrget_t psc_ctl_thrgets[] = {
/* BMAPRLS	*/ NULL,
/* CONN		*/ NULL,
/* CTL		*/ psc_ctlthr_get,
/* CTLAC	*/ psc_ctlacthr_get,
/* LNETAC	*/ NULL,
/* REPLFIN	*/ NULL,
/* REPLPND	*/ NULL,
/* REPLREAP	*/ NULL,
/* RIC		*/ NULL,
/* RII		*/ NULL,
/* RIM		*/ NULL,
/* SLVR_CRC	*/ NULL,
/* TIOS		*/ NULL,
/* USKLNDPL	*/ NULL
};

PFLCTL_SVR_DEFS;

void
slictlthr_main(const char *fn)
{
//	psc_ctlparam_register("faults", psc_ctlparam_faults);
	psc_ctlparam_register("log.file", psc_ctlparam_log_file);
	psc_ctlparam_register("log.format", psc_ctlparam_log_format);
	psc_ctlparam_register("log.level", psc_ctlparam_log_level);
	psc_ctlparam_register("pause", psc_ctlparam_pause);
	psc_ctlparam_register("pool", psc_ctlparam_pool);
	psc_ctlparam_register("rlim.nofile", psc_ctlparam_rlim_nofile);
	psc_ctlparam_register("run", psc_ctlparam_run);

	psc_ctlthr_main(fn, slictlops, nitems(slictlops), SLITHRT_CTLAC);
}
