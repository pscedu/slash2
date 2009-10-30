/* $Id$ */

/*
 * Control interface for querying and modifying
 * parameters of a running mount_slash instance.
 */

#include <sys/param.h>
#include <sys/socket.h>

#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "pfl/cdefs.h"
#include "psc_util/ctl.h"
#include "psc_util/ctlsvr.h"
#include "psc_util/strlcpy.h"

#include "ctl_cli.h"
#include "mount_slash.h"
#include "msl_fuse.h"
#include "slashrpc.h"
#include "slerr.h"

struct psc_lockedlist	 psc_mlists;

struct psc_poolmaster	 msctl_replstmc_poolmaster;
struct psc_poolmaster	 msctl_replstsc_poolmaster;
struct psc_poolmgr	*msctl_replstmc_pool;
struct psc_poolmgr	*msctl_replstsc_pool;

psc_atomic32_t		 msctl_replstid = PSC_ATOMIC32_INIT(0);
struct psc_lockedlist	 msctl_replsts = PLL_INITIALIZER(&msctl_replsts,
    struct msctl_replstq, mrsq_lentry);

#define REPLRQ_BMAPNO_ALL (-1)

int
msctl_getcreds(int fd, struct slash_creds *cr)
{
	struct ucred ucr;
	socklen_t len;

	len = sizeof(ucr);
	if (getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &ucr, &len))
		return (-errno);
	cr->uid = ucr.uid;
	cr->gid = ucr.gid;
	return (0);
}

int
msctlrep_replrq(int fd, struct psc_ctlmsghdr *mh, void *m)
{
	char fn[PATH_MAX], *cpn, *next;
	struct msctlmsg_replrq *mrq = m;
	struct srm_generic_rep *mp;
	struct srm_replrq_req *mq;
	struct pscrpc_request *rq;
	struct slash_fidgen fg;
	struct slash_creds cr;
	struct stat stb;
	fuse_ino_t pinum;
	uint32_t n;
	int rc;

	if (mrq->mrq_nios < 1 ||
	    mrq->mrq_nios >= nitems(mrq->mrq_iosv))
		return (psc_ctlsenderr(fd, mh,
		    "replication request: %s",
		    slstrerror(EINVAL)));

	rc = msctl_getcreds(fd, &cr);
	if (rc)
		return (psc_ctlsenderr(fd, mh, "unable to obtain credentials: %s",
		    mrq->mrq_fn, slstrerror(rc)));

	/* ensure path exists in the slash fs */
	rc = translate_pathname(mrq->mrq_fn, fn);
	if (rc)
		return (psc_ctlsenderr(fd, mh, "%s: %s",
		    mrq->mrq_fn, slstrerror(rc)));

	/* lookup FID/inum */
	pinum = 0; /* gcc */
	fg.fg_fid = SL_ROOT_INUM;
	for (cpn = fn + 1; cpn; cpn = next) {
		pinum = fg.fg_fid;
		if ((next = strchr(cpn, '/')) != NULL)
			*next++ = '\0';
		rc = slash_lookup_cache(&cr, pinum, cpn, &fg, &stb);
		if (rc)
			return (psc_ctlsenderr(fd, mh, "%s: %s",
			    mrq->mrq_fn, slstrerror(rc)));
	}

	if (!S_ISREG(stb.st_mode))
		return (psc_ctlsenderr(fd, mh,
		    "%s: %s", mrq->mrq_fn, slstrerror(ENOTSUP)));

	rc = checkcreds(&stb, &cr, W_OK);
	if (rc)
		return (psc_ctlsenderr(fd, mh,
		    "%s: %s", mrq->mrq_fn, slstrerror(rc)));

	rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    mh->mh_type == SCMT_ADDREPLRQ ?
	    SRMT_ADDREPLRQ : SRMT_DELREPLRQ, rq, mq, mp);
	if (rc)
		return (psc_ctlsenderr(fd, mh,
		    "%s: %s", mrq->mrq_fn, slstrerror(rc)));

	/* parse I/O systems specified */
	for (n = 0; n < mrq->mrq_nios; n++, mq->nrepls++)
		if ((mq->repls[n].bs_id =
		    libsl_str2id(mrq->mrq_iosv[n])) == IOS_ID_ANY) {
			pscrpc_req_finished(rq);
			return (psc_ctlsenderr(fd, mh,
			    "%s: unknown I/O system", mrq->mrq_iosv[n]));
		}
	memcpy(&mq->fg, &fg, sizeof(mq->fg));
	mq->bmapno = mrq->mrq_bmapno;

	rc = RSX_WAITREP(rq, mp);
	pscrpc_req_finished(rq);
	if (rc)
		return (psc_ctlsenderr(fd, mh,
		    "%s: %s", mrq->mrq_fn, slstrerror(rc)));
	if (mp->rc)
		return (psc_ctlsenderr(fd, mh,
		    "%s: %s", mrq->mrq_fn, slstrerror(mp->rc)));
	return (1);
}

int
msctlrep_getreplst(int fd, struct psc_ctlmsghdr *mh, void *m)
{
	char fn[PATH_MAX], *cpn, *next;
	struct msctl_replst_slave_cont *mrsc;
	struct srm_replst_master_req *mq;
	struct srm_replst_master_rep *mp;
	struct msctlmsg_replrq *mrq = m;
	struct msctl_replst_cont *mrc;
	struct msctl_replstq *mrsq;
	struct pscrpc_request *rq;
	struct slash_fidgen fg;
	struct slash_creds cr;
	struct stat stb;
	fuse_ino_t pinum;
	int rv, rc;

	if (strcmp(mrq->mrq_fn, "") == 0) {
		strlcpy(mrq->mrq_fn, "active replications", sizeof(mrq->mrq_fn));
		fg.fg_fid = REPLRQ_FID_ALL;
		goto issue;
	}

	rc = msctl_getcreds(fd, &cr);
	if (rc)
		return (psc_ctlsenderr(fd, mh, "unable to obtain credentials: %s",
		    mrq->mrq_fn, slstrerror(rc)));

	rc = translate_pathname(mrq->mrq_fn, fn);
	if (rc)
		return (psc_ctlsenderr(fd, mh, "%s: %s",
		    mrq->mrq_fn, slstrerror(rc)));

	pinum = 0; /* gcc */
	fg.fg_fid = SL_ROOT_INUM;
	for (cpn = fn + 1; cpn; cpn = next) {
		pinum = fg.fg_fid;
		if ((next = strchr(cpn, '/')) != NULL)
			*next++ = '\0';
		rc = slash_lookup_cache(&cr, pinum, cpn, &fg, &stb);
		if (rc)
			return (psc_ctlsenderr(fd, mh,
			    "%s: %s", mrq->mrq_fn, slstrerror(rc)));
	}

	if (!S_ISREG(stb.st_mode))
		return (psc_ctlsenderr(fd, mh,
		    "%s: %s", mrq->mrq_fn, slstrerror(ENOTSUP)));

	rc = -checkcreds(&stb, &cr, W_OK);
	if (rc)
		return (psc_ctlsenderr(fd, mh,
		    "%s: %s", mrq->mrq_fn, slstrerror(rc)));

 issue:
	rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_GETREPLST, rq, mq, mp);
	if (rc)
		return (psc_ctlsenderr(fd, mh, "%s: %s",
		    mrq->mrq_fn, slstrerror(rc)));

	mq->inum = fg.fg_fid;
	mq->id = psc_atomic32_inc_return(&msctl_replstid);

	rv = 1;
	mrsq = PSCALLOC(sizeof(*mrsq));
	mrsq->mrsq_id = mq->id;
	lc_init(&mrsq->mrsq_lc, struct msctl_replst_cont, mrc_lentry);
	pll_add(&msctl_replsts, mrsq);

	rc = RSX_WAITREP(rq, mp);
	pscrpc_req_finished(rq);
	if (rc) {
		rv = psc_ctlsenderr(fd, mh, "%s: %s",
		    mrq->mrq_fn, slstrerror(rc));
		goto out;
	}
	if (mp->rc) {
		rv = psc_ctlsenderr(fd, mh, "%s: %s",
		    mrq->mrq_fn, slstrerror(mp->rc));
		goto out;
	}

	while ((mrc = lc_getwait(&mrsq->mrsq_lc)) != NULL) {
		/* XXX fill in mrs_fn */
		rv = psc_ctlmsg_sendv(fd, mh, &mrc->mrc_mrs);
		while ((mrsc = lc_getwait(&mrc->mrc_bdata)) != NULL) {
			rv = psc_ctlmsg_sendv(fd, mh, &mrsc->mrsc_mrsl);
			psc_pool_return(msctl_replstsc_pool, mrsc);
			if (!rv)
				break;
		}
		while ((mrsc = lc_getnb(&mrc->mrc_bdata)) != NULL)
			psc_pool_return(msctl_replstsc_pool, mrsc);
		lc_unregister(&mrc->mrc_bdata);
		psc_pool_return(msctl_replstmc_pool, mrc);
		if (!rv)
			break;
	}

 out:
	pll_remove(&msctl_replsts, mrsq);
	while ((mrc = lc_getnb(&mrsq->mrsq_lc)) != NULL) {
		while ((mrsc = lc_getwait(&mrc->mrc_bdata)) != NULL)
			psc_pool_return(msctl_replstsc_pool, mrsc);
		lc_unregister(&mrc->mrc_bdata);
		psc_pool_return(msctl_replstmc_pool, mrc);
	}
	free(mrsq);
	return (rv);
}

struct psc_ctlop msctlops[] = {
	PSC_CTLDEFOPS,
	{ msctlrep_replrq,	sizeof(struct msctlmsg_replrq) },
	{ msctlrep_replrq,	sizeof(struct msctlmsg_replrq) },
	{ msctlrep_getreplst,	sizeof(struct msctlmsg_replst) }
};

void (*psc_ctl_getstats[])(struct psc_thread *, struct psc_ctlmsg_stats *) = {
	psc_ctlthr_stat
};
int psc_ctl_ngetstats = nitems(psc_ctl_getstats);

int (*psc_ctl_cmds[])(int, struct psc_ctlmsghdr *, void *) = {
};
int psc_ctl_ncmds = nitems(psc_ctl_cmds);

void *
msctlthr_begin(__unusedx void *arg)
{
	psc_ctlthr_main(ctlsockfn, msctlops, nitems(msctlops));
}

void
msctlthr_spawn(void)
{
	struct psc_thread *thr;

	psc_poolmaster_init(&msctl_replstmc_poolmaster,
	    struct msctl_replst_cont, mrc_lentry, 0,
	    0, 32, 0, NULL, NULL, NULL, "replstmc");
	msctl_replstmc_pool = psc_poolmaster_getmgr(
	    &msctl_replstmc_poolmaster);

	_psc_poolmaster_init(&msctl_replstsc_poolmaster,
	    sizeof(struct msctl_replst_slave_cont) + SRM_REPLST_PAGESIZ,
	    offsetof(struct msctl_replst_slave_cont, mrsc_lentry),
	    PPMF_AUTO, 32, 32, 64, NULL, NULL, NULL, NULL, "replstsc");
	msctl_replstsc_pool = psc_poolmaster_getmgr(
	    &msctl_replstsc_poolmaster);

	psc_ctlparam_register("log.file", psc_ctlparam_log_file);
	psc_ctlparam_register("log.format", psc_ctlparam_log_format);
	psc_ctlparam_register("log.level", psc_ctlparam_log_level);
	psc_ctlparam_register("pool", psc_ctlparam_pool);

	thr = pscthr_init(MSTHRT_CTL, 0, msctlthr_begin, NULL,
	    sizeof(struct psc_ctlthr), "msctlthr");
	pscthr_setready(thr);
}
