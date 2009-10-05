/* $Id$ */

/*
 * Control interface for querying and modifying
 * parameters of a running mount_slash instance.
 */

#include <sys/param.h>
#include <sys/socket.h>

#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_util/cdefs.h"
#include "psc_util/ctl.h"
#include "psc_util/ctlsvr.h"

#include "control.h"
#include "mount_slash.h"
#include "msl_fuse.h"
#include "slashrpc.h"

struct psc_lockedlist psc_mlists;

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
msctlrep_addreplrq(int fd, struct psc_ctlmsghdr *mh, void *m)
{
	char fn[PATH_MAX], *cpn, *next;
	struct msctlmsg_replrq *mrq = m;
	struct srm_addreplrq_req *mq;
	struct srm_generic_rep *mp;
	struct pscrpc_request *rq;
	struct slash_fidgen fg;
	struct slash_creds cr;
	struct stat stb;
	fuse_ino_t pino;
	int rc;

	rc = msctl_getcreds(fd, &cr);
	if (rc)
		goto out;

	rc = translate_pathname(mrq->mrq_fn, fn);
	if (rc)
		return (psc_ctlsenderr(fd, mh, "%s: %s", mrq->mrq_fn, strerror(rc)));

	pino = 1; /* root inum */
	for (cpn = mrq->mrq_fn; cpn; cpn = next) {
		if ((next = strchr(cpn, '/')) != NULL)
			*next++ = '\0';
		rc = slash_lookup_cache(&cr, pino, cpn, &fg, &stb);
		if (rc)
			return (psc_ctlsenderr(fd, mh,
			    "%s: %s", mrq->mrq_fn, strerror(rc)));
		pino = fg.fg_fid;
	}

	if (!S_ISREG(stb.st_mode))
		return (psc_ctlsenderr(fd, mh,
		    "%s: %s", mrq->mrq_fn, strerror(ENOTSUP)));

	rc = -checkcreds(&stb, &cr, W_OK);
	if (rc)
		return (psc_ctlsenderr(fd, mh,
		    "%s: %s", mrq->mrq_fn, strerror(rc)));

	rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_ADDREPLRQ, rq, mq, mp);
	if (rc)
		return (psc_ctlsenderr(fd, mh,
		    "%s: %s", mrq->mrq_fn, strerror(rc)));

	mq->ino = fg.fg_fid;
	mq->bmapno = mrq->mrq_bmapno;

	rc = RSX_WAITREP(rq, mp);
	if (rc)
		return (psc_ctlsenderr(fd, mh,
		    "%s: %s", mrq->mrq_fn, strerror(rc)));
	rc = 0;
 out:
	return (rc);
}

int
msctlrep_delreplrq(int fd, struct psc_ctlmsghdr *mh, void *m)
{
	return (ENOTSUP);
}

int
msctlrep_getreplst(int fd, struct psc_ctlmsghdr *mh, void *m)
{
	return (ENOTSUP);
}

struct psc_ctlop msctlops[] = {
	PSC_CTLDEFOPS,
	{ msctlrep_addreplrq,	sizeof(struct msctlmsg_replrq) },
	{ msctlrep_delreplrq,	sizeof(struct msctlmsg_replrq) },
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

	psc_ctlparam_register("log.file", psc_ctlparam_log_file);
	psc_ctlparam_register("log.format", psc_ctlparam_log_format);
	psc_ctlparam_register("log.level", psc_ctlparam_log_level);
	psc_ctlparam_register("pool", psc_ctlparam_pool);

	thr = pscthr_init(MSTHRT_CTL, 0, msctlthr_begin, NULL,
	    sizeof(struct psc_ctlthr), "msctlthr");
	pscthr_setready(thr);
}
