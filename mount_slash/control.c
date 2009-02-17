/* $Id$ */

/*
 * Control interface for querying and modifying
 * parameters of a running mount_slash instance.
 */

#include "psc_util/cdefs.h"
#include "psc_util/ctl.h"
#include "psc_util/ctlsvr.h"

#include "control.h"
#include "mount_slash.h"

struct psc_lockedlist psc_mlists;

struct psc_ctlop msctlops[] = {
	PSC_CTLDEFOPS
};

void (*psc_ctl_getstats[])(struct psc_thread *, struct psc_ctlmsg_stats *) = {
	psc_ctlthr_stat
};
int psc_ctl_ngetstats = NENTRIES(psc_ctl_getstats);

int (*psc_ctl_cmds[])(int, struct psc_ctlmsghdr *, void *) = {
};
int psc_ctl_ncmds = NENTRIES(psc_ctl_cmds);

void *
msctlthr_begin(__unusedx void *arg)
{
	psc_ctlthr_main(ctlsockfn, msctlops, NENTRIES(msctlops));
}

void
msctlthr_spawn(void)
{
	psc_ctlparam_register("log.level", psc_ctlparam_log_level);
	psc_ctlparam_register("pool", psc_ctlparam_pool);
	pscthr_init(&pscControlThread, MSTHRT_CTL, msctlthr_begin,
	    PSCALLOC(sizeof(struct psc_ctlthr)),
	    sizeof(struct psc_ctlthr), "msctlthr");
}
