/* $Id$ */

/*
 * Control interface for querying and modifying
 * parameters of a running instance of sliod.
 */

#include "psc_util/cdefs.h"
#include "psc_util/ctl.h"
#include "psc_util/ctlsvr.h"

#include "control.h"

struct psc_lockedlist psc_mlists;

struct psc_ctlop slioctlops[] = {
	PSC_CTLDEFOPS
};

void (*psc_ctl_getstats[])(struct psc_thread *, struct psc_ctlmsg_stats *) = {
/* 0 */	psc_ctlthr_stat
};
int psc_ctl_ngetstats = NENTRIES(psc_ctl_getstats);

int (*psc_ctl_cmds[])(int, struct psc_ctlmsghdr *, void *) = {
};
int psc_ctl_ncmds = NENTRIES(psc_ctl_cmds);

void
slioctlthr_main(const char *fn)
{
	psc_ctlparam_register("log.level", psc_ctlparam_log_level);
	psc_ctlparam_register("pool", psc_ctlparam_pool);
	psc_ctlthr_main(fn, slioctlops, NENTRIES(slioctlops));
}
