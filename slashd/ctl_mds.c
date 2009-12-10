/* $Id$ */

/*
 * Interface for controlling live operation of a slashd instance.
 */

#include "pfl/cdefs.h"
#include "psc_util/ctl.h"
#include "psc_util/ctlsvr.h"

#include "ctl_mds.h"

struct psc_lockedlist psc_mlists;

struct psc_ctlop slmctlops[] = {
	PSC_CTLDEFOPS
};

void (*psc_ctl_getstats[])(struct psc_thread *, struct psc_ctlmsg_stats *) = {
/* 0 */	psc_ctlthr_stat
};
int psc_ctl_ngetstats = nitems(psc_ctl_getstats);

int (*psc_ctl_cmds[])(int, struct psc_ctlmsghdr *, void *) = {
};
int psc_ctl_ncmds = nitems(psc_ctl_cmds);

void
slmctlthr_main(const char *fn)
{
	psc_ctlparam_register("log.file", psc_ctlparam_log_file);
	psc_ctlparam_register("log.format", psc_ctlparam_log_format);
	psc_ctlparam_register("log.level", psc_ctlparam_log_level);
	psc_ctlparam_register("pool", psc_ctlparam_pool);

	psc_ctlthr_main(fn, slmctlops, nitems(slmctlops));
}
