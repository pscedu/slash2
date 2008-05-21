/* $Id$ */

/*
 * Control interface for querying and modifying
 * parameters of a running slashd instance.
 */

#include "psc_util/cdefs.h"
#include "psc_util/ctl.h"
#include "psc_util/ctlsvr.h"

#include "control.h"

struct psc_ctlop slctlops[] = {
	PSC_CTLDEFOPS
};

void (*psc_ctl_getstats[])(struct psc_thread *, struct psc_ctlmsg_stats *) = {
/* 0 */	psc_ctlthr_stat
};
int psc_ctl_ngetstats = NENTRIES(psc_ctl_getstats);

void
slctlthr_main(const char *fn)
{
	psc_ctlthr_main(fn, slctlops, NENTRIES(slctlops));
}
