/* $Id$ */

/*
 * Control interface for querying and modifying
 * parameters of a running mount_slash instance.
 */

#include "psc_util/cdefs.h"
#include "psc_util/ctl.h"
#include "psc_util/ctlsvr.h"

#include "control.h"

struct psc_ctlop msctlops[] = {
	PSC_CTLDEFOPS
};

void (*psc_ctl_getstats[])(struct psc_thread *, struct psc_ctlmsg_stats *) = {
	psc_ctlthr_stat
};
int psc_ctl_ngetstats = NENTRIES(psc_ctl_getstats);

void *
msctlthr_begin(__unusedx void *arg)
{
	const char *fn;

	if ((fn = getenv("CTLSOCK")) == NULL)
		fn = _PATH_MSCTLSOCK;

	psc_ctlthr_main(fn, msctlops, NENTRIES(msctlops));
	return (NULL);
}
