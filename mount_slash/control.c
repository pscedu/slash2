/* $Id$ */

/*
 * Control interface for querying and modifying
 * parameters of a running instance of mount_slash.
 */

#include "psc_util/cdefs.h"
#include "psc_util/ctl.h"
#include "psc_util/ctlsvr.h"

#include "control.h"

struct psc_ctlop msctlops[] = {
	PSC_CTLDEFOPS
};

void *
msctlthr_begin(__unusedx void *arg)
{
	const char *fn;

	if ((fn = getenv("CTLSOCK")) == NULL)
		fn = _PATH_MSCTLSOCK

	psc_ctlthr_main(fn, msctlops, NENTRIES(msctlops));
	return (NULL);
}
