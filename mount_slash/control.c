/* $Id$ */

/*
 * Control interface for querying and modifying
 * parameters of a running instance of mount_slash.
 */

#include "psc_util/thread.h"
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
	psc_ctlthr_main(_PATH_MSCTLSOCK, msctlops, NENTRIES(msctlops));
	return (NULL);
}
