/* $Id$ */

/*
 * Control interface for querying and modifying
 * parameters of a running instance of sliod.
 */

#include "psc_util/cdefs.h"
#include "psc_util/ctl.h"
#include "psc_util/ctlsvr.h"

#include "control.h"

struct psc_ctlop slioctlops[] = {
	PSC_CTLDEFOPS
};

void
slioctlthr_main(const char *fn)
{
	psc_ctlthr_main(fn, slioctlops, NENTRIES(slioctlops));
}
