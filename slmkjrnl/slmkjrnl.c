#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <sys/param.h>

#include <string.h>
#include <inttypes.h>

#include "pfl.h"

#include "psc_util/journal.h"
#include "psc_util/log.h"

#include "sljournal.h"
#include "pathnames.h"

int
main(void)
{
	pfl_init();
	pjournal_format(_PATH_SLJOURNAL, SLJ_MDS_JNENTS, SLJ_MDS_ENTSIZE, 
			SLJ_MDS_RA, 0);
	pjournal_dump(_PATH_SLJOURNAL);

	return (0);
}
