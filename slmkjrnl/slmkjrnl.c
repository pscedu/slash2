#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <sys/param.h>

#include <string.h>
#include <inttypes.h>

#include "pfl.h"

#include "psc_util/assert.h"
#include "psc_util/crc.h"
#include "psc_util/journal.h"
#include "psc_util/lock.h"
#include "psc_util/log.h"

#include "sljournal.h"
#include "pathnames.h"

struct psc_journal mkJournal;

int
main(void)
{
	int fd; 

	pfl_init();
	
	if ((fd = open(_PATH_SLJOURNAL, O_CREAT|O_TRUNC, 0700) < 0))
		psc_fatal("Could not create or truncate the journal %s", 
			  _PATH_SLJOURNAL);
	close(fd);
	
	pjournal_init(&mkJournal, _PATH_SLJOURNAL, 0, SLJ_MDS_JNENTS, 
		      SLJ_MDS_ENTSIZE, SLJ_MDS_RA);
	
	return (pjournal_format(&mkJournal));
}
