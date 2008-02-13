/* $Id$ */

#include <sys/param.h>
#include <sys/mman.h>

#include <stdio.h>

#include "slconfig.h"
#include "sb.h"
#include "pathnames.h"

struct slash_sb_mem	 sbm;
int			 sbfd;

void
slash_superblock_init(void)
{
	char fn[PATH_MAX];
	int rc;

	rc = snprintf(fn, sizeof(fn), "%s/%s",
	    nodeInfo.node_res->res_fsroot, _PATH_SB);
	if (rc == -1)
		psc_fatal("snprintf");
	sbfd = open(fn, O_RDONLY);
	if (sbfd == -1)
		psc_fatal("%s", fn);
	sbm.sbm_sbs = mmap(NULL, sizeof(*sbm.sbm_sbs),
	    PROT_READ | PROT_WRITE, MAP_SHARED, sbfd, 0);
	if (sbm.sbm_sbs == MAP_FAILED)
		psc_fatal("mmap");
}
