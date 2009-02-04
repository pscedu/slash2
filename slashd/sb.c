/* $Id$ */

#include <sys/param.h>
#include <sys/mman.h>

#include <stdio.h>

#include "slconfig.h"
#include "sb.h"
#include "pathnames.h"

struct slash_sb_mem	 slSuperBlk;
int			 slSuperFd;

void
slash_superblock_init(void)
{
	char fn[PATH_MAX];
	int rc;

	LOCK_INIT(&slSuperBlk.sbm_lock);

	rc = snprintf(fn, sizeof(fn), "%s/%s",
	    nodeInfo.node_res->res_fsroot, _PATH_SB);
	if (rc == -1)
		psc_fatal("snprintf");
	slSuperFd = open(fn, O_RDWR);
	if (slSuperFd == -1)
		psc_fatal("%s", fn);
	slSuperBlk.sbm_sbs = mmap(NULL, sizeof(*slSuperBlk.sbm_sbs),
	    PROT_READ | PROT_WRITE, MAP_SHARED, slSuperFd, 0);
	if (slSuperBlk.sbm_sbs == MAP_FAILED)
		psc_fatal("mmap");
}
