/* $Id$ */

#include <sys/types.h>

#include "cfd.h"

struct slashrpc_export {
	uid_t		uid;
	gid_t		gid;
	u64		cfd;
	struct cfdtree	cfdtree;
};
