/* $Id$ */

#include <sys/types.h>

struct cfdent {
	slash_fid_t		fid;
	u64			cfd;
	SPLAY_ENTRY(cfdent)	entry;
};

int
cfdcmp(const void *a, const void *b)
{
	struct cfdent *ca = a;
	struct cfdent *cb = b;

	if (ca->cfd < cb->cfd)
		return (-1);
	else if (ca->cfd > cb->cfd)
		return (1);
	return (0);
}

SPLAY_HEAD(cfdtree, cfdent);
SPLAY_PROTOTYPE(cfd, cfdcmp);

struct slashrpc_export {
	uid_t		uid;
	gid_t		gid;
	u64		cfd;
	struct cfdtree	cfdtree;
};
