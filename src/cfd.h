/* $Id$ */

#ifndef _CFD_H_
#define _CFD_H_

#include "psc_types.h"
#include "psc_ds/tree.h"

#include "fid.h"

struct pscrpc_export;

struct cfdent {
	slash_fid_t		fid;
	u64			cfd;
	SPLAY_ENTRY(cfdent)	entry;
};

SPLAY_HEAD(cfdtree, cfdent);

int cfdcmp(const void *, const void *);
int cfdnew(u64 *, struct pscrpc_export *, const char *);
int cfd2fid(slash_fid_t *, struct pscrpc_export *rq, u64 cfd);

SPLAY_PROTOTYPE(cfdtree, cfdent, entry, cfdcmp);

#endif /* _CFD_H_ */
