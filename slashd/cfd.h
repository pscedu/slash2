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

struct cfdent *
	cfdinsert(u64, struct pscrpc_export *, const slash_fid_t *);
int	cfdcmp(const void *, const void *);
void	cfdnew(u64 *, struct pscrpc_export *, const slash_fid_t *);
int	cfd2fid(slash_fid_t *, struct pscrpc_export *rq, u64 cfd);
int	cfdfree(struct pscrpc_export *rq, u64 cfd);

SPLAY_PROTOTYPE(cfdtree, cfdent, entry, cfdcmp);

#endif /* _CFD_H_ */
