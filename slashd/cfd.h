/* $Id$ */

#ifndef _CFD_H_
#define _CFD_H_

#include "psc_types.h"
#include "psc_ds/tree.h"

#include "fid.h"

struct pscrpc_export;

struct cfdent {
	slfid_t			fid;
	u64			cfd;
	void                   *pri;
	SPLAY_ENTRY(cfdent)	entry;				
};

SPLAY_HEAD(cfdtree, cfdent);

struct cfdent *
	cfdinsert(u64, struct pscrpc_export *, slfid_t);
int	cfdcmp(const void *, const void *);
int	cfdnew(u64 *, struct pscrpc_export *, slfid_t);
int	cfd2fid(struct pscrpc_export *, u64, slfid_t *);
int	cfdfree(struct pscrpc_export *, u64);

/* Server specific cfd ops.  Primarily used to operate on the cfdent's
 *  'pri' structure.  All calls must be made with the exp lock held.
 */
struct cfd_svrops {
	int (*cfd_new)(struct cfdent *c, struct pscrpc_export *);
	int (*cfd_free)(struct cfdent *c, struct pscrpc_export *);
	int (*cfd_insert)(struct cfdent *c, struct pscrpc_export *);
};

#define CFD_SVROP(cfd, exp, OP) {			\
		LOCK_ENSURE(&(exp)->exp_lock);		\
		if (cfdOps && cfdOps->cfd_##OP)		\
			(*cfdOps->cfd_##OP)(cfd, exp);	\
	}


SPLAY_PROTOTYPE(cfdtree, cfdent, entry, cfdcmp);

#endif /* _CFD_H_ */
