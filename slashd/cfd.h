/* $Id$ */

#ifndef _CFD_H_
#define _CFD_H_

#include "psc_types.h"
#include "psc_ds/tree.h"

#include "fid.h"
#include "slashrpc.h"

struct pscrpc_export;
struct cfdops;

struct cfdent {
	struct srt_fd_buf	fdb;
	void                   *pri;
	int                     type;
	struct cfdops          *cfdops;
	SPLAY_ENTRY(cfdent)	entry;
};

#define CFD_FILE        01
#define CFD_DIR         02
#define CFD_CLOSING     04
#define CFD_FORCE_CLOSE 010

/*
 * Server specific cfd ops.  Primarily used to operate on the cfdent's
 *  'pri' structure.  All calls must be made with the exp lock held.
 */
struct cfdops {
	int (*cfd_init)(struct cfdent *, struct pscrpc_export *);
	int (*cfd_free)(struct cfdent *, struct pscrpc_export *);
	int (*cfd_insert)(struct cfdent *, struct pscrpc_export *);
	void *(*cfd_get_pri)(struct cfdent *, struct pscrpc_export *);
};

struct cfdent * cfdget(struct pscrpc_export *, u64);
int	cfdcmp(const void *, const void *);
int     cfdnew(slfid_t, struct pscrpc_export *, void *,
	       struct cfdent **, struct cfdops *);
int	cfdfree(struct pscrpc_export *, u64);
void    cfdfreeall(struct pscrpc_export *);
int	cfdlookup(struct pscrpc_export *, u64, void *);

SPLAY_HEAD(cfdtree, cfdent);
SPLAY_PROTOTYPE(cfdtree, cfdent, entry, cfdcmp);

#if 0
extern struct cfd_svrops *cfdOps;
#define CFD_GEN_SVROP(OP)						\
	static __inline int						\
	cfd_svrop_##OP(struct cfdent *cfd, struct pscrpc_export *exp)	\
	{								\
		LOCK_ENSURE(&exp->exp_lock);				\
		if (cfdOps && cfdOps->cfd_##OP)				\
			return (*cfdOps->cfd_##OP)(cfd, exp);		\
		return (-ENOTSUP);					\
	}

CFD_GEN_SVROP(init)
CFD_GEN_SVROP(free)
CFD_GEN_SVROP(insert)
#endif

#endif /* _CFD_H_ */
