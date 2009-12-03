/* $Id$ */

#ifndef _CFD_H_
#define _CFD_H_

#include "psc_ds/tree.h"

#include "fid.h"
#include "slashrpc.h"

struct pscrpc_export;
struct cfdops;

/* client file descriptor entry */
struct cfdent {
	struct srt_fd_buf	 cfd_fdb;
	void			*cfd_pri;
	int			 cfd_type;
	struct cfdops		*cfd_ops;
	SPLAY_ENTRY(cfdent)	 cfd_entry;
};

#define CFD_FILE		(1 << 0)
#define CFD_DIR			(1 << 1)
#define CFD_CLOSING		(1 << 2)
#define CFD_FORCE_CLOSE		(1 << 3)

/*
 * Server specific cfd ops.  Primarily used to operate on the cfdent's
 *  'pri' structure.  All calls must be made with the exp lock held.
 */
struct cfdops {
	int	 (*cfd_init)(struct cfdent *, struct pscrpc_export *);
	int	 (*cfd_free)(struct cfdent *, struct pscrpc_export *);
	int	 (*cfd_insert)(struct cfdent *, struct pscrpc_export *);
	void	*(*cfd_get_pri)(struct cfdent *, struct pscrpc_export *);
};

struct cfdent *
	cfdget(struct pscrpc_export *, enum slconn_type, uint64_t);
int	cfdcmp(const void *, const void *);
int	cfdnew(slfid_t, struct pscrpc_export *, enum slconn_type,
	    void *, struct cfdent **, struct cfdops *, int);
int	cfdfree(struct pscrpc_export *, enum slconn_type, uint64_t);
void	cfdfreeall(struct pscrpc_export *, enum slconn_type);
int	cfdlookup(struct pscrpc_export *, enum slconn_type, uint64_t,
	    void *);

SPLAY_HEAD(cfdtree, cfdent);
SPLAY_PROTOTYPE(cfdtree, cfdent, cfd_entry, cfdcmp);

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
