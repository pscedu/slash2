/* $Id$ */

#ifndef _SL_CFD_H_
#define _SL_CFD_H_

#include "psc_ds/tree.h"

#include "fid.h"
#include "slashrpc.h"

struct pscrpc_export;
struct cfdops;

/* client file descriptor entry */
struct cfdent {
	struct srt_fd_buf	 cfd_fdb;
	void			*cfd_pri;
	int			 cfd_flags;
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
	int	 (*cfd_init)(struct cfdent *, void *, struct pscrpc_export *);
	int	 (*cfd_free)(struct cfdent *, struct pscrpc_export *);
	void	*(*cfd_get_pri)(struct cfdent *, struct pscrpc_export *);
};

struct cfdent *
	cfdget(struct pscrpc_export *, uint64_t);
int	cfdcmp(const void *, const void *);
int	cfdnew(slfid_t, struct pscrpc_export *, enum slconn_type,
	    void *, struct cfdent **, int);
int	cfdfree(struct pscrpc_export *, uint64_t);
void	cfdfreeall(struct pscrpc_export *, enum slconn_type);
int	cfdlookup(struct pscrpc_export *, uint64_t, void *);

SPLAY_HEAD(cfdtree, cfdent);
SPLAY_PROTOTYPE(cfdtree, cfdent, cfd_entry, cfdcmp);

extern struct cfdops		cfd_ops;

#endif /* _SL_CFD_H_ */
