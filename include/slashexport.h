/* $Id$ */

#ifndef _SL_EXP_H_
#define _SL_EXP_H_

#include <sys/types.h>

#include "cfd.h"

struct cfdtree;
struct pscrpc_export;

/* Format here is SERVER_CLIENT */
enum slash_exp_types {
	MDS_ION_EXP = (1<<0),
	MDS_CLI_EXP = (1<<1),
	MDS_MDS_EXP = (1<<2),
	ION_CLI_EXP = (1<<3),
	ION_MDS_EXP = (1<<4),
	ION_ION_EXP = (1<<5),
	CLI_MDS_EXP = (1<<6),
	EXP_CLOSING = (1<<7)
};

struct slashrpc_export {
	uint64_t			 slexp_conn_gen;
	uint64_t			 slexp_nextcfd;
	struct cfdtree			 slexp_cfdtree;
	int				 slexp_type;
	void				*slexp_data;
	struct pscrpc_export		*slexp_export;
	SPLAY_ENTRY(slashrpc_export)	 slexp_entry;
};

SPLAY_HEAD(slexptree, slashrpc_export);

struct slashrpc_export *
	slashrpc_export_get(struct pscrpc_export *);

int	slexpcmp(const void *, const void *);

SPLAY_PROTOTYPE(slexptree, slashrpc_export, slexp_entry, slexpcmp);

extern struct slexptree	slexptree;
extern psc_spinlock_t	slexptreelock;

#endif /* _SL_EXP_H_ */
