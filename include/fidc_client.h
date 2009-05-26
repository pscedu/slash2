#ifndef __FIDC_CLIENT_H__
#define __FIDC_CLIENT_H__ 1

#include "fid.h"

struct fidc_membh;
struct psclist_head;

struct fidc_child {
	struct fidc_membh  *fcc_parent;
	struct fidc_membh  *fcc_fcmh;
	struct psclist_head fcc_lentry;
	double              fcc_age;
	atomic_t            fcc_ref;
	int                 fcc_hash;
	char                fcc_name[];
};

extern void 
fidc_child_free(struct fidc_child *);

extern void
fidc_child_add(struct fidc_membh *, struct fidc_membh *, 
	       const char *);

extern void
fidc_child_unlink(struct fidc_membh *p, const char *name);

extern struct fidc_child *
fidc_child_get(struct fidc_membh *, const char *, size_t);

extern int
fidc_child_rename(slfid_t, const char *, slfid_t, const char *);

extern int
fidc_child_reap_cb(struct fidc_membh *f);

extern int
fidc_child_wait_locked(struct fidc_membh *p, struct fidc_child *fcc);


extern void
fidc_child_fail(struct fidc_child *fcc);

extern void
fidc_child_add_fcmh(struct fidc_child *fcc, struct fidc_membh *c);
#endif
