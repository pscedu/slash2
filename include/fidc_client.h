#ifndef __FIDC_CLIENT_H__
#define __FIDC_CLIENT_H__ 1

#include "fid.h"

struct fidc_membh;
struct psclist_head;

struct fidc_child {
	struct fidc_membh  *fcc_fcmh;
	struct fidc_membh  *fcc_parent;
	char               *fcc_name;
	ssize_t             fcc_len;
	atomic_t            fcc_ref;
	struct psclist_head fcc_lentry;
};

extern void 
fidc_child_free(struct fidc_child *);

extern struct fidc_child *
fidc_child_add(struct fidc_membh *, struct fidc_membh *, 
	       const char *);

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
