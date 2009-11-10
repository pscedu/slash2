/* $Id$ */

#ifndef _FIDC_CLIENT_H_
#define _FIDC_CLIENT_H_

#include "psc_ds/list.h"
#include "psc_util/lock.h"

#include "fid.h"

struct fidc_membh;

struct fidc_private {
	struct fidc_membh  *fcc_parent;
	struct fidc_membh  *fcc_fcmh;
	struct slash_fidgen fcc_fg;
	struct psclist_head fcc_lentry;
	psc_spinlock_t      fcc_lock;
	struct timespec		fcc_age;
	atomic_t            fcc_ref;
	int                 fcc_hash;
	char                fcc_name[];
};

struct fidc_membh *fidc_child_lookup(struct fidc_membh *, const char *);

void	fidc_child_add(struct fidc_membh *, struct fidc_membh *, const char *);
int	fidc_child_reap_cb(struct fidc_membh *);
void	fidc_child_rename(struct fidc_membh *, const char *, struct fidc_membh *, const char *);
void	fidc_child_unlink(struct fidc_membh *, const char *);

#endif /* _FIDC_CLIENT_H_ */
