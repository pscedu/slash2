/* $Id$ */

#ifndef _FIDC_CLIENT_H_
#define _FIDC_CLIENT_H_

#include "psc_ds/list.h"
#include "psc_util/lock.h"

#include "fid.h"

struct fidc_membh;

struct fidc_private {
	struct timespec		 fcc_age;
	int			 fcc_hash;
	char			 fcc_name[];
};

struct fidc_membh *fidc_child_lookup(struct fidc_membh *, const char *);

void	fidc_child_add(struct fidc_membh *, struct fidc_membh *, const char *);
int	fidc_child_reap_cb(struct fidc_membh *);
void	fidc_child_rename(struct fidc_membh *, const char *, struct fidc_membh *, const char *);
void	fidc_child_unlink(struct fidc_membh *, const char *);

#endif /* _FIDC_CLIENT_H_ */
