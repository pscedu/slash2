/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015, Google, Inc.
 * Copyright (c) 2006-2013, Pittsburgh Supercomputing Center (PSC).
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or (at
 * your option) any later version.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License contained in the file
 * `COPYING-GPL' at the top of this distribution or at
 * https://www.gnu.org/licenses/gpl-2.0.html for more details.
 * ---------------------------------------------------------------------
 * %END_LICENSE%
 */

#ifndef _SL_CTLSVR_CLI_H_
#define _SL_CTLSVR_CLI_H_

#include "pfl/list.h"
#include "pfl/lockedlist.h"
#include "pfl/lock.h"
#include "pfl/waitq.h"

#include "mount_slash/ctl_cli.h"

struct psc_ctlmsghdr;

struct msctl_replstq {
	struct psclist_head		 mrsq_lentry;
	struct psc_waitq		 mrsq_waitq;
	int				 mrsq_id;
	int				 mrsq_fd;
	int				 mrsq_rc;
	const struct psc_ctlmsghdr	*mrsq_mh;
	psc_spinlock_t			 mrsq_lock;
	slfid_t				 mrsq_fid;
	int				 mrsq_refcnt;
};

void mrsq_release(struct msctl_replstq *, int);

extern struct psc_lockedlist msctl_replsts;

#endif /* _SL_CTLSVR_CLI_H_ */
