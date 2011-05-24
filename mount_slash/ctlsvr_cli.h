/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2011, Pittsburgh Supercomputing Center (PSC).
 *
 * Permission to use, copy, and modify this software and its documentation
 * without fee for personal use or non-commercial use within your organization
 * is hereby granted, provided that the above copyright notice is preserved in
 * all copies and that the copyright and this permission notice appear in
 * supporting documentation.  Permission to redistribute this software to other
 * organizations or individuals is not permitted without the written permission
 * of the Pittsburgh Supercomputing Center.  PSC makes no representations about
 * the suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

#ifndef _SL_CTLSVR_CLI_H_
#define _SL_CTLSVR_CLI_H_

#include "psc_ds/list.h"
#include "psc_ds/lockedlist.h"
#include "psc_util/lock.h"
#include "psc_util/waitq.h"

#include "mount_slash/ctl_cli.h"

struct psc_ctlmsghdr;

struct msctl_replstq {
	struct psclist_head		 mrsq_lentry;
	struct psc_waitq		 mrsq_waitq;
	int				 mrsq_id;
	int				 mrsq_fd;
	int				 mrsq_eof;	/* whether processing has finished */
	int				 mrsq_ctlrc;	/* EOF/OK return code to ctl layer */
	const struct psc_ctlmsghdr	*mrsq_mh;
	psc_spinlock_t			 mrsq_lock;
	slfid_t				 mrsq_fid;
	int				 mrsq_refcnt;
};

void mrsq_release(struct msctl_replstq *, int);

extern struct psc_lockedlist msctl_replsts;

#endif /* _SL_CTLSVR_CLI_H_ */
