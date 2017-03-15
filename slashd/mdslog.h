/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015, Google, Inc.
 * Copyright 2009-2015, Pittsburgh Supercomputing Center
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

#ifndef _MDSLOG_H_
#define _MDSLOG_H_

struct slm_progress {
	uint64_t		 cur_batchno;
	uint64_t		 cur_xid;		/* journal xid of update */
	uint64_t		 sync_xid;		/* on disk */
	struct psc_waitq	 waitq;
	psc_spinlock_t		 lock;

	void			*prg_handle;
	void			*prg_buf;

	void			*log_handle;
	void			*log_buf;
	off_t			 log_offset;
};

extern struct slm_progress	 nsupd_prg;
extern struct slm_progress	 reclaim_prg;
extern struct psc_journal_cursor mds_cursor;

extern uint64_t			 slm_reclaim_proc_batchno;

#endif /* _MDSLOG_H_ */
