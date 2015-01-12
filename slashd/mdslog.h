/* $Id$ */
/* %PSC_COPYRIGHT_GPL% */

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

extern uint64_t			 slm_reclaim_proc_batchno;

#endif /* _MDSLOG_H_ */
