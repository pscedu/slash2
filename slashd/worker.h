/* $Id$ */
/* %PSC_COPYRIGHT% */

#ifndef _WORKER_H_
#define _WORKER_H_

#include "psc_ds/list.h"
#include "psc_ds/listcache.h"

struct pfl_workrq {
	int			(*wkrq_cbf)(void *);
	struct psc_listentry	  wkrq_lentry;
};

#define pfl_workq_getitem(cb, type)	_pfl_workq_getitem((cb), sizeof(type))

#define	pfl_workq_lock()		LIST_CACHE_LOCK(&pfl_workq)
#define	pfl_workq_unlock()		LIST_CACHE_LOCK(&pfl_workq)
#define	pfl_workq_waitempty()		psc_waitq_wait(&pfl_workq.plc_wq_want,	\
					    &pfl_workq.plc_lock)

void   pfl_wkthr_spawn(int, int, const char *);
void *_pfl_workq_getitem(int (*)(void *), size_t);
void   pfl_workq_init(size_t);
void   pfl_workq_putitem(void *);

extern struct psc_listcache	pfl_workq;

#endif /* _WORKER_H_ */
