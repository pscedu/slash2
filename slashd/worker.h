/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2012-2013, Pittsburgh Supercomputing Center (PSC).
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
 *
 * Pittsburgh Supercomputing Center	phone: 412.268.4960  fax: 412.268.5832
 * 300 S. Craig Street			e-mail: remarks@psc.edu
 * Pittsburgh, PA 15213			web: http://www.psc.edu/
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

#ifndef _WORKER_H_
#define _WORKER_H_

#include "pfl/list.h"
#include "pfl/listcache.h"

struct pfl_workrq {
	int			(*wkrq_cbf)(void *);
	struct psc_listentry	  wkrq_lentry;
};

struct pfl_wk_thread {
	struct psc_listcache	 *wkt_workq;
};

#define pfl_wkthr(thr)			((struct pfl_wk_thread *)(thr)->pscthr_private)

#define pfl_workq_getitem(cb, type)	_pfl_workq_getitem((cb), sizeof(type))

#define	pfl_workq_lock()		LIST_CACHE_LOCK(&pfl_workq)
#define	pfl_workq_unlock()		LIST_CACHE_LOCK(&pfl_workq)
#define	pfl_workq_waitempty()		psc_waitq_wait(&pfl_workq.plc_wq_want,	\
					    &pfl_workq.plc_lock)

void   pfl_wkthr_main(struct psc_thread *);
void   pfl_wkthr_spawn(int, int, const char *);
void *_pfl_workq_getitem(int (*)(void *), size_t);
void   pfl_workq_init(size_t);
void  _pfl_workq_putitemq(struct psc_listcache *, void *, int);

#define _pfl_workq_putitem(p, tail)	_pfl_workq_putitemq(&pfl_workq, (p), (tail))
#define pfl_workq_putitem_head(p)	_pfl_workq_putitem((p), 0)
#define pfl_workq_putitem_tail(p)	_pfl_workq_putitem((p), 1)
#define pfl_workq_putitem(p)		_pfl_workq_putitem((p), 1)
#define pfl_workq_putitemq(lc, p)	_pfl_workq_putitemq((lc), (p), 1)
#define pfl_workq_putitemq_head(lc, p)	_pfl_workq_putitemq((lc), (p), 0)

extern struct psc_listcache	pfl_workq;

#endif /* _WORKER_H_ */
