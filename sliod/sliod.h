/* $Id$ */

#ifndef __SLIOD_H__
#define __SLIOD_H__

#include "psc_rpc/service.h"
#include "psc_util/thread.h"
#include "psc_util/waitq.h"

#define SLIOTHRT_CTL	0	/* control */
#define SLIOTHRT_LND	1	/* lustre networking helper */
#define SLIOTHRT_RPC	2	/* RPC comm */
#define SLIOTHRT_TINTV	3	/* timer interval */
#define SLIOTHRT_TIOS	4	/* iostats updater */

struct slio_ctlthr {
	int sc_st_nclients;
	int sc_st_nsent;
	int sc_st_nrecv;
};

struct slio_rpcthr {
	struct pscrpc_thread	 srt_prt;
};

#define slioctlthr(thr) ((struct slio_ctlthr *)(thr)->pscthr_private)
#define sliorpcthr(thr) ((struct slio_rpcthr *)(thr)->pscthr_private)

void slio_init(void);
void sliotimerthr_spawn(void);

extern struct psc_thread	slioControlThread;

#endif /* __SLIOD_H__ */
