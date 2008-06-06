/* $Id$ */

#ifndef __SLIOD_H__
#define __SLIOD_H__

#include "psc_rpc/service.h"
#include "psc_util/thread.h"

#define SLIOTHRT_CTL	0	/* control */
#define SLIOTHRT_LND	1	/* lustre networking helper */
#define SLIOTHRT_RIC	2	/* RPC comm */
#define SLIOTHRT_RIM	3	/* RPC comm */
#define SLIOTHRT_RII	4	/* RPC comm */
#define SLIOTHRT_TINTV	5	/* timer interval */
#define SLIOTHRT_TIOS	6	/* iostats updater */

struct slio_ctlthr {
	int sc_st_nclients;
	int sc_st_nsent;
	int sc_st_nrecv;
};

struct slash_ricthr {
	struct pscrpc_thread	 srt_prt;
};

struct slash_rimthr {
	struct pscrpc_thread	 srt_prt;
};

struct slash_riithr {
	struct pscrpc_thread	 srt_prt;
};

#define slioctlthr(thr)	((struct slio_ctlthr *)(thr)->pscthr_private)
#define slricthr(thr)	((struct slash_ricthr *)(thr)->pscthr_private)
#define slrimthr(thr)	((struct slash_rimthr *)(thr)->pscthr_private)
#define slriithr(thr)	((struct slash_riithr *)(thr)->pscthr_private)

void slric_init(void);
void slrim_init(void);
void slrii_init(void);
void sliotimerthr_spawn(void);
void slioctlthr_main(const char *);

#endif /* __SLIOD_H__ */
