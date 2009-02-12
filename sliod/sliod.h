/* $Id$ */

#ifndef _SLIOD_H_
#define _SLIOD_H_

#include "psc_rpc/service.h"
#include "psc_util/thread.h"

#define SLIOTHRT_CTL		0	/* control */
#define SLIOTHRT_LNETAC		1	/* lustre net accept thr */
#define SLIOTHRT_USKLNDPL	1	/* userland socket lustre net dev poll thr */
#define SLIOTHRT_RIC		2	/* RPC comm */
#define SLIOTHRT_RIM		3	/* RPC comm */
#define SLIOTHRT_RII		4	/* RPC comm */
#define SLIOTHRT_TINTV		5	/* timer interval */
#define SLIOTHRT_TIOS		6	/* iostats updater */

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

PSCTHR_MKCAST(slioctlthr, slio_ctlthr, SLIOTHRT_CTL)
PSCTHR_MKCAST(slricthr, slash_ricthr, SLIOTHRT_RIC)
PSCTHR_MKCAST(slrimthr, slash_rimthr, SLIOTHRT_RIM)
PSCTHR_MKCAST(slriithr, slash_riithr, SLIOTHRT_RII)

void slric_init(void);
void slrim_init(void);
void slrii_init(void);
void sliotimerthr_spawn(void);
void slioctlthr_main(const char *);

#endif /* _SLIOD_H_ */
