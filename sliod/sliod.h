/* $Id$ */

#ifndef _SLIOD_H_
#define _SLIOD_H_

#include "psc_rpc/service.h"
#include "psc_util/cdefs.h"
#include "psc_util/thread.h"

#define SLIOTHRT_CTL		0	/* control */
#define SLIOTHRT_LNETAC		1	/* lustre net accept thr */
#define SLIOTHRT_USKLNDPL	1	/* userland socket lustre net dev poll thr */
#define SLIOTHRT_RIC		2	/* RPC comm */
#define SLIOTHRT_RIM		3	/* RPC comm */
#define SLIOTHRT_RII		4	/* RPC comm */
#define SLIOTHRT_TINTV		5	/* timer interval */
#define SLIOTHRT_TIOS		6	/* iostats updater */
#define SLIOTHRT_SLVR_CRC       7       /* sliver crc updaters */

#define NSLVRCRC_THRS           4       /* perhaps ncores? */

extern struct slashrpc_cservice *rmi_csvc;

struct slash_ricthr {
	struct pscrpc_thread	 srt_prt;
};

struct slash_rimthr {
	struct pscrpc_thread	 srt_prt;
};

struct slash_riithr {
	struct pscrpc_thread	 srt_prt;
};

PSCTHR_MKCAST(slricthr, slash_ricthr, SLIOTHRT_RIC)
PSCTHR_MKCAST(slrimthr, slash_rimthr, SLIOTHRT_RIM)
PSCTHR_MKCAST(slriithr, slash_riithr, SLIOTHRT_RII)

void slric_init(void);
void slrim_init(void);
void slrii_init(void);
void sliotimerthr_spawn(void);
__dead void slioctlthr_main(const char *);

#endif /* _SLIOD_H_ */
