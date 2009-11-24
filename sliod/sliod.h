/* $Id$ */

#ifndef _SLIOD_H_
#define _SLIOD_H_

#include "pfl/cdefs.h"
#include "psc_rpc/service.h"
#include "psc_util/thread.h"

#include "fid.h"
#include "sltypes.h"

/* sliod thread types */
#define SLITHRT_CTL		0	/* control */
#define SLITHRT_LNETAC		1	/* lustre net accept thr */
#define SLITHRT_USKLNDPL	2	/* userland socket lustre net dev poll thr */
#define SLITHRT_RIC		3	/* RPC comm */
#define SLITHRT_RIM		4	/* RPC comm */
#define SLITHRT_RII		5	/* RPC comm */
#define SLITHRT_TINTV		6	/* timer interval */
#define SLITHRT_TIOS		7	/* iostats updater */
#define SLITHRT_SLVR_CRC	8	/* sliver crc updaters */
#define SLITHRT_REPLFIN		9	/* process finished replication requests */
#define SLITHRT_REPLINF		10	/* monitor in-flight replication work */
#define SLITHRT_REPLPND		11	/* process enqueued replication requests */

#define NSLVRCRC_THRS		4	/* perhaps ncores? */

struct bmapc_memb;
struct fidc_membh;
struct srt_bmapdesc_buf;

struct sliric_thread {
	struct pscrpc_thread	 sirct_prt;
};

struct slirim_thread {
	struct pscrpc_thread	 sirmt_prt;
};

struct slirii_thread {
	struct pscrpc_thread	 sirit_prt;
};

PSCTHR_MKCAST(sliricthr, sliric_thread, SLITHRT_RIC)
PSCTHR_MKCAST(slirimthr, slirim_thread, SLITHRT_RIM)
PSCTHR_MKCAST(sliriithr, slirii_thread, SLITHRT_RII)

struct iod_resm_info {
	struct slashrpc_cservice *iri_csvc;
	psc_spinlock_t		  iri_lock;
	struct psc_waitq	  iri_waitq;
};

void	slitimerthr_spawn(void);
__dead void slictlthr_main(const char *);

struct fidc_membh *
	iod_inode_lookup(const struct slash_fidgen *);
int	iod_inode_open(struct fidc_membh *, int);

int	iod_bmap_load(struct fidc_membh *, sl_bmapno_t, int, struct bmapc_memb **);

int     iod_inode_getsize(struct slash_fidgen *, off_t *);

extern struct slashrpc_cservice	*rmi_csvc;
extern struct slash_creds	 rootcreds;

#endif /* _SLIOD_H_ */
