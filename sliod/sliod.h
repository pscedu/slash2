/* $Id$ */

#ifndef _SLIOD_H_
#define _SLIOD_H_

#include "psc_rpc/service.h"
#include "psc_util/cdefs.h"
#include "psc_util/thread.h"

#include "fid.h"

/* sliod thread types */
#define SLIOTHRT_CTL		0	/* control */
#define SLIOTHRT_LNETAC		1	/* lustre net accept thr */
#define SLIOTHRT_USKLNDPL	2	/* userland socket lustre net dev poll thr */
#define SLIOTHRT_RIC		3	/* RPC comm */
#define SLIOTHRT_RIM		4	/* RPC comm */
#define SLIOTHRT_RII		5	/* RPC comm */
#define SLIOTHRT_TINTV		6	/* timer interval */
#define SLIOTHRT_TIOS		7	/* iostats updater */
#define SLIOTHRT_SLVR_CRC	8	/* sliver crc updaters */

#define NSLVRCRC_THRS		4	/* perhaps ncores? */

struct bmapc_memb;
struct fidc_membh;
struct srt_bmapdesc_buf;

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

void	slric_init(void);
void	slrim_init(void);
void	slrii_init(void);
void	sliotimerthr_spawn(void);
__dead void slioctlthr_main(const char *);

struct fidc_membh *
	iod_inode_lookup(struct slash_fidgen *);
int	iod_inode_open(struct fidc_membh *, int);

int	iod_bmap_load(struct fidc_membh *, struct srt_bmapdesc_buf *,
		int, struct bmapc_memb **);

int     iod_inode_getsize(slfid_t, off_t *);

extern struct slashrpc_cservice *rmi_csvc;

extern struct slash_creds rootcreds;

#endif /* _SLIOD_H_ */
