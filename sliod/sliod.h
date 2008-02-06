/* $Id$ */

#define SLIOTHRT_CTL	0	/* control */
#define SLIOTHRT_LND	1	/* lustre networking helper */
#define SLIOTHRT_RPC	2	/* RPC comm */

struct slio_ctlthr {
};

void slio_init(void);

extern struct psc_thread slioControlThread;
