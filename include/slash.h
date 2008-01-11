/* $Id$ */

/* Slash server thread types. */
#define SLTHRT_CTL	0	/* control */
#define SLTHRT_RPCMDS	1	/* metadata messages */
#define SLTHRT_RPCIO	2	/* I/O messages */
#define SLTHRT_LND	3	/* lustre networking device helper */

void slmds_init(void);
