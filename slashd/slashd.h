/* $Id$ */

#ifndef _SLASH_H_
#define _SLASH_H_

//#define _PATH_SLASHCONF	"/etc/slash.conf"
#define _PATH_SLASHCONF		"config/example.conf"

/* Slash server thread types. */
#define SLTHRT_CTL	0	/* control */
#define SLTHRT_RPCMDS	1	/* metadata messages */
#define SLTHRT_RPCIO	2	/* I/O messages */
#define SLTHRT_LND	3	/* lustre networking device helper */

struct slash_ctlthr {
	int sc_st_nclients;
	int sc_st_nsent;
	int sc_st_nrecv;
};

#define slctlthr(thr) ((struct slash_ctlthr *)(thr)->pscthr_private)

void slmds_init(void);

#endif /* _SLASH_H_ */
