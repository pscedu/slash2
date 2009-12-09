/* $Id$ */

/*
 * Control interface for querying and modifying
 * parameters of a running sliod instance.
 */

#include "psc_rpc/rpc.h"

#include "fid.h"
#include "slconfig.h"
#include "sltypes.h"

struct slictlmsg_replwkst {
	struct slash_fidgen	srws_fg;
	char			srws_peer_addr[RESM_ADDRBUF_SZ];
	sl_bmapno_t		srws_bmapno;
	uint32_t		srws_len;
	uint32_t		srws_offset;
};

/* sliricthr thread stat aliases */
#define pcst_nwrite		pcst_u32_1

/* sliod message types */
#define SLICMT_GET_REPLWKST	(NPCMT + 0)

/* sliod control commands */
#define SICC_EXIT		0
#define SICC_RECONFIG		1
