/* $Id$ */

#include "psc_ds/list.h"

#include "fid.h"
#include "sltypes.h"

struct sli_repl_buf {
	struct psclist_head	 srb_lentry;
	unsigned char		 srb_data[0];
};

struct sli_repl_workrq {
	struct slash_fidgen	 srw_fg;
	uint64_t		 srw_nid;
	uint32_t		 srw_len;
	uint32_t		 srw_status;
	sl_bmapno_t		 srw_bmapno;
	struct psclist_head	 srw_lentry;
	struct sli_repl_buf	*srw_srb;
};

void sli_repl_addwk(uint64_t, struct slash_fidgen *, sl_bmapno_t);
void sli_repl_finishwk(struct sli_repl_workrq *, int);

extern struct psc_poolmgr	*sli_repl_bufpool;
