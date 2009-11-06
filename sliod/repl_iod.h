/* $Id$ */

#include "psc_ds/list.h"

#include "sltypes.h"

struct sli_repl_buf {
	struct psclist_head	 srb_lentry;
	unsigned char		 srb_data[0];
};

struct sli_repl_workrq {
	uint64_t		 srw_fid;
	uint64_t		 srw_nid;
	uint32_t		 srw_len;
	sl_bmapno_t		 srw_bmapno;
	struct psclist_head	 srw_lentry;
	int			 srw_status;
	struct sli_repl_buf	*srw_srb;
};

void sli_repl_addwk(uint64_t, uint64_t, sl_bmapno_t);
void sli_repl_finishwk(struct sli_repl_workrq *, int);

extern struct psc_poolmgr	*sli_repl_bufpool;
