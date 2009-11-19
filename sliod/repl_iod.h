/* $Id$ */

#include "psc_ds/list.h"

#include "fid.h"
#include "sltypes.h"

struct sli_repl_workrq {
	struct slash_fidgen	 srw_fg;
	struct bmapc_memb	*srw_bcm;
	struct fidc_membh	*srw_fcmh;
	struct psclist_head	 srw_lentry;
	uint64_t		 srw_nid;
	uint32_t		 srw_len;
	uint32_t		 srw_status;
	sl_bmapno_t		 srw_bmapno;
	struct slvr_ref		*srw_slvr_ref[2];
};

void sli_repl_addwk(uint64_t, struct slash_fidgen *, sl_bmapno_t, int);
void sli_repl_finishwk(struct sli_repl_workrq *, int);
void sli_repl_init(void);

extern struct pscrpc_nbreqset	 sli_replwk_nbset;
