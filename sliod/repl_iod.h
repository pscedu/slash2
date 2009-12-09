/* $Id$ */

#include "psc_ds/list.h"

#include "fid.h"
#include "sltypes.h"

struct sli_repl_workrq {
	struct slash_fidgen	 srw_fg;
	sl_bmapno_t		 srw_bmapno;

	struct bmapc_memb	*srw_bcm;
	struct fidc_membh	*srw_fcmh;
	struct psclist_head	 srw_state_lentry;	/* entry for which state list */
	struct psclist_head	 srw_active_lentry;	/* entry in global active list */
	uint64_t		 srw_nid;
	uint32_t		 srw_len;		/* bmap size */
	uint32_t		 srw_status;		/* return code to pass back to MDS */
	uint32_t		 srw_offset;		/* which sliver we're transmitting */

	/* a single I/O may cross sliver boundaries, but is always <SLASH_SLVR_SIZE */
	struct slvr_ref		*srw_slvr_ref[2];
};

void sli_repl_addwk(uint64_t, struct slash_fidgen *, sl_bmapno_t, int);
void sli_repl_init(void);

extern struct pscrpc_nbreqset	 sli_replwk_nbset;
extern struct psc_listcache	 sli_replwkq_pending;
extern struct psc_listcache	 sli_replwkq_finished;
extern struct psc_listcache	 sli_replwkq_inflight;
extern struct psc_lockedlist	 sli_replwkq_active;
