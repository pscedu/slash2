/* $Id$ */

#ifndef _SLASH_IOD_BMAP_H_
#define _SLASH_IOD_BMAP_H_

#include <sys/time.h>

#include "psc_types.h"

#include "psc_ds/list.h"
#include "psc_ds/listcache.h"
#include "psc_rpc/rpc.h"
#include "psc_util/lock.h"

#include "bmap.h"
#include "inode.h"
#include "offtree.h"
#include "slvr.h"

extern struct psc_listcache iodBmapLru;

SPLAY_HEAD(biod_slvrtree, slvr_ref);
SPLAY_PROTOTYPE(biod_slvrtree, slvr_ref, slvr_tentry, slvr_cmp);

struct bmap_iod_info {
	psc_spinlock_t          biod_lock;
	struct bmapc_memb      *biod_bmap;
	struct iobd_slvrtree    biod_slvrs;     
	struct slash_bmap_wire *biod_bmap_wire;
	struct psclist_head     biod_lentry;
	struct timespec         biod_age;
};

#define bmap_2_biodi(b) ((struct bmap_iod_info *)(b)->bcm_pri)
#define bmap_2_biodi_age(b) bmap_2_biodi(b)->biod_age
#define bmap_2_biodi_lentry(b) bmap_2_biodi(b)->biod_lentry
#define bmap_2_biodi_oftr(b)			\
	bmap_2_biodi(b)->biod_oftr
//	(((struct iodbmap_data *)((b)->bcm_pri))->iobd_oftr)
#define bmap_2_biodi_wire(b)			\
	bmap_2_biodi(b)->biod_bmap_wire


enum iod_bmap_modes {
	BMAP_IOD_RETRIEVE  = (1 << (0 + BMAP_RSVRD_MODES)),
	BMAP_IOD_RELEASING = (1 << (1 + BMAP_RSVRD_MODES))
};

#define slvr_2_biod(s) ((struct bmap_iod_info *)(s)->slvr_iobd)
#define slvr_2_bmap(s) slvr_2_biod(s)->iobd_bmap
#define slvr_2_oftr(s) slvr_2_biod(s)->iobd_oftr

#endif /* _SLASH_IOD_BMAP_H_ */
