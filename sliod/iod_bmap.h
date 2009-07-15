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
#include "offtree.h"
#include "slvr.h"

extern struct psc_listcache iodBmapLru;

SPLAY_HEAD(biod_slvrtree, slvr_ref);
SPLAY_PROTOTYPE(biod_slvrtree, slvr_ref, slvr_tentry, slvr_cmp);

struct bmap_iod_info {
	psc_spinlock_t        biod_lock;
	struct bmapc_memb    *biod_bmap;
	struct offtree_root  *biod_oftr;
	struct pscrpc_export *biod_export;
	struct iobd_slvrtree  biod_slvrs;     
	// XXX the fdbuf is going to go here too..
	struct psclist_head   biod_lentry;
	struct timespec       biod_age;
};

#define bmap_2_biodi(b) ((struct bmap_iod_info *)(b)->bcm_pri)
#define bmap_2_biodi_age(b) bmap_2_biodi(b)->biod_age
#define bmap_2_biodi_lentry(b) bmap_2_biodi(b)->biod_lentry
#define bmap_2_biodi_oftr(b)			\
	bmap_2_biodi(b)->biod_oftr
//	(((struct iodbmap_data *)((b)->bcm_pri))->iobd_oftr)
#define bmap_2_biodi_exp(b)			\
	bmap_2_biodi(b)->biod_export

#define	BMAP_IOD_RD	(1 << 0)	/* bmap has read creds */
#define	BMAP_IOD_WR	(1 << 1)	/* write creds */
#define	BMAP_IOD_DIO	(1 << 2)	/* bmap is in dio mode */
#define	BMAP_IOD_MCIP	(1 << 3)	/* "mode change in progress" */
#define	BMAP_IOD_MCC	(1 << 4)	/* "mode change complete" */

#define slvr_2_biod(s) ((struct bmap_iod_info *)(s)->slvr_iobd)
#define SLVR_2_bmap(s) slvr_2_biod(s)->iobd_bmap
#define SLVR_2_oftr(s) slvr_2_biod(s)->iobd_oftr

#endif /* _SLASH_IOD_BMAP_H_ */
