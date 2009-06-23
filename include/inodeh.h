#ifndef _SLASH_INODEH_H_
#define _SLASH_INODEH_H_

#include "psc_types.h"
#include "psc_util/lock.h"
#include "inode.h"
#include "jflush.h"

typedef struct slash_inode_handle {
	sl_inode_mds_t     inoh_ino;
	sl_inode_extras_t *inoh_extras;
	psc_spinlock_t     inoh_lock;
	struct jflush_item inoh_jfi;
	int                inoh_flags;
} sl_inodeh_t;

#endif
