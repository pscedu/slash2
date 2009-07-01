#ifndef _SLASH_INODEH_H_
#define _SLASH_INODEH_H_

#include "psc_types.h"
#include "psc_util/lock.h"
#include "inode.h"
#include "jflush.h"

struct slash_inode_handle {
	struct slash_inode_od         inoh_ino;
	struct slash_inode_extras_od *inoh_extras;
	struct fidc_membh            *inoh_fcmh;
	psc_spinlock_t                inoh_lock;
	struct jflush_item            inoh_jfi;
	int                           inoh_flags;
};

#define INOH_LOCK(h) spinlock(&(h)->inoh_lock)
#define INOH_ULOCK(h) freelock(&(h)->inoh_lock)
#define INOH_LOCK_ENSURE(h) LOCK_ENSURE(&(h)->inoh_lock)

enum slash_inode_handle_flags {
	INOH_INO_DIRTY    = (1<<0), /* Inode structures need to be written */
	INOH_EXTRAS_DIRTY = (1<<1), /* Replication structures need written */
	INOH_HAVE_EXTRAS  = (1<<2),
	INOH_INO_NEW      = (1<<3), /* The inode info has never been written 
				       to disk */
	INOH_LOAD_EXTRAS  = (1<<4)
};

#define FCMH_2_INODEP(f) (&(f)->fcmh_memb.fcm_inodeh.inoh_ino)

#define INOH_FLAG(field, str) ((field) ? (str) : "")
#define DEBUG_INOH_FLAGS(i)					\
	INOH_FLAG((i)->inoh_flags & INOH_INO_DIRTY, "D"),       \
	INOH_FLAG((i)->inoh_flags & INOH_EXTRAS_DIRTY, "d"),	\
	INOH_FLAG((i)->inoh_flags & INOH_HAVE_EXTRAS, "X"),	\
	INOH_FLAG((i)->inoh_flags & INOH_INO_NEW, "N")


#define INOH_FLAGS_FMT "%s%s%s%s"

#define DEBUG_INOH(level, i, fmt, ...)					\
	psc_logs((level), PSS_OTHER, 					\
		 " inoh@%p f:"FIDFMT" fl:"INOH_FLAGS_FMT		\
		 "v:%x bsz:%u nr:%u cs:%u lblk:%"_P_U64"d "		\
		 "repl0:%u crc:%"_P_U64"x "				\
		 ":: "fmt,						\
		 (i), FIDFMTARGS(&(i)->inoh_ino.ino_fg),		\
		 DEBUG_INOH_FLAGS(i),					\
		 (i)->inoh_ino.ino_version,				\
		 (i)->inoh_ino.ino_bsz,					\
		 (i)->inoh_ino.ino_nrepls,				\
		 (i)->inoh_ino.ino_csnap,				\
		 (i)->inoh_ino.ino_lblk,				\
		 (i)->inoh_ino.ino_repls[0].bs_id,			\
		 (i)->inoh_ino.ino_crc,					\
		 ## __VA_ARGS__)

#endif
