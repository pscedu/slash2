/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2007-2011, Pittsburgh Supercomputing Center (PSC).
 *
 * Permission to use, copy, and modify this software and its documentation
 * without fee for personal use or non-commercial use within your organization
 * is hereby granted, provided that the above copyright notice is preserved in
 * all copies and that the copyright and this permission notice appear in
 * supporting documentation.  Permission to redistribute this software to other
 * organizations or individuals is not permitted without the written permission
 * of the Pittsburgh Supercomputing Center.  PSC makes no representations about
 * the suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

/*
 * Inodes contain part of the metadata about files resident in a SLASH
 * network.  Inode handles are in-memory representations of this
 * metadata.
 */

#ifndef _SLASHD_INODE_H_
#define _SLASHD_INODE_H_

#include <sys/types.h>

#include <inttypes.h>
#include <limits.h>

#include "pfl/str.h"
#include "psc_util/lock.h"

#include "cache_params.h"
#include "fid.h"
#include "fidcache.h"
#include "sltypes.h"

#define SL_DEF_SNAPSHOTS	1

/*
 * Define metafile offsets.  At the beginning of the metafile is the
 * SLASH2 inode, which is always loaded.
 */
#define SL_EXTRAS_START_OFF	((off_t)0x0200)
#define SL_BMAP_START_OFF	((off_t)0x0600)

/*
 * Point to an offset within the linear metadata file which holds a
 * snapshot.  Snapshots are read-only and their metadata may not be
 * expanded.  Once the offset is established the slash_block structure
 * is used to index up to sn_nblks.
 */
typedef struct {
	int64_t			sn_off;
	int64_t			sn_nblks;
	int64_t			sn_date;
} sl_snap_t;

#define INO_VERSION		0x0002

/*
 * The inode structure lives at the beginning of the metafile and holds
 * the block store array along with snapshot pointers.
 */
struct slash_inode_od {
	uint16_t		 ino_version;
	uint16_t		 ino_flags;			/* immutable, etc. */
	uint32_t		 ino_bsz;			/* bmap size */
	uint32_t		 ino_nrepls;			/* if 0, use ino_prepl */
	uint32_t		 ino_replpol;			/* BRPOL_* policies */
	sl_replica_t		 ino_repls[SL_DEF_REPLICAS];	/* embed a few replicas	*/
	uint64_t		 ino_repl_nblks[SL_DEF_REPLICAS];/* embed a few replicas */
};

struct slash_inode_extras_od {
	sl_snap_t		 inox_snaps[SL_DEF_SNAPSHOTS];	/* snapshot pointers */
	sl_replica_t		 inox_repls[SL_INOX_NREPLICAS];
	uint64_t		 inox_repl_nblks[SL_INOX_NREPLICAS];
};

struct slash_inode_handle {
	struct slash_inode_od	 inoh_ino;
	struct slash_inode_extras_od *inoh_extras;
	struct fidc_membh	*inoh_fcmh;
	psc_spinlock_t		 inoh_lock;
	int			 inoh_flags;
};

#define	INOH_INO_DIRTY		(1 << 0)			/* inode needs to be written */
#define	INOH_EXTRAS_DIRTY	(1 << 1)			/* inoh_extras needs written */
#define	INOH_HAVE_EXTRAS	(1 << 2)			/* inoh_extras are loaded in mem */
#define	INOH_INO_NEW		(1 << 3)			/* not yet written to disk */
#define	INOH_INO_NOTLOADED	(1 << 4)

#define INOH_LOCK(ih)		spinlock(&(ih)->inoh_lock)
#define INOH_ULOCK(ih)		freelock(&(ih)->inoh_lock)
#define INOH_RLOCK(ih)		reqlock(&(ih)->inoh_lock)
#define INOH_URLOCK(ih, lk)	ureqlock(&(ih)->inoh_lock, (lk))
#define INOH_LOCK_ENSURE(ih)	LOCK_ENSURE(&(ih)->inoh_lock)

#define inoh_2_mdsio_data(ih)	fcmh_2_mdsio_data((ih)->inoh_fcmh)
#define inoh_2_fsz(ih)		fcmh_2_fsz((ih)->inoh_fcmh)
#define inoh_2_fid(ih)		fcmh_2_fid((ih)->inoh_fcmh)

#define INOH_FLAGS_FMT		"%s%s%s%s%s"
#define DEBUG_INOH_FLAGS(i)						\
	(i)->inoh_flags & INOH_INO_DIRTY	? "D" : "",		\
	(i)->inoh_flags & INOH_EXTRAS_DIRTY	? "d" : "",		\
	(i)->inoh_flags & INOH_HAVE_EXTRAS	? "X" : "",		\
	(i)->inoh_flags & INOH_INO_NEW		? "N" : "",		\
	(i)->inoh_flags & INOH_INO_NOTLOADED	? "L" : ""

#define DEBUG_INOH(level, ih, fmt, ...)					\
	_log_debug_inoh(PFL_CALLERINFO(), (level), (ih), (fmt), ## __VA_ARGS__)

struct sl_ino_compat {
	int			(*sic_read_ino)(struct slash_inode_handle *);
	int			(*sic_read_inox)(struct slash_inode_handle *);
	int			(*sic_read_bmap)(struct bmapc_memb *, void *);
};

int	mds_inode_update(struct slash_inode_handle *, int);
int	mds_inode_update_interrupted(struct slash_inode_handle *, int *);
int	mds_inode_read(struct slash_inode_handle *);
int	mds_inode_write(struct slash_inode_handle *, void *, void *);
int	mds_inox_write(struct slash_inode_handle *, void *, void *);

int	mds_inox_load_locked(struct slash_inode_handle *);
int	mds_inox_ensure_loaded(struct slash_inode_handle *);

extern struct sl_ino_compat sl_ino_compat_table[];

static __inline void
slash_inode_handle_init(struct slash_inode_handle *ih,
    struct fidc_membh *f)
{
	ih->inoh_fcmh = f;
	INIT_SPINLOCK(&ih->inoh_lock);
	ih->inoh_flags = INOH_INO_NOTLOADED;
}

static __inline char *
_debug_ino(char *buf, size_t siz, const struct slash_inode_od *ino)
{
	char nbuf[LINE_MAX], rbuf[LINE_MAX];
	int nr, j;

	nr = ino->ino_nrepls;
	if (nr < 0)
		nr = 1;
	else if (nr > SL_DEF_REPLICAS)
		nr = SL_DEF_REPLICAS;

	rbuf[0] = '\0';
	for (j = 0; j < nr; j++) {
		if (j)
			strlcat(rbuf, ",", sizeof(rbuf));
		snprintf(nbuf, sizeof(nbuf), "%u",
		    ino->ino_repls[j].bs_id);
		strlcat(rbuf, nbuf, sizeof(rbuf));
	}
	snprintf(buf, siz, "bsz:%u nr:%u nbpol:%u repl:%s",
	    ino->ino_bsz, ino->ino_nrepls, ino->ino_replpol, rbuf);
	return (buf);
}

static __inline void
_log_debug_inoh(const struct pfl_callerinfo *pfl_callerinfo, int level,
    const struct slash_inode_handle *ih, const char *fmt, ...)
{
	char buf[LINE_MAX], mbuf[LINE_MAX];
	va_list ap;

	va_start(ap, fmt);
	vsnprintf(mbuf, sizeof(mbuf), fmt, ap);
	va_end(ap);

	_psclog_pci(pfl_callerinfo, level, 0,
	    "inoh@%p fcmh=%p f+g="SLPRI_FG" fl:%#x:"INOH_FLAGS_FMT" %s :: %s",
	    ih, ih->inoh_fcmh, SLPRI_FG_ARGS(&ih->inoh_fcmh->fcmh_fg),
	    ih->inoh_flags, DEBUG_INOH_FLAGS(ih),
	    _debug_ino(buf, sizeof(buf), &ih->inoh_ino), mbuf);
}

#endif /* _SLASHD_INODE_H_ */
