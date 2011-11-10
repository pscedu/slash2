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
 *
 * A 64-bit checksum follows this structure on disk.
 */
struct slash_inode_od {
	uint16_t		 ino_version;
	uint16_t		 ino_flags;			/* immutable, etc. */
	uint32_t		 ino_bsz;			/* bmap size */
	uint32_t		 ino_nrepls;			/* number of replicas */
	uint32_t		 ino_replpol;			/* BRPOL_* policies */
	sl_replica_t		 ino_repls[SL_DEF_REPLICAS];	/* embed a few replicas	*/
	uint64_t		 ino_repl_nblks[SL_DEF_REPLICAS];/* embed a few replicas */
};

enum ino_flags {
	INO_BMAP_AFFINITY = (1 << 0) /* Try to assign new bmaps to existing backing objects */
};

/*
 * A 64-bit checksum follows this structure on disk.
 */
struct slash_inode_extras_od {
	sl_snap_t		 inox_snaps[SL_DEF_SNAPSHOTS];	/* snapshot pointers */
	sl_replica_t		 inox_repls[SL_INOX_NREPLICAS];
	uint64_t		 inox_repl_nblks[SL_INOX_NREPLICAS];
};

#define INOX_SZ			sizeof(struct slash_inode_extras_od)

struct slash_inode_handle {
	struct slash_inode_od	 inoh_ino;
	struct slash_inode_extras_od *inoh_extras;
	struct fidc_membh	*inoh_fcmh;
	int			 inoh_flags;
};

#define	INOH_HAVE_EXTRAS	(1 << 0)			/* inoh_extras are loaded in mem */
#define	INOH_INO_NEW		(1 << 1)			/* not yet written to disk */
#define	INOH_INO_NOTLOADED	(1 << 2)
#define	INOH_IN_IO		(1 << 3)			/* being written to ZFS */

#define INOH_GETLOCK(ih)	(&(ih)->inoh_fcmh->fcmh_lock)
#define INOH_LOCK(ih)		spinlock(INOH_GETLOCK(ih))
#define INOH_ULOCK(ih)		freelock(INOH_GETLOCK(ih))
#define INOH_RLOCK(ih)		reqlock(INOH_GETLOCK(ih))
#define INOH_URLOCK(ih, lk)	ureqlock(INOH_GETLOCK(ih), (lk))
#define INOH_LOCK_ENSURE(ih)	LOCK_ENSURE(INOH_GETLOCK(ih))

#define inoh_2_mdsio_data(ih)	fcmh_2_mdsio_data((ih)->inoh_fcmh)
#define inoh_2_fsz(ih)		fcmh_2_fsz((ih)->inoh_fcmh)
#define inoh_2_fid(ih)		fcmh_2_fid((ih)->inoh_fcmh)

#define INOH_FLAGS_FMT		"%s%s%s%s"
#define DEBUG_INOH_FLAGS(i)						\
	(i)->inoh_flags & INOH_HAVE_EXTRAS	? "X" : "",		\
	(i)->inoh_flags & INOH_INO_NEW		? "N" : "",		\
	(i)->inoh_flags & INOH_INO_NOTLOADED	? "L" : "",		\
	(i)->inoh_flags & INOH_IN_IO		? "I" : ""

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

int	mds_inodes_odsync(struct fidc_membh *, void (*logf)(void *, uint64_t, int));

extern struct sl_ino_compat sl_ino_compat_table[];

static __inline void
slash_inode_handle_init(struct slash_inode_handle *ih,
    struct fidc_membh *f)
{
	ih->inoh_fcmh = f;
	ih->inoh_flags = INOH_INO_NOTLOADED;
}

static __inline char *
_dump_ino(char *buf, size_t siz, const struct slash_inode_od *ino)
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
_log_debug_inoh(const struct pfl_callerinfo *pci, int level,
    const struct slash_inode_handle *ih, const char *fmt, ...)
#define _pfl_callerinfo pci
{
	char buf[LINE_MAX], mbuf[LINE_MAX];
	va_list ap;

	va_start(ap, fmt);
	vsnprintf(mbuf, sizeof(mbuf), fmt, ap);
	va_end(ap);

	psclog(level,
	    "inoh@%p fcmh=%p f+g="SLPRI_FG" fl:%#x:"INOH_FLAGS_FMT" %s :: %s",
	    ih, ih->inoh_fcmh, SLPRI_FG_ARGS(&ih->inoh_fcmh->fcmh_fg),
	    ih->inoh_flags, DEBUG_INOH_FLAGS(ih),
	    _dump_ino(buf, sizeof(buf), &ih->inoh_ino), mbuf);
}
#undef _pfl_callerinfo

static __inline void
dump_ino(const struct slash_inode_od *ino)
{
	char buf[BUFSIZ];

	fprintf(stderr, "%s", _dump_ino(buf, sizeof(buf), ino));
}

#endif /* _SLASHD_INODE_H_ */
