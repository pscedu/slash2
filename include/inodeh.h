/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
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

#ifndef _SLASH_INODEH_H_
#define _SLASH_INODEH_H_

#include <inttypes.h>

#include "psc_util/lock.h"
#include "psc_util/strlcat.h"

#include "inode.h"
#include "jflush.h"
#include "fidcache.h"

struct slash_inode_handle {
	struct slash_inode_od		 inoh_ino;
	struct slash_inode_extras_od	*inoh_extras;
	struct fidc_membh		*inoh_fcmh;
	psc_spinlock_t			 inoh_lock;
	struct jflush_item		 inoh_jfi;
	int				 inoh_flags;
};

#define INOH_LOCK(ih)			spinlock(&(ih)->inoh_lock)
#define INOH_ULOCK(ih)			freelock(&(ih)->inoh_lock)
#define INOH_RLOCK(ih)			reqlock(&(ih)->inoh_lock)
#define INOH_URLOCK(ih, lk)		ureqlock(&(ih)->inoh_lock, (lk))
#define INOH_LOCK_ENSURE(ih)		LOCK_ENSURE(&(ih)->inoh_lock)

enum {
	INOH_INO_DIRTY     = (1 << 0),	/* Inode structures need to be written */
	INOH_EXTRAS_DIRTY  = (1 << 1),	/* Replication structures need written */
	INOH_HAVE_EXTRAS   = (1 << 2),	/* inoh_extras are loaded in mem */
	INOH_INO_NEW       = (1 << 3),	/* Inode has never been written to disk */
	INOH_INO_NOTLOADED = (1 << 4)
};

static __inline void
slash_inode_handle_init(struct slash_inode_handle *i,
    struct fidc_membh *f, jflush_handler handler)
{
	i->inoh_fcmh = f;
	i->inoh_extras = NULL;
	LOCK_INIT(&i->inoh_lock);
	jfi_init(&i->inoh_jfi, handler, NULL, i);
	i->inoh_flags = INOH_INO_NOTLOADED;
}

static __inline char *
_debug_ino(char *buf, size_t siz, const struct slash_inode_od *ino)
{
	char nbuf[LINE_MAX], rbuf[LINE_MAX];
	int nr, j;

	nr = ino->ino_nrepls;
	if (nr < 0)
		nr = 1;
	else if (nr > INO_DEF_NREPLS)
		nr = INO_DEF_NREPLS;

	rbuf[0] = '\0';
	for (j = 0; j < nr; j++) {
		if (j)
			psc_strlcat(rbuf, ",", sizeof(rbuf));
		snprintf(nbuf, sizeof(nbuf), "%u",
		    ino->ino_repls[j].bs_id);
		psc_strlcat(rbuf, nbuf, sizeof(rbuf));
	}

	snprintf(buf, siz,
	    "f:"FIDFMT" v:%x bsz:%u nr:%u cs:%u repl:%s crc:%"PRIx64,
	    FIDFMTARGS(&ino->ino_fg), ino->ino_version,
	    ino->ino_bsz, ino->ino_nrepls,
	    ino->ino_csnap, rbuf, ino->ino_crc);
	return (buf);
}

static __inline void
debug_ino(const struct slash_inode_od *ino)
{
	char buf[BUFSIZ];

	_debug_ino(buf, sizeof(buf), ino);
	printf("%s\n", buf);
}

#define INOH_FLAGS_FMT "%s%s%s%s"
#define DEBUG_INOH_FLAGS(i)						\
	(i)->inoh_flags & INOH_INO_DIRTY	? "D" : "",		\
	(i)->inoh_flags & INOH_EXTRAS_DIRTY	? "d" : "",		\
	(i)->inoh_flags & INOH_HAVE_EXTRAS	? "X" : "",		\
	(i)->inoh_flags & INOH_INO_NEW		? "N" : ""

static __inline void
debug_inoh(const struct slash_inode_handle *ih)
{
	char buf[BUFSIZ];

	_debug_ino(buf, sizeof(buf), &ih->inoh_ino);
	printf("fl:"INOH_FLAGS_FMT" %s\n", DEBUG_INOH_FLAGS(ih), buf);
}

static __inline void
_log_debug_inoh(const char *file, const char *func, int lineno,
    int level, const struct slash_inode_handle *ih, const char *fmt, ...)
{
	char buf[LINE_MAX], mbuf[LINE_MAX];
	va_list ap;

	va_start(ap, fmt);
	vsnprintf(mbuf, sizeof(mbuf), fmt, ap);
	va_end(ap);

	psclog(file, func, lineno, PSS_GEN, level, 0,
	    "inoh@%p fl:"INOH_FLAGS_FMT" %s :: %s",
	    ih, DEBUG_INOH_FLAGS(ih), _debug_ino(buf, sizeof(buf),
	    &ih->inoh_ino), mbuf);
}

#define DEBUG_INOH(level, ih, fmt, ...)					\
	_log_debug_inoh(__FILE__, __func__, __LINE__, (level), (ih),	\
	    (fmt), ## __VA_ARGS__)

#endif
