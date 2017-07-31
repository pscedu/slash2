/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015, Google, Inc.
 * Copyright 2006-2016, Pittsburgh Supercomputing Center
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or (at
 * your option) any later version.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License contained in the file
 * `COPYING-GPL' at the top of this distribution or at
 * https://www.gnu.org/licenses/gpl-2.0.html for more details.
 * ---------------------------------------------------------------------
 * %END_LICENSE%
 */

#ifndef _FIDC_IOD_H_
#define _FIDC_IOD_H_

#include "fid.h"
#include "fidcache.h"
#include "slconn.h"
#include "sliod.h"
#include "sltypes.h"

struct fidc_membh;

struct fcmh_iod_info {
	int			fii_fd;		/* open file descriptor */
	int			fii_nwrite;	/* # of sliver writes */
	uint32_t		fii_predio_boff;/* offset within bmap */
	sl_bmapno_t		fii_predio_lastbno;
	int			fii_predio_nseq;/* num sequential io's */
	struct psclist_head	fii_lentry;	/* all fcmhs with readahead */
};

/* sliod-specific fcmh_flags */
#define FCMH_IOD_BACKFILE	(_FCMH_FLGSHFT << 0)    /* backing file exists */
#define FCMH_IOD_DIRTYFILE	(_FCMH_FLGSHFT << 1)    /* backing file is dirty */
#define FCMH_IOD_SYNCFILE	(_FCMH_FLGSHFT << 2)    /* flusing backing file */

#define fcmh_2_fd(fcmh)		fcmh_2_fii(fcmh)->fii_fd

static __inline struct fcmh_iod_info *
fcmh_2_fii(struct fidc_membh *f)
{
	return (fcmh_get_pri(f));
}

static __inline struct fidc_membh *
fii_2_fcmh(struct fcmh_iod_info *fii)
{
	struct fidc_membh *fcmh;

	psc_assert(fii);
	fcmh = (void *)fii;
	return (fcmh - 1);
}

#define sli_fcmh_get(fgp, fp)	sl_fcmh_get_fg((fgp), (fp))
#define sli_fcmh_peek(fgp, fp)  sl_fcmh_peek_fg((fgp), (fp))

void	sli_fg_makepath(const struct sl_fidgen *, char *);

int	sli_rmi_lookup_fid(struct slrpc_cservice *,
	    const struct sl_fidgen *, const char *,
	    struct sl_fidgen *, int *);

#endif /* _FIDC_IOD_H_ */
