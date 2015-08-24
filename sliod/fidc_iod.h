/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2015, Pittsburgh Supercomputing Center (PSC).
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
 *
 * Pittsburgh Supercomputing Center	phone: 412.268.4960  fax: 412.268.5832
 * 300 S. Craig Street			e-mail: remarks@psc.edu
 * Pittsburgh, PA 15213			web: http://www.psc.edu/
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
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
};

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

/* sliod-specific fcmh_flags */
#define FCMH_IOD_BACKFILE	(_FCMH_FLGSHFT << 0)    /* backing file exists */

#define fcmh_2_fd(fcmh)		fcmh_2_fii(fcmh)->fii_fd

#define sli_fcmh_get(fgp, fp)	fidc_lookup((fgp), FIDC_LOOKUP_CREATE, (fp))
#define sli_fcmh_peek(fgp, fp)  fidc_lookup((fgp), FIDC_LOOKUP_NONE, (fp))

void	sli_fg_makepath(const struct sl_fidgen *, char *);
int	sli_fcmh_getattr(struct fidc_membh *);
int	sli_fcmh_lookup_fid(struct slashrpc_cservice *,
	    const struct sl_fidgen *, const char *,
	    struct sl_fidgen *, int *);

#endif /* _FIDC_IOD_H_ */
