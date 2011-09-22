/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2011, Pittsburgh Supercomputing Center (PSC).
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

#ifndef _FIDC_IOD_H_
#define _FIDC_IOD_H_

#include "fid.h"
#include "fidcache.h"
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

#define fcmh_2_fd(fcmh)		fcmh_2_fii(fcmh)->fii_fd

int sli_fcmh_getattr(struct fidc_membh *);

#define sli_fcmh_get(fgp, fp)	fidc_lookup((fgp), FIDC_LOOKUP_CREATE, NULL, 0, (fp))

void sli_fg_makepath(const struct slash_fidgen *, char *);

#endif /* _FIDC_IOD_H_ */
