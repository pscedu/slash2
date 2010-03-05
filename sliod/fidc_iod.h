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

#ifndef _FIDC_IOD_H_
#define _FIDC_IOD_H_

#include "fid.h"
#include "fidcache.h"
#include "sliod.h"
#include "sltypes.h"

struct fidc_membh;

struct fcoo_iod_info {
	int			fii_fd;		/* open file descriptor */
};

#define fcoo_2_fii(fcoo)	((struct fcoo_iod_info *)fcoo_get_pri(fcoo))
#define fcmh_2_fd(fcmh)		fcoo_2_fii((fcmh)->fcmh_fcoo)->fii_fd

#define sli_fcmh_get(fgp, fcmhp)	fcmh_getload((fgp), &rootcreds, (fcmhp))

int fcmh_load_fii(struct fidc_membh *, enum rw);

#endif /* _FIDC_IOD_H_ */
