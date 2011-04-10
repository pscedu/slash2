/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2010, Pittsburgh Supercomputing Center (PSC).
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
 * SLASH daemon subsystem definitions.
 */

#ifndef _SLSUBSYS_H_
#define _SLSUBSYS_H_

#include "pfl/subsys.h"

/* SLash2 SubSystems (SLSS) used to pass caller info */
#define SLSS_BMAP	(_PSS_LAST + 0)
#define SLSS_FCMH	(_PSS_LAST + 1)
#define _SLSS_LAST	(_PSS_LAST + 2)

static __inline void
sl_subsys_register(void)
{
	psc_subsys_register(SLSS_BMAP, "bmap");
	psc_subsys_register(SLSS_FCMH, "fcmh");
}

#endif /* _SLSUBSYS_H_ */
