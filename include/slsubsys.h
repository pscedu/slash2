/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright (c) 2010-2015, Pittsburgh Supercomputing Center (PSC).
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

/*
 * SLASH2 shared daemon subsystem definitions.
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
	pfl_subsys_register(SLSS_BMAP, "bmap");
	pfl_subsys_register(SLSS_FCMH, "fcmh");
}

static __inline void
sl_subsys_unregister(void)
{
	pfl_subsys_unregister(SLSS_FCMH);
	pfl_subsys_unregister(SLSS_BMAP);
}

#endif /* _SLSUBSYS_H_ */
