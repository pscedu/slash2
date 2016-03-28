/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright (c) 2006-2013, Pittsburgh Supercomputing Center (PSC).
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

#include "pfl/cdefs.h"
#include "pfl/alloc.h"

#include "slconfig.h"
#include "sliod.h"

void
slcfg_init_res(__unusedx struct sl_resource *res)
{
}

void
slcfg_destroy_res(__unusedx struct sl_resource *res)
{
}

void
slcfg_init_resm(__unusedx struct sl_resm *resm)
{
}

void
slcfg_destroy_resm(__unusedx struct sl_resm *resm)
{
}

void
slcfg_init_site(__unusedx struct sl_site *site)
{
}

void
slcfg_destroy_site(__unusedx struct sl_site *site)
{
}

int	 cfg_site_pri_sz;
int	 cfg_res_pri_sz;
int	 cfg_resm_pri_sz = sizeof(struct resm_iod_info);
