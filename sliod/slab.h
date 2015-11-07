/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015, Google, Inc.
 * Copyright 2008-2015, Pittsburgh Supercomputing Center
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
 * The slab interface provides a backing for storing regions of file
 * space in CLI memory.  The slab API provides hooks into the RPC
 * layer for managing transportation over the network.
 */

#ifndef _SL_BUFFER_H_
#define _SL_BUFFER_H_

#include "pfl/cdefs.h"
#include "pfl/list.h"
#include "pfl/pool.h"

/*
 * Used for both read caching and write aggregation.
 */
struct sl_buffer {
	void			*slb_base;		/* point to the data buffer */
	struct psclist_head	 slb_mgmt_lentry;	/* chain lru or outgoing q  */
};

void sl_buffer_cache_init(void);

extern struct psc_poolmgr	*sl_bufs_pool;

#endif /* _SL_BUFFER_H_ */
