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

#ifndef _CACHEPARAMS_H_
#define _CACHEPARAMS_H_

/* Begin hand computed */
#define SLASH_SLVRS_PER_BMAP	128
#define SLASH_SLVR_SIZE		(1024 * (off_t)1024)
#define SLASH_SLVR_BLKSZ	(32 * 1024)

/* XXX make it 4 so that status bits of a replica do not straddle a byte boundary */
#define SL_BITS_PER_REPLICA	3

/* End hand computed */

#define SL_REPLICA_MASK		((uint8_t)((1 << SL_BITS_PER_REPLICA) - 1))
#define SL_REPLICA_NBYTES	((SL_MAX_REPLICAS * SL_BITS_PER_REPLICA) / NBBY)	/* 64-bit align */

#define SLASH_BMAP_SIZE		(SLASH_SLVRS_PER_BMAP * SLASH_SLVR_SIZE)		/* 128 MiB */

#define SLASH_SLVR_BLKMASK	(SLASH_SLVR_BLKSZ - 1)
#define SLASH_BLKS_PER_SLVR	(SLASH_SLVR_SIZE / SLASH_SLVR_BLKSZ)

#endif /* _CACHEPARAMS_H_ */
