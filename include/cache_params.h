/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2013, Pittsburgh Supercomputing Center (PSC).
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

#ifndef _CACHEPARAMS_H_
#define _CACHEPARAMS_H_

/* Begin hand computed */

#define FIDC_MDS_DEFSZ		(2 * 32 * 1024)
#define FIDC_CLI_DEFSZ		(2 * 1024)
#define FIDC_ION_DEFSZ		21851	/* used to be (2 * 8 * 1024) */

#define SLASH_SLVRS_PER_BMAP	128
#define SLASH_SLVR_SIZE		(1024 * (off_t)1024)
#define SLASH_SLVR_BLKSZ	(32 * 1024)

#define SLB_DEF			64
#define SLB_MIN			64
#define SLB_MAX			128

/* XXX make it 4 so that status bits of a replica do not straddle a byte boundary */
#define SL_BITS_PER_REPLICA	3

/* End hand computed */

#define SL_REPLICA_MASK		((uint8_t)((1 << SL_BITS_PER_REPLICA) - 1))
#define SL_REPLICA_NBYTES	((SL_MAX_REPLICAS * SL_BITS_PER_REPLICA) / NBBY)	/* 64-bit align */

#define SLASH_BMAP_SIZE		(SLASH_SLVRS_PER_BMAP * SLASH_SLVR_SIZE)		/* 128 MiB */

#define SLASH_SLVR_BLKMASK	(SLASH_SLVR_BLKSZ - 1)
#define SLASH_BLKS_PER_SLVR	(SLASH_SLVR_SIZE / SLASH_SLVR_BLKSZ)

/* aliases */
#define SLASH_CRCS_PER_BMAP	SLASH_SLVRS_PER_BMAP
#define SLASH_BMAP_CRCSIZE	SLASH_SLVR_SIZE

#endif /* _CACHEPARAMS_H_ */
