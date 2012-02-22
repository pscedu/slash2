/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2012, Pittsburgh Supercomputing Center (PSC).
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

#ifndef _CACHEPARAMS_H_
#define _CACHEPARAMS_H_

/* Begin hand computed */

#define FIDC_MDS_DEFSZ		(128 * 1024)
#define FIDC_CLI_DEFSZ		1024
#define FIDC_ION_DEFSZ		(8 * 1024)

#define SLASH_SLVRS_PER_BMAP	128
#define SLASH_SLVR_SIZE		(1024 * (off_t)1024)
#define SLASH_SLVR_BLKSZ	(32 * 1024)

#define SLB_NBLK		32
#define SLB_NDEF		64
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
