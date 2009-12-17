/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2009, Pittsburgh Supercomputing Center (PSC).
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

enum fid_cache_users {
	FIDC_USER_CLI = 0,
	FIDC_USER_ION = 1,
	FIDC_USER_MDS = 2
};

/* Begin hand computed */
#define FIDC_MDS_DEFSZ			32768	/* Number of fcmh's to allocate by default */
#define FIDC_MDS_MAXSZ			1048576	/* Max fcmh's */

#define FIDC_CLI_DEFSZ			32768	/* Number of fcmh's to allocate by default */
#define FIDC_CLI_MAXSZ			131072	/* Max fcmh's */

#define FIDC_ION_DEFSZ			4096	/* Number of fcmh's to allocate by default */
#define FIDC_ION_MAXSZ			524288	/* Max fcmh's */

#define FIDC_CLI_HASH_SZ		(FIDC_MDS_DEFSZ * 2)
#define FIDC_ION_HASH_SZ		(FIDC_CLI_DEFSZ * 2)
#define FIDC_MDS_HASH_SZ		(FIDC_ION_DEFSZ * 2)

#define SLASH_SLVRS_PER_BMAP		128
#define SLASH_SLVR_SIZE			(1024*1024)
#define SLASH_BMAP_SIZE			(SLASH_SLVRS_PER_BMAP * SLASH_SLVR_SIZE)
#define SLASH_BMAP_SHIFT		15	/* 2^SLASH_BMAP_SHIFT should == SLASH_BMAP_BLKSZ */
/* End hand computed */

#define SLASH_BMAP_BLKSZ		32768

#define SLASH_BMAP_BLKMASK		(SLASH_BMAP_BLKSZ-1)

#define SLASH_SLVR_BLKSZ		SLASH_BMAP_BLKSZ
#define SLASH_SLVR_BLKMASK		(SLASH_SLVR_BLKSZ-1)
#define SLASH_BLKS_PER_SLVR		(SLASH_SLVR_SIZE/SLASH_SLVR_BLKSZ)

#define SLASH_MAXBLKS_PER_REQ		(LNET_MTU / SLASH_BMAP_BLKSZ)

#define BMAP_MAX_GET 63

#endif /* _CACHEPARAMS_H_ */
