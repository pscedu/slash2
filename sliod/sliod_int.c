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

#include <sys/time.h>

#include <string.h>
#include <unistd.h>

#include "pfl/cdefs.h"
#include "psc_ds/list.h"
#include "psc_ds/tree.h"
#include "psc_rpc/rsx.h"
#include "psc_util/alloc.h"
#include "psc_util/atomic.h"
#include "psc_util/lock.h"
#include "psc_util/log.h"
#include "psc_util/waitq.h"

#include "bmap.h"
#include "bmap_iod.h"
#include "fid.h"
#include "fidc_iod.h"
#include "fidcache.h"
#include "rpc_iod.h"
#include "slashrpc.h"
#include "slashrpc.h"
#include "slconfig.h"
#include "sliod.h"
#include "slvr.h"

int
iod_inode_getinfo(struct slash_fidgen *fg, uint64_t *size,
    uint64_t *nblks, uint32_t *utimgen)
{
	struct fidc_membh *f;
	struct stat stb;

	f = fidc_lookup_fg(fg);
	psc_assert(f);

	if (fstat(fcmh_2_fd(f), &stb))
		return (-errno);

	*size = stb.st_size;
	*nblks = stb.st_blocks;

	FCMH_LOCK(f);
	*utimgen = f->fcmh_sstb.sst_utimgen;
	/* fcmh_op_done_type() will drop the lock.
	 */
	fcmh_op_done_type(f, FCMH_OPCNT_LOOKUP_FIDC);
	return (0);
}

struct fidc_membh *
iod_inode_lookup(const struct slash_fidgen *fg)
{
	struct fidc_membh *f;
	int rc;

	rc = fidc_lookup(fg, FIDC_LOOKUP_CREATE, NULL, 0, &f);
	psc_assert(rc == 0);
	return (f);
}
