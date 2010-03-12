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

struct slash_creds rootcreds = { 0, 0 };

void
iod_bmap_init(struct bmapc_memb *b)
{
	struct bmap_iod_info *biod;

	biod = b->bcm_pri;
	biod->biod_bmap = b;
	biod->biod_bcr_xid = biod->biod_bcr_xid_last = 0;
	INIT_PSCLIST_ENTRY(&biod->biod_lentry);
	LOCK_INIT(&biod->biod_lock);
	SPLAY_INIT(&biod->biod_slvrs);
}

uint64_t
iod_inode_getsize(struct slash_fidgen *fg)
{
	struct fidc_membh *f;
	uint64_t size;

	f = fidc_lookup_fg(fg);
	psc_assert(f);
	size = f->fcmh_sstb.sst_size;
	fcmh_op_done_type(f, FCMH_OPCNT_LOOKUP_FIDC);
	return (size);
}

struct fidc_membh *
iod_inode_lookup(const struct slash_fidgen *fg)
{
	struct fidc_membh *f;
	int rc;

	rc = fidc_lookupf(fg, FIDC_LOOKUP_CREATE | FIDC_LOOKUP_LOAD,
	    NULL, 0, &rootcreds, &f);
	psc_assert(rc == 0);
	return (f);
}

/**
 * iod_bmap_retrieve - load the relevant bmap information from the metadata
 *   server.  In the case of the ION the bmap sections of interest are the
 *   CRC table and the CRC states bitmap.  For now we only load this
 *   information on read.
 * @b: bmap to load.
 * @rw: the bmap access mode.
 * Return zero on success or errno code on failure (likely an RPC problem).
 */
int
iod_bmap_retrieve(struct bmapc_memb *b, enum rw rw)
{
	int				 rc;
	struct srm_bmap_wire_req	*mq;
	struct srm_bmap_wire_rep	*mp;
	struct pscrpc_request		*rq;
	struct iovec			 iov;
	struct pscrpc_bulk_desc		*desc;

	psc_assert(!bmap_2_biodi_wire(b));

	rc = RSX_NEWREQ(sli_rmi_getimp(), SRMC_VERSION,
			SRMT_GETBMAPCRCS, rq, mq, mp);
	if (rc) {
		DEBUG_BMAP(PLL_ERROR, b, "could not create request (%d)", rc);
		goto out;
	}

	mq->rw = rw;
	mq->bmapno = b->bcm_blkno;
	memcpy(&mq->fg, &b->bcm_fcmh->fcmh_fg, sizeof(mq->fg));
	//memcpy(&mq->sbdb, sbdb, sizeof(*sbdb));

	iov.iov_len = sizeof(struct slash_bmap_wire);
	iov.iov_base = bmap_2_biodi_wire(b) =
		PSCALLOC(sizeof(struct slash_bmap_wire));

	rc = rsx_bulkclient(rq, &desc, BULK_PUT_SINK, SRMI_BULK_PORTAL, &iov, 1);
	if (rc) {
		psc_error("rsx_bulkclient() failed rc=%d ", rc);
		goto out;
	}
	rc = RSX_WAITREP(rq, mp);
	if (rc || mp->rc) {
		rc = rc ? rc : mp->rc;
		DEBUG_BMAP(PLL_ERROR, b, "req failed (%d)", rc);
		goto out;
	}
	/* Need to copy any of our slvr crc's into the table.
	 */
	spinlock(&bmap_2_biodi(b)->biod_lock);
	if (!SPLAY_EMPTY(bmap_2_biodi_slvrs(b))) {
		struct slvr_ref *s;

		SPLAY_FOREACH(s, biod_slvrtree, bmap_2_biodi_slvrs(b))
		    /* Only replace the crc if datardy is true (meaning that
		     *   all init operations have be done) and that the
		     *   crc is clean (meaning that the crc reflects the slab
		     *   contents.
		     */
		    if (!(s->slvr_flags & SLVR_CRCDIRTY) &&
			s->slvr_flags & SLVR_DATARDY) {
			    slvr_2_crc(s) = s->slvr_crc;
			    slvr_2_crcbits(s) |= (BMAP_SLVR_DATA|BMAP_SLVR_CRC);
		    }
	}
	freelock(&bmap_2_biodi(b)->biod_lock);
 out:
	/* Unblock threads no matter what.
	 *  XXX need some way to denote that a crcget rpc failed?
	 */
	if (rc)
		PSCFREE(bmap_2_biodi_wire(b));
	return (rc);
}

struct bmap_ops bmap_ops = {
	iod_bmap_init,
	iod_bmap_retrieve,
	NULL
};
