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

__static int
iod_bmap_fetch_crcs(struct bmapc_memb *b, enum rw rw)
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
	if (rc) {
		PSCFREE(bmap_2_biodi_wire(b));
	}
	return (rc);
}

int
iod_inode_getsize(struct slash_fidgen *fg, off_t *fsize)
{
	struct fidc_membh *f;
	struct stat stb;
	int rc;

	f = fidc_lookup_fg(fg);
	psc_assert(f);
	psc_assert(f->fcmh_fcoo);
	/* XXX May want to replace this syscall with an inode cache
	 *   lookup.
	 */
	rc = fstat(fcmh_2_fd(f), &stb);
	if (!rc)
		*fsize = stb.st_size;
	else
		DEBUG_FCMH(PLL_ERROR, f, "fstat failed (rc=%d)", rc);

	fcmh_dropref(f);

	return (rc);
}

struct fidc_membh *
iod_inode_lookup(const struct slash_fidgen *fg)
{
	struct fidc_membh *f;
	int rc;

	rc = fidc_lookup(fg, FIDC_LOOKUP_CREATE | FIDC_LOOKUP_COPY |
	    FIDC_LOOKUP_REFRESH, NULL, FCMH_SETATTRF_NONE, &rootcreds, &f);
	psc_assert(f);
	return (f);
}

/*
 * iod_inode_open - Associate an fcmh with a file handle to a data
 *	store for the file on the local file system.
 * @f: FID cache member handle of file to open.
 * @rw: read or write operation.
 */
int
iod_inode_open(struct fidc_membh *f, enum rw rw)
{
	int rc, oflags;

	rc = 0;
	oflags = O_RDWR;
	psc_assert(rw == SL_READ || rw == SL_WRITE);

	spinlock(&f->fcmh_lock);
	rc = fcmh_load_fcoo(f);
	if (rc < 0)
		goto out;

	if (rw == SL_WRITE) {
		oflags |= O_CREAT;
		f->fcmh_fcoo->fcoo_oref_wr++;
	} else {
		f->fcmh_fcoo->fcoo_oref_rd++;
	}

	freelock(&f->fcmh_lock);

	if (f->fcmh_state & FCMH_FCOO_STARTING) {
		fcmh_2_fd(f) = fid_fileops_fg(&f->fcmh_fg,
		    oflags, 0600);
		if (fcmh_2_fd(f) < 0) {
			fidc_fcoo_startfailed(f);
			rc = -errno;
		} else {
			fidc_fcoo_startdone(f);
		}
	}
 out:
	if (rc)
		psc_error("failed rc=%d "FIDFMT, rc,
		    FIDFMTARGS(&f->fcmh_fg));
	return (rc);
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
iod_bmap_retrieve(struct bmapc_memb *b, enum rw rw, __unusedx void *arg)
{
	return (iod_bmap_fetch_crcs(b, rw));
}

int
iod_bmap_load(struct fidc_membh *f, sl_blkno_t n, enum rw rw,
    struct bmapc_memb **bp)
{
	return (bmap_get(f, n, rw, bp, NULL));
}

void	(*bmap_init_privatef)(struct bmapc_memb *) = iod_bmap_init;
int	(*bmap_retrievef)(struct bmapc_memb *, enum rw, void *) = iod_bmap_retrieve;
void	(*bmap_final_cleanupf)(struct bmapc_memb *);
