/* $Id$ */

#include <sys/time.h>

#include <string.h>
#include <unistd.h>

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
#include "fidcache.h"
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
	INIT_PSCLIST_ENTRY(&biod->biod_lentry);
	LOCK_INIT(&biod->biod_lock);
	SPLAY_INIT(&biod->biod_slvrs);
}

void
iod_bmap_free(struct bmapc_memb *b)
{
	psc_pool_return(bmap_pool, b);
}

__static int
iod_bmap_fetch_crcs(struct bmapc_memb *b, int rw)
{
	int				 rc;
	struct srm_bmap_wire_req	*mq;
	struct srm_bmap_wire_rep	*mp;
	struct pscrpc_request		*rq;
	struct iovec			 iov;
	struct pscrpc_bulk_desc		*desc;

	psc_assert(b->bcm_mode & BMAP_IOD_RETRIEVE);
	psc_assert(!bmap_2_biodi_wire(b));

	rc = RSX_NEWREQ(rmi_csvc->csvc_import, SRMC_VERSION,
			SRMT_GETBMAPCRCS, rq, mq, mp);
	if (rc) {
		DEBUG_BMAP(PLL_ERROR, b, "could not create request (%d)", rc);
		goto out;
	}

	mq->rw = rw;
	mq->bmapno = b->bcm_blkno;
	memcpy(&mq->fg, fcmh_2_fgp(b->bcm_fcmh), sizeof(mq->fg));
	//memcpy(&mq->sbdb, sbdb, sizeof(*sbdb));

	iov.iov_len = sizeof(struct slash_bmap_wire);
	iov.iov_base = bmap_2_biodi_wire(b) =
		PSCALLOC(sizeof(struct slash_bmap_wire));

	rc = rsx_bulkclient(rq, &desc, BULK_PUT_SINK, SRMI_BULK_PORTAL, &iov, 1);
	if (rc) {
		PSCFREE(bmap_2_biodi_wire(b));
		psc_error("rsx_bulkclient() failed rc=%d ", rc);
		goto out;
	}
	rc = RSX_WAITREP(rq, mp);
	if (rc || mp->rc) {
		rc = rc ? rc : mp->rc;
		DEBUG_BMAP(PLL_ERROR, b, "req failed (%d)", rc);
		PSCFREE(bmap_2_biodi_wire(b));
		bmap_2_biodi_wire(b) = NULL;
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
	BMAP_LOCK(b);
	b->bcm_mode &= ~BMAP_IOD_RETRIEVE;
	if (rc)
		b->bcm_mode |= BMAP_IOD_RETRFAIL;
	psc_waitq_wakeall(&b->bcm_waitq);
	BMAP_ULOCK(b);

	return (rc);
}

int
iod_inode_getsize(slfid_t fid, off_t *fsize)
{
	struct fidc_membh *f;
	struct stat stb;
	int rc;

	f = fidc_lookup_simple(fid);
	psc_assert(f);
	psc_assert(f->fcmh_fcoo);
	/* XXX May want to replace this syscall with an inode cache
	 *   lookup.
	 */
	rc = fstat(f->fcmh_fcoo->fcoo_fd, &stb);
	if (!rc)
		*fsize = stb.st_size;
	else
		DEBUG_FCMH(PLL_ERROR, f, "fstat failed (rc=%d)", rc);

	fidc_membh_dropref(f);

	return (rc);
}

struct fidc_membh *
iod_inode_lookup(const struct slash_fidgen *fg)
{
	int rc;
	struct fidc_membh *f;
	/* Note that these creds are bogus, just used to satisfy the current
	 *  fidc_lookup_load_inode() code.
	 */
	struct fidc_memb m;

	COPYFID(fcm_2_fgp(&m), fg);
	rc = fidc_lookup_copy_inode(fg, &m, &rootcreds, &f);
	psc_assert(f);

	return (f);
}

/*
 * Attach the SLASH file to a file on the local file system.
 */
int
iod_inode_open(struct fidc_membh *f, int rw)
{
	int	rc;
	int	oflags;

	rc = 0;
	oflags = O_RDWR;
	psc_assert(rw == SL_READ || rw == SL_WRITE);

	spinlock(&f->fcmh_lock);
	if (f->fcmh_fcoo || (f->fcmh_state & FCMH_FCOO_CLOSING)) {
		rc = fidc_fcoo_wait_locked(f, FCOO_START);
		if (rc < 0) {
			freelock(&f->fcmh_lock);
			goto out;
		}
	} else
		fidc_fcoo_start_locked(f);

	if (rw == SL_WRITE) {
		oflags |= O_CREAT;
		f->fcmh_fcoo->fcoo_oref_wr++;
	} else {
		f->fcmh_fcoo->fcoo_oref_rd++;
	}

	freelock(&f->fcmh_lock);

	if (f->fcmh_state & FCMH_FCOO_STARTING) {

		f->fcmh_fcoo->fcoo_fd = fid_fileops_fg(fcmh_2_fgp(f), oflags, 0600);
		if (f->fcmh_fcoo->fcoo_fd < 0) {
			fidc_fcoo_startfailed(f);
			rc = -errno;
		} else {
			fidc_fcoo_startdone(f);
		}
	}
 out:
	if (rc)
		psc_error("failed rc=%d "FIDFMT, rc,
			  FIDFMTARGS(fcmh_2_fgp(f)));
	return (rc);
}

/**
 * iod_bmap_load - load the relevant bmap information from the metadata
 *   server.  In the case of the ION the bmap sections of interest are the
 *   crc table and the crc states bitmap.  For now we only load this
 *   information on read.
 * @f: the fid cache handle for the inode in question.
 * @sdbd: the key to authenticate with the mds.
 * @rw: the bmap mode.
 * @bmap:  return the bmap that has been loaded.
 * Return: error if rpc fails.
 */
int
iod_bmap_load(struct fidc_membh *f, sl_bmapno_t bmapno, int rw,
    struct bmapc_memb **bmap)
{
	int rc=0;
	struct bmapc_memb *b;

	psc_assert(bmap);

	b = bmap_lookup_add(f, bmapno, iod_bmap_init);

	spinlock(&b->bcm_lock);
	/* For the time being I don't think we need to key actions
	 *  off of the BMAP_INIT bit so just get rid of it.
	 */
	b->bcm_mode &= ~BMAP_INIT;

	if (rw == SL_READ) {
 retry_getcrcs:
		/* Check the retrieve bit first since it may be set
		 *  before the biodi_wire pointer.
		 */
		if (b->bcm_mode & BMAP_IOD_RETRIEVE) {
			/* Another thread is already getting this
			 *  bmap's crc table.
			 */
			psc_waitq_wait(&b->bcm_waitq, &b->bcm_lock);
			spinlock(&b->bcm_lock);
			goto retry_getcrcs;

		} else {
			if (!bmap_2_biodi_wire(b)) {
				/* This thread will retrieve the crc
				 *  table.  Set the bit and drop the
				 *  lock prior to making the rpc.
				 */
				b->bcm_mode |= BMAP_IOD_RETRIEVE;
				freelock(&b->bcm_lock);

				rc = iod_bmap_fetch_crcs(b, rw);
			} else
				/* biodi_wire already exists.
				 */
				freelock(&b->bcm_lock);
		}

	} else if (rw == SL_WRITE)
		freelock(&b->bcm_lock);

	else
		psc_fatalx("invalid rw mode (%d)", rw);

	*bmap = b;

	return (rc);
}
