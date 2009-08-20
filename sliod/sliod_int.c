/* $Id$ */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif 

#include <sys/time.h>

#include <string.h>
#include <unistd.h>

#include "psc_ds/list.h"
#include "psc_ds/tree.h"
#include "psc_rpc/rsx.h"
#include "psc_util/alloc.h"
#include "psc_util/assert.h"
#include "psc_util/atomic.h"
#include "psc_util/log.h"
#include "psc_util/spinlock.h"
#include "psc_util/waitq.h"

#include "bmap.h"
#include "fid.h"
#include "fidcache.h"
#include "iod_bmap.h"
#include "slashrpc.h"
#include "slashrpc.h"
#include "slconfig.h"
#include "sliod.h"
#include "slvr.h"

__static void 
iod_biodi_init(struct bmap_iod_info *biod, struct bmapc_memb *b)
{
	biod->biod_bmap = b;
	INIT_PSCLIST_ENTRY(&biod->biod_lentry);
	LOCK_INIT(&biod->biod_lock);
}       

__static void
iod_bmap_init(struct bmapc_memb *b, struct fidc_membh *f, sl_blkno_t bmapno)
{
        memset(b, 0, sizeof(*b));
        LOCK_INIT(&b->bcm_lock);
        atomic_set(&b->bcm_opcnt, 0);
        psc_waitq_init(&b->bcm_waitq);
        b->bcm_pri = PSCALLOC(sizeof(struct bmap_iod_info));	
	b->bcm_fcmh = f;
	b->bcm_blkno = bmapno;

	iod_biodi_init(b->bcm_pri, b);	
}

__static void
iod_bmap_free(struct bmapc_memb *b)
{
	struct bmap_iod_info *iobd;

	iobd = b->bcm_pri;
	PSCFREE(b->bcm_pri);
	PSCFREE(b);
}

__static int
iod_bmap_fetch_crcs(struct bmapc_memb *b, struct srt_bdb_secret *s)
{
	int rc=0;
	struct srm_bmap_wire_req *mq;
	struct srm_bmap_wire_rep *mp;
	struct pscrpc_request *rq;
	struct pscrpc_bulk_desc *desc;
	struct iovec iov;

	psc_assert(b->bcm_mode & BMAP_IOD_RETRIEVE);
	psc_assert(!bmap_2_biodi_wire(b));
	
	rc = RSX_NEWREQ(rmi_csvc->csvc_import, SRMC_VERSION,
			SRMT_GETBMAPCRCS, rq, mq, mp);
        if (rc) {
		DEBUG_BMAP(PLL_ERROR, b, "could not create request (%d)", rc);
		goto out;
        }

	memcpy(&mq->sbdb, s, sizeof(*s));

	iov.iov_len = sizeof(struct slash_bmap_wire);
	iov.iov_base = bmap_2_biodi_wire(b) = 
		PSCALLOC(sizeof(struct slash_bmap_wire));

	rsx_bulkclient(rq, &desc, BULK_PUT_SINK, SRMC_BULK_PORTAL, &iov, 1);
	
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
		
		SPLAY_FOREACH(s, biod_slvrtree, bmap_2_biodi_slvrs(b));
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
	BMAP_ULOCK(b);

	psc_waitq_wakeall(&b->bcm_waitq);

	return (rc);
}


struct fidc_membh *
iod_inode_lookup(struct slash_fidgen *fg)
{
	int rc;
	struct fidc_membh *f;
	/* Note that these creds are bogus, just used to satisfy the current
	 *  fidc_lookup_load_inode() code.
	 */
	struct fidc_memb m;
	struct slash_creds creds = {0,0};

	COPYFID(fcm_2_fgp(&m), fg);
	rc = fidc_lookup_copy_inode(fg, &m, &creds, &f);	
	psc_assert(f);

	return (f);
}

int
iod_inode_open(struct fidc_membh *f, int rw)
{
	int rc=0, oflags=O_RDWR;

	spinlock(&f->fcmh_lock);
        if (f->fcmh_fcoo || (f->fcmh_state & FCMH_FCOO_CLOSING)) {
                rc = fidc_fcoo_wait_locked(f, FCOO_START);
                if (rc < 0) {
                        freelock(&f->fcmh_lock);
                        goto out;
                }
        } else
                fidc_fcoo_start_locked(f);

	if (rw == SL_READ)
		f->fcmh_fcoo->fcoo_oref_rw[0]++;

	else if (rw == SL_WRITE)
		f->fcmh_fcoo->fcoo_oref_rw[1]++;
	else
		psc_fatalx("rw mode=%d is invalid", rw);

	freelock(&f->fcmh_lock);

	if (f->fcmh_state & FCMH_FCOO_STARTING) {
		if (rw == SL_WRITE)
			oflags |= O_CREAT;

		rc = f->fcmh_fcoo->fcoo_fd = 
			fid_fileops(fcmh_2_fid(f), oflags);
                if (rc < 0) {
                        fidc_fcoo_startfailed(f);
			rc = -errno;
                } else {
                        fidc_fcoo_startdone(f);
			rc = 0;
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
iod_bmap_load(struct fidc_membh *f, struct srt_bmapdesc_buf *sbdb, 
	      int rw, struct bmapc_memb **bmap)
{
	int rc=0;
	struct bmapc_memb *b;

	psc_assert(bmap);
	
	b = bmap_lookup_add(f, sbdb->sbdb_secret.sbs_bmapno, iod_bmap_init);

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

				rc = iod_bmap_fetch_crcs(b,
				    &sbdb->sbdb_secret);
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


