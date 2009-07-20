#define _XOPEN_SOURCE 500

#include <unistd.h>

#include "fid.h"
#include "bmap.h"
#include "fidcache.h"
#include "offtree.h"
#include "slashrpc.h"
#include "slconfig.h"
#include "sliod.h"
#include "iod_bmap.h"

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
        bmap_2_iooftr(b) = offtree_create(SLASH_BMAP_SIZE, SLASH_BMAP_BLKSZ,
					  SLASH_BMAP_WIDTH, SLASH_BMAP_DEPTH,
					  f, sl_buffer_alloc, sl_oftm_addref,
					  sl_oftiov_pin_cb);
        psc_assert(bmap_2_iooftr(b));
	
	b->bcm_fcmh = f;
	b->bcm_blkno = bmapno;
	iod_biodi_init((struct bmap_iod_info *)b->bcm_pri);	
}

__static void
iod_bmap_free(struct bmapc_memb *b)
{
	struct bmap_iod_info *iobd;

	iobd = b->bcm_pri;
	offtree_destroy(bmap_2_iooftr(b));
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

	memcpy(&sbrw->sbdb, s, sizeof(*s));

	iov.iov_len = sizeof(struct slash_bmap_wire);
	iov.iov_base = 	bmap_2_biodi_wire(b) = 
		PSCALLOC(sizeof(struct slash_bmap_wire));

	rsx_bulkclient(rq, &desc, BULK_PUT_SINK, SRMC_BULK_PORTAL, iov, 1);
	
	rc = RSX_WAITREP(rq, mp);
        if (rc || mp->rc) {
                rc = rc ? rc : mp->rc;
		DEBUG_BMAP(PLL_ERROR, b, "req failed (%d)", rc);
		PSCFREE(bmap_2_biodi_wire(b));
		bmap_2_biodi_wire(b) = NULL;
		goto out;
        }
 out:
	b->bcm_mode &= ~BMAP_IOD_RETRIEVE;
	/* Unblock threads no matter what.
	 */
	psc_waitq_wait(&b->bcm_waitq, &b->bcm_lock);

	return (rc);
}

void
iod_oftrq_build(struct offtree_req *r, const struct bmapc_memb *b, 
		  uint32_t sblk, uint32_t nblks, int op)
{
	psc_assert((sblk * nblks) <= SLASH_BMAP_SIZE);
        psc_assert(op == OFTREQ_OP_WRITE || op == OFTREQ_OP_READ);

	r->oftrq_op = op;
	r->oftrq_bmap = b;
	r->oftrq_nblks = nblks;
	r->oftrq_op |= OFTREQ_OP_DIO;
	r->oftrq_off = sblk * SLASH_BMAP_SIZE;
	r->oftrq_len = nblks * SLASH_BMAP_SIZE;

	r->oftrq_darray = PSCALLOC(sizeof(struct dynarray));
        r->oftrq_root   = bmap_2_msoftr(b);
        r->oftrq_memb   = &r->oftrq_root->oftr_memb;
        r->oftrq_width  = r->oftrq_depth = 0;
        DEBUG_OFFTREQ(PLL_TRACE, r, "newly built request");	
}

void
iod_oftrq_destroy(struct offtree_req *r)
{
	struct bmapc_memb *b = r->oftrq_bmap;

        psc_assert(b);
        psc_assert(r->oftrq_darray);

        PSCFREE(r->oftrq_darray);
        atomic_dec(&b->bcm_opcnt);
        psc_assert(atomic_read(&b->bcm_opcnt) >= 0);
	/*
        if (r->oftrq_fill.oftfill_reqset) {
                pscrpc_set_destroy(r->oftrq_fill.oftfill_reqset);
                r->oftrq_fill.oftfill_reqset = NULL;
		}
	*/
}


/* Don't forget that this call must map offtree buffers for either sync or 
 *   source.
 */
int
iod_oftr_io(slfid_t fid, off_t off, size_t len, int op)
{
	int rc;

	rc = offtree_region_preprw(r);
	
}

struct fidc_membh *
iod_inode_lookup(slfid_t fid)
{
	int rc;
	struct fidc_membh *f;
	/* Note that these creds are bogus, just used to satisfy the current
	 *  fidc_lookup_load_inode() code.
	 */
	struct slash_creds creds = {0,0};

	rc = fidc_lookup_load_inode(fid, &creds, &f);	
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

	if (rw == SL_FREAD)
		f->fcmh_fcoo->fcoo_oref_rw[0]++;

	else if (rw == SL_FWRITE)
		f->fcmh_fcoo->fcoo_oref_rw[1]++;
	else
		psc_assert("rw mode=%d is invalid", rw);

	freelock(&c->fcmh_lock);

	if (c->fcmh_state & FCMH_FCOO_STARTING) {
		if (rw == SL_FWRITE) 
			oflags |= O_CREAT;

		rc = c->fcmh_fcoo->fcoo_fd = fid_fileops(fid, oflags);
                if (!rc)
                        fidc_fcoo_startdone(c);
                else
                        fidc_fcoo_startfailed(c);
        }
 out:
	if (rc)
		psc_error("failed rc=%d "FIDFMT, 
			  FIDFMTARGS(fcmh_2_fg(f)), rc);
        return (rc);
}

int
iod_bmap_load(struct fidc_membh *f, struct srt_bmapdesc_buf *sbdb, 
	      int rw, struct bmapc_memb **bmap)
{
	int rc=0;
	struct bmapc_memb *b;
	
	b = bmap_lookup_add(f, sbdb->sbs_bmapno, iod_bmap_init);

	spinlock(&b->bcm_lock);
	b->bcm_mode &= ~BMAP_INIT;

	if (rw == SL_READ) {
	retry_getcrcs:
		if (b->bcm_mode & BMAP_IOD_RETRIEVE) {
			psc_waitq_wait(&b->bcm_waitq, &b->bcm_lock);
			spinlock(&b->bcm_lock);
                        goto retry_getcrcs;

		} else {
			if (!bmap_2_biodi_wire(b)) {
				b->bcm_mode |= BMAP_IOD_RETRIEVE;
				freelock(&b->bcm_lock);
				/* Drop the lock prior to rpc.
				 */
				rc = iod_bmap_fetch_crcs(b, sbdb);
			} else
				freelock(&b->bcm_lock);
		}		

	} else if (rw == SL_WRITE) 
		freelock(&b->bcm_lock);

	else
		psc_assert("invalid rw mode (%d)", rw);

	return (rc);
}


