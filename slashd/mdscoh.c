
#include "psc_util/assert.h"
#include "psc_util/atomic.h"
#include "psc_ds/listcache.h"

#include "cache_params.h"
#include "mds.h"
#include "mdsexpc.h"
#include "../slashd/rpc.h"

struct psc_thread pndgCacheCbThread;
list_cache_t pndgBmapCbs, inflBmapCbs;
struct pscrpc_nbreqset *bmapCbSet;

#define CB_ARG_SLOT 0

__static int
mdscoh_reap(void)
{
	return(nbrequest_reap(&bmapCbSet));
}

static inline void
mdscoh_bmap_inflight_mode_check(int mode)
{
	if (mode & MEXPBCM_CIO_REQD) {
		psc_assert(!(mode & MEXPBCM_DIO_REQD));
		psc_assert(!(mode & MEXPBCM_CIO));
	} else if (mode & MEXPBCM_DIO_REQD) {
		psc_assert(!(mode & MEXPBCM_CIO_REQD));
		psc_assert(!(mode & MEXPBCM_DIO));
	} else
		psc_fatalx("Neither MEXPBCM_CIO_REQD or MEXPBCM_DIO_REQD set");
}

int 
mdscoh_cb(struct pscrpc_request *req, struct pscrpc_async_args *a)
{
	struct mexpbcm *bref=req->rq_async_args.pointer_arg[CB_ARG_SLOT];
	struct srm_bmap_dio_req *mq;
	struct srm_generic_rep *mp;
	int c;

	psc_assert(bref);
	
	mq = psc_msg_buf(req->rq_reqmsg, 0, sizeof(*mq));
	mp = psc_msg_buf(req->rq_repmsg, 0, sizeof(*mp));

	lc_del(&bref->mexpbcm_lentry, &inflBmapCbs);

	MEXPBCM_LOCK(bref);

	DEBUG_BMAP(PLL_TRACE, bref->mexpbcm_bmap, 
		   "bref=%p m=%u msgc=%u rc=%d", 
		   bref, mode, atomic_read(&bref->mexpbcm_msgcnt), mp->rc);
	/* XXX figure what to do here if the rc < 0
	 */
	psc_assert(mq->mode & bref->mexpbcm_mode);
	bref->mexpbcm_mode &= ~mq->dio;

	c = atomic_read(&bref->mexpbcm_msgcnt);
	if (!c) {
		if (bref->mexpbcm_mode & MEXPBCM_CIO_REQD) {
			psc_assert(bref->mexpbcm_mode & MEXPBCM_DIO);
			bref->mexpbcm_mode &= ~MEXPBCM_DIO;

		} else if (bref->mexpbcm_mode & MEXPBCM_DIO_REQD) {
			psc_assert(!(bref->mexpbcm_mode & MEXPBCM_DIO));
			bref->mexpbcm_mode |= MEXPBCM_DIO;

		} else
			psc_fatalx("Invalid mode %d", bref->mexpbcm_mode);
		
	} else if (c > 0) {
		/* The bref mode has changed again, verify the mode and 
		 *  requeue.
		 */
		mdscoh_bmap_inflight_mode_check(bref->mexpbcm_mode);
		lc_queue(&pndgBmapCbs, &bref->mexpbcm_lentry);

	} else if (c < 0) 
		psc_fatalx("Negative refcount (ouch) %d", c);

	MEXPBCM_ULOCK(bref);		
}


__static int
mdscoh_queue_req_locked(struct mexpbcm *bref)
{
        struct pscrpc_request *req;
	struct srm_bmap_dio_req *mq;
	struct srm_generic_rep *mp;
	struct slashrpc_export *sexp=bref->mexpbcm_export->exp_private;
	struct slashrpc_cservice *csvc=
		((struct mexp_cli *)sexp->sexp_data)->mexpc->mc_csvc;
	int rc=0, mode=bref->mexpbcm_mode;

	DEBUG_BMAP(PLL_TRACE, bref->mexpbcm_bmap, "bref=%p m=%u msgc=%u", 
		   bref, mode, atomic_read(&bref->mexpbcm_msgcnt));	

	psc_assert(atomic_read(&bref->mexpbcm_msgcnt) > 0);
	mdscoh_bmap_inflight_mode_check(mode);
		
	if (csvc->csvc_failed)
		return (-1);
	
	if (!csvc->csvc_initialized) {
		struct pscrpc_connection *c=
			csvc->csvc_import->imp_connection;
		rc = rpc_issue_connect(c->c_peer.nid, csvc->csvc_import, 
				       SRCM_MAGIC, SRCM_VERSION);
		if (rc)
			return (rc);
		else
			csvc->csvc_initialized = 1;
	}
	rc = rsx_newreq(csvc->csvc_import, SRCM_VERSION, SRMT_BMAPDIO, 
			sizeof(*mq), sizeof(*mp), &req, mq);
	if (rc)
		return (rc);	

	req->rq_async_args.pointer_arg[CB_ARG_SLOT] = bref;

	mq->fid = fcmh_2_fid(bref->mexpbcm_bmap->bcm_fcmh);
	mq->blkno = bref->mexpbcm_blkno;
	mq->dio = (mode & MEXPBCM_DIO_REQD) ? 
		MEXPBCM_DIO_REQD : MEXPBCM_CIO_REQD;

	nbreqset_add(bmapCbSet, req);
	lc_queue(&inflBmapCbs, &bref->mexpbcm_lentry);
	/* Note that this req has been sent.
	 */
	atomic_set(&bref->mexpbcm_msgcnt, 0);
	
	return (rc);
}

__static void
mdscohthr_begin(void)
{
        struct mexpbcm *bref;
	int rc;	

        while (1) {
                bref = lc_getwait(&pndgCacheCbs);

                MEXPBCM_LOCK(bref);
		rc = mdscoh_queue_req_locked(bref);
		if (rc)
			psc_fatalx("mdscoh_queue_req_locked() failed with "
				   "(rc==%d) for bref %p", rc, bref);
		MEXPBCM_ULOCK(bref);
		mdscoh_reap();
        }
}

void
mdscoh_init(void)
{
	lc_register(&pndgBmapCbs, "pendingBmapCbs");
	lc_register(&inflBmapCbs, "inflightBmapCbs");

	bmapCbSet = nbreqset_init(NULL, mdscoh_cb);
	pscthr_init(&pndgCacheCbThread, SLTHRT_MDSCOH, mdscohthr_begin,
                    PSCALLOC(sizeof(struct slash_mdscohthr)), "mdscohthr");
}
