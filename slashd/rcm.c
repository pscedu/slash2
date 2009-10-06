/* $Id$ */

/*
 * Routines for issuing RPC requests for CLIENT from MDS.
 */

#include "psc_ds/vbitmap.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_util/spinlock.h"
#include "psc_util/thread.h"

#include "fid.h"
#include "slashdthr.h"
#include "slashrpc.h"

struct vbitmap	*slrcmthr_uniqidmap;
psc_spinlock_t	 slrcmthr_uniqidmap_lock;

void *
slrcmthr_main(__unusedx void *arg)
{
	struct slash_rcmthr *srcm;
	struct psc_thread *thr;

	thr = pscthr_get();
	srcm = slrcmthr(thr);
	spinlock(&slrcmthr_uniqidmap_lock);
	vbitmap_unset(slrcmthr_uniqidmap, srcm->srcm_uniqid);
	vbitmap_setnextpos(slrcmthr_uniqidmap, 0);
	freelock(&slrcmthr_uniqidmap_lock);
	return (NULL);
}

/*
 * slrcm_issue_getreplst - issue a GETREPLST reply to a CLIENT from MDS.
 */
int
slrcm_issue_getreplst(struct pscrpc_import *imp, slfid_t fid,
    sl_blkno_t bmapno, int32_t id, char repls[SL_REPLICA_NBYTES],
    int last)
{
	struct srm_replst_req *mq;
	struct srm_replst_rep *mp;
	struct pscrpc_request *rq;
	int rc;

	if ((rc = RSX_NEWREQ(imp, SRCM_VERSION,
	    SRMT_GETREPLST, rq, mq, mp)) != 0)
		return (rc);
	mq->ino = fid;
	mq->bmapno = bmapno;
	mq->id = id;
	mq->last = last;
	memcpy(mq->repls, repls, sizeof(mq->repls));
	rc = RSX_WAITREP(rq, mp);
	pscrpc_req_finished(rq);
	return (rc);
}

/*
 * slrcm_issue_releasebmap - issue a RELEASEBMAP request to a CLIENT from MDS.
 */
int
slrcm_issue_releasebmap(struct pscrpc_import *imp)
{
	struct srm_releasebmap_req *mq;
	struct srm_generic_rep *mp;
	struct pscrpc_request *rq;
	int rc;

	if ((rc = RSX_NEWREQ(imp, SRCM_VERSION,
	    SRMT_RELEASEBMAP, rq, mq, mp)) != 0)
		return (rc);
	if ((rc = RSX_WAITREP(rq, mp)) == 0)
		rc = mp->rc;
	pscrpc_req_finished(rq);
	return (rc);
}
