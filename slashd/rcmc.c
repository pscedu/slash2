/* $Id$ */

/*
 * Routines for issuing RPC requests for CLIENT from MDS.
 */

#include <sys/param.h>

#include <dirent.h>

#include "psc_ds/vbitmap.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_util/lock.h"
#include "psc_util/thread.h"

#include "fid.h"
#include "inodeh.h"
#include "mds_bmap.h"
#include "mdsrpc.h"
#include "pathnames.h"
#include "repl_mds.h"
#include "slashd.h"
#include "slashrpc.h"
#include "slerr.h"

#include "zfs-fuse/zfs_slashlib.h"

struct vbitmap	 slrcmthr_uniqidmap = VBITMAP_INIT_AUTO;
psc_spinlock_t	 slrcmthr_uniqidmap_lock = LOCK_INITIALIZER;

uint64_t
sl_get_repls_inum(void)
{
	struct slash_fidgen fg;
	struct stat stb;
	int rc;

	rc = zfsslash2_lookup(zfsVfs, SL_ROOT_INUM,
	    SL_PATH_REPLS, &fg, &rootcreds, &stb);
	if (rc)
		psc_fatalx("lookup repldir: %s", slstrerror(rc));
	return (fg.fg_fid);
}

int
slrmcthr_replst_slave_eof(struct sl_replrq *rrq)
{
	struct srm_replst_slave_req *mq;
	struct srm_replst_slave_rep *mp;
	struct slmrcm_thread *srcm;
	struct pscrpc_request *rq;
	struct psc_thread *thr;
	int rc;

	thr = pscthr_get();
	srcm = slmrcmthr(thr);

	rc = RSX_NEWREQ(srcm->srcm_csvc->csvc_import,
	    SRCM_VERSION, SRMT_REPL_GETST_SLAVE, rq, mq, mp);
	if (rc)
		return (rc);

	mq->fid = REPLRQ_FID(rrq);
	mq->id = srcm->srcm_id;
	mq->rc = EOF;
	rc = RSX_WAITREP(rq, mp);
	pscrpc_req_finished(rq);
	return (rc);
}

int
slrmcthr_replst_slave_waitrep(struct pscrpc_request *rq)
{
	struct srm_replst_slave_req *mq;
	struct srm_replst_slave_rep *mp;
	struct pscrpc_bulk_desc *desc;
	struct slmrcm_thread *srcm;
	struct psc_thread *thr;
	struct iovec iov;
	int rc;

	thr = pscthr_get();
	srcm = slmrcmthr(thr);

	iov.iov_base = srcm->srcm_page;
	iov.iov_len = srcm->srcm_pagelen;

	mq = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq));
	mq->len = srcm->srcm_pagelen;
	rc = rsx_bulkclient(rq, &desc, BULK_GET_SOURCE,
	    SRCM_BULK_PORTAL, &iov, 1);
	if (rc == 0)
		rc = RSX_WAITREP(rq, mp);
	pscrpc_req_finished(rq);
	return (rc);
}

int
slrcmthr_walk_brepls(struct sl_replrq *rrq, struct bmapc_memb *bcm,
    sl_blkno_t n, struct pscrpc_request **rqp)
{
	struct srm_replst_slave_req *mq;
	struct srm_replst_slave_rep *mp;
	struct slmrcm_thread *srcm;
	struct psc_thread *thr;
	int len, rc;

	thr = pscthr_get();
	srcm = slmrcmthr(thr);

	rc = 0;
	len = howmany(rrq->rrq_inoh->inoh_ino.ino_nrepls *
	    SL_BITS_PER_REPLICA, NBBY);
	if (srcm->srcm_pagelen + len > SRM_REPLST_PAGESIZ) {
		if (*rqp) {
			rc = slrmcthr_replst_slave_waitrep(*rqp);
			if (rc)
				return (rc);
		}

		rc = RSX_NEWREQ(srcm->srcm_csvc->csvc_import,
		    SRCM_VERSION, SRMT_REPL_GETST_SLAVE, *rqp, mq, mp);
		if (rc)
			return (rc);
		mq->id = srcm->srcm_id;
		mq->boff = n;
		mq->fid = REPLRQ_FID(rrq);

		srcm->srcm_pagelen = 0;
	}
	memcpy(srcm->srcm_page + srcm->srcm_pagelen,
	    bmap_2_bmdsiod(bcm)->bh_repls, len);
	srcm->srcm_pagelen += len;
	return (rc);
}

/*
 * slrcm_issue_getreplst - issue a GETREPLST reply to a CLIENT from MDS.
 */
int
slrcm_issue_getreplst(struct sl_replrq *rrq, int is_eof)
{
	struct srm_replst_master_req *mq;
	struct srm_replst_master_rep *mp;
	struct slmrcm_thread *srcm;
	struct pscrpc_request *rq;
	struct psc_thread *thr;
	int rc;

	thr = pscthr_get();
	srcm = slmrcmthr(thr);

	rc = RSX_NEWREQ(srcm->srcm_csvc->csvc_import,
	    SRCM_VERSION, SRMT_REPL_GETST, rq, mq, mp);
	if (rc)
		return (rc);
	mq->id = srcm->srcm_id;
	if (rrq) {
		mq->fid = REPLRQ_FID(rrq);
		mq->nbmaps = REPLRQ_NBMAPS(rrq);
		mq->nrepls = REPLRQ_NREPLS(rrq);
		memcpy(mq->repls, REPLRQ_INO(rrq)->ino_repls,
		    MIN(mq->nrepls, INO_DEF_NREPLS) * sizeof(*mq->repls));
		if (mq->nrepls > INO_DEF_NREPLS)
			memcpy(mq->repls + INO_DEF_NREPLS,
			    rrq->rrq_inoh->inoh_extras->inox_repls,
			    (REPLRQ_NREPLS(rrq) - INO_DEF_NREPLS) *
			    sizeof(*mq->repls));
	}
	if (is_eof)
		mq->rc = EOF;
	rc = RSX_WAITREP(rq, mp);
	pscrpc_req_finished(rq);
	return (rc);
}

void *
slrcmthr_main(__unusedx void *arg)
{
	struct slmrcm_thread *srcm;
	struct pscrpc_request *rq;
	struct psc_thread *thr;
	struct bmapc_memb *bcm;
	struct sl_replrq *rrq;
	int rc, dummy;
	sl_blkno_t n;

	thr = pscthr_get();
	srcm = slmrcmthr(thr);
	srcm->srcm_page = PSCALLOC(SRM_REPLST_PAGESIZ);
	srcm->srcm_pagelen = SRM_REPLST_PAGESIZ;

	rc = 0;
	rq = NULL;
	if (srcm->srcm_fg.fg_fid == FID_ANY) {
		spinlock(&replrq_tree_lock);
		SPLAY_FOREACH(rrq, replrqtree, &replrq_tree) {
			slrcm_issue_getreplst(rrq, 0);
			for (n = 0; n < REPLRQ_NBMAPS(rrq); n++) {
				bcm = mds_bmap_load(REPLRQ_FCMH(rrq), n);
				BMAP_LOCK(bcm);
				rc = slrcmthr_walk_brepls(rrq, bcm, n, &rq);
				bmap_op_done(bcm);
				BMAP_ULOCK(bcm);
				if (rc)
					break;
			}
			if (rq)
				slrmcthr_replst_slave_waitrep(rq);
			slrmcthr_replst_slave_eof(rrq);
			if (rc)
				break;
		}
		freelock(&replrq_tree_lock);
	} else if ((rrq = mds_repl_findrq(&srcm->srcm_fg, &dummy)) != NULL) {
		slrcm_issue_getreplst(rrq, 0);
		for (n = 0; n < REPLRQ_NBMAPS(rrq); n++) {
			bcm = mds_bmap_load(REPLRQ_FCMH(rrq), n);
			BMAP_LOCK(bcm);
			rc = slrcmthr_walk_brepls(rrq, bcm, n, &rq);
			bmap_op_done(bcm);
				BMAP_ULOCK(bcm);
			if (rc)
				break;
		}
		if (rq)
			slrmcthr_replst_slave_waitrep(rq);
		slrmcthr_replst_slave_eof(rrq);
		mds_repl_unrefrq(rrq);
	}

	/* signal EOF */
	slrcm_issue_getreplst(NULL, 1);

	free(srcm->srcm_page);

	spinlock(&slrcmthr_uniqidmap_lock);
	vbitmap_unset(&slrcmthr_uniqidmap, srcm->srcm_uniqid);
	vbitmap_setnextpos(&slrcmthr_uniqidmap, 0);
	freelock(&slrcmthr_uniqidmap_lock);
	return (NULL);
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
