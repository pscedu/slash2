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
#include "pathnames.h"
#include "rpc.h"
#include "slashd.h"
#include "slashdthr.h"
#include "slashrpc.h"
#include "util.h"

#include "zfs-fuse/zfs_slashlib.h"

struct vbitmap	 slrcmthr_uniqidmap = VBITMAP_INIT_AUTO;
psc_spinlock_t	 slrcmthr_uniqidmap_lock = LOCK_INITIALIZER;

struct slash_creds rootcreds = { 0, 0 };

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

void *
slrcmthr_main(__unusedx void *arg)
{
	struct slash_rcmthr *srcm;
	struct psc_thread *thr;
	struct sl_replrq *rrq;
	int rc, dummy;

	thr = pscthr_get();
	srcm = slrcmthr(thr);

	rc = 1;
	if (srcm->srcm_fg.fg_fid == FID_ANY) {
		spinlock(&replrq_tree_lock);
		SPLAY_FOREACH(rrq, replrqtree, &replrq_tree) {
			rc = slrcm_issue_getreplst(srcm->srcm_csvc->csvc_import,
			    rrq->rrq_inoh->inoh_ino.ino_fg.fg_fid, srcm->srcm_id,
			    0, 0, 0);
			if (!rc)
				break;
		}
		freelock(&replrq_tree_lock);
	} else {
		rrq = mds_replrq_find(&srcm->srcm_fg, &dummy);
		if (rrq)
			rc = slrcm_issue_getreplst(srcm->srcm_csvc->csvc_import,
			    rrq->rrq_inoh->inoh_ino.ino_fg.fg_fid, srcm->srcm_id,
			    0, 0, 0);
	}

	/* signal EOF */
	if (!rc)
		slrcm_issue_getreplst(srcm->srcm_csvc->csvc_import,
		    0, srcm->srcm_id, 0, 0, 1);

	spinlock(&slrcmthr_uniqidmap_lock);
	vbitmap_unset(&slrcmthr_uniqidmap, srcm->srcm_uniqid);
	vbitmap_setnextpos(&slrcmthr_uniqidmap, 0);
	freelock(&slrcmthr_uniqidmap_lock);
	return (NULL);
}

/*
 * slrcm_issue_getreplst - issue a GETREPLST reply to a CLIENT from MDS.
 */
int
slrcm_issue_getreplst(struct pscrpc_import *imp, slfid_t fid, int32_t id,
    int bold, int bact, int last)
{
	struct srm_replst_req *mq;
	struct srm_replst_rep *mp;
	struct pscrpc_request *rq;
	int rc;

	if ((rc = RSX_NEWREQ(imp, SRCM_VERSION,
	    SRMT_GETREPLST, rq, mq, mp)) != 0)
		return (rc);
	mq->ino = fid;
	mq->id = id;
	mq->last = last;
	mq->st_bold = bold;
	mq->st_bact = bact;
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
