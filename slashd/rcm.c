/* $Id$ */

/*
 * Routines for issuing RPC requests for CLIENT from MDS.
 */

#include <dirent.h>

#include "psc_ds/vbitmap.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_util/lock.h"
#include "psc_util/thread.h"

#include "fid.h"
#include "pathnames.h"
#include "rpc.h"
#include "slashdthr.h"
#include "slashrpc.h"
#include "util.h"
#include "zfs-fuse/zfs_slashlib.h"

struct vbitmap	*slrcmthr_uniqidmap = VBITMAP_INIT_AUTO;
psc_spinlock_t	 slrcmthr_uniqidmap_lock = LOCK_INITIALIZER;

#define SL_ROOT_INUM 1
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
	char *buf, fn[NAME_MAX + 1];
	struct slash_rcmthr *srcm;
	struct slash_fidgen fg;
	struct psc_thread *thr;
	struct dirent *d;
	struct stat stb;
	size_t siz, tsiz;
	off_t off, toff;
	uint16_t inum;
	int rc, trc;
	void *data;

	thr = pscthr_get();
	srcm = slrcmthr(thr);

	off = 0;
	siz = 8 * 1024;
	buf = PSCALLOC(siz);

	inum = sl_get_repls_inum();
	rc = zfsslash2_opendir(zfsVfs, inum,
	    &rootcreds, &fg, &stb, &data);
	for (;;) {
		rc = zfsslash2_readdir(zfsVfs, inum, &rootcreds,
		    INT_MAX, off, buf, &tsiz, NULL, 0, data);
		if (rc)
			break;

		for (toff = 0; toff < (off_t)tsiz;
		    off += d->d_reclen, toff += d->d_reclen) {
			d = (void *)(buf + toff);

			psc_assert(d->d_reclen > 0);
			if (d->d_name[0] == '.' ||
			    d->d_fileno == 0)
				continue;

			snprintf(fn, sizeof(buf), "%016"PRIx64"",
			    srcm->srcm_fid);
			if (srcm->srcm_fid == FID_ANY ||
			    strcmp(d->d_name, fn) == 0) {
				rc = slrcm_issue_getreplst(srcm->srcm_csvc->csvc_import,
				    srcm->srcm_fid, srcm->srcm_id, 0, 0, 0);
				if (rc)
					break;
			}
		}
	}
	trc = zfsslash2_release(zfsVfs, inum, &rootcreds, data);
	if (rc == 0)
		rc = trc;

	free(buf);

	/* signal EOF */
	slrcm_issue_getreplst(srcm->srcm_csvc->csvc_import,
	    0, srcm->srcm_id, 0, 0, 1);

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
