/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2007-2018, Pittsburgh Supercomputing Center
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or (at
 * your option) any later version.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License contained in the file
 * `COPYING-GPL' at the top of this distribution or at
 * https://www.gnu.org/licenses/gpl-2.0.html for more details.
 * ---------------------------------------------------------------------
 * %END_LICENSE%
 */

/*
 * Interface for controlling live operation of a mount_slash instance.
 */

#include <sys/param.h>
#include <sys/socket.h>

#include "pfl/cdefs.h"
#include "pfl/ctl.h"
#include "pfl/ctlsvr.h"
#include "pfl/fs.h"
#include "pfl/net.h"
#include "pfl/rpc.h"
#include "pfl/rsx.h"
#include "pfl/str.h"

#include "bmap.h"
#include "bmap_cli.h"
#include "ctl.h"
#include "ctl_cli.h"
#include "ctlsvr.h"
#include "fidc_cli.h"
#include "mount_slash.h"
#include "pathnames.h"
#include "rpc_cli.h"
#include "slashrpc.h"
#include "subsys_cli.h"
#include "slerr.h"

#include "slashd/inode.h"

struct psc_thread	*msl_ctlthr0;
void			*msl_ctlthr0_private;

/*
 * We use the same ID to attribute different RPCs to the same request.
 */
psc_atomic32_t		 msctl_repl_id = PSC_ATOMIC32_INIT(0);
struct psc_lockedlist	 msctl_replsts = PLL_INIT(&msctl_replsts,
    struct msctl_replstq, mrsq_lentry);

int
msctl_getcreds(int s, struct pscfs_creds *pcrp)
{
	uid_t uid;
	gid_t gid;
	int rc;

	rc = pfl_socket_getpeercred(s, &uid, &gid);
	pcrp->pcr_uid = uid;
	pcrp->pcr_gid = gid;
	pcrp->pcr_ngid = 1;
	uidmap_ext_cred(pcrp);
	gidmap_ext_cred(pcrp);
	return (rc);
}

int
msctl_getclientctx(__unusedx int s, struct pscfs_clientctx *pfcc)
{
	pfcc->pfcc_pid = -1;
	return (0);
}

char *
fill_iosbuf(struct msctlmsg_replrq *mrq, char *buf, size_t len)
{
	size_t i, adj = 0;
	int rc;

	for (i = 0; i < mrq->mrq_nios; i++) {
		rc = snprintf(buf + adj, len - adj, "%s%s",
		    i ? "," : "", mrq->mrq_iosv[i]);
		if (rc == -1)
			break;
		adj += rc;
	}
	return (buf);
}

int
msctlrep_replrq(int fd, struct psc_ctlmsghdr *mh, void *m)
{
	sl_replica_t repls[SL_MAX_REPLICAS];
	struct slrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct msctlmsg_replrq *mrq = m;
	struct pscfs_clientctx pfcc;
	struct srm_replrq_req *mq;
	struct srm_replrq_rep *mp;
	struct pscfs_creds pcr;
	struct fidc_membh *f;
	struct sl_fidgen fg;
	uint32_t n, nrepls = 0;
	char *res_name;
	int rc;
	sl_bmapno_t bno;
	struct bmap *b;

	rc = msctl_getcreds(fd, &pcr);
	if (rc)
		return (psc_ctlsenderr(fd, mh, NULL,
		    SLPRI_FID": unable to obtain credentials: %s",
		    mrq->mrq_fid, strerror(rc)));

	/*
	 * Reject if the user is not root and replication add/del
	 * is not enabled.
	 */
	if (pcr.pcr_uid && !msl_repl_enable)
		return (psc_ctlsenderr(fd, mh, NULL,
		    "replication op disabled for non-root users: %s", 
		    strerror(EACCES)));

	if (mrq->mrq_nios < 1 ||
	    mrq->mrq_nios >= nitems(mrq->mrq_iosv))
		return (psc_ctlsenderr(fd, mh, NULL,
		    "replication request: %s", strerror(EINVAL)));

	rc = msctl_getclientctx(fd, &pfcc);
	if (rc)
		return (psc_ctlsenderr(fd, mh, NULL,
		    SLPRI_FID": unable to obtain client context: %s",
		    mrq->mrq_fid, strerror(rc)));

	rc = msl_fcmh_load_fid(mrq->mrq_fid, &f, NULL);
	if (rc)
		return (psc_ctlsenderr(fd, mh, NULL, SLPRI_FID": %s",
		    mrq->mrq_fid, strerror(rc)));

	FCMH_LOCK(f);
	if (fcmh_isreg(f) || fcmh_isdir(f)) {
		rc = fcmh_checkcreds(f, NULL, &pcr, W_OK);
		/* Allow owner to replicate read-only files. */
		if (rc == EACCES &&
		    f->fcmh_sstb.sst_uid == pcr.pcr_uid)
			rc = 0;
	} else
		rc = ENOTSUP;
	fg = f->fcmh_fg;

	/*
	 * Give up write lease ASAP to allow replication to proceed.
	 */
	for (bno = 0; bno < fcmh_2_nbmaps(f); bno++) {
		rc = bmap_getf(f, bno, SL_WRITE, BMAPGETF_NORETRIEVE, &b);
		if (rc == ENOENT) {
			rc = 0;
			continue;
		}
		if (!(b->bcm_flags & BMAPF_WR)) {
			BMAP_ULOCK(b);
			continue;
		}
		b->bcm_flags |= BMAPF_LEASEEXPIRE;
		msl_bmap_cache_rls(b);
		bmap_op_done(b);
	}
	fcmh_op_done(f);

	if (rc)
		return (psc_ctlsenderr(fd, mh, NULL, SLPRI_FID": %s",
		    mrq->mrq_fid, strerror(rc)));

	/* 
	 * Parse I/O systems specified. We could allow an unknown I/O server
	 * to be passed to the MDS.  If so, remember the IOS ID also encodes
	 * the site ID and the MDS must take corresponding steps as well.
	 *
	 * It is rare, but an I/O server do retire and its ID is taken out of 
	 * the config file. However, its ID should never be reused.
	 */ 
	for (n = 0; n < mrq->mrq_nios; n++, nrepls++) {
		res_name = mrq->mrq_iosv[n];
		if ((repls[n].bs_id = libsl_str2id(res_name)) == IOS_ID_ANY) {
			rc = psc_ctlsenderr(fd, mh, NULL,
			    "%s: unknown I/O system", mrq->mrq_iosv[n]);
			goto out;
		}
	}

 again: 
	if (mh->mh_type == MSCMT_ADDREPLRQ)
		MSL_RMC_NEWREQ(f, csvc, SRMT_REPL_ADDRQ, rq, mq,
		    mp, rc, 0);
	else
		MSL_RMC_NEWREQ(f, csvc, SRMT_REPL_DELRQ, rq, mq,
		    mp, rc, 0);
	if (rc) {
		rc = psc_ctlsenderr(fd, mh, NULL, SLPRI_FID": %s",
		    mrq->mrq_fid, strerror(rc));
		goto out;
	}

	memcpy(&mq->fg, &fg, sizeof(mq->fg));
	memcpy(&mq->repls, repls, sizeof(mq->repls));
	mq->nrepls = nrepls;
	mq->bmapno = mrq->mrq_bmapno;
	mq->nbmaps = mrq->mrq_nbmaps;

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc == 0)
		rc = mp->rc;
	if (rc) {
		if (rc == -PFLERR_WOULDBLOCK) {
			if (mp->nbmaps_processed < mrq->mrq_nbmaps) {
				mrq->mrq_bmapno += mp->nbmaps_processed;
				mrq->mrq_nbmaps -= mp->nbmaps_processed;
				goto again;
			}
			rc = psc_ctlsenderr(fd, mh, NULL, SLPRI_FID": "
			    "invalid reply received from MDS",
			    mrq->mrq_fid);
		} else
			rc = psc_ctlsenderr(fd, mh, NULL, SLPRI_FID": %s",
			    mrq->mrq_fid, strerror(rc));
	} else {
		char iosbuf[LINE_MAX];

		psclogs(PLL_INFO, SLCSS_INFO,
		    "repl-%s fid="SLPRI_FID" "
		    "ios=%s bno=%d",
		    mh->mh_type == MSCMT_ADDREPLRQ ? "add" : "remove",
		    fcmh_2_fid(f), fill_iosbuf(mrq, iosbuf,
		    sizeof(iosbuf)), mrq->mrq_bmapno);
		rc = 1;
	}

 out:
	pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
	return (rc);
}

int
msctlrep_getreplst(int fd, struct psc_ctlmsghdr *mh, void *m)
{
	struct slrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_replst_master_req *mq;
	struct srm_replst_master_rep *mp;
	struct msctlmsg_replrq *mrq = m;
	struct fidc_membh *f = NULL;
	struct pscfs_clientctx pfcc;
	struct msctl_replstq mrsq;
	struct pscfs_creds pcr;
	struct sl_fidgen fg;
	int added = 0, rc;

	struct psc_thread *thr;
	struct psc_ctlthr *pct;
	struct pfl_ctl_data *pcd;
	struct pfl_mutex *fdlock;

	thr = pscthr_get();
	pct = psc_ctlthr(thr);
	pcd = pct->pct_ctldata;
	fdlock = &pcd->pcd_mutex;

	if (mrq->mrq_fid == FID_ANY) {
		fg.fg_fid = FID_ANY;
		fg.fg_gen = FGEN_ANY;
		OPSTAT_INCR("getreplst-any");
		goto issue;
	}
	OPSTAT_INCR("getreplst-file");

	rc = msctl_getcreds(fd, &pcr);
	if (rc)
		return (psc_ctlsenderr(fd, mh, NULL,
		    SLPRI_FID": unable to obtain credentials: %s",
		    mrq->mrq_fid, strerror(rc)));
	rc = msctl_getclientctx(fd, &pfcc);
	if (rc)
		return (psc_ctlsenderr(fd, mh, NULL,
		    SLPRI_FID": unable to obtain client context: %s",
		    mrq->mrq_fid, strerror(rc)));

	rc = msl_fcmh_load_fid(mrq->mrq_fid, &f, NULL);
	if (rc)
		return (psc_ctlsenderr(fd, mh, NULL, SLPRI_FID": %s",
		    mrq->mrq_fid, strerror(rc)));

	FCMH_LOCK(f);
	if (fcmh_isreg(f) || fcmh_isdir(f))
		rc = fcmh_checkcreds(f, NULL, &pcr, R_OK);
	else
		rc = ENOTSUP;
	fg = f->fcmh_fg;
	fcmh_op_done(f);

	if (rc)
		return (psc_ctlsenderr(fd, mh, NULL, SLPRI_FID": %s",
		    mrq->mrq_fid, strerror(rc)));

 issue:

	/* handled by slm_rmc_handle_getreplst() of the MDS */
	MSL_RMC_NEWREQ(f, csvc, SRMT_REPL_GETST, rq, mq, mp, rc, 0);
	if (rc) {
		rc = psc_ctlsenderr(fd, mh, NULL, SLPRI_FID": %s",
		    fg.fg_fid, strerror(rc));
		goto out;
	}

	mq->fg = fg;
	mq->id = psc_atomic32_inc_getnew(&msctl_repl_id);

	memset(&mrsq, 0, sizeof(mrsq));
	INIT_PSC_LISTENTRY(&mrsq.mrsq_lentry);
	INIT_SPINLOCK(&mrsq.mrsq_lock);
	psc_waitq_init(&mrsq.mrsq_waitq, "msrq");
	mrsq.mrsq_id = mq->id;
	mrsq.mrsq_fd = fd;
	mrsq.mrsq_fdlock = fdlock;
	mrsq.mrsq_fid = mrq->mrq_fid;
	mrsq.mrsq_mh = mh;

	/*
 	 * Let us be found in mrsq_lookup(). If the connection
 	 * is dropped, our reference count will be dropped for
 	 * us so we won't stuck forever.
 	 */
	pll_add(&msctl_replsts, &mrsq);
	added = 1;

	psclog_diag("add: mrsq@%p: fd = %d, id = %d.", &mrsq, fd, mq->id);

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc == 0)
		rc = mp->rc;
	if (rc) {
		rc = psc_ctlsenderr(fd, mh, NULL, SLPRI_FID": %s",
		    fg.fg_fid, strerror(rc));
		goto out;
	}

	/*
	 * Don't wait forever. Otherwise, we will tie up all control threads.
	 */
	spinlock(&mrsq.mrsq_lock);
	rc = psc_waitq_waitrel_s(&mrsq.mrsq_waitq, &mrsq.mrsq_lock, 60);
	spinlock(&mrsq.mrsq_lock);
	if (!mrsq.mrsq_rc) {
		OPSTAT_INCR("getreplst-timeouts");
		mrsq.mrsq_rc = rc;
	}

	rc = 1;
	if (mrsq.mrsq_rc && mrsq.mrsq_rc != EOF)
		rc = psc_ctlsenderr(fd, mh, NULL, SLPRI_FID": %s",
		    fg.fg_fid, strerror(mrsq.mrsq_rc));

 out:
	if (added) {
		pll_remove(&msctl_replsts, &mrsq);
		psc_waitq_destroy(&mrsq.mrsq_waitq);
	}
	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
	return (rc);
}

int
msctlhnd_get_fattr(int fd, struct psc_ctlmsghdr *mh, void *m)
{
	struct msctlmsg_fattr *mfa = m;
	struct pscfs_clientctx pfcc;
	struct fidc_membh *f = NULL;
	struct pscfs_creds pcr;
	int rc;

	rc = msctl_getcreds(fd, &pcr);
	if (rc)
		return (psc_ctlsenderr(fd, mh, NULL,
		    SLPRI_FID": unable to obtain credentials: %s",
		    mfa->mfa_fid, strerror(rc)));
	rc = msctl_getclientctx(fd, &pfcc);
	if (rc)
		return (psc_ctlsenderr(fd, mh, NULL,
		    SLPRI_FID": unable to obtain client context: %s",
		    mfa->mfa_fid, strerror(rc)));

	rc = msl_fcmh_load_fid(mfa->mfa_fid, &f, NULL);
	if (rc)
		return (psc_ctlsenderr(fd, mh, NULL, SLPRI_FID": %s",
		    mfa->mfa_fid, strerror(rc)));

	FCMH_LOCK(f);
	if (fcmh_isreg(f) || fcmh_isdir(f))
		rc = fcmh_checkcreds(f, NULL, &pcr, R_OK);
	else
		rc = ENOTSUP;
	FCMH_ULOCK(f);

	if (rc == 0)
		rc = msl_fcmh_fetch_inode(f);

	if (rc) {
		rc = psc_ctlsenderr(fd, mh, NULL, SLPRI_FID": %s",
		    mfa->mfa_fid, strerror(rc));
		goto out;
	}

	FCMH_LOCK(f);
	switch (mfa->mfa_attrid) {
	case SL_FATTR_IOS_AFFINITY:
		mfa->mfa_val = !!(fcmh_2_fci(f)->fci_inode.flags &
		    INOF_IOS_AFFINITY);
		break;
	case SL_FATTR_REPLPOL:
		mfa->mfa_val = fcmh_2_fci(f)->fci_inode.newreplpol;
		break;
	default:
		rc = psc_ctlsenderr(fd, mh, NULL, SLPRI_FID": %s",
		    mfa->mfa_fid, strerror(rc));
		goto out;
	}
	FCMH_ULOCK(f);

	rc = psc_ctlmsg_sendv(fd, mh, mfa, NULL);

 out:
	if (f)
		fcmh_op_done(f);
	return (rc);
}

int
msctlhnd_set_fattr(int fd, struct psc_ctlmsghdr *mh, void *m)
{
	struct slrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct msctlmsg_fattr *mfa = m;
	struct srm_set_fattr_req *mq;
	struct srm_set_fattr_rep *mp;
	struct pscfs_clientctx pfcc;
	struct sl_fidgen fg;
	struct pscfs_creds pcr;
	struct fidc_membh *f;
	int rc;

	rc = msctl_getcreds(fd, &pcr);
	if (rc)
		return (psc_ctlsenderr(fd, mh, NULL,
		    SLPRI_FID": unable to obtain credentials: %s",
		    mfa->mfa_fid, strerror(rc)));
	rc = msctl_getclientctx(fd, &pfcc);
	if (rc)
		return (psc_ctlsenderr(fd, mh, NULL,
		    SLPRI_FID": unable to obtain client context: %s",
		    mfa->mfa_fid, strerror(rc)));

	rc = msl_fcmh_load_fid(mfa->mfa_fid, &f, NULL);
	if (rc)
		return (psc_ctlsenderr(fd, mh, NULL, SLPRI_FID": %s",
		    mfa->mfa_fid, strerror(rc)));

	FCMH_LOCK(f);
	if (fcmh_isreg(f) || fcmh_isdir(f))
		rc = fcmh_checkcreds(f, NULL, &pcr, W_OK);
	else
		rc = ENOTSUP;
	fg = f->fcmh_fg;
	fcmh_op_done(f);

	if (rc)
		return (psc_ctlsenderr(fd, mh, NULL, SLPRI_FID": %s",
		    mfa->mfa_fid, strerror(rc)));

	MSL_RMC_NEWREQ(f, csvc, SRMT_SET_FATTR, rq, mq, mp, rc, 0);
	if (rc) {
		rc = psc_ctlsenderr(fd, mh, NULL, SLPRI_FID": %s",
		    mfa->mfa_fid, strerror(rc));
		goto out;
	}
	mq->attrid = mfa->mfa_attrid;
	mq->val = mfa->mfa_val;
	mq->fg = fg;
	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		rc = psc_ctlsenderr(fd, mh, NULL, "%s: %s",
		    mfa->mfa_fid, strerror(rc));

 out:
	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
	return (rc);
}

int
msctlhnd_set_bmapreplpol(int fd, struct psc_ctlmsghdr *mh, void *m)
{
	struct msctlmsg_bmapreplpol *mfbrp = m;
	struct slrpc_cservice *csvc = NULL;
	struct srm_set_bmapreplpol_req *mq;
	struct srm_set_bmapreplpol_rep *mp;
	struct pscrpc_request *rq = NULL;
	struct pscfs_clientctx pfcc;
	struct pscfs_creds pcr;
	struct fidc_membh *f;
	struct sl_fidgen fg;
	int rc;

	rc = msctl_getcreds(fd, &pcr);
	if (rc)
		return (psc_ctlsenderr(fd, mh, NULL,
		    SLPRI_FID": unable to obtain credentials: %s",
		    mfbrp->mfbrp_fid, strerror(rc)));
	rc = msctl_getclientctx(fd, &pfcc);
	if (rc)
		return (psc_ctlsenderr(fd, mh, NULL,
		    SLPRI_FID": unable to obtain client context: %s",
		    mfbrp->mfbrp_fid, strerror(rc)));

	rc = msl_fcmh_load_fid(mfbrp->mfbrp_fid, &f, NULL);
	if (rc)
		return (psc_ctlsenderr(fd, mh, NULL, SLPRI_FID": %s",
		    mfbrp->mfbrp_fid, strerror(rc)));

	FCMH_LOCK(f);
	if (fcmh_isreg(f))
		rc = fcmh_checkcreds(f, NULL, &pcr, W_OK);
	else
		rc = ENOTSUP;
	fg = f->fcmh_fg;
	fcmh_op_done(f);

	if (rc)
		return (psc_ctlsenderr(fd, mh, NULL, SLPRI_FID": %s",
		    mfbrp->mfbrp_fid, strerror(rc)));

	MSL_RMC_NEWREQ(f, csvc, SRMT_SET_BMAPREPLPOL, rq, mq, mp, rc, 0);
	if (rc) {
		rc = psc_ctlsenderr(fd, mh, NULL, SLPRI_FID": %s",
		    mfbrp->mfbrp_fid, strerror(rc));
		goto out;
	}
	mq->pol = mfbrp->mfbrp_pol;
	mq->bmapno = mfbrp->mfbrp_bmapno;
	mq->nbmaps = mfbrp->mfbrp_nbmaps;
	mq->fg = fg;
	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		rc = psc_ctlsenderr(fd, mh, NULL, SLPRI_FID": %s",
		    mfbrp->mfbrp_fid, strerror(rc));

 out:
	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
	return (rc);
}

void
msctlparam_mds_get(char buf[PCP_VALUE_MAX])
{
	strlcpy(buf, msl_rmc_resm->resm_name, PCP_VALUE_MAX);
}

void
msctlparam_prefios_get(char buf[PCP_VALUE_MAX])
{
	struct sl_resource *r;

	r = libsl_id2res(msl_pref_ios);
	if (r)
		strlcpy(buf, r->res_name, PCP_VALUE_MAX);
	else
		strlcpy(buf, "N/A", PCP_VALUE_MAX);
}

int
msctlparam_prefios_set(const char *val)
{
	struct sl_resource *r;

	r = libsl_str2res(val);
	if (r == NULL)
		return (-1);
	slc_setprefios(r->res_id);
	return (0);
}

void
msctlparam_map_get(char buf[PCP_VALUE_MAX])
{
	snprintf(buf, PCP_VALUE_MAX, "%d", msl_map_enable);
}

int
msctlparam_map_set(const char *val)
{
	int newval;

	newval = strtol(val, NULL, 0);
	if (newval != 0 && newval != 1)
		return (1);
	if (!msl_has_mapfile && newval == 1)
		return (1);
	msl_map_enable = newval;
	return (0);
}

void
slctlparam_max_pages_get(char *val)
{
	snprintf(val, PCP_VALUE_MAX, "%d", msl_predio_max_pages);
}

int
slctlparam_max_pages_set(const char *val)
{
	int newval;

	newval = strtol(val, NULL, 0);
	if (newval < 0 || newval > 2*SLASH_BMAP_SIZE/BMPC_BUFSZ)
		return (1);
	msl_predio_max_pages = newval;
	return (0);
}

int
slctlmsg_bmap_send(int fd, struct psc_ctlmsghdr *mh,
    struct slctlmsg_bmap *scb, struct bmap *b)
{
	struct bmap_cli_info *bci;
	const char *res;
	sl_ios_id_t id;

	bci = bmap_2_bci(b);
	scb->scb_fg = b->bcm_fcmh->fcmh_fg;
	scb->scb_bno = b->bcm_bmapno;
	scb->scb_opcnt = psc_atomic32_read(&b->bcm_opcnt);
	scb->scb_flags = b->bcm_flags;

	id = bci->bci_sbd.sbd_ios;
	if (pfl_memchk(&bci->bci_sbd, 0, sizeof(bci->bci_sbd)))
		res = "<init>";
	else if (id == IOS_ID_ANY)
		res = "<any>";
	else
		res = libsl_id2res(id)->res_name;
	strlcpy(scb->scb_resname, res, sizeof(scb->scb_resname));
	scb->scb_addr = (long)b;
	return (psc_ctlmsg_sendv(fd, mh, scb, NULL));
}

int
msctlmsg_biorq_send(int fd, struct psc_ctlmsghdr *mh,
    struct msctlmsg_biorq *msr, struct bmpc_ioreq *r)
{
	sl_ios_id_t id = r->biorq_last_sliod;

	BIORQ_LOCK(r);
	memset(msr, 0, sizeof(*msr));
	msr->msr_fid = r->biorq_bmap->bcm_fcmh->fcmh_sstb.sst_fid;
	msr->msr_bno = r->biorq_bmap->bcm_bmapno;
	msr->msr_ref = r->biorq_ref;
	msr->msr_off = r->biorq_off;
	msr->msr_len = r->biorq_len;
	msr->msr_flags = r->biorq_flags;
	msr->msr_retries = r->biorq_retries;
	strlcpy(msr->msr_last_sliod, id == IOS_ID_ANY ? "<any>" :
	    libsl_id2res(id)->res_name, sizeof(msr->msr_last_sliod));
	msr->msr_expire.tv_sec = r->biorq_expire.tv_sec;
	msr->msr_expire.tv_nsec = r->biorq_expire.tv_nsec;
	msr->msr_npages = psc_dynarray_len(&r->biorq_pages);
	msr->msr_addr = (long)r;
	BIORQ_ULOCK(r);

	return (psc_ctlmsg_sendv(fd, mh, msr, NULL));
}

/* See also ms_biorq_prdat() in msctl.c */
int
msctlrep_getbiorq(int fd, struct psc_ctlmsghdr *mh, void *m)
{
	struct msctlmsg_biorq *msr = m;
	struct bmap_pagecache *bmpc;
	struct psc_hashbkt *hb;
	struct fidc_membh *f;
	struct bmpc_ioreq *r;
	struct bmap *b;
	int rc = 1;

	PSC_HASHTBL_FOREACH_BUCKET(hb, &sl_fcmh_hashtbl) {
		psc_hashbkt_lock(hb);
		PSC_HASHBKT_FOREACH_ENTRY(&sl_fcmh_hashtbl, f, hb) {
			pfl_rwlock_rdlock(&f->fcmh_rwlock);
			RB_FOREACH(b, bmaptree, &f->fcmh_bmaptree) {

				BMAP_LOCK(b);

				bmpc = bmap_2_bmpc(b);
				PLL_FOREACH(r, &bmpc->bmpc_pndg_biorqs) {
					rc = msctlmsg_biorq_send(fd, mh,
					    msr, r);
					if (!rc)
						break;
				}
				if (!rc) {
					BMAP_ULOCK(b);
					break;
				}

				BMAP_ULOCK(b);
				if (!rc)
					break;
			}
			pfl_rwlock_unlock(&f->fcmh_rwlock);
			if (!rc)
				break;
		}
		psc_hashbkt_unlock(hb);
		if (!rc)
			break;
	}
	return (rc);
}

int
msctlmsg_bmpce_send(int fd, struct psc_ctlmsghdr *mh,
    struct msctlmsg_bmpce *mpce, struct bmap *b,
    struct bmap_pagecache_entry *e)
{
	// XXX lock
	memset(mpce, 0, sizeof(*mpce));
	mpce->mpce_fid = b->bcm_fcmh->fcmh_sstb.sst_fid;
	mpce->mpce_bno = b->bcm_bmapno;
	mpce->mpce_ref = e->bmpce_ref;
	mpce->mpce_flags = e->bmpce_flags;
	mpce->mpce_off = e->bmpce_off;
	mpce->mpce_start = e->bmpce_start;
	mpce->mpce_nwaiters =e->bmpce_waitq ?
	    psc_waitq_nwaiters(e->bmpce_waitq) : 0;
	mpce->mpce_npndgaios = pll_nitems(&e->bmpce_pndgaios);
	return (psc_ctlmsg_sendv(fd, mh, mpce, NULL));
}

int
msctlrep_getbmpce(int fd, struct psc_ctlmsghdr *mh, void *m)
{
	struct msctlmsg_bmpce *mpce = m;
	struct bmap_pagecache_entry *e;
	struct bmap_cli_info *bci;
	struct psc_hashbkt *hb;
	struct fidc_membh *f;
	struct bmap *b;
	int rc = 1;

	PSC_HASHTBL_FOREACH_BUCKET(hb, &sl_fcmh_hashtbl) {
		psc_hashbkt_lock(hb);
		PSC_HASHBKT_FOREACH_ENTRY(&sl_fcmh_hashtbl, f, hb) {
			pfl_rwlock_rdlock(&f->fcmh_rwlock);
			RB_FOREACH(b, bmaptree, &f->fcmh_bmaptree) {
				bci = bmap_2_bci(b);
				pfl_rwlock_rdlock(&bci->bci_rwlock);
				RB_FOREACH(e, bmap_pagecachetree,
				    &bmap_2_bmpc(b)->bmpc_tree) {
					rc = msctlmsg_bmpce_send(fd, mh,
					    mpce, b, e);
					if (!rc)
						break;
				}
				pfl_rwlock_unlock(&bci->bci_rwlock);
				if (!rc)
					break;
			}
			pfl_rwlock_unlock(&f->fcmh_rwlock);
			if (!rc)
				break;
		}
		psc_hashbkt_unlock(hb);
		if (!rc)
			break;
	}
	return (rc);
}

int
mslctl_resfield_connected(int fd, struct psc_ctlmsghdr *mh,
    struct psc_ctlmsg_param *pcp, char **levels, int nlevels, int set,
    struct sl_resource *r)
{
	struct slrpc_cservice *csvc;
	struct sl_resm *m;
	char nbuf[8];

	if (set && strcmp(pcp->pcp_value, "0") &&
	    strcmp(pcp->pcp_value, "1"))
		return (psc_ctlsenderr(fd, mh, NULL,
		    "connected: invalid value"));

	m = res_getmemb(r);
	if (set) {
		if (r->res_type == SLREST_MDS)
			csvc = slc_getmcsvc_nb(m, 0);
		else
			csvc = slc_geticsvc_nb(m, 0);
		if (strcmp(pcp->pcp_value, "0") == 0 && csvc)
			sl_csvc_disconnect(csvc);
		if (csvc)
			sl_csvc_decref(csvc);
		return (1);
	}
	if (r->res_type == SLREST_MDS)
		csvc = slc_getmcsvcf(m, CSVCF_NONBLOCK | CSVCF_NORECON, 0);
	else
		csvc = slc_geticsvcf(m, CSVCF_NONBLOCK | CSVCF_NORECON, 0);
	snprintf(nbuf, sizeof(nbuf), "%d", csvc ? 1 : 0);
	if (csvc)
		sl_csvc_decref(csvc);
	return (psc_ctlmsg_param_send(fd, mh, pcp, PCTHRNAME_EVERYONE,
	    levels, nlevels, nbuf));
}

int
mslctl_resfield_mtime(int fd, struct psc_ctlmsghdr *mh,
    struct psc_ctlmsg_param *pcp, char **levels, int nlevels, int set,
    struct sl_resource *r)
{
	struct slrpc_cservice *csvc;
	struct sl_resm *m;
	char nbuf[32];

	if (set)
		return (psc_ctlsenderr(fd, mh, NULL,
		    "mtime: field is read-only"));

	m = res_getmemb(r);
	if (r->res_type == SLREST_MDS)
		csvc = slc_getmcsvcf(m, CSVCF_NONBLOCK | CSVCF_NORECON, 0);
	else
		csvc = slc_geticsvcf(m, CSVCF_NONBLOCK | CSVCF_NORECON, 0);
	snprintf(nbuf, sizeof(nbuf), "%"PSCPRI_TIMET,
	    csvc ? csvc->csvc_mtime.tv_sec : (time_t)0);
	if (csvc)
		sl_csvc_decref(csvc);
	return (psc_ctlmsg_param_send(fd, mh, pcp, PCTHRNAME_EVERYONE,
	    levels, nlevels, nbuf));
}

int
mslctl_resfield_timeouts(int fd, struct psc_ctlmsghdr *mh,
    struct psc_ctlmsg_param *pcp, char **levels, int nlevels, int set,
    struct sl_resource *r)
{
	struct resprof_cli_info *rpci;
	char nbuf[16];

	if (set)
		return (psc_ctlsenderr(fd, mh, NULL,
		    "timeouts: field is read-only"));
	rpci = res2rpci(r);
	snprintf(nbuf, sizeof(nbuf), "%d", rpci->rpci_timeouts);
	return (psc_ctlmsg_param_send(fd, mh, pcp, PCTHRNAME_EVERYONE,
	    levels, nlevels, nbuf));
}

int
mslctl_resfield_infl_rpcs(int fd, struct psc_ctlmsghdr *mh,
    struct psc_ctlmsg_param *pcp, char **levels, int nlevels, int set,
    struct sl_resource *r)
{
	struct resprof_cli_info *rpci;
	char nbuf[16];

	if (set)
		return (psc_ctlsenderr(fd, mh, NULL,
		    "infl_rpcs: field is read-only"));
	rpci = res2rpci(r);
	snprintf(nbuf, sizeof(nbuf), "%d", rpci->rpci_infl_rpcs);
	return (psc_ctlmsg_param_send(fd, mh, pcp, PCTHRNAME_EVERYONE,
	    levels, nlevels, nbuf));
}

int
mslctl_resfield_total_rpcs(int fd, struct psc_ctlmsghdr *mh,
    struct psc_ctlmsg_param *pcp, char **levels, int nlevels, int set,
    struct sl_resource *r)
{
	struct resprof_cli_info *rpci;
	char nbuf[16];

	if (set)
		return (psc_ctlsenderr(fd, mh, NULL,
		    "total_rpcs: field is read-only"));
	rpci = res2rpci(r);
	snprintf(nbuf, sizeof(nbuf), "%d", rpci->rpci_total_rpcs);
	return (psc_ctlmsg_param_send(fd, mh, pcp, PCTHRNAME_EVERYONE,
	    levels, nlevels, nbuf));
}

int
mslctl_resfield_max_infl_rpcs(int fd, struct psc_ctlmsghdr *mh,
    struct psc_ctlmsg_param *pcp, char **levels, int nlevels, int set,
    struct sl_resource *r)
{
	struct resprof_cli_info *rpci;
	char nbuf[16];
	long val;
	char *endp = NULL;

	rpci = res2rpci(r);
	if (set) {
		val = strtol(pcp->pcp_value, &endp, 10);
		if (endp == pcp->pcp_value || *endp != '\0')
		    return (psc_ctlsenderr(fd, mh, NULL,
			"max_infl_rpcs: invalid value"));
		rpci->rpci_max_infl_rpcs = (int)val;
		return (0);

	}
	snprintf(nbuf, sizeof(nbuf), "%d", rpci->rpci_max_infl_rpcs);
	return (psc_ctlmsg_param_send(fd, mh, pcp, PCTHRNAME_EVERYONE,
	    levels, nlevels, nbuf));
}

const struct slctl_res_field slctl_resmds_fields[] = {
	{ "connected",		mslctl_resfield_connected },
	{ "timeouts",		mslctl_resfield_timeouts },
	{ "infl_rpcs",		mslctl_resfield_infl_rpcs },
	{ "total_rpcs",		mslctl_resfield_total_rpcs },
	{ "max_infl_rpcs",	mslctl_resfield_max_infl_rpcs },
	{ "mtime",		mslctl_resfield_mtime },
	{ NULL, NULL }
};

const struct slctl_res_field slctl_resios_fields[] = {
	{ "connected",		mslctl_resfield_connected },
	{ "timeouts",		mslctl_resfield_timeouts },
	{ "infl_rpcs",		mslctl_resfield_infl_rpcs },
	{ "total_rpcs",		mslctl_resfield_total_rpcs },
	{ "max_infl_rpcs",	mslctl_resfield_max_infl_rpcs },
	{ "mtime",		mslctl_resfield_mtime },
	{ NULL, NULL }
};

struct psc_ctlop msctlops[] = {
	PSC_CTLDEFOPS,
/* ADDREPLRQ		*/ { msctlrep_replrq,		sizeof(struct msctlmsg_replrq) },
/* DELREPLRQ		*/ { msctlrep_replrq,		sizeof(struct msctlmsg_replrq) },
/* GETCONNS		*/ { slctlrep_getconn,		sizeof(struct slctlmsg_conn) },
/* GETFCMH		*/ { slctlrep_getfcmh,		sizeof(struct slctlmsg_fcmh) },
/* GETREPLST		*/ { msctlrep_getreplst,	sizeof(struct msctlmsg_replst) },
/* GETREPLST_SLAVE	*/ { NULL,			0 },
/* GET_BMAPREPLPOL	*/ { NULL,			0 },
/* GET_FATTR		*/ { msctlhnd_get_fattr,	sizeof(struct msctlmsg_fattr) },
/* SET_BMAPREPLPOL	*/ { msctlhnd_set_bmapreplpol,	sizeof(struct msctlmsg_bmapreplpol) },
/* SET_FATTR		*/ { msctlhnd_set_fattr,	sizeof(struct msctlmsg_fattr) },
/* GETBMAP		*/ { slctlrep_getbmap,		sizeof(struct slctlmsg_bmap) },
/* GETBIORQ		*/ { msctlrep_getbiorq,		sizeof(struct msctlmsg_biorq) },
/* GETBMPCE		*/ { msctlrep_getbmpce,		sizeof(struct msctlmsg_bmpce) },
};

void
msctlthr_main(__unusedx struct psc_thread *thr)
{
	char expandbuf[PATH_MAX];
	char *s, *fn = (void *)msl_ctlsockfn;
	int rc;
	struct psc_thread *me;

	me = pscthr_get();

	for (;;) {
		/*
		 * Under wokfs, %n will expand to "mount_wokfs" and not
		 * "mount_slash" so substitute it here.
		 */
		s = strstr(fn, "%n");
		if (s == NULL)
			break;
		rc = snprintf(expandbuf, sizeof(expandbuf), "%.*s%s%s",
		    (int)(s - fn), fn, "mount_slash", s + 2);
		if (rc == -1)
			psc_fatal("expand %s", msl_ctlsockfn);
		fn = expandbuf;
	}

	psc_ctlthr_main(fn, msctlops, nitems(msctlops), 0, MSTHRT_CTLAC);
	/*
	 * If we did not enter this loop, the thread will die.  As part of
	 * its destruction, _pscthr_destroy() will be called to free the
	 * thread structure.
	 */
	psc_ctlthr_mainloop(me);
}

void
msctlthr_spawn(void)
{
	struct psc_thread *thr;

	pflrpc_register_ctlops(msctlops);
	pflfs_register_ctlops(msctlops);

	psc_ctlparam_register("faults", psc_ctlparam_faults);

#ifdef Linux
	psc_ctlparam_register("log.file", psc_ctlparam_log_file);
#endif
	psc_ctlparam_register("log.format", psc_ctlparam_log_format);
	psc_ctlparam_register("log.level", psc_ctlparam_log_level);
	psc_ctlparam_register("log.points", psc_ctlparam_log_points);
	psc_ctlparam_register("opstats", psc_ctlparam_opstats);
	psc_ctlparam_register("pause", psc_ctlparam_pause);
	psc_ctlparam_register("pool", psc_ctlparam_pool);
	psc_ctlparam_register("rlim", psc_ctlparam_rlim);
	psc_ctlparam_register("run", psc_ctlparam_run);
	psc_ctlparam_register("rusage", psc_ctlparam_rusage);

	psc_ctlparam_register_simple("sys.logrotate",
	    slctlparam_logrotate_get, slctlparam_logrotate_set);

	psc_ctlparam_register_var("sys.nbrq_outstanding",
	    PFLCTL_PARAMT_INT, 0, &sl_nbrqset->set_remaining);
	psc_ctlparam_register_var("sys.nbrqthr_wait", PFLCTL_PARAMT_INT,
	    0, &sl_nbrqset->set_compl.pc_wq.wq_nwaiters);
	psc_ctlparam_register("sys.resources", slctlparam_resources);
	psc_ctlparam_register_simple("sys.uptime",
	    slctlparam_uptime_get, NULL);
	psc_ctlparam_register_simple("sys.version",
	    slctlparam_version_get, NULL);

	psc_ctlparam_register_var("sys.attr_timeout", PFLCTL_PARAMT_INT,
	    PFLCTL_PARAMF_RDWR, &msl_attributes_timeout);

	psc_ctlparam_register_var("sys.bmap_max_cache",
	    PFLCTL_PARAMT_INT, PFLCTL_PARAMF_RDWR, &slc_bmap_max_cache);

	psc_ctlparam_register_var("sys.bmap_reassign",
	    PFLCTL_PARAMT_INT, PFLCTL_PARAMF_RDWR, &msl_bmap_reassign);

	/* XXX: add max_fs_iosz */
	psc_ctlparam_register_var("sys.datadir", PFLCTL_PARAMT_STR, 0,
	    (char *)sl_datadir);

	psc_ctlparam_register_var("sys.enable_namecache", PFLCTL_PARAMT_INT,
	    PFLCTL_PARAMF_RDWR, &msl_enable_namecache);

	psc_ctlparam_register_var("sys.mountpoint", PFLCTL_PARAMT_STR,
	    0, mountpoint);
	psc_ctlparam_register_var("sys.offline_nretries",
	    PFLCTL_PARAMT_INT, PFLCTL_PARAMF_RDWR, &msl_max_nretries);

	psc_ctlparam_register_simple("sys.pref_ios",
	    msctlparam_prefios_get, msctlparam_prefios_set);
	psc_ctlparam_register_simple("sys.mds", msctlparam_mds_get,
	    NULL);
	psc_ctlparam_register_var("sys.fuse_direct_io", PFLCTL_PARAMT_INT,
	    PFLCTL_PARAMF_RDWR, &msl_fuse_direct_io);

	psc_ctlparam_register_var("sys.force_dio",
	    PFLCTL_PARAMT_INT, PFLCTL_PARAMF_RDWR, &msl_force_dio);

	psc_ctlparam_register_simple("sys.map_enable",
	    msctlparam_map_get, msctlparam_map_set);

	psc_ctlparam_register_var("sys.max_retries", PFLCTL_PARAMT_INT,
	    PFLCTL_PARAMF_RDWR, &msl_max_retries);

	psc_ctlparam_register_var("sys.max_namecache_per_directory", PFLCTL_PARAMT_INT,
	    PFLCTL_PARAMF_RDWR, &msl_max_namecache_per_directory);

	psc_ctlparam_register_var("sys.pid", PFLCTL_PARAMT_INT, 0,
	    &pfl_pid);

	psc_ctlparam_register_simple("sys.predio_max_pages",
	    slctlparam_max_pages_get, slctlparam_max_pages_set);
	psc_ctlparam_register_var("sys.predio_pipe_size",
	    PFLCTL_PARAMT_INT, PFLCTL_PARAMF_RDWR, &msl_predio_pipe_size);

	psc_ctlparam_register_var("sys.read_only", PFLCTL_PARAMT_INT,
	    PFLCTL_PARAMF_RDWR, &msl_read_only);

	psc_ctlparam_register_var("sys.repl_enable", PFLCTL_PARAMT_INT,
	    PFLCTL_PARAMF_RDWR, &msl_repl_enable);

	psc_ctlparam_register_var("sys.root_squash", PFLCTL_PARAMT_INT,
	    PFLCTL_PARAMF_RDWR, &msl_root_squash);

	psc_ctlparam_register_var("sys.rpc_timeout", PFLCTL_PARAMT_INT,
	    PFLCTL_PARAMF_RDWR, &pfl_rpc_timeout);

	pfl_rpc_max_retry = 0;
	psc_ctlparam_register_var("sys.rpc_max_retry", PFLCTL_PARAMT_INT,
	    PFLCTL_PARAMF_RDWR, &pfl_rpc_max_retry);

#ifdef Linux
	psc_ctlparam_register("sys.rss", psc_ctlparam_get_rss);
	psc_ctlparam_register("sys.vsz", psc_ctlparam_get_vsz);
#endif

	psc_ctlparam_register_var("sys.statfs_pref_ios_only",
	    PFLCTL_PARAMT_INT, PFLCTL_PARAMF_RDWR,
	    &msl_statfs_pref_ios_only);
	psc_ctlparam_register_var("sys.ios_max_inflight_rpcs",
	    PFLCTL_PARAMT_INT, PFLCTL_PARAMF_RDWR,
	    &msl_ios_max_inflight_rpcs);
	psc_ctlparam_register_var("sys.mds_max_inflight_rpcs",
	    PFLCTL_PARAMT_INT, PFLCTL_PARAMF_RDWR,
	    &msl_mds_max_inflight_rpcs);

	psc_ctlparam_register_var("sys.enable_sillyrename", PFLCTL_PARAMT_INT,
	    PFLCTL_PARAMF_RDWR, &msl_enable_sillyrename);

	thr = pscthr_init(MSTHRT_CTL, msctlthr_main,
	    sizeof(struct psc_ctlthr), "msctlthr0");
	/* stash thread so mslfsop_destroy() can kill ctlthr */
	msl_ctlthr0 = thr;
	msl_ctlthr0_private = thr->pscthr_private;
	pscthr_setready(thr);
}
