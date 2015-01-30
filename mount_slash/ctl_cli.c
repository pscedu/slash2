/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2007-2015, Pittsburgh Supercomputing Center (PSC).
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
 *
 * Pittsburgh Supercomputing Center	phone: 412.268.4960  fax: 412.268.5832
 * 300 S. Craig Street			e-mail: remarks@psc.edu
 * Pittsburgh, PA 15213			web: http://www.psc.edu/
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
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
#include "ctlsvr_cli.h"
#include "fidc_cli.h"
#include "mount_slash.h"
#include "rpc_cli.h"
#include "slashrpc.h"
#include "slerr.h"

#include "slashd/inode.h"

psc_atomic32_t		 msctl_id = PSC_ATOMIC32_INIT(0);
struct psc_lockedlist	 msctl_replsts = PLL_INIT(&msctl_replsts,
    struct msctl_replstq, mrsq_lentry);

#define REPLRQ_BMAPNO_ALL (-1)

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
	return (rc);
}

int
msctl_getclientctx(__unusedx int s, struct pscfs_clientctx *pfcc)
{
	pfcc->pfcc_pid = -1;
	return (0);
}

int
msctlrep_replrq(int fd, struct psc_ctlmsghdr *mh, void *m)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct msctlmsg_replrq *mrq = m;
	struct pscfs_clientctx pfcc;
	struct srm_replrq_rep *mp;
	struct srm_replrq_req *mq;
	struct sl_fidgen fg;
	struct pscfs_creds pcr;
	struct fidc_membh *f;
	uint32_t n;
	int rc;

	if (mrq->mrq_nios < 1 ||
	    mrq->mrq_nios >= nitems(mrq->mrq_iosv))
		return (psc_ctlsenderr(fd, mh,
		    "replication request: %s", slstrerror(EINVAL)));

	rc = msctl_getcreds(fd, &pcr);
	if (rc)
		return (psc_ctlsenderr(fd, mh,
		    SLPRI_FID": unable to obtain credentials: %s",
		    mrq->mrq_fid, slstrerror(rc)));
	rc = msctl_getclientctx(fd, &pfcc);
	if (rc)
		return (psc_ctlsenderr(fd, mh,
		    SLPRI_FID": unable to obtain client context: %s",
		    mrq->mrq_fid, slstrerror(rc)));

	rc = fidc_lookup_load(mrq->mrq_fid, &f, &pfcc);
	if (rc)
		return (psc_ctlsenderr(fd, mh, SLPRI_FID": %s",
		    mrq->mrq_fid, slstrerror(rc)));

	FCMH_LOCK(f);
	if (fcmh_isreg(f) || fcmh_isdir(f)) {
		rc = fcmh_checkcreds_ctx(f, &pfcc, &pcr, W_OK);
		if (rc == EACCES &&
		    f->fcmh_sstb.sst_uid == pcr.pcr_uid)
			rc = 0;
	} else
		rc = ENOTSUP;
	fg = f->fcmh_fg;
	fcmh_op_done(f);

	if (rc)
		return (psc_ctlsenderr(fd, mh, SLPRI_FID": %s",
		    mrq->mrq_fid, slstrerror(rc)));

	if (mh->mh_type == MSCMT_ADDREPLRQ)
		MSL_RMC_NEWREQ_PFCC(&pfcc, f, csvc, SRMT_REPL_ADDRQ, rq,
		    mq, mp, rc);
	else
		MSL_RMC_NEWREQ_PFCC(&pfcc, f, csvc, SRMT_REPL_DELRQ, rq,
		    mq, mp, rc);
	if (rc) {
		rc = psc_ctlsenderr(fd, mh, SLPRI_FID": %s",
		    mrq->mrq_fid, slstrerror(rc));
		goto out;
	}

	/* parse I/O systems specified */
	for (n = 0; n < mrq->mrq_nios; n++, mq->nrepls++)
		if ((mq->repls[n].bs_id =
		    libsl_str2id(mrq->mrq_iosv[n])) == IOS_ID_ANY) {
			rc = psc_ctlsenderr(fd, mh,
			    "%s: unknown I/O system", mrq->mrq_iosv[n]);
			goto out;
		}
	memcpy(&mq->fg, &fg, sizeof(mq->fg));
	mq->bmapno = mrq->mrq_bmapno;

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		rc = psc_ctlsenderr(fd, mh, SLPRI_FID": %s",
		    mrq->mrq_fid, slstrerror(rc));
	else
		rc = 1;

 out:
	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
	return (rc);
}

int
msctlrep_getreplst(int fd, struct psc_ctlmsghdr *mh, void *m)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_replst_master_req *mq;
	struct srm_replst_master_rep *mp;
	struct msctlmsg_replrq *mrq = m;
	struct fidc_membh *f = NULL;
	struct pscfs_clientctx pfcc;
	struct msctl_replstq mrsq;
	struct sl_fidgen fg;
	struct pscfs_creds pcr;
	int added = 0, rc;

	if (mrq->mrq_fid == FID_ANY) {
		fg.fg_fid = FID_ANY;
		fg.fg_gen = FGEN_ANY;
		goto issue;
	}

	rc = msctl_getcreds(fd, &pcr);
	if (rc)
		return (psc_ctlsenderr(fd, mh,
		    SLPRI_FID": unable to obtain credentials: %s",
		    mrq->mrq_fid, slstrerror(rc)));
	rc = msctl_getclientctx(fd, &pfcc);
	if (rc)
		return (psc_ctlsenderr(fd, mh,
		    SLPRI_FID": unable to obtain client context: %s",
		    mrq->mrq_fid, slstrerror(rc)));

	rc = fidc_lookup_load(mrq->mrq_fid, &f, &pfcc);
	if (rc)
		return (psc_ctlsenderr(fd, mh, SLPRI_FID": %s",
		    mrq->mrq_fid, slstrerror(rc)));

	FCMH_LOCK(f);
	if (fcmh_isreg(f) || fcmh_isdir(f))
		rc = fcmh_checkcreds_ctx(f, &pfcc, &pcr, R_OK);
	else
		rc = ENOTSUP;
	fg = f->fcmh_fg;
	fcmh_op_done(f);

	if (rc)
		return (psc_ctlsenderr(fd, mh, SLPRI_FID": %s",
		    mrq->mrq_fid, slstrerror(rc)));

 issue:
	MSL_RMC_NEWREQ_PFCC(&pfcc, f, csvc, SRMT_REPL_GETST, rq, mq, mp,
	    rc);
	if (rc) {
		rc = psc_ctlsenderr(fd, mh, SLPRI_FID": %s",
		    fg.fg_fid, slstrerror(rc));
		goto out;
	}

	mq->fg = fg;
	mq->id = psc_atomic32_inc_getnew(&msctl_id);

	memset(&mrsq, 0, sizeof(mrsq));
	INIT_PSC_LISTENTRY(&mrsq.mrsq_lentry);
	INIT_SPINLOCK(&mrsq.mrsq_lock);
	psc_waitq_init(&mrsq.mrsq_waitq);
	mrsq.mrsq_id = mq->id;
	mrsq.mrsq_fd = fd;
	mrsq.mrsq_fid = mrq->mrq_fid;
	mrsq.mrsq_mh = mh;

	pll_add(&msctl_replsts, &mrsq);
	added = 1;

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc == 0)
		rc = mp->rc;
	if (rc) {
		rc = psc_ctlsenderr(fd, mh, SLPRI_FID": %s",
		    fg.fg_fid, slstrerror(rc));
		goto out;
	}

	spinlock(&mrsq.mrsq_lock);
	while (mrsq.mrsq_rc == 0) {
		psc_waitq_wait(&mrsq.mrsq_waitq, &mrsq.mrsq_lock);
		spinlock(&mrsq.mrsq_lock);
	}

	freelock(&mrsq.mrsq_lock);
	PLL_LOCK(&msctl_replsts);
	spinlock(&mrsq.mrsq_lock);
	while (mrsq.mrsq_refcnt) {
		PLL_ULOCK(&msctl_replsts);
		psc_waitq_wait(&mrsq.mrsq_waitq, &mrsq.mrsq_lock);
		PLL_LOCK(&msctl_replsts);
		spinlock(&mrsq.mrsq_lock);
	}
	pll_remove(&msctl_replsts, &mrsq);
	PLL_ULOCK(&msctl_replsts);
	psc_waitq_destroy(&mrsq.mrsq_waitq);
	added = 0;

	rc = 1;
	if (mrsq.mrsq_rc && mrsq.mrsq_rc != EOF)
		rc = psc_ctlsenderr(fd, mh, SLPRI_FID": %s",
		    fg.fg_fid, slstrerror(mrsq.mrsq_rc));

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
		return (psc_ctlsenderr(fd, mh,
		    SLPRI_FID": unable to obtain credentials: %s",
		    mfa->mfa_fid, slstrerror(rc)));
	rc = msctl_getclientctx(fd, &pfcc);
	if (rc)
		return (psc_ctlsenderr(fd, mh,
		    SLPRI_FID": unable to obtain client context: %s",
		    mfa->mfa_fid, slstrerror(rc)));

	rc = fidc_lookup_load(mfa->mfa_fid, &f, &pfcc);
	if (rc)
		return (psc_ctlsenderr(fd, mh, SLPRI_FID": %s",
		    mfa->mfa_fid, slstrerror(rc)));

	FCMH_LOCK(f);
	if (fcmh_isreg(f) || fcmh_isdir(f))
		rc = fcmh_checkcreds_ctx(f, &pfcc, &pcr, R_OK);
	else
		rc = ENOTSUP;
	FCMH_ULOCK(f);

	if (rc == 0)
		rc = slc_fcmh_fetch_inode(f);

	if (rc) {
		rc = psc_ctlsenderr(fd, mh, SLPRI_FID": %s",
		    mfa->mfa_fid, slstrerror(rc));
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
		rc = psc_ctlsenderr(fd, mh, SLPRI_FID": %s",
		    mfa->mfa_fid, slstrerror(rc));
		goto out;
	}
	FCMH_ULOCK(f);

	rc = psc_ctlmsg_sendv(fd, mh, mfa);

 out:
	if (f)
		fcmh_op_done(f);
	return (rc);
}

int
msctlhnd_set_fattr(int fd, struct psc_ctlmsghdr *mh, void *m)
{
	struct slashrpc_cservice *csvc = NULL;
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
		return (psc_ctlsenderr(fd, mh,
		    SLPRI_FID": unable to obtain credentials: %s",
		    mfa->mfa_fid, slstrerror(rc)));
	rc = msctl_getclientctx(fd, &pfcc);
	if (rc)
		return (psc_ctlsenderr(fd, mh,
		    SLPRI_FID": unable to obtain client context: %s",
		    mfa->mfa_fid, slstrerror(rc)));

	rc = fidc_lookup_load(mfa->mfa_fid, &f, &pfcc);
	if (rc)
		return (psc_ctlsenderr(fd, mh, SLPRI_FID": %s",
		    mfa->mfa_fid, slstrerror(rc)));

	FCMH_LOCK(f);
	if (fcmh_isreg(f) || fcmh_isdir(f))
		rc = fcmh_checkcreds_ctx(f, &pfcc, &pcr, W_OK);
	else
		rc = ENOTSUP;
	fg = f->fcmh_fg;
	fcmh_op_done(f);

	if (rc)
		return (psc_ctlsenderr(fd, mh, SLPRI_FID": %s",
		    mfa->mfa_fid, slstrerror(rc)));

	MSL_RMC_NEWREQ_PFCC(&pfcc, f, csvc, SRMT_SET_FATTR, rq, mq, mp,
	    rc);
	if (rc) {
		rc = psc_ctlsenderr(fd, mh, SLPRI_FID": %s",
		    mfa->mfa_fid, slstrerror(rc));
		goto out;
	}
	mq->attrid = mfa->mfa_attrid;
	mq->val = mfa->mfa_val;
	mq->fg = fg;
	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		rc = psc_ctlsenderr(fd, mh, "%s: %s",
		    mfa->mfa_fid, slstrerror(rc));

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
	struct slashrpc_cservice *csvc = NULL;
	struct srm_set_bmapreplpol_req *mq;
	struct srm_set_bmapreplpol_rep *mp;
	struct pscrpc_request *rq = NULL;
	struct pscfs_clientctx pfcc;
	struct sl_fidgen fg;
	struct pscfs_creds pcr;
	struct fidc_membh *f;
	int rc;

	rc = msctl_getcreds(fd, &pcr);
	if (rc)
		return (psc_ctlsenderr(fd, mh,
		    SLPRI_FID": unable to obtain credentials: %s",
		    mfbrp->mfbrp_fid, slstrerror(rc)));
	rc = msctl_getclientctx(fd, &pfcc);
	if (rc)
		return (psc_ctlsenderr(fd, mh,
		    SLPRI_FID": unable to obtain client context: %s",
		    mfbrp->mfbrp_fid, slstrerror(rc)));

	rc = fidc_lookup_load(mfbrp->mfbrp_fid, &f, &pfcc);
	if (rc)
		return (psc_ctlsenderr(fd, mh, SLPRI_FID": %s",
		    mfbrp->mfbrp_fid, slstrerror(rc)));

	FCMH_LOCK(f);
	if (fcmh_isreg(f))
		rc = fcmh_checkcreds_ctx(f, &pfcc, &pcr, W_OK);
	else
		rc = ENOTSUP;
	fg = f->fcmh_fg;
	fcmh_op_done(f);

	if (rc)
		return (psc_ctlsenderr(fd, mh, SLPRI_FID": %s",
		    mfbrp->mfbrp_fid, slstrerror(rc)));

	MSL_RMC_NEWREQ_PFCC(&pfcc, f, csvc, SRMT_SET_BMAPREPLPOL, rq,
	    mq, mp, rc);
	if (rc) {
		rc = psc_ctlsenderr(fd, mh, SLPRI_FID": %s",
		    mfbrp->mfbrp_fid, slstrerror(rc));
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
		rc = psc_ctlsenderr(fd, mh, SLPRI_FID": %s",
		    mfbrp->mfbrp_fid, slstrerror(rc));

 out:
	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
	return (rc);
}

void
msctlparam_prefios_get(char buf[PCP_VALUE_MAX])
{
	struct sl_resource *r;

	r = libsl_id2res(prefIOS);
	if (r)
		strlcpy(buf, r->res_name, PCP_VALUE_MAX);
	else
		strlcpy(buf, "(null)", PCP_VALUE_MAX);
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
	return (psc_ctlmsg_sendv(fd, mh, scb));
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

	return (psc_ctlmsg_sendv(fd, mh, msr));
}

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

	PSC_HASHTBL_FOREACH_BUCKET(hb, &fidcHtable) {
		psc_hashbkt_lock(hb);
		PSC_HASHBKT_FOREACH_ENTRY(&fidcHtable, f, hb) {
			FCMH_LOCK(f);
			SPLAY_FOREACH(b, bmap_cache, &f->fcmh_bmaptree) {

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

				RB_FOREACH(r, bmpc_biorq_tree,
				    &bmpc->bmpc_new_biorqs) {
					rc = msctlmsg_biorq_send(fd, mh,
					    msr, r);
					if (!rc)
						break;
				}
				BMAP_ULOCK(b);
				if (!rc)
					break;
			}
			FCMH_ULOCK(f);
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
	mpce->mpce_ref = psc_atomic32_read(&e->bmpce_ref);
	mpce->mpce_flags = e->bmpce_flags;
	mpce->mpce_off = e->bmpce_off;
//	mpce->mpce_start = e->bmpce_start;
	mpce->mpce_laccess = e->bmpce_laccess;
	mpce->mpce_nwaiters =e->bmpce_waitq ?
	    psc_waitq_nwaiters(e->bmpce_waitq) : 0;
	mpce->mpce_npndgaios = pll_nitems(&e->bmpce_pndgaios);
	return (psc_ctlmsg_sendv(fd, mh, mpce));
}

int
msctlrep_getbmpce(int fd, struct psc_ctlmsghdr *mh, void *m)
{
	struct msctlmsg_bmpce *mpce = m;
	struct bmap_pagecache_entry *e;
	struct psc_hashbkt *hb;
	struct fidc_membh *f;
	struct bmap *b;
	int rc = 1;

	PSC_HASHTBL_FOREACH_BUCKET(hb, &fidcHtable) {
		psc_hashbkt_lock(hb);
		PSC_HASHBKT_FOREACH_ENTRY(&fidcHtable, f, hb) {
			FCMH_LOCK(f);
			SPLAY_FOREACH(b, bmap_cache, &f->fcmh_bmaptree) {
				BMAP_LOCK(b);
				SPLAY_FOREACH(e, bmap_pagecachetree,
				    &bmap_2_bmpc(b)->bmpc_tree) {
					rc = msctlmsg_bmpce_send(fd, mh,
					    mpce, b, e);
					if (!rc)
						break;
				}
				BMAP_ULOCK(b);
				if (!rc)
					break;
			}
			FCMH_ULOCK(f);
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
mslctl_resfieldi_connected(int fd, struct psc_ctlmsghdr *mh,
    struct psc_ctlmsg_param *pcp, char **levels, int nlevels, int set,
    struct sl_resource *r)
{
	struct slashrpc_cservice *csvc;
	struct sl_resm *m;
	char nbuf[8];

	m = res_getmemb(r);
	if (set) {
		if (strcmp(pcp->pcp_value, "0") == 0) {
			csvc = slc_geticsvc_nb(m);
			if (csvc) {
				sl_csvc_disconnect(csvc);
				sl_csvc_decref(csvc);
			}
		} else if (strcmp(pcp->pcp_value, "1") == 0) {
			csvc = slc_geticsvc_nb(m);
			if (csvc)
				sl_csvc_decref(csvc);
		} else
			return (psc_ctlsenderr(fd, mh,
			    "connected: invalid value"));
		return (1);
	}
	csvc = slc_geticsvcf(m, CSVCF_NONBLOCK | CSVCF_NORECON);
	snprintf(nbuf, sizeof(nbuf), "%d", csvc ? 1 : 0);
	if (csvc)
		sl_csvc_decref(csvc);
	return (psc_ctlmsg_param_send(fd, mh, pcp, PCTHRNAME_EVERYONE,
	    levels, nlevels, nbuf));
}

int
mslctl_resfieldi_infl_rpcs(int fd, struct psc_ctlmsghdr *mh,
    struct psc_ctlmsg_param *pcp, char **levels, int nlevels, int set,
    struct sl_resource *r)
{
	struct resm_cli_info *rmci;
	struct sl_resm *m;
	char nbuf[16];

	if (set)
		return (psc_ctlsenderr(fd, mh,
		    "infl_rpcs: field is read-only"));
	m = res_getmemb(r);
	rmci = resm2rmci(m);
	snprintf(nbuf, sizeof(nbuf), "%d",
	    atomic_read(&rmci->rmci_infl_rpcs));
	return (psc_ctlmsg_param_send(fd, mh, pcp, PCTHRNAME_EVERYONE,
	    levels, nlevels, nbuf));
}

const struct slctl_res_field slctl_resmds_fields[] = { { NULL, NULL } };

const struct slctl_res_field slctl_resios_fields[] = {
	{ "connected",		mslctl_resfieldi_connected },
	{ "infl_rpcs",		mslctl_resfieldi_infl_rpcs },
	{ NULL, NULL }
};

struct psc_ctlop msctlops[] = {
	PSC_CTLDEFOPS
/* ADDREPLRQ		*/ , { msctlrep_replrq,		sizeof(struct msctlmsg_replrq) }
/* DELREPLRQ		*/ , { msctlrep_replrq,		sizeof(struct msctlmsg_replrq) }
/* GETCONNS		*/ , { slctlrep_getconn,	sizeof(struct slctlmsg_conn) }
/* GETFCMH		*/ , { slctlrep_getfcmh,	sizeof(struct slctlmsg_fcmh) }
/* GETREPLST		*/ , { msctlrep_getreplst,	sizeof(struct msctlmsg_replst) }
/* GETREPLST_SLAVE	*/ , { NULL,			0 }
/* GET_BMAPREPLPOL	*/ , { NULL,			0 }
/* GET_FATTR		*/ , { msctlhnd_get_fattr,	sizeof(struct msctlmsg_fattr) }
/* SET_BMAPREPLPOL	*/ , { msctlhnd_set_bmapreplpol,sizeof(struct msctlmsg_bmapreplpol) }
/* SET_FATTR		*/ , { msctlhnd_set_fattr,	sizeof(struct msctlmsg_fattr) }
/* GETBMAP		*/ , { slctlrep_getbmap,	sizeof(struct slctlmsg_bmap) }
/* GETBIORQ		*/ , { msctlrep_getbiorq,	sizeof(struct msctlmsg_biorq) }
/* GETBMPCE		*/ , { msctlrep_getbmpce,	sizeof(struct msctlmsg_bmpce) }
};

psc_ctl_thrget_t psc_ctl_thrgets[] = {
/* ATTR_FLSH	*/ NULL,
/* BENCH	*/ NULL,
/* BRELEASE	*/ NULL,
/* BWATCH	*/ NULL,
/* CONN		*/ NULL,
/* CTL		*/ psc_ctlthr_get,
/* CTLAC	*/ psc_ctlacthr_get,
/* EQPOLL	*/ NULL,
/* FCMHREAP	*/ NULL,
/* FLUSH	*/ NULL,
/* FS		*/ NULL,
/* FSMGR	*/ NULL,
/* NBRQ		*/ NULL,
/* RCI		*/ NULL,
/* RCM		*/ NULL,
/* READAHEAD	*/ NULL,
/* OPSTIMER	*/ NULL,
/* USKLNDPL	*/ NULL,
/* WORKER	*/ NULL
};

PFLCTL_SVR_DEFS;

void
msctlthr_main(__unusedx struct psc_thread *thr)
{
	psc_ctlthr_main(ctlsockfn, msctlops, nitems(msctlops),
	    MSTHRT_CTLAC);
}

void
msctlthr_spawn(void)
{
	struct psc_thread *thr;

	psc_ctlparam_register("faults", psc_ctlparam_faults);
	psc_ctlparam_register("log.file", psc_ctlparam_log_file);
	psc_ctlparam_register("log.format", psc_ctlparam_log_format);
	psc_ctlparam_register("log.level", psc_ctlparam_log_level);
	psc_ctlparam_register("log.points", psc_ctlparam_log_points);
	psc_ctlparam_register("opstats", psc_ctlparam_opstats);
	psc_ctlparam_register("pause", psc_ctlparam_pause);
	psc_ctlparam_register("pool", psc_ctlparam_pool);
	psc_ctlparam_register("rlim", psc_ctlparam_rlim);
	psc_ctlparam_register("run", psc_ctlparam_run);
	psc_ctlparam_register("rusage", psc_ctlparam_rusage);

	psc_ctlparam_register_var("sys.nbrq_outstanding",
	    PFLCTL_PARAMT_INT, 0,
	    &sl_nbrqset->nb_reqset->set_remaining);
	psc_ctlparam_register("sys.resources", slctlparam_resources);
	psc_ctlparam_register_simple("sys.uptime",
	    slctlparam_uptime_get, NULL);
	psc_ctlparam_register_simple("sys.version",
	    slctlparam_version_get, NULL);

	/* XXX: add max_fs_iosz */

	psc_ctlparam_register_var("sys.mountpoint", PFLCTL_PARAMT_STR,
	    0, mountpoint);
	psc_ctlparam_register_var("sys.offline_nretries",
	    PFLCTL_PARAMT_ATOMIC32, PFLCTL_PARAMF_RDWR,
	    &slc_max_nretries);
	psc_ctlparam_register_simple("sys.pref_ios",
	    msctlparam_prefios_get, msctlparam_prefios_set);
	psc_ctlparam_register_var("sys.readahead_pgs",
	    PFLCTL_PARAMT_ATOMIC32, PFLCTL_PARAMF_RDWR,
	    &slc_max_readahead);

	thr = pscthr_init(MSTHRT_CTL, msctlthr_main, NULL,
	    sizeof(struct psc_ctlthr), "msctlthr0");
	pscthr_setready(thr);
}
