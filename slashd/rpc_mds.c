/* $Id$ */

#include <stdio.h>

#include "psc_ds/tree.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/service.h"
#include "psc_util/strlcpy.h"

#include "rpc_mds.h"
#include "slashd.h"
#include "slashrpc.h"

lnet_process_id_t lpid;

int
slm_rim_issue_ping(struct pscrpc_import *imp)
{
	const struct srm_generic_rep *mp;
	struct pscrpc_request *rq;
	struct srm_ping_req *mq;
	int rc;

	if ((rc = RSX_NEWREQ(imp, SRIM_VERSION,
	    SRMT_PING, rq, mq, mp)) != 0)
		return (rc);
	rc = RSX_WAITREP(rq, mp);
	pscrpc_req_finished(rq);
	return (rc);
}

void *
slmiconnthr_main(void *arg)
{
	struct slashrpc_cservice *csvc;
	struct slmiconn_thread *smict;
	struct psc_thread *thr = arg;
	struct mds_resm_info *mri;
	struct mds_site_info *msi;
	struct sl_resm *resm;
	int rc;

	smict = slmiconnthr(thr);
	resm = smict->smict_resm;
	mri = resm->resm_pri;
	msi = resm->resm_res->res_site->site_pri;
	for (;;) {
		spinlock(&mri->mri_lock);
		if (mri->mri_csvc == NULL) {
			/* try to establish connection to ION */
			csvc = rpc_csvc_create(SRIM_REQ_PORTAL,
			    SRIM_REP_PORTAL);
			rc = rpc_issue_connect(resm->resm_nid,
			    csvc->csvc_import, SRIM_MAGIC,
			    SRIM_VERSION);
			spinlock(&mri->mri_lock);

			/*
			 * Check if he connected to us while
			 * we were trying to connect to him.
			 */
			if (mri->mri_csvc) {
				slashrpc_csvc_free(csvc);
				freelock(&mri->mri_lock);
				goto live;
			}
			if (rc) {
				/* failure; try again in a bit */
				slashrpc_csvc_free(csvc);
				psc_waitq_waitrel_s(&mri->mri_waitq,
				    &mri->mri_lock, 10);
				continue;
			}
			mri->mri_csvc = csvc;
			freelock(&mri->mri_lock);
		}
		freelock(&mri->mri_lock);

 live:
		psc_waitq_wakeall(&msi->msi_waitq);

		/* Now just PING for connection lifetime. */
		spinlock(&mri->mri_lock);
		while (!mri->mri_csvc->csvc_import->imp_failed) {
			psc_waitq_waitrel_s(&mri->mri_waitq,
			    &mri->mri_lock, 60);
			spinlock(&mri->mri_lock);
			if (mri->mri_csvc->csvc_import->imp_failed)
				break;
			freelock(&mri->mri_lock);
			slm_rim_issue_ping(mri->mri_csvc->csvc_import);
			spinlock(&mri->mri_lock);
		}
		freelock(&mri->mri_lock);
		sched_yield();
	}
}

void
rpc_initsvc(void)
{
	struct pscrpc_svc_handle *svh;

	if (LNetGetId(1, &lpid))
		psc_fatalx("LNetGetId");

	/* Setup request service for MDS from ION. */
	svh = PSCALLOC(sizeof(*svh));
	svh->svh_nbufs = SLM_RMI_NBUFS;
	svh->svh_bufsz = SLM_RMI_BUFSZ;
	svh->svh_reqsz = SLM_RMI_BUFSZ;
	svh->svh_repsz = SLM_RMI_REPSZ;
	svh->svh_req_portal = SRMI_REQ_PORTAL;
	svh->svh_rep_portal = SRMI_REP_PORTAL;
	svh->svh_type = SLMTHRT_RMI;
	svh->svh_nthreads = SLM_RMI_NTHREADS;
	svh->svh_handler = slm_rmi_handler;
	strlcpy(svh->svh_svc_name, SLM_RMI_SVCNAME, sizeof(svh->svh_svc_name));
	pscrpc_thread_spawn(svh, struct slmrmi_thread);

	/* Setup request service for MDS from MDS. */
	svh = PSCALLOC(sizeof(*svh));
	svh->svh_nbufs = SLM_RMM_NBUFS;
	svh->svh_bufsz = SLM_RMM_BUFSZ;
	svh->svh_reqsz = SLM_RMM_BUFSZ;
	svh->svh_repsz = SLM_RMM_REPSZ;
	svh->svh_req_portal = SRMM_REQ_PORTAL;
	svh->svh_rep_portal = SRMM_REP_PORTAL;
	svh->svh_type = SLMTHRT_RMM;
	svh->svh_nthreads = SLM_RMM_NTHREADS;
	svh->svh_handler = slrmm_handler;
	strlcpy(svh->svh_svc_name, SLM_RMM_SVCNAME, sizeof(svh->svh_svc_name));
	pscrpc_thread_spawn(svh, struct slmrmm_thread);

	/* Setup request service for MDS from client. */
	svh = PSCALLOC(sizeof(*svh));
	svh->svh_nbufs = SLM_RMC_NBUFS;
	svh->svh_bufsz = SLM_RMC_BUFSZ;
	svh->svh_reqsz = SLM_RMC_BUFSZ;
	svh->svh_repsz = SLM_RMC_REPSZ;
	svh->svh_req_portal = SRMC_REQ_PORTAL;
	svh->svh_rep_portal = SRMC_REP_PORTAL;
	svh->svh_type = SLMTHRT_RMC;
	svh->svh_nthreads = SLM_RMC_NTHREADS;
	svh->svh_handler = slrmc_handler;
	strlcpy(svh->svh_svc_name, SLM_RMC_SVCNAME, sizeof(svh->svh_svc_name));
	pscrpc_thread_spawn(svh, struct slmrmc_thread);
}
