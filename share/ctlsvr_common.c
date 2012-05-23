/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2011, Pittsburgh Supercomputing Center (PSC).
 *
 * Permission to use, copy, and modify this software and its documentation
 * without fee for personal use or non-commercial use within your organization
 * is hereby granted, provided that the above copyright notice is preserved in
 * all copies and that the copyright and this permission notice appear in
 * supporting documentation.  Permission to redistribute this software to other
 * organizations or individuals is not permitted without the written permission
 * of the Pittsburgh Supercomputing Center.  PSC makes no representations about
 * the suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

#include <stdio.h>

#include "lnet/lib-types.h"
#include "lnet/lib-lnet.h"

#include "pfl/str.h"
#include "psc_rpc/rpc.h"
#include "psc_util/ctl.h"
#include "psc_util/ctlsvr.h"
#include "psc_util/lock.h"

#include "ctl.h"
#include "ctlsvr.h"
#include "fidcache.h"
#include "slconfig.h"
#include "slconn.h"

void
slctl_fillconn(struct slctlmsg_conn *scc,
    struct slashrpc_cservice *csvc)
{
	struct pscrpc_import *imp;
	lnet_peer_t *lp;

	memset(scc, 0, sizeof(*scc));

	if (csvc == NULL)
		return;

	scc->scc_flags = psc_atomic32_read(&csvc->csvc_flags);
	scc->scc_refcnt = psc_atomic32_read(&csvc->csvc_refcnt);

	imp = csvc->csvc_import;
	if (imp == NULL || imp->imp_connection == NULL)
		return;

	lp = lnet_find_peer_locked(imp->imp_connection->c_peer.nid);
	if (lp == NULL)
		return;

	scc->scc_txcr = lp->lp_txcredits;
}

/**
 * slctlrep_getconns - Send a response to a "GETCONNS" inquiry.
 * @fd: client socket descriptor.
 * @mh: already filled-in control message header.
 * @m: control message to examine and reuse.
 */
int
slctlrep_getconns(int fd, struct psc_ctlmsghdr *mh, void *m)
{
	struct slashrpc_cservice *csvc;
	struct slctlmsg_conn *scc = m;
	struct pscrpc_import *imp;
	struct sl_resm_nid *mn;
	struct sl_resource *r;
	struct sl_resm *resm;
	struct sl_site *s;
	int i, j, rc = 1;
	struct {
		struct slashrpc_cservice *csvc;
		uint32_t stkvers;
	} *expc;

	CONF_LOCK();
	CONF_FOREACH_RESM(s, r, i, resm, j) {
		if (resm == nodeResm ||
		    (RES_ISCLUSTER(r)))
			continue;

		slctl_fillconn(scc, resm->resm_csvc);
		mn = psc_dynarray_getpos(&resm->resm_nids, 0);
		strlcpy(scc->scc_addrbuf, mn->resmnid_addrbuf,
		    sizeof(scc->scc_addrbuf));
		scc->scc_stkvers = resm->resm_stkvers;
		scc->scc_type = resm->resm_type;

		rc = psc_ctlmsg_sendv(fd, mh, scc);
		if (!rc)
			goto done;
	}
 done:
	CONF_ULOCK();

	if (!rc)
		return (rc);

	PLL_LOCK(&client_csvcs);
	PLL_FOREACH(csvc, &client_csvcs) {
		slctl_fillconn(scc, csvc);

		imp = csvc->csvc_import;
		if (imp && imp->imp_connection)
			pscrpc_id2str(imp->imp_connection->c_peer,
			    scc->scc_addrbuf);
		else
			strlcpy(scc->scc_addrbuf, "?",
			    sizeof(scc->scc_addrbuf));
		expc = (void *)csvc->csvc_params.scp_csvcp;
		scc->scc_stkvers = expc->stkvers;
		scc->scc_type = SLCTL_REST_CLI;

		rc = psc_ctlmsg_sendv(fd, mh, scc);
		if (!rc)
			break;
	}
	PLL_ULOCK(&client_csvcs);
	return (rc);
}

__static int
slctlmsg_fcmh_send(int fd, struct psc_ctlmsghdr *mh,
    struct slctlmsg_fcmh *scf, struct fidc_membh *f)
{
	scf->scf_fg = f->fcmh_fg;
	scf->scf_ptruncgen = f->fcmh_sstb.sst_ptruncgen;
	scf->scf_utimgen = f->fcmh_sstb.sst_utimgen;
	scf->scf_st_mode = f->fcmh_sstb.sst_mode;
	scf->scf_uid = f->fcmh_sstb.sst_uid;
	scf->scf_gid = f->fcmh_sstb.sst_gid;
	scf->scf_flags = f->fcmh_flags;
	scf->scf_refcnt = f->fcmh_refcnt;
	scf->scf_size = fcmh_2_fsz(f);
	scf->scf_blksize = f->fcmh_sstb.sst_blksize;
	return (psc_ctlmsg_sendv(fd, mh, scf));
}

int
slctlrep_getfcmhs(int fd, struct psc_ctlmsghdr *mh, void *m)
{
	struct slctlmsg_fcmh *scf = m;
	struct psc_hashbkt *b;
	struct fidc_membh *f;
	int rc;

	rc = 1;
	if (scf->scf_fg.fg_fid == FID_ANY) {
		PSC_HASHTBL_FOREACH_BUCKET(b, &fidcHtable) {
			psc_hashbkt_lock(b);
			PSC_HASHBKT_FOREACH_ENTRY(&fidcHtable, f, b) {
				rc = slctlmsg_fcmh_send(fd, mh, scf, f);
				if (!rc)
					break;
			}
			psc_hashbkt_unlock(b);
			if (!rc)
				break;
		}
	} else {
		f = fidc_lookup_fid(scf->scf_fg.fg_fid);
		if (f) {
			rc = slctlmsg_fcmh_send(fd, mh, scf, f);
			fcmh_op_done_type(f, FCMH_OPCNT_LOOKUP_FIDC);
		} else
			rc = psc_ctlsenderr(fd, mh,
			    "FID "SLPRI_FID" not in cache",
			    scf->scf_fg.fg_fid);
	}
	return (rc);
}
