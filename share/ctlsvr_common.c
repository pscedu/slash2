/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2010, Pittsburgh Supercomputing Center (PSC).
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
	struct sl_resource *r;
	struct sl_resm *resm;
	struct sl_site *s;
	int i, j, rc = 1;

	CONF_LOCK();
	CONF_FOREACH_SITE(s)
		SITE_FOREACH_RES(s, r, i)
			RES_FOREACH_MEMB(r, resm, j) {
				if (resm == nodeResm)
					continue;

				memset(scc, 0, sizeof(*scc));

				strlcpy(scc->scc_addrbuf,
				    resm->resm_addrbuf,
				    sizeof(scc->scc_addrbuf));
				scc->scc_type = r->res_type;

				csvc = resm->resm_csvc;
				if (csvc && sl_csvc_get(&csvc,
				    (psc_atomic32_read(
				     &csvc->csvc_flags) &
				     CSVCF_USE_MULTIWAIT) |
				    CSVCF_NORECON | CSVCF_NONBLOCK,
				    NULL, UINT64_C(0), 0, 0, 0, 0,
				    csvc->csvc_lockinfo.lm_ptr,
				    csvc->csvc_waitinfo, 0, NULL)) {
					scc->scc_flags = psc_atomic32_read(
					    &csvc->csvc_flags);
					scc->scc_refcnt = psc_atomic32_read(
					    &csvc->csvc_refcnt) - 1;
					sl_csvc_decref(csvc);
				}

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
		memset(scc, 0, sizeof(*scc));

		imp = csvc->csvc_import;
		if (imp && imp->imp_connection)
			pscrpc_id2str(imp->imp_connection->c_peer,
			    scc->scc_addrbuf);
		scc->scc_type = SLCTL_REST_CLI;
		scc->scc_flags = psc_atomic32_read(&csvc->csvc_flags);
		scc->scc_refcnt = psc_atomic32_read(&csvc->csvc_refcnt);

		rc = psc_ctlmsg_sendv(fd, mh, scc);
		if (!rc)
			break;
	}
	PLL_ULOCK(&client_csvcs);
	return (rc);
}

__static int
slctlmsg_fcmh_send(int fd, struct psc_ctlmsghdr *mh,
    struct slctlmsg_fcmh *scf, struct fidc_membh *fcmh)
{
	scf->scf_fg = fcmh->fcmh_fg;
	scf->scf_ptruncgen = fcmh->fcmh_sstb.sst_ptruncgen;
	scf->scf_utimgen = fcmh->fcmh_sstb.sst_utimgen;
	scf->scf_st_mode = fcmh->fcmh_sstb.sst_mode;
	scf->scf_uid = fcmh->fcmh_sstb.sst_uid;
	scf->scf_gid = fcmh->fcmh_sstb.sst_gid;
	scf->scf_flags = fcmh->fcmh_flags;
	scf->scf_refcnt = fcmh->fcmh_refcnt;
	scf->scf_size = fcmh_2_fsz(fcmh);
	return (psc_ctlmsg_sendv(fd, mh, scf));
}

int
slctlrep_getfcmh(int fd, struct psc_ctlmsghdr *mh, void *m)
{
	struct slctlmsg_fcmh *scf = m;
	struct fidc_membh *fcmh;
	struct psc_hashbkt *b;
	int rc;

	rc = 1;
	if (scf->scf_fg.fg_fid == FID_ANY) {
		PSC_HASHTBL_FOREACH_BUCKET(b, &fidcHtable) {
			psc_hashbkt_lock(b);
			PSC_HASHBKT_FOREACH_ENTRY(&fidcHtable, fcmh, b) {
				rc = slctlmsg_fcmh_send(fd, mh, scf, fcmh);
				if (!rc)
					break;
			}
			psc_hashbkt_unlock(b);
			if (!rc)
				break;
		}
	} else {
		fcmh = fidc_lookup_fid(scf->scf_fg.fg_fid);
		if (fcmh) {
			rc = slctlmsg_fcmh_send(fd, mh, scf, fcmh);
			fcmh_op_done_type(fcmh, FCMH_OPCNT_LOOKUP_FIDC);
		} else
			rc = psc_ctlsenderr(fd, mh,
			    "FID "SLPRI_FID" not in cache",
			    scf->scf_fg.fg_fid);
	}
	return (rc);
}
