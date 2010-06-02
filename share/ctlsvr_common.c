/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
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
#include "psc_util/ctl.h"
#include "psc_util/ctlsvr.h"
#include "psc_util/lock.h"

#include "ctl.h"
#include "ctlsvr.h"
#include "fidcache.h"
#include "slconfig.h"

/**
 * slctlrep_getconns - send a response to a "GETCONNS" inquiry.
 * @fd: client socket descriptor.
 * @mh: already filled-in control message header.
 * @m: control message to examine and reuse.
 */
int
slctlrep_getconns(int fd, struct psc_ctlmsghdr *mh, void *m)
{
	struct slctlmsg_conn *scc = m;
	struct sl_resource *r;
	struct sl_resm *resm;
	struct sl_site *s;
	int i, j, rc = 1;

	memset(scc, 0, sizeof(*scc));

	CONF_LOCK();
	CONF_FOREACH_SITE(s)
		SITE_FOREACH_RES(s, r, i)
			RES_FOREACH_MEMB(r, resm, j) {
				/* XXX strip off ad@PSC:1.1.1.1@tcp9 */
				strlcpy(scc->scc_addrbuf,
				    resm->resm_addrbuf,
				    sizeof(scc->scc_addrbuf));
				scc->scc_type = r->res_type;

#if 0
				if (zexp->zexp_exp->exp_connection)
					psc_id2str(zexp->zexp_exp->exp_connection->c_peer,
					    zcc->zcc_nidstr);

				scc->scc_flags = resm->resm_csvc->csvc_flags;
				scc->scc_refcnt = resm->resm_csvc->csvc_refcnt;
				scc->scc_xflags = 0;
				if (imp->imp_failed == 0 &&
				    imp->imp_invalid == 0)
					scc->scc_xflags |= SCCF_ONLINE;
#endif

				rc = psc_ctlmsg_sendv(fd, mh, scc);
				if (!rc)
					goto done;
			}
 done:
	CONF_UNLOCK();

#if 0
	lock
	foreach (mexpcli) {
		snprintf(scc->scc_addrbuf, sizeof(scc->scc_addrbuf),
		    "@clients:%s", psc_nid2str());
		scc->scc_type = 0;
		rc = psc_ctlmsg_sendv(fd, mh, scc);
		if (!rc)
			break;
	}
	unlock
#endif

	return (rc);
}

__static int
slctlmsg_file_send(int fd, struct psc_ctlmsghdr *mh,
    struct slctlmsg_file *scf, struct fidc_membh *fcmh)
{
	scf->scf_fg = fcmh->fcmh_fg;
	scf->scf_age = fcmh->fcmh_age.tv_sec;
	scf->scf_gen = fcmh->fcmh_sstb.sst_gen;
	scf->scf_ptruncgen = fcmh->fcmh_sstb.sst_ptruncgen;
	scf->scf_st_mode = fcmh->fcmh_sstb.sst_mode;
	scf->scf_flags = fcmh->fcmh_state;
	scf->scf_refcnt = fcmh->fcmh_refcnt;
	return (psc_ctlmsg_sendv(fd, mh, scf));
}

int
slctlrep_getfile(int fd, struct psc_ctlmsghdr *mh, void *m)
{
	struct slctlmsg_file *scf = m;
	struct fidc_membh *fcmh;
	struct psc_hashbkt *b;
	int rc;

	rc = 1;
	if (scf->scf_fg.fg_fid == FID_ANY) {
		PSC_HASHTBL_FOREACH_BUCKET(b, &fidcHtable) {
			psc_hashbkt_lock(b);
			PSC_HASHBKT_FOREACH_ENTRY(&fidcHtable, fcmh, b) {
				rc = slctlmsg_file_send(fd, mh, scf, fcmh);
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
			rc = slctlmsg_file_send(fd, mh, scf, fcmh);
			fcmh_op_done_type(fcmh, FCMH_OPCNT_LOOKUP_FIDC);
		} else
			rc = psc_ctlsenderr(fd, mh,
			    "FID "FID_FMT" not in cache",
			    FID_FMTARG(scf->scf_fg.fg_fid));
	}
	return (rc);
}
