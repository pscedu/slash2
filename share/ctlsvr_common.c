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

#include "control.h"
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
	struct sl_resm *m;
	struct sl_site *s;
	int i, j, rc = 1;

	memset(scc, 0, sizeof(*scc));

	CONF_LOCK();
	CONF_FOREACH_SITE(s) {
		strlcpy(scc->scc_sitename, s->site_name,
		    sizeof(scc->scc_sitename));
		SITE_FOREACH_RES(s, r, i) {
			strlcpy(scc->scc_resname, r->res_name,
			    sizeof(scc->scc_resname));
			RES_FOREACH_MEMB(r, m, j) {
				strlcpy(scc->scc_resmaddr, m->resm_addrbuf,
				    sizeof(scc->scc_resmaddr));
				scc->scc_type = r->res_type;
//				if (zexp->zexp_exp->exp_connection)
//					psc_id2str(zexp->zexp_exp->exp_connection->c_peer,
//					    zcc->zcc_nidstr);

//				scc->scc_flags = m->resm_csvc->csvc_flags;
//				scc->scc_refcnt = m->resm_csvc->csvc_refcnt;
//				scc->scc_xflags = 0;
//				if (imp->imp_failed == 0 &&
//				    imp->imp_invalid == 0)
//					scc->scc_xflags |= SCCF_ONLINE;

				rc = psc_ctlmsg_sendv(fd, mh, scc);
				if (!rc)
					goto done;
			}
		}
	}
 done:
	CONF_UNLOCK();
	return (rc);
}

int
slctlrep_getclients(int fd, struct psc_ctlmsghdr *mh, void *m)
{
//				if ()
//					scc->scc_xflags |= SCCF_CLIENT;
}

int
slctlrep_getallconns(int fd, struct psc_ctlmsghdr *mh, void *m)
{
	int rc;

	rc = slctlrep_getconns(fd, mh, m);
	if (rc)
		rc = slctlrep_getclients(fd, mh, m);
	return (rc);
}
