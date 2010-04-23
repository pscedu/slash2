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
