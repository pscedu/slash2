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

#include <string.h>

#include "pfl/cdefs.h"
#include "pfl/str.h"
#include "psc_util/ctl.h"
#include "psc_util/ctlcli.h"

#include "ctl.h"
#include "ctlcli.h"
#include "fidcache.h"
#include "slconfig.h"
#include "slconn.h"

__static const char *slconn_restypes[] = {
	"client",
	"archiver",
	"serialfs",
	"compute",
	"mds",
	"parallelfs"
};

void
sl_conn_prhdr(__unusedx struct psc_ctlmsghdr *mh, __unusedx const void *m)
{
	printf("network connection status\n"
	    " %-16s %5s %33s %7s %7s %6s\n",
	    "resource", "host", "type", "flags", "#refs", "status");
}

void
sl_conn_prdat(const struct psc_ctlmsghdr *mh, const void *m)
{
	static char lastsite[SITE_NAME_MAX], lastres[RES_NAME_MAX];
	char *site, *nid, *res, *status, addrbuf[RESM_ADDRBUF_SZ];
	const struct slctlmsg_conn *scc = m;

	/* res@site:1.1.1.1@tcp0 */
	strlcpy(addrbuf, scc->scc_addrbuf, sizeof(addrbuf));
	res = addrbuf;
	site = res + strcspn(res, "@");
	if (*site != '\0')
		*site++ = '\0';
	nid = site + strcspn(site, ":");
	if (*nid != '\0')
		*nid++ = '\0';
	if (psc_ctl_lastmsgtype != mh->mh_type ||
	    strcmp(lastsite, site)) {
		strlcpy(lastsite, site, sizeof(lastsite));
		printf(" %s\n", site);
	}
	if (strcmp(lastres, res))
		strlcpy(lastres, res, sizeof(lastres));
	else
		res = "";
	if (scc->scc_flags & SCCF_ONLINE)
		status = "online";
	else if (scc->scc_cflags & CSVCF_CONNECTING)
		status = "connecting";
	else
		/* XXX differentiate between down and inactive */
		status = "offline";
	printf("   %12s %15s %8s     %c %4d %s\n", res, nid,
	    slconn_restypes[scc->scc_type],
	    scc->scc_flags & CSVCF_USE_MULTIWAIT ? 'M' : '-',
	    scc->scc_refcnt, status);
}

void
sl_file_prhdr(__unusedx struct psc_ctlmsghdr *mh, __unusedx const void *m)
{
	printf("files\n"
	    " %-16s %5s %33s %7s %7s %6s\n",
	    "resource", "host", "type", "flags", "#refs", "status");
}

void
sl_file_prdat(const struct psc_ctlmsghdr *mh, const void *m)
{
	const struct slctlmsg_file *scf = m;

	printf("   %12s %c%c%c%c%c%c%c%c%c%c\n",
	    "",
	    scf->scf_flags & FCMH_CAC_FREE	? 'F' : '-',
	    scf->scf_flags & FCMH_CAC_CLEAN	? 'C' : '-',
	    scf->scf_flags & FCMH_CAC_DIRTY	? 'D' : '-',
	    scf->scf_flags & FCMH_CAC_FAILED	? 'X' : '-',
	    scf->scf_flags & FCMH_CAC_FREEING	? 'R' : '-',
	    scf->scf_flags & FCMH_CAC_INITING	? 'I' : '-',
	    scf->scf_flags & FCMH_CAC_WAITING	? 'W' : '-',
	    scf->scf_flags & FCMH_HAVE_ATTRS	? 'A' : '-',
	    scf->scf_flags & FCMH_GETTING_ATTRS	? 'G' : '-',
	    scf->scf_flags & FCMH_WAITING_ATTRS	? 'N' : '-');

}
