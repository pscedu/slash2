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


//flags - Multiwait


__static const char *slconn_restypes[] = {
	"client",
	"archiver",
	"serialfs",
	"compute",
	"mds",
	"parallelfs"
};

void
sl_packshow_conn(__unusedx const char *thr)
{
	psc_ctlmsg_push(SLICMT_GETCONNS, sizeof(struct slctlmsg_conn));
}

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
	const char *site, *nid, *res, *status;
	const struct slctlmsg_conn *scc = m;
	int sitelen;

	/* res@site:1.1.1.1@tcp0 */
	res = scc->scc_addrbuf;
	site = res + strcpsn(res, "@");
	if (*site != '\0')
		*site++ = '\0';
	nid = site + strcpsn(site, ":");
	if (*nid != '\0')
		*nid++ = '\0';
	if (psc_ctl_lastmsgtype != mh->mh_type ||
	    strcmp(lastsite, scc->scc_sitename)) {
		strlcpy(lastsite, scc->scc_sitename, sizeof(lastsite));
		printf(" %s\n", scc->scc_sitename);
	}
	if (strcmp(lastres, res))
		strlcpy(lastres, res, sizeof(lastres));
	else
		res = "";
	if (scc->scc_flags & SCCF_ONLINE)
		status = "online";
	else if (scc->scc_cflags & CSVC_CONNECTING)
		status = "connecting";
	else
		/* XXX differentiate between down and inactive */
		status = "offline";
	printf("   %12s %15s %8s     %c %4d %s\n", res, nid,
	    slconn_restypes[scc->scc_type],
	    scc->scc_flags & CSVCF_MULTIWAIT ? "M" : "-",
	    scc->scc_refcnt, status);
}
