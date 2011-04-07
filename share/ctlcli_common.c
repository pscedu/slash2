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

#include <string.h>

#include "pfl/cdefs.h"
#include "pfl/str.h"
#include "psc_util/ctl.h"
#include "psc_util/ctlcli.h"
#include "psc_util/fmt.h"

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
	"parallelfs",
	"standalone",
	"?"
};

void
sl_conn_prhdr(__unusedx struct psc_ctlmsghdr *mh, __unusedx const void *m)
{
	printf("%-16s %41s %-10s %5s %4s\n",
	    "network-resource", "host", "type", "flags", "#ref");
}

void
sl_conn_prdat(const struct psc_ctlmsghdr *mh, const void *m)
{
	static char lastsite[SITE_NAME_MAX], lastres[RES_NAME_MAX];
	char *p, *site, *nid, *res, addrbuf[RESM_ADDRBUF_SZ];
	const struct slctlmsg_conn *scc = m;
	int type;

	strlcpy(addrbuf, scc->scc_addrbuf, sizeof(addrbuf));
	if (scc->scc_type == SLCTL_REST_CLI) {
		site = "clients";
		res = lastres;
		nid = addrbuf;
	} else {
		/* res@site:1.1.1.1@tcp0 */
		res = addrbuf;
		site = res + strcspn(res, "@");
		if (*site != '\0')
			*site++ = '\0';
		nid = site + strcspn(site, ":");
		if (*nid != '\0')
			*nid++ = '\0';
	}
	p = strchr(nid, '@');
	if (p)
		*p = '\0';

	if (psc_ctl_lastmsgtype != mh->mh_type ||
	    strcmp(lastsite, site)) {
		strlcpy(lastsite, site, sizeof(lastsite));
		printf("%s\n", site);
	}
	if (strcmp(lastres, res))
		strlcpy(lastres, res, sizeof(lastres));
	else
		res = "";

	type = scc->scc_type;
	if (type < 0 || type >= nitems(slconn_restypes))
		type = nitems(slconn_restypes) - 1;

	printf("  %-14s %41s %-10s %c%c%c%c%c %4d\n", res, nid,
	    strcmp(lastres, res) ? "" : slconn_restypes[scc->scc_type],
	    scc->scc_flags & CSVCF_CONNECTING		? 'C' : '-',
	    scc->scc_flags & CSVCF_CONNECTED		? 'O' : '-',
	    scc->scc_flags & CSVCF_USE_MULTIWAIT	? 'M' : '-',
	    scc->scc_flags & CSVCF_ABANDON		? 'A' : '-',
	    scc->scc_flags & CSVCF_WANTFREE		? 'F' : '-',
	    scc->scc_refcnt);
}

void
sl_fcmh_prhdr(__unusedx struct psc_ctlmsghdr *mh, __unusedx const void *m)
{
	printf("%-16s %10s %6s %5s %5s "
	    "%8s %4s %9s %4s %4s\n",
	    "file-ID_(in_hex)", "flags", "mode", "uid", "gid",
	    "size", "#ref", "fgen", "pgen", "ugen");
}

void
sl_fcmh_prdat(__unusedx const struct psc_ctlmsghdr *mh, const void *m)
{
	const struct slctlmsg_fcmh *scf = m;
	char buf[PSCFMT_HUMAN_BUFSIZ];

	psc_fmt_human(buf, scf->scf_size);
	printf("%016"SLPRIxFID" %c%c%c%c%c%c%c%c%c%c "
	    "%6o %5u %5u %8s %4d %9s %4u %4u\n",
	    scf->scf_fg.fg_fid,
	    scf->scf_flags & FCMH_CAC_FREE	? 'F' : '-',
	    scf->scf_flags & FCMH_CAC_IDLE	? 'i' : '-',
	    scf->scf_flags & FCMH_CAC_BUSY	? 'B' : '-',
	    scf->scf_flags & FCMH_CAC_INITING	? 'I' : '-',
	    scf->scf_flags & FCMH_CAC_WAITING	? 'W' : '-',
	    scf->scf_flags & FCMH_CAC_TOFREE	? 'T' : '-',
	    scf->scf_flags & FCMH_CAC_REAPED	? 'R' : '-',
	    scf->scf_flags & FCMH_HAVE_ATTRS	? 'A' : '-',
	    scf->scf_flags & FCMH_GETTING_ATTRS	? 'G' : '-',
	    scf->scf_flags & FCMH_CTOR_FAILED	? 'f' : '-',
	    scf->scf_st_mode, scf->scf_uid, scf->scf_gid, buf,
	    scf->scf_refcnt, sl_sprinta_fgen(scf->scf_fg.fg_gen),
	    scf->scf_ptruncgen, scf->scf_utimgen);
}
