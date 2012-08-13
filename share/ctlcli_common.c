/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2012, Pittsburgh Supercomputing Center (PSC).
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

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <ctype.h>
#include <netdb.h>
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
	"archival",
	NULL,
	"mds",
	"parallel",
	NULL,
	"serial"
};

void
sl_conn_prhdr(__unusedx struct psc_ctlmsghdr *mh, __unusedx const void *m)
{
	printf("%-10s %37s %-8s %6s %5s %4s %4s\n",
	    "resource", "host", "type", "flags", "stvrs", "txcr", "#ref");
}

void
sl_conn_gethostname(const char *p, char *buf)
{
	union {
		struct sockaddr_storage	ss;
		struct sockaddr_in	sin;
		struct sockaddr		sa;
	} sun;

	if (psc_ctl_nodns)
		goto cancel;

	memset(&sun, 0, sizeof(sun));
	if (inet_pton(AF_INET, p, &sun.sin.sin_addr) != 1)
		goto cancel;

	sun.sa.sa_family = AF_INET;
	if (getnameinfo(&sun.sa, sizeof(struct sockaddr_in), buf,
	    NI_MAXHOST, NULL, 0, NI_NAMEREQD))
		goto cancel;

	return;

 cancel:
	strlcpy(buf, p, NI_MAXHOST);
}

void
sl_conn_prdat(const struct psc_ctlmsghdr *mh, const void *m)
{
	static char lastsite[SITE_NAME_MAX], lastres[RES_NAME_MAX];
	char *p, *site, nid[NI_MAXHOST], *res, addrbuf[RESM_ADDRBUF_SZ];
	const struct slctlmsg_conn *scc = m;
	const char *addr, *stype, *prid;
	int type;

	type = scc->scc_type;
	if (type < 0 || type >= nitems(slconn_restypes))
		type = nitems(slconn_restypes) - 1;
	stype = slconn_restypes[scc->scc_type];

	strlcpy(addrbuf, scc->scc_addrbuf, sizeof(addrbuf));
	if (scc->scc_type == SLCTL_REST_CLI) {
		/* 'U' pid '-' ip '@' lnet */
		site = "clients";
		prid = addrbuf;
		addr = prid;
		if (*addr == 'U') {
			addr++;
			while (isdigit(*addr))
				addr++;
			if (*addr == '-')
				addr++;
			else
				addr = prid;
		}
		stype = "";
		res = "";
	} else {
		/* res '@' site */
		res = addrbuf;
		site = res + strcspn(res, "@");
		if (*site != '\0')
			*site++ = '\0';
		p = site + strcspn(site, ":");
		if (*p == ':')
			*p++ = '\0';
		addr = p;

		if (strcmp(res, lastres) == 0 &&
		    strcmp(site, lastsite) == 0) {
			res = "";
			stype = "";
		}
	}
	p = strchr(addr, '@');
	if (p)
		*p = '\0';
	sl_conn_gethostname(addr, nid);

	if (psc_ctl_lastmsgtype != mh->mh_type ||
	    strcmp(site, lastsite))
		printf("%s\n", site);

	printf("  %-11s %35s %-8s %c%c%c%c%c %5d %4d %4d\n",
	    res, nid, stype,
	    scc->scc_flags & CSVCF_CONNECTING		? 'C' : '-',
	    scc->scc_flags & CSVCF_CONNECTED		? 'O' : '-',
	    scc->scc_flags & CSVCF_ABANDON		? 'A' : '-',
	    scc->scc_flags & CSVCF_WANTFREE		? 'F' : '-',
	    scc->scc_flags & CSVCF_PING			? 'P' : '-',
	    scc->scc_stkvers, scc->scc_txcr, scc->scc_refcnt);

	strlcpy(lastsite, site, sizeof(lastsite));
	strlcpy(lastres, res, sizeof(lastres));
}

#ifdef _SLASH_MDS
# define BLKSIZE_LABEL "msize"
#else
# define BLKSIZE_LABEL "blksize"
#endif

void
sl_fcmh_prhdr(__unusedx struct psc_ctlmsghdr *mh, __unusedx const void *m)
{
	int w;

	w = psc_ctl_get_display_maxwidth() - PSC_CTL_DISPLAY_WIDTH;
	printf("%-16s %11s %6s %5s %5s "
	    "%7s %4s %9s %4s %4s",
	    "file-ID_(in_hex)", "flags", "mode", "uid", "gid",
	    "size", "#ref", "fgen", "pgen", "ugen");
	if (w > 6)
		printf(" %6s", BLKSIZE_LABEL);
	printf("\n");
}

void
sl_fcmh_prdat(__unusedx const struct psc_ctlmsghdr *mh, const void *m)
{
	const struct slctlmsg_fcmh *scf = m;
	char buf[PSCFMT_HUMAN_BUFSIZ];
	int w;

	w = psc_ctl_get_display_maxwidth() - PSC_CTL_DISPLAY_WIDTH;
	psc_fmt_human(buf, scf->scf_size);
	printf("%016"SLPRIxFID" %c%c%c%c%c%c%c%c%c%c%c "
	    "%6o %5u %5u "
	    "%7s %4d %9s %4u %4u",
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
	    scf->scf_flags & FCMH_NO_BACKFILE	? 'N' : '-',
	    scf->scf_st_mode, scf->scf_uid, scf->scf_gid, buf,
	    scf->scf_refcnt, sl_sprinta_fgen(scf->scf_fg.fg_gen),
	    scf->scf_ptruncgen, scf->scf_utimgen);
	if (w > 6)
		printf(" %6"PRIu64, scf->scf_blksize);
	printf("\n");
}
