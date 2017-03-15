/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright (c) 2009-2015, Pittsburgh Supercomputing Center (PSC).
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or (at
 * your option) any later version.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License contained in the file
 * `COPYING-GPL' at the top of this distribution or at
 * https://www.gnu.org/licenses/gpl-2.0.html for more details.
 * ---------------------------------------------------------------------
 * %END_LICENSE%
 */

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <ctype.h>
#include <curses.h>
#include <netdb.h>
#include <string.h>

#include "pfl/cdefs.h"
#include "pfl/ctl.h"
#include "pfl/ctlcli.h"
#include "pfl/fmt.h"
#include "pfl/str.h"

#include "ctl.h"
#include "ctlcli.h"
#include "fidcache.h"
#include "slconfig.h"
#include "slconn.h"

#include "slashd/bmap_mds.h"

/* based on enum sl_res_type */
__static const char *slconn_restypes[] = {
	"client",
	"archival",
	NULL,
	"mds",
	"parallel",
	NULL,
	"local"
};

int
sl_conn_prhdr(__unusedx struct psc_ctlmsghdr *mh, __unusedx const void *m)
{
	printf("%-11s %38s %-7s %5s %7s %4s %5s %11s\n",
	    "resource", "host", "type", "flags", "stkvers", "txcr", "#ref", "uptime");
	return(PSC_CTL_DISPLAY_WIDTH+15);
}

void
sl_conn_gethostname(const char *p, char *buf)
{
	union {
		struct sockaddr_storage	ss;
		struct sockaddr_in	sin;
		struct sockaddr		sa;
	} saun;

	if (psc_ctl_nodns)
		goto cancel;

	memset(&saun, 0, sizeof(saun));
	if (inet_pton(AF_INET, p, &saun.sin.sin_addr) != 1)
		goto cancel;

	saun.sa.sa_family = AF_INET;
	if (getnameinfo(&saun.sa, sizeof(struct sockaddr_in), buf,
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
	int col, connected = 0;

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

	printf("  %-11s %36.36s %-8s %c",
	    res, nid, stype,
	    scc->scc_flags & CSVCF_CONNECTING		? 'C' : '-');

	if (scc->scc_flags & CSVCF_CONNECTED) {
		printf("O");
		connected = 1;
	} else {
		printf("-");
	}

	printf("%c%c ",
	    scc->scc_flags & CSVCF_WATCH		? 'W' : '-',
	    scc->scc_flags & CSVCF_PING			? 'P' : '-');

	/*
 	 * If you see unexpected color, please double check
 	 * your msctl/slictl/slmctl path.
 	 */
	if (scc->scc_flags & CSVCF_CTL_OLDER)
		col = COLOR_RED;
	else if (scc->scc_flags & CSVCF_CTL_NEWER)
		col = COLOR_BLUE;
	else
		col = COLOR_GREEN;

	if (scc->scc_stkvers)
		setcolor(col);
	printf("%7d ", scc->scc_stkvers);
	if (scc->scc_stkvers)
		uncolor();

	printf("%4d %5d ", scc->scc_txcr, scc->scc_refcnt);

	if (connected)
		printf("%4ldd%02ldh%02ldm\n",
		    scc->scc_uptime / (60 * 60 * 24),
		    (scc->scc_uptime % (60 * 60 * 24)) / (60 * 60),
		    (scc->scc_uptime % (60 * 60)) / 60);
	else
		printf("   --------\n");

	strlcpy(lastsite, site, sizeof(lastsite));
	strlcpy(lastres, res, sizeof(lastres));
}

int
sl_bmap_prhdr(__unusedx struct psc_ctlmsghdr *mh, __unusedx const void *m)
{
	int width;

	width = psc_ctl_get_display_maxwidth() - PSC_CTL_DISPLAY_WIDTH;
	if (width > 16)
		printf("%-16s ", "addr");
	printf("%-16s %6s %-9s %5s %18s %7s\n",
	    "fid", "bmapno", "flags", "refs", "ios", "seqno");
	return(PSC_CTL_DISPLAY_WIDTH);
}

void
sl_bmap_prdat(__unusedx const struct psc_ctlmsghdr *mh, const void *m)
{
	const struct slctlmsg_bmap *scb = m;
	int width;

	width = psc_ctl_get_display_maxwidth() - PSC_CTL_DISPLAY_WIDTH;
	if (width > 16)
		printf("%16"PRIx64" ", scb->scb_addr);
	printf("%016"SLPRIxFID" %6d "
	    "%c%c%c%c%c%c%c%c%c "
	    "%5u %18s %7"PRIu64"\n",
	    scb->scb_fg.fg_fid, scb->scb_bno,
	    scb->scb_flags & BMAPF_RD		? 'R' : '-',
	    scb->scb_flags & BMAPF_WR		? 'W' : '-',
	    scb->scb_flags & BMAPF_LOADED	? 'L' : '-',
	    scb->scb_flags & BMAPF_LOADING	? 'l' : '-',
	    scb->scb_flags & BMAPF_DIO		? 'D' : '-',
	    scb->scb_flags & BMAPF_TOFREE	? 'F' : '-',
	    scb->scb_flags & BMAPF_MODECHNG	? 'G' : '-',
	    scb->scb_flags & BMAPF_WAITERS	? 'w' : '-',
	    scb->scb_flags & BMAPF_BUSY		? 'B' : '-',
	    scb->scb_opcnt, scb->scb_resname, scb->scb_seq);
}

#ifdef _SLASH_MDS
# define BLKSIZE_LABEL "msize"
#else
# define BLKSIZE_LABEL "blksize"
#endif

int
sl_fcmh_prhdr(__unusedx struct psc_ctlmsghdr *mh, __unusedx const void *m)
{
	int width;

	width = psc_ctl_get_display_maxwidth() - PSC_CTL_DISPLAY_WIDTH;
	printf("%-16s %10s %6s %5s %5s "
	    "%7s %3s %7s %4s %6s",
	    "fid", "flags", "mode", "uid", "gid",
	    "size", "ref", "fgen", "pgen", "ugen");
	if (width > 6)
		printf(" %6s", BLKSIZE_LABEL);
	printf("\n");
	return(PSC_CTL_DISPLAY_WIDTH);
}

void
sl_fcmh_prdat(__unusedx const struct psc_ctlmsghdr *mh, const void *m)
{
	const struct slctlmsg_fcmh *scf = m;
	char fidbuf[SL_FIDBUF_LEN];
	char buf[PSCFMT_HUMAN_BUFSIZ];
	int width;

	sl_sprintf_fgen(scf->scf_fg.fg_gen, fidbuf, SL_FIDBUF_LEN);
	width = psc_ctl_get_display_maxwidth() - PSC_CTL_DISPLAY_WIDTH;
	pfl_fmt_human(buf, scf->scf_size);
	printf("%016"SLPRIxFID" %c%c%c%c%c%c%c%c%c "
	    "%6o %5u %5u %7s "
	    "%3d %7s "
	    "%4u %6u",
	    scf->scf_fg.fg_fid,
	    scf->scf_flags & FCMH_FREE		? 'F' : '-',
	    scf->scf_flags & FCMH_IDLE		? 'i' : '-',
	    scf->scf_flags & FCMH_INITING	? 'I' : '-',
	    scf->scf_flags & FCMH_WAITING	? 'W' : '-',
	    scf->scf_flags & FCMH_TOFREE	? 'T' : '-',
	    scf->scf_flags & FCMH_HAVE_ATTRS	? 'A' : '-',
	    scf->scf_flags & FCMH_GETTING_ATTRS	? 'G' : '-',
	    scf->scf_flags & FCMH_BUSY		? 'S' : '-',
	    scf->scf_flags & FCMH_DELETED	? 'D' : '-',
	    scf->scf_st_mode, scf->scf_uid, scf->scf_gid, buf,
	    scf->scf_refcnt, fidbuf, scf->scf_ptruncgen, scf->scf_utimgen);
	if (width > 6)
		printf(" %6"PRIu64, scf->scf_blksize);
	printf("\n");
}
