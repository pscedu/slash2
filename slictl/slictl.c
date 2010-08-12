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
#include <stdlib.h>
#include <unistd.h>

#include "pfl/cdefs.h"
#include "pfl/pfl.h"
#include "psc_util/ctl.h"
#include "psc_util/ctlcli.h"
#include "psc_util/fmt.h"
#include "psc_util/log.h"

#include "sliod/ctl_iod.h"
#include "ctl.h"
#include "ctlcli.h"
#include "pathnames.h"

void
packshow_conns(__unusedx const char *thr)
{
	psc_ctlmsg_push(SLICMT_GETCONNS, sizeof(struct slctlmsg_conn));
}

void
packshow_replwkst(__unusedx const char *arg)
{
	psc_ctlmsg_push(SLICMT_GET_REPLWKST, sizeof(struct slictlmsg_replwkst));
}

void
packshow_files(__unusedx const char *thr)
{
	struct slctlmsg_file *scf;

	scf = psc_ctlmsg_push(SLICMT_GETFILES, sizeof(struct slctlmsg_file));
	scf->scf_fg.fg_fid = FID_ANY;
}

void
sliricthr_prdat(const struct psc_ctlmsg_stats *pcst)
{
	printf(" #write %8u", pcst->pcst_nwrite);
}

void
replwkst_prhdr(__unusedx struct psc_ctlmsghdr *mh, __unusedx const void *m)
{
	printf("replication work status\n"
	    " %-16s %5s %33s %7s %7s %6s\n",
	    "fid", "bmap#", "peer", "total", "xfer", "%prog");
}

void
replwkst_prdat(__unusedx const struct psc_ctlmsghdr *mh, const void *m)
{
	char totbuf[PSCFMT_HUMAN_BUFSIZ], curbuf[PSCFMT_HUMAN_BUFSIZ];
	char rbuf[PSCFMT_RATIO_BUFSIZ];
	const struct slictlmsg_replwkst *srws = m;

	psc_fmt_ratio(rbuf, srws->srws_data_cur, srws->srws_data_tot);
	printf(" %016"PRIx64" %5d %33s ",
	    srws->srws_fg.fg_fid, srws->srws_bmapno,
	    srws->srws_peer_addr);
	if (psc_ctl_inhuman)
		printf("%7d %7d", srws->srws_data_tot,
		    srws->srws_data_cur);
	else {
		psc_fmt_human(totbuf, srws->srws_data_tot);
		psc_fmt_human(curbuf, srws->srws_data_cur);
		printf("%7s %7s", totbuf, curbuf);
	}
	printf(" %6s\n", rbuf);
}

struct psc_ctlshow_ent psc_ctlshow_tab[] = {
	{ "connections",	packshow_conns },
	{ "files",		packshow_files },
	{ "loglevels",		psc_ctl_packshow_loglevel },
	{ "replwkst",		packshow_replwkst },
	{ "stats",		psc_ctl_packshow_stats }
};
int psc_ctlshow_ntabents = nitems(psc_ctlshow_tab);

struct psc_ctlmsg_prfmt psc_ctlmsg_prfmts[] = {
	PSC_CTLMSG_PRFMT_DEFS,
	{ replwkst_prhdr,	replwkst_prdat,	sizeof(struct slictlmsg_replwkst),	NULL },
	{ sl_conn_prhdr,	sl_conn_prdat,	sizeof(struct slctlmsg_conn),		NULL }
};
int psc_ctlmsg_nprfmts = nitems(psc_ctlmsg_prfmts);

struct psc_ctl_thrstatfmt psc_ctl_thrstatfmts[] = {
/* CTL		*/ { psc_ctlthr_prdat },
/* LNETAC	*/ { NULL },
/* USKLNDPL	*/ { NULL },
/* RIC		*/ { sliricthr_prdat }
};
int psc_ctl_nthrstatfmts = nitems(psc_ctl_thrstatfmts);

struct psc_ctlcmd_req psc_ctlcmd_reqs[] = {
	{ "exit",	SICC_EXIT }
};
int psc_ctlcmd_nreqs = nitems(psc_ctlcmd_reqs);

const char *progname;

__dead void
usage(void)
{
	fprintf(stderr,
	    "usage: %s [-HI] [-c cmd] [-h table] [-i iostat] [-L listspec]\n"
	    "\t[-m meter] [-P pool] [-p param[=value]] [-S socket] [-s value]\n",
	    progname);
	exit(1);
}

struct psc_ctlopt opts[] = {
	{ 'c', PCOF_FUNC, psc_ctlparse_cmd },
	{ 'H', PCOF_FLAG, &psc_ctl_noheader },
	{ 'h', PCOF_FUNC, psc_ctlparse_hashtable },
	{ 'I', PCOF_FLAG, &psc_ctl_inhuman },
	{ 'i', PCOF_FUNC, psc_ctlparse_iostats },
	{ 'L', PCOF_FUNC, psc_ctlparse_lc },
	{ 'm', PCOF_FUNC, psc_ctlparse_meter },
	{ 'P', PCOF_FUNC, psc_ctlparse_pool },
	{ 'p', PCOF_FUNC, psc_ctlparse_param },
	{ 's', PCOF_FUNC, psc_ctlparse_show }
};
int nopts = nitems(opts);

int
main(int argc, char *argv[])
{
	pfl_init();
	progname = argv[0];
	psc_ctlcli_main(SL_PATH_SLICTLSOCK, argc, argv, opts, nopts);
	exit(0);
}
