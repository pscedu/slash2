/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2007-2011, Pittsburgh Supercomputing Center (PSC).
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

#include <err.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "pfl/cdefs.h"
#include "pfl/getopt.h"
#include "pfl/pfl.h"
#include "pfl/str.h"
#include "psc_util/ctl.h"
#include "psc_util/ctlcli.h"
#include "psc_util/fmt.h"
#include "psc_util/log.h"

#include "sliod/ctl_iod.h"
#include "ctl.h"
#include "ctlcli.h"
#include "pathnames.h"

int		 verbose;
int		 recursive;
const char	*progname;
const char	*daemon_name = "sliod";

void
packshow_conns(__unusedx char *conn)
{
	psc_ctlmsg_push(SLICMT_GETCONNS, sizeof(struct slctlmsg_conn));
}

void
packshow_replwkst(__unusedx char *fid)
{
	psc_ctlmsg_push(SLICMT_GET_REPLWKST, sizeof(struct slictlmsg_replwkst));
}

void
packshow_fcmhs(__unusedx char *fid)
{
	struct slctlmsg_fcmh *scf;

	scf = psc_ctlmsg_push(SLICMT_GETFCMH, sizeof(struct slctlmsg_fcmh));
	scf->scf_fg.fg_fid = FID_ANY;
}

void
sliricthr_pr(const struct psc_ctlmsg_thread *pcst)
{
	printf(" #write %8u", pcst->pcst_nwrite);
}

void
replwkst_prhdr(__unusedx struct psc_ctlmsghdr *mh, __unusedx const void *m)
{
	printf("%-16s %6s %33s %7s %7s %6s\n",
	    "replwk-stat-fid", "bmap#", "peer", "xfer", "total", "%prog");
}

void
replwkst_prdat(__unusedx const struct psc_ctlmsghdr *mh, const void *m)
{
	char totbuf[PSCFMT_HUMAN_BUFSIZ], curbuf[PSCFMT_HUMAN_BUFSIZ];
	char rbuf[PSCFMT_RATIO_BUFSIZ];
	const struct slictlmsg_replwkst *srws = m;

	psc_fmt_ratio(rbuf, srws->srws_data_cur, srws->srws_data_tot);
	printf("%016"SLPRIxFID" %6d %33s ",
	    srws->srws_fg.fg_fid, srws->srws_bmapno,
	    srws->srws_peer_addr);
	if (psc_ctl_inhuman)
		printf("%7d %7d", srws->srws_data_cur,
		    srws->srws_data_tot);
	else {
		psc_fmt_human(totbuf, srws->srws_data_tot);
		psc_fmt_human(curbuf, srws->srws_data_cur);
		printf("%7s %7s", curbuf, totbuf);
	}
	printf(" %6s\n", rbuf);
}

void
slictlcmd_export(int ac, char *av[])
{
	struct slictlmsg_fileop *sfop;
	int i, c;

	PFL_OPT_RESET();
	while ((c = getopt(ac, av, "Rv")) != -1)
		switch (c) {
		case 'R':
			recursive = 1;
			break;
		case 'v':
			verbose = 1;
			break;
		default:
			goto usage;
		}
	ac -= optind;
	av += optind;

	if (ac < 2)
 usage:
		errx(1, "usage: export [-Rv] file ... dst");
	for (i = 0; i < ac - 1; i++) {
		sfop = psc_ctlmsg_push(SLICMT_EXPORT, sizeof(*sfop));
		if (recursive)
			sfop->sfop_flags |= SLI_CTL_FOPF_RECURSIVE;
		if (verbose)
			sfop->sfop_flags |= SLI_CTL_FOPF_VERBOSE;
		strlcpy(sfop->sfop_fn, av[i], sizeof(sfop->sfop_fn));
		strlcpy(sfop->sfop_fn2, av[ac - 1], sizeof(sfop->sfop_fn));
	}
}

void
slictlcmd_import(int ac, char *av[])
{
	int i, c, preserve_path = 0;
	struct slictlmsg_fileop *sfop;
	struct stat sb;

	PFL_OPT_RESET();
	while ((c = getopt(ac, av, "Rv")) != -1)
		switch (c) {
		case 'R':
			recursive = 1;
			break;
		case 'v':
			verbose = 1;
			break;
		default:
			goto usage;
		}
	ac -= optind;
	av += optind;

	if (ac < 2)
 usage:
		errx(1, "usage: import [-PRv] file ... dst");
	for (i = 0; i < ac - 1; i++) {
		if (stat(av[i], &sb) == -1)
			err(1, "%s", av[i]);
		sfop = psc_ctlmsg_push(SLICMT_IMPORT, sizeof(*sfop));
		if (recursive)
			sfop->sfop_flags |= SLI_CTL_FOPF_RECURSIVE;
		if (verbose)
			sfop->sfop_flags |= SLI_CTL_FOPF_VERBOSE;
		strlcpy(sfop->sfop_fn, av[i], sizeof(sfop->sfop_fn));
		strlcpy(sfop->sfop_fn2, av[ac - 1], sizeof(sfop->sfop_fn));
	}
}

void
slictlcmd_stop(int ac, char *av[])
{
	if (ac > 1)
		errx(1, "stop: unknown arguments");
	psc_ctlmsg_push(SLICMT_STOP, 0);
}

struct psc_ctlshow_ent psc_ctlshow_tab[] = {
	PSC_CTLSHOW_DEFS,
	{ "connections",	packshow_conns },
	{ "fcmhs",		packshow_fcmhs },
	{ "replwkst",		packshow_replwkst },

	/* aliases */
	{ "conns",		packshow_conns },
	{ "files",		packshow_fcmhs },
	{ "fidcache",		packshow_fcmhs }
};

struct psc_ctlmsg_prfmt psc_ctlmsg_prfmts[] = {
	PSC_CTLMSG_PRFMT_DEFS,
	{ replwkst_prhdr,	replwkst_prdat,		sizeof(struct slictlmsg_replwkst),	NULL },
	{ sl_conn_prhdr,	sl_conn_prdat,		sizeof(struct slctlmsg_conn),		NULL },
	{ sl_fcmh_prhdr,	sl_fcmh_prdat,		sizeof(struct slctlmsg_fcmh),		NULL },
	{ NULL,			NULL,			sizeof(struct slictlmsg_fileop),	NULL },
	{ NULL,			NULL,			sizeof(struct slictlmsg_fileop),	NULL },
	{ NULL,			NULL,			0,					NULL }
};

psc_ctl_prthr_t psc_ctl_prthrs[] = {
/* ASYNC_IO	*/ NULL,
/* BMAPRLS	*/ NULL,
/* CONN		*/ NULL,
/* CTL		*/ psc_ctlthr_pr,
/* CTLAC	*/ psc_ctlacthr_pr,
/* LNETAC	*/ NULL,
/* REPLFIN	*/ NULL,
/* REPLPND	*/ NULL,
/* REPLREAP	*/ NULL,
/* RIC		*/ sliricthr_pr,
/* RII		*/ NULL,
/* RIM		*/ NULL,
/* SLVR_CRC	*/ NULL,
/* STATFS	*/ NULL,
/* TIOS		*/ NULL,
/* USKLNDPL	*/ NULL
};

struct psc_ctlcmd_req psc_ctlcmd_reqs[] = {
	{ "export",	slictlcmd_export },
	{ "import",	slictlcmd_import },
	{ "stop",	slictlcmd_stop }
};

PFLCTL_CLI_DEFS;

__dead void
usage(void)
{
	fprintf(stderr,
	    "usage: %s [-HInRv] [-p paramspec] [-S socket] [-s value] [cmd arg ...]\n",
	    progname);
	exit(1);
}

struct psc_ctlopt opts[] = {
	{ 'H', PCOF_FLAG, &psc_ctl_noheader },
	{ 'I', PCOF_FLAG, &psc_ctl_inhuman },
	{ 'i', PCOF_FUNC, psc_ctlparse_iostats },
	{ 'L', PCOF_FUNC, psc_ctlparse_lc },
	{ 'n', PCOF_FLAG, &psc_ctl_nodns },
	{ 'p', PCOF_FUNC, psc_ctlparse_param },
	{ 'P', PCOF_FUNC, psc_ctlparse_pool },
	{ 'R', PCOF_FLAG, &recursive },
	{ 's', PCOF_FUNC, psc_ctlparse_show },
	{ 'v', PCOF_FLAG, &verbose }
};

int
main(int argc, char *argv[])
{
	pfl_init();
	progname = argv[0];
	psc_ctlcli_main(SL_PATH_SLICTLSOCK, argc, argv, opts,
	    nitems(opts));
	exit(0);
}
