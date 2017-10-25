/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright 2007-2016, Pittsburgh Supercomputing Center
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

#include <err.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "pfl/cdefs.h"
#include "pfl/ctl.h"
#include "pfl/ctlcli.h"
#include "pfl/fmt.h"
#include "pfl/getopt.h"
#include "pfl/log.h"
#include "pfl/pfl.h"
#include "pfl/str.h"

#include "sliod/ctl_iod.h"
#include "sliod/slvr.h"

#include "ctl.h"
#include "slerr.h"
#include "ctlcli.h"
#include "pathnames.h"

const char	*daemon_name = "sliod";

void
packshow_bmaps(__unusedx char *bmaps)
{
	psc_ctlmsg_push(SLICMT_GETBMAP, sizeof(struct slctlmsg_bmap));
}

void
packshow_conns(__unusedx char *conn)
{
	psc_ctlmsg_push(SLICMT_GETCONN, sizeof(struct slctlmsg_conn));
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

	scf = psc_ctlmsg_push(SLICMT_GETFCMH, sizeof(*scf));
	scf->scf_fg.fg_fid = FID_ANY;
}

int
replwkst_prhdr(__unusedx struct psc_ctlmsghdr *mh, __unusedx const void *m)
{
	printf("%-16s %6s %28s %4s %7s %7s %6s\n",
	    "replwk-stat-fid", "bmap#", "peer", "refs", "xfer", "total", "%prog");
	return(PSC_CTL_DISPLAY_WIDTH);
}

void
replwkst_prdat(__unusedx const struct psc_ctlmsghdr *mh, const void *m)
{
	char totbuf[PSCFMT_HUMAN_BUFSIZ], curbuf[PSCFMT_HUMAN_BUFSIZ];
	char rbuf[PSCFMT_RATIO_BUFSIZ];
	const struct slictlmsg_replwkst *srws = m;

	pfl_fmt_ratio(rbuf, srws->srws_data_cur, srws->srws_data_tot);
	printf("%016"SLPRIxFID" %6d %28s %4d ",
	    srws->srws_fg.fg_fid, srws->srws_bmapno,
	    srws->srws_peer_addr, srws->srws_refcnt);
	if (psc_ctl_inhuman)
		printf("%7d %7d", srws->srws_data_cur,
		    srws->srws_data_tot);
	else {
		pfl_fmt_human(totbuf, srws->srws_data_tot);
		pfl_fmt_human(curbuf, srws->srws_data_cur);
		printf("%7s %7s", curbuf, totbuf);
	}
	printf(" %6s\n", rbuf);
}

int
slvr_prhdr(__unusedx struct psc_ctlmsghdr *mh, __unusedx const void *m)
{
	printf("%-16s %6s %3s %4s %7s %5s %9s\n",
	    "slvr-fid", "bmap#", "sl#", "refs", "flags", "err", "time");
	return(PSC_CTL_DISPLAY_WIDTH);
}

void
slvr_prdat(__unusedx const struct psc_ctlmsghdr *mh, const void *m)
{
	const struct slictlmsg_slvr *ss = m;

	printf("%016"SLPRIxFID" %6d %3d %4d %c%c%c%c%c%c "
	    "%5d %9"PRId64" \n",
	    ss->ss_fid, ss->ss_bno, ss->ss_slvrno, ss->ss_refcnt,
	    ss->ss_flags & SLVRF_FAULTING	? 'f' : '-',
	    ss->ss_flags & SLVRF_DATARDY	? 'd' : '-',
	    ss->ss_flags & SLVRF_DATAERR	? 'E' : '-',
	    ss->ss_flags & SLVRF_LRU		? 'l' : '-',
	    ss->ss_flags & SLVRF_FREEING	? 'F' : '-',
	    ss->ss_flags & SLVRF_ACCESSED	? 'a' : '-',
	    ss->ss_err, ss->ss_ts.tv_sec);
}

void
slictlcmd_export(int ac, char *av[])
{
	int i, c, recursive = 0, verbose = 0;
	struct slictlmsg_fileop *sfop;

	PFL_OPT_RESET();
	while ((c = getopt(ac, av, "+Rv")) != -1)
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
	int i, c, recursive = 0, verbose = 0, xrepl = 0;
	struct slictlmsg_fileop *sfop;
	char fn[PATH_MAX], *endp;
	slfid_t pfid = FID_ANY;

	PFL_OPT_RESET();
	while ((c = getopt(ac, av, "+F:Rvx")) != -1)
		switch (c) {
		case 'F':
			pfid = strtoull(optarg, &endp, 0);
			if (endp == optarg || *endp)
				errx(1, "%s: invalid parent FID", optarg);
			break;
		case 'R':
			recursive = 1;
			break;
		case 'v':
			verbose = 1;
			break;
		case 'x':
			xrepl = 1;
			break;
		default:
			goto usage;
		}
	ac -= optind;
	av += optind;

	if (ac < 2)
 usage:
		errx(1, "usage: import [-Rvx] [-F pfid] file ... dst");
	for (i = 0; i < ac - 1; i++) {
		if (realpath(av[i], fn) == NULL) {
			warn("%s", av[i]);
			continue;
		}
		sfop = psc_ctlmsg_push(SLICMT_IMPORT, sizeof(*sfop));
		if (recursive)
			sfop->sfop_flags |= SLI_CTL_FOPF_RECURSIVE;
		if (verbose)
			sfop->sfop_flags |= SLI_CTL_FOPF_VERBOSE;
		if (xrepl)
			sfop->sfop_flags |= SLI_CTL_FOPF_XREPL;
		strlcpy(sfop->sfop_fn, fn, sizeof(sfop->sfop_fn));
		strlcpy(sfop->sfop_fn2, av[ac - 1],
		    sizeof(sfop->sfop_fn2));
		sfop->sfop_pfid = pfid;
	}
}

void
slictlcmd_stop(int ac, __unusedx char *av[])
{
	if (ac > 1)
		errx(1, "stop: invalid arguments");
	psc_ctlmsg_push(SLICMT_STOP, 0);
}

struct psc_ctlshow_ent psc_ctlshow_tab[] = {
	PSC_CTLSHOW_DEFS,
	{ "bmaps",		packshow_bmaps },
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
	{ NULL,			NULL,			0,					NULL },
	{ sl_bmap_prhdr,	sl_bmap_prdat,		sizeof(struct slctlmsg_bmap),		NULL },
	{ slvr_prhdr,		slvr_prdat,		sizeof(struct slictlmsg_slvr),		NULL }
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
	extern const char *__progname;

	fprintf(stderr,
	    "usage: %s [-HInV] [-p paramspec] [-S socket] [-s value] [cmd arg ...]\n",
	    __progname);
	exit(1);
}

void
slictl_show_version(void)
{
	fprintf(stderr, "%d\n", sl_stk_version);
}

struct psc_ctlopt opts[] = {
	{ 'H', PCOF_FLAG, &psc_ctl_noheader },
	{ 'I', PCOF_FLAG, &psc_ctl_inhuman },
	{ 'n', PCOF_FLAG, &psc_ctl_nodns },
	{ 'p', PCOF_FUNC, psc_ctlparse_param },
	{ 's', PCOF_FUNC, psc_ctlparse_show },
	{ 'V', PCOF_FLAG, slictl_show_version },
};

int
main(int argc, char *argv[])
{
	pfl_init();
	sl_errno_init();
	pscthr_init(0, NULL, 0, "slictl");

	psc_ctlcli_main(SL_PATH_SLICTLSOCK, argc, argv, opts,
	    nitems(opts));
	exit(0);
}
