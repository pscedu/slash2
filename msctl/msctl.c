/* $Id$ */

#include <err.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "pfl/pfl.h"
#include "psc_util/cdefs.h"
#include "psc_util/ctl.h"
#include "psc_util/ctlcli.h"
#include "psc_util/log.h"
#include "psc_util/strlcpy.h"

#include "pathnames.h"

#include "mount_slash/control.h"
#include "inode.h"
#include "msctl.h"

struct replrq_arg {
	int code;
	int bmapno;
};

void
pack_replst(const char *fn, __unusedx void *arg)
{
	struct msctlmsg_replst *mrs;

	mrs = psc_ctlmsg_push(SCMT_GETREPLST,
	    sizeof(struct msctlmsg_replst));
	if (strlcpy(mrs->mrs_fn, fn,
	    sizeof(mrs->mrs_fn)) >= sizeof(mrs->mrs_fn))
		errx(1, "%s: too long", fn);
}

void
pack_replrq(const char *fn, void *arg)
{
	struct msctlmsg_replrq *mrq;
	struct replrq_arg *ra = arg;

	mrq = psc_ctlmsg_push(ra->code,
	    sizeof(struct msctlmsg_replrq));
	mrq->mrq_bmapno = ra->bmapno;
	if (strlcpy(mrq->mrq_fn, fn,
	    sizeof(mrq->mrq_fn)) >= sizeof(mrq->mrq_fn))
		errx(1, "%s: too long", fn);
}

void
parse_replrq(int code, char *replrqspec,
    void (*packf)(const char *, void *), int allow_bmapno)
{
	char *endp, *bmapnos, *bmapno, *next;
	struct replrq_arg ra;

	ra.code = code;
	ra.bmapno = REPLRQ_BMAPNO_ALL;

	bmapnos = strchr(replrqspec, ':');
	if (bmapnos) {
		*bmapnos++ = '\0';
		if (allow_bmapno) {
			for (bmapno = bmapnos;
			    bmapno && *bmapno != '\0';
			    bmapno = next) {
				if ((next = strchr(bmapno, ',')) != NULL)
					*next++ = '\0';
				ra.bmapno = strtol(bmapno, &endp, 10);
				if (ra.bmapno < 1 || bmapno[0] == '\0' ||
				    *endp != '\0')
					errx(1, "%s: invalid replication request",
					    replrqspec);
				walk(optarg, packf, &ra);
			}
		} else {
			if (replrqspec[0] != '\0')
				errx(1, "%s: bmap specification not allowed",
				    replrqspec);
			packf("", &ra);
		}
	} else
		walk(replrqspec, packf, &ra);
}

int
replst_check(struct psc_ctlmsghdr *mh, __unusedx const void *m)
{
	__unusedx struct msctlmsg_replst *mrs;

	if (mh->mh_size < sizeof(*mrs) ||
	    (mh->mh_size - sizeof(*mrs)) % SL_REPLICA_NBYTES)
		return (sizeof(*mrs));
	return (0);
}

void
replst_prhdr(__unusedx struct psc_ctlmsghdr *mh, __unusedx const void *m)
{
	printf("replication status\n");
}

void
replst_prdat(__unusedx const struct psc_ctlmsghdr *mh, const void *m)
{
	const struct msctlmsg_replst *mrs = m;

	printf(" %s\n", mrs->mrs_fn);
}

struct psc_ctlshow_ent psc_ctlshow_tab[] = {
	{ "loglevels",	psc_ctl_packshow_loglevel },
	{ "stats",	psc_ctl_packshow_stats }
};
int psc_ctlshow_ntabents = nitems(psc_ctlshow_tab);

struct psc_ctlmsg_prfmt psc_ctlmsg_prfmts[] = {
	PSC_CTLMSG_PRFMT_DEFS,
	{ NULL,		NULL,		0, NULL },
	{ NULL,		NULL,		0, NULL },
	{ replst_prhdr,	replst_prdat,	0, replst_check }
};
int psc_ctlmsg_nprfmts = nitems(psc_ctlmsg_prfmts);

struct psc_ctl_thrstatfmt psc_ctl_thrstatfmts[] = {
/* CTL		*/	{ psc_ctlthr_prdat },
};
int psc_ctl_nthrstatfmts = nitems(psc_ctl_thrstatfmts);

struct psc_ctlcmd_req psc_ctlcmd_reqs[] = {
};
int psc_ctlcmd_nreqs = nitems(psc_ctlcmd_reqs);

const char *progname;
int recursive;

__dead void
usage(void)
{
	fprintf(stderr,
	    "usage: %s [-HIR] [-c cmd] [-h table] [-i iostat] [-L listspec] [-m meter]\n"
	    "\t[-P pool] [-p param[=value]] [-Q replrqspec] [-r replrqspec] [-S socket]\n"
	    "\t[-s value] [-U replrqspec]\n",
	    progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	const char *sockfn;
	int c;

	pfl_init();
	progname = argv[0];
	sockfn = _PATH_MSCTLSOCK;
	while ((c = getopt(argc, argv, "c:Hh:Ii:L:m:P:p:Q:Rr:S:s:U:")) != -1)
		switch (c) {
		case 'c':
			psc_ctlparse_cmd(optarg);
			break;
		case 'H':
			psc_ctl_noheader = 1;
			break;
		case 'h':
			psc_ctlparse_hashtable(optarg);
			break;
		case 'I':
			psc_ctl_inhuman = 1;
			break;
		case 'i':
			psc_ctlparse_iostats(optarg);
			break;
		case 'L':
			psc_ctlparse_lc(optarg);
			break;
		case 'm':
			psc_ctlparse_meter(optarg);
			break;
		case 'P':
			psc_ctlparse_pool(optarg);
			break;
		case 'p':
			psc_ctlparse_param(optarg);
			break;
		case 'Q':
			parse_replrq(SCMT_ADDREPLRQ,
			    optarg, pack_replrq, 1);
			break;
		case 'R':
			recursive = 1;
			break;
		case 'r':
			parse_replrq(0, optarg, pack_replst, 0);
			break;
		case 'S':
			sockfn = optarg;
			break;
		case 's':
			psc_ctlparse_show(optarg);
			break;
		case 'U':
			parse_replrq(SCMT_DELREPLRQ,
			    optarg, pack_replrq, 1);
			break;
		default:
			usage();
		}

	argc -= optind;
	if (argc)
		usage();

	psc_ctlcli_main(sockfn);
	exit(0);
}
