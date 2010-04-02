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

#include <err.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "pfl/cdefs.h"
#include "pfl/pfl.h"
#include "pfl/str.h"
#include "psc_ds/vbitmap.h"
#include "psc_util/bitflag.h"
#include "psc_util/ctl.h"
#include "psc_util/ctlcli.h"
#include "psc_util/fmt.h"
#include "psc_util/log.h"

#include "mount_slash/ctl_cli.h"
#include "bmap.h"
#include "msctl.h"
#include "pathnames.h"
#include "slashrpc.h"
#include "slconfig.h"
#include "slerr.h"

struct msctlmsg_replst		 current_mrs;
struct psc_vbitmap		 current_mrs_bmask;
struct psclist_head		 current_mrs_bdata =
				    PSCLIST_HEAD_INIT(current_mrs_bdata);
const struct msctlmsg_replst	 zero_mrs;

struct replst_slave_bdata {
	struct psclist_head	rsb_lentry;
	sl_bmapno_t		rsb_boff;
	sl_bmapno_t		rsb_nbmaps;
	unsigned char		rsb_data[0];
};

struct replrq_arg {
	char	iosv[SITE_NAME_MAX][SL_MAX_REPLICAS];
	int	nios;
	int	code;
	int	bmapno;
};

/* keep in sync with BRP_* constants */
const char *repl_policies[] = {
	"one-time",
	"persist",
	NULL
};

int
rsb_cmp(const void *a, const void *b)
{
	const struct replst_slave_bdata *x = a, *y = b;

	if (x->rsb_boff < y->rsb_boff)
		return (-1);
	else if (x->rsb_boff > y->rsb_boff)
		return (1);
	return (0);
}

int
rsb_isfull(void)
{
	struct replst_slave_bdata *rsb;
	sl_bmapno_t range = 0;

	psclist_for_each_entry(rsb, &current_mrs_bdata, rsb_lentry) {
		if (rsb->rsb_boff != range)
			return (0);
		range += rsb->rsb_nbmaps;
	}
	return (1);
}

void
rsb_accul_replica_stats(struct replst_slave_bdata *rsb, int iosidx,
    sl_bmapno_t *bact, sl_bmapno_t *bold)
{
	sl_bmapno_t n;
	int off;

	off = iosidx * SL_BITS_PER_REPLICA + SL_NBITS_REPLST_BHDR;
	for (n = 0; n < rsb->rsb_nbmaps; n++,
	    off += SL_BITS_PER_REPLICA * current_mrs.mrs_nios +
	    SL_NBITS_REPLST_BHDR) {
		switch (SL_REPL_GET_BMAP_IOS_STAT(rsb->rsb_data, off)) {
		case SL_REPLST_SCHED:
		case SL_REPLST_OLD:
			++*bold;
			break;
		case SL_REPLST_ACTIVE:
			++*bact;
			break;
		}
	}
}

void
pack_replst(const char *fn, __unusedx void *arg)
{
	struct msctlmsg_replst *mrs;

	mrs = psc_ctlmsg_push(MSCMT_GETREPLST,
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
	int n;

	mrq = psc_ctlmsg_push(ra->code,
	    sizeof(struct msctlmsg_replrq));
	mrq->mrq_bmapno = ra->bmapno;
	mrq->mrq_nios = ra->nios;
	for (n = 0; n < ra->nios; n++)
		strlcpy(mrq->mrq_iosv[n], ra->iosv[n],
		    sizeof(mrq->mrq_iosv[0]));
	if (strlcpy(mrq->mrq_fn, fn,
	    sizeof(mrq->mrq_fn)) >= sizeof(mrq->mrq_fn))
		errx(1, "%s: too long", fn);
}

void
parse_replrq(int code, char *replrqspec,
    void (*packf)(const char *, void *))
{
	char *files, *endp, *bmapnos, *bmapno, *next, *bend, *iosv, *ios;
	struct replrq_arg ra;
	int bmax;
	long l;

	iosv = replrqspec;
	ra.code = code;

	bmapnos = strchr(replrqspec, ':');
	if (bmapnos == NULL) {
		warnx("%s: no bmaps specified in replication "
		    "request specification", replrqspec);
		return;
	}
	*bmapnos++ = '\0';

	files = strchr(bmapnos, ':');
	if (files == NULL) {
		warnx("%s: no files specified in replication "
		    "request specification", replrqspec);
		return;
	}
	*files++ = '\0';

	/* parse I/O systems */
	ra.nios = 0;
	for (ios = iosv; ios; ios = next) {
		if ((next = strchr(ios, ',')) != NULL)
			*next++ = '\0';
		if (ra.nios >= nitems(ra.iosv))
			errx(1, "%s: too many replicas specified",
			    replrqspec);
		if (strchr(ios, '@') == NULL)
			errx(1, "%s: no I/O system site specified", ios);
		if (strlcpy(ra.iosv[ra.nios++], ios,
		    sizeof(ra.iosv[0])) >= sizeof(ra.iosv[0]))
			errx(1, "%s: I/O system name too long", ios);
	}

	/* handle special all-bmap case */
	if (strcmp(bmapnos, "*") == 0) {
		ra.bmapno = REPLRQ_BMAPNO_ALL;
		walk(files, packf, &ra);
		return;
	}

	/* parse bmap specs */
	for (bmapno = bmapnos; bmapno; bmapno = next) {
		if ((next = strchr(bmapno, ',')) != NULL)
			*next++ = '\0';
		l = strtol(bmapno, &endp, 10);
		if (l < 0 || endp == bmapno)
			errx(1, "%s: invalid replication request",
			    replrqspec);
		ra.bmapno = bmax = l;
		/* parse bmap range */
		if (*endp == '-') {
			endp++;
			l = strtol(endp, &bend, 10);
			if (l < 0 || l < ra.bmapno ||
			    bend == endp || *bend != '\0')
				errx(1, "%s: invalid replication request",
				    replrqspec);
			bmax = l;
		} else if (*endp != '\0')
			errx(1, "%s: invalid replication request",
			    replrqspec);
		for (; ra.bmapno <= bmax; ra.bmapno++)
			walk(files, packf, &ra);
	}
}

int
lookup_repl_policy(const char *name)
{
	int n;

	for (n = 0; n < NBRP; n++)
		if (strcmp(name, repl_policies[n]) == 0)
			return (n);
	errx(1, "%s: invalid replication policy", name);
}

void
h_fncmd_new_repl_policy(char *val, char *fn)
{
	struct msctlmsg_fncmd_newreplpol *mfnrp;
	int rp;

	if (val)
		errx(1, "new-repl-policy: no policy specified");
	if (fn == NULL)
		errx(1, "new-repl-policy: no file specified");

	rp = lookup_repl_policy(val);

	mfnrp = psc_ctlmsg_push(MSCMT_SET_NEWREPLPOL, sizeof(*mfnrp));
	mfnrp->mfnrp_pol = rp;
	if (strlcpy(mfnrp->mfnrp_fn, fn,
	    sizeof(mfnrp->mfnrp_fn)) >= sizeof(mfnrp->mfnrp_fn)) {
		errno = ENAMETOOLONG;
		err(1, "%s", fn);
	}
}

void
h_fncmd_bmap_repl_policy(char *val, char *bmapspec)
{
	struct msctlmsg_fncmd_bmapreplpol *mfbrp;
	char *fn, *bmapno, *next, *endp, *bend;
	sl_bmapno_t bmin, bmax;
	long l;
	int rp;

	if (val == NULL)
		errx(1, "bmap-repl-policy: no policy specified");
	if (bmapspec == NULL)
		errx(1, "bmap-repl-policy: no bmapspec specified");

	rp = lookup_repl_policy(val);

	fn = strchr(bmapspec, ':');
	if (fn == NULL)
		errx(1, "bmap-repl-policy: no file specified");
	*fn++ = '\0';

	for (bmapno = bmapspec; bmapno; bmapno = next) {
		if ((next = strchr(bmapno, ',')) != NULL)
			*next++ = '\0';
		l = strtol(bmapno, &endp, 10);
		if (l < 0 || (sl_bmapno_t)l >= UINT32_MAX || endp == bmapno)
			errx(1, "%s: invalid bmap number", bmapno);
		bmin = bmax = l;

		/* parse bmap range */
		if (*endp == '-') {
			endp++;
			l = strtol(endp, &bend, 10);
			if (l < 0 || (sl_bmapno_t)l <= bmin ||
			    (sl_bmapno_t)l >= UINT32_MAX ||
			    bend == endp || *bend != '\0')
				errx(1, "%s: invalid bmapspec", endp);
			bmax = l;
		} else if (*endp != '\0')
			errx(1, "%s: invalid bmapspec", bmapno);
		for (; bmin <= bmax; bmin++) {
			mfbrp = psc_ctlmsg_push(MSCMT_SET_BMAPREPLPOL,
			    sizeof(*mfbrp));
			mfbrp->mfbrp_pol = rp;
			mfbrp->mfbrp_bmapno = bmin;
			if (strlcpy(mfbrp->mfbrp_fn, fn,
			    sizeof(mfbrp->mfbrp_fn)) >=
			    sizeof(mfbrp->mfbrp_fn)) {
				errno = ENAMETOOLONG;
				err(1, "%s", fn);
			}
		}
	}
}

struct fncmd_handler {
	const char	 *fh_name;
	void		(*fh_handler)(char *, char *);
} fncmds[] = {
	{ "new-repl-policy",	h_fncmd_new_repl_policy },
	{ "bmap-repl-policy",	h_fncmd_bmap_repl_policy }
};

void
parse_fncmd(char *cmd)
{
	char *p, *val;
	int n;

	p = strchr(cmd, ':');
	if (p)
		*p++ = '\0';
	val = strchr(cmd, '=');
	if (val)
		*val++ = '\0';
	for (n = 0; n < nitems(fncmds); n++)
		if (strcmp(fncmds[n].fh_name, cmd) == 0) {
			fncmds[n].fh_handler(p, val);
			return;
		}
	warnx("%s: unknown file command", cmd);
}

int
replst_slave_check(struct psc_ctlmsghdr *mh, const void *m)
{
	const struct msctlmsg_replst_slave *mrsl = m;
	__unusedx struct srsm_replst_bhdr *srsb;
	struct replst_slave_bdata *rsb;
	uint32_t nb, nbytes, len;
	int rc;

	if (memcmp(&current_mrs, &zero_mrs, sizeof(current_mrs)) == 0)
		errx(1, "received unexpected replication status slave message");

	if (mh->mh_size < sizeof(*mrsl))
		return (sizeof(*mrsl));

	nbytes = howmany((SL_BITS_PER_REPLICA * current_mrs.mrs_nios +
	    SL_NBITS_REPLST_BHDR) * mrsl->mrsl_nbmaps, NBBY);

	len = mh->mh_size - sizeof(*mrsl);
	if (len > SRM_REPLST_PAGESIZ || len != nbytes)
		return (sizeof(*mrsl));
	nb = mrsl->mrsl_nbmaps + psc_vbitmap_nset(&current_mrs_bmask);
	if (nb > current_mrs.mrs_nbmaps)
		errx(1, "invalid value in replication status slave message");

	rc = psc_vbitmap_setrange(&current_mrs_bmask, mrsl->mrsl_boff, mrsl->mrsl_nbmaps);
	if (rc)
		psc_fatalx("replication status bmap data: %s", slstrerror(rc));

	rsb = PSCALLOC(sizeof(*rsb) + nbytes);
	rsb->rsb_nbmaps = mrsl->mrsl_nbmaps;
	rsb->rsb_boff = mrsl->mrsl_boff;
	memcpy(rsb->rsb_data, mrsl->mrsl_data, nbytes);
	psclist_add_sorted(&current_mrs_bdata, &rsb->rsb_lentry, rsb_cmp,
	    offsetof(struct replst_slave_bdata, rsb_lentry));

	if (nb != current_mrs.mrs_nbmaps || !rsb_isfull())
		return (-1);
	return (0);
}

void
replst_slave_prhdr(__unusedx struct psc_ctlmsghdr *mh, __unusedx const void *m)
{
	/* XXX add #repls, #bmaps */
	printf("replication status\n"
	    " %-62s %4s %4s %6s\n",
	    "file", "#blk", "#old", "%prog");
}

void
replst_slave_prdat(__unusedx const struct psc_ctlmsghdr *mh,
    __unusedx const void *m)
{
	char map[SL_NREPLST], pmap[SL_NREPLST], rbuf[PSCFMT_RATIO_BUFSIZ];
	struct replst_slave_bdata *rsb, *nrsb;
	struct srsm_replst_bhdr bhdr;
	sl_blkno_t bact, bold, nb;
	int n, nbw, off, dlen;
	uint32_t iosidx;

	map[SL_REPLST_SCHED] = 's';
	map[SL_REPLST_OLD] = 'o';
	map[SL_REPLST_ACTIVE] = '+';
	map[SL_REPLST_INACTIVE] = '-';
	map[SL_REPLST_TRUNCPNDG] = 't';

	pmap[SL_REPLST_SCHED] = 'S';
	pmap[SL_REPLST_OLD] = 'O';
	pmap[SL_REPLST_ACTIVE] = '*';
	pmap[SL_REPLST_INACTIVE] = '-';
	map[SL_REPLST_TRUNCPNDG] = 'T';

	dlen = PSC_CTL_DISPLAY_WIDTH - strlen("repl-policy: ") -
	    strlen(repl_policies[BRP_ONETIME]);
	n = printf(" %s", current_mrs.mrs_fn);
	if (n > dlen)
		printf("\n    ");
	else
		printf("%*s", dlen - n, "");
	printf("repl-policy: ");
	if (current_mrs.mrs_newreplpol >= NBRP)
		printf("<unknown: %d>\n", current_mrs.mrs_newreplpol);
	else
		printf("%s\n", repl_policies[current_mrs.mrs_newreplpol]);

	for (iosidx = 0; iosidx < current_mrs.mrs_nios; iosidx++) {
		nbw = 0;
		bact = bold = 0;
		psclist_for_each_entry(rsb, &current_mrs_bdata, rsb_lentry)
			rsb_accul_replica_stats(rsb, iosidx, &bact, &bold);

		psc_fmt_ratio(rbuf, bact, bact + bold);
		printf("     %-58s %4d %4d %6s",
		    current_mrs.mrs_iosv[iosidx],
		    bact + bold, bold, rbuf);
		psclist_for_each_entry(rsb, &current_mrs_bdata, rsb_lentry) {
			pfl_bitstr_copy(&bhdr, 0, rsb->rsb_data,
			    SL_BITS_PER_REPLICA * iosidx, SL_NBITS_REPLST_BHDR);
			off = SL_BITS_PER_REPLICA * iosidx + SL_NBITS_REPLST_BHDR;
			for (nb = 0; nb < rsb->rsb_nbmaps; nb++, nbw++,
			    off += SL_BITS_PER_REPLICA * current_mrs.mrs_nios +
			    SL_NBITS_REPLST_BHDR) {
				if (nbw > 76)
					nbw = 0;
				if (nbw == 0)
					printf("\n\t");
				putchar((bhdr.srsb_repl_policy == BRP_PERSIST ? pmap : map)
				    [SL_REPL_GET_BMAP_IOS_STAT(rsb->rsb_data, off)]);
			}
		}
		putchar('\n');
	}

	/* reset current_mrs for next replst */
	psclist_for_each_entry_safe(rsb, nrsb, &current_mrs_bdata, rsb_lentry) {
		psclist_del(&rsb->rsb_lentry);
		PSCFREE(rsb);
	}
	memcpy(&current_mrs, &zero_mrs, sizeof(current_mrs));
}

int
replst_savdat(__unusedx struct psc_ctlmsghdr *mh, const void *m)
{
	const struct msctlmsg_replst *mrs = m;
	int blen;

	if (mh->mh_size != sizeof(*mrs))
		return (sizeof(*mrs));

	if (memcmp(&current_mrs, &zero_mrs, sizeof(current_mrs)))
		psc_fatalx("communication error: replication status not completed");

	if (mrs->mrs_nios > SL_MAX_REPLICAS)
		psc_fatalx("communication error: replication status has too many replicas");

	memcpy(&current_mrs, mrs, sizeof(current_mrs));
	psc_vbitmap_resize(&current_mrs_bmask, current_mrs.mrs_nbmaps);
	psc_vbitmap_clearall(&current_mrs_bmask);

	blen = current_mrs.mrs_nbmaps * howmany(SL_BITS_PER_REPLICA *
	    current_mrs.mrs_nios, NBBY);
	if (current_mrs.mrs_nbmaps == 0) {
		replst_slave_prhdr(NULL, NULL);
		for (blen = 0; blen < PSC_CTL_DISPLAY_WIDTH; blen++)
			putchar('=');
		putchar('\n');
		replst_slave_prdat(NULL, NULL);
	}
	return (-1);
}

struct psc_ctlshow_ent psc_ctlshow_tab[] = {
	{ "loglevels",	psc_ctl_packshow_loglevel },
	{ "stats",	psc_ctl_packshow_stats }
};
int psc_ctlshow_ntabents = nitems(psc_ctlshow_tab);

struct psc_ctlmsg_prfmt psc_ctlmsg_prfmts[] = {
	PSC_CTLMSG_PRFMT_DEFS,
	{ NULL,			NULL,			0, NULL },
	{ NULL,			NULL,			0, NULL },
	{ NULL,			NULL,			0, replst_savdat },
	{ replst_slave_prhdr,	replst_slave_prdat,	0, replst_slave_check },
	{ NULL,			NULL,			0, NULL },
	{ NULL,			NULL,			0, NULL }
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
	    "usage: %s [-HIR] [-c cmd] [-f cmd] [-h table] [-i iostat] [-L listspec] [-m meter]\n"
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
	sockfn = SL_PATH_MSCTLSOCK;
	while ((c = getopt(argc, argv, "c:f:Hh:Ii:L:m:P:p:Q:Rr:S:s:U:")) != -1)
		switch (c) {
		case 'c':
			psc_ctlparse_cmd(optarg);
			break;
		case 'f':
			parse_fncmd(optarg);
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
			parse_replrq(MSCMT_ADDREPLRQ,
			    optarg, pack_replrq);
			break;
		case 'R':
			recursive = 1;
			break;
		case 'r':
			if (optarg[0] == ':')
				pack_replst("", NULL);
			else
				walk(optarg, pack_replst, NULL);
			break;
		case 'S':
			sockfn = optarg;
			break;
		case 's':
			psc_ctlparse_show(optarg);
			break;
		case 'U':
			parse_replrq(MSCMT_DELREPLRQ,
			    optarg, pack_replrq);
			break;
		default:
			usage();
		}

	argc -= optind;
	if (argc)
		usage();

	psc_ctlcli_main(sockfn);
	if (memcmp(&current_mrs, &zero_mrs, sizeof(current_mrs)))
		errx(1, "communication error: replication status "
		    "not completed (%zd/%zd)",
		    psc_vbitmap_getsize(&current_mrs_bmask) -
		    psc_vbitmap_nfree(&current_mrs_bmask),
		    psc_vbitmap_getsize(&current_mrs_bmask));
	exit(0);
}
