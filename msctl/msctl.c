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

#include <sys/types.h>
#include <sys/statvfs.h>

#include <ctype.h>
#include <curses.h>
#include <err.h>
#include <paths.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <term.h>
#include <unistd.h>

#include "pfl/cdefs.h"
#include "pfl/pfl.h"
#include "pfl/str.h"
#include "psc_util/bitflag.h"
#include "psc_util/ctl.h"
#include "psc_util/ctlcli.h"
#include "psc_util/fmt.h"
#include "psc_util/log.h"

#include "mount_slash/ctl_cli.h"
#include "bmap.h"
#include "ctl.h"
#include "ctlcli.h"
#include "msctl.h"
#include "pathnames.h"
#include "slashrpc.h"
#include "slconfig.h"
#include "slerr.h"

int				 verbose;
int				 has_col;

struct msctlmsg_replst		 current_mrs;
int				 current_mrs_eof;
struct psclist_head		 current_mrs_bdata =
				    PSCLIST_HEAD_INIT(current_mrs_bdata);

struct replst_slave_bdata {
	struct psclist_head	 rsb_lentry;
	sl_bmapno_t		 rsb_boff;
	sl_bmapno_t		 rsb_nbmaps;
	unsigned char		 rsb_data[0];
};

struct replrq_arg {
	char			 iosv[SITE_NAME_MAX][SL_MAX_REPLICAS];
	int			 nios;
	int			 opcode;
	int			 bmapno;
};

struct bmap_range {
	sl_bmapno_t		 bmin;
	sl_bmapno_t		 bmax;
	struct psc_listentry	 lentry;
};

struct repl_policy_arg {
	int			 opcode;
	int			 replpol;
	struct psc_lockedlist	 branges;
};

struct fnfidpair {
	struct stat		 ffp_stb;
	char			 ffp_fn[PATH_MAX];
	slfid_t			 ffp_fid;
	struct psc_hashent	 ffp_hentry;
};

/* keep in sync with BRPOL_* constants */
const char *repl_policies[] = {
	"one-time",
	"persist",
	NULL
};

struct psc_hashtbl fnfidpairs;

slfid_t
fn2fid(const char *fn)
{
	struct fnfidpair *ffp;
	struct statvfs sfb;
	struct stat stb;
	slfid_t fid;

	if (stat(fn, &stb) == -1)
		err(1, "stat %s", fn);
	if (statvfs(fn, &sfb) == -1)
		err(1, "statvfs %s", fn);

#ifndef HAVE_NO_FUSE_FSID
	if (sfb.f_fsid != SLASH_FSID)
		errx(1, "%s: file is not in a SLASH file system %lx",
		    fn, sfb.f_fsid);
#endif

	fid = stb.st_ino;
	ffp = psc_hashtbl_search(&fnfidpairs, NULL, NULL, &fid);

	if (ffp)
		return (ffp->ffp_fid);

	ffp = PSCALLOC(sizeof(*ffp));
	ffp->ffp_stb = stb;
	psc_hashent_init(&fnfidpairs, ffp);
	strlcpy(ffp->ffp_fn, fn, sizeof(ffp->ffp_fn));
	ffp->ffp_fid = stb.st_ino;
	psc_hashtbl_add_item(&fnfidpairs, ffp);
	return (stb.st_ino);
}

const char *
fid2fn(slfid_t fid, struct stat *stb)
{
	static char fn[PATH_MAX];
	struct fnfidpair *ffp;

	ffp = psc_hashtbl_search(&fnfidpairs, NULL, NULL, &fid);
	if (ffp) {
		if (stb)
			*stb = ffp->ffp_stb;
		return (PCPP_STR(ffp->ffp_fn));
	}
	if (stb)
		memset(stb, 0, sizeof(*stb));
	snprintf(fn, sizeof(fn), "<"SLPRI_FID">", fid);
	return (PCPP_STR(fn));
}

int
rsb_cmp(const void *a, const void *b)
{
	const struct replst_slave_bdata *x = a, *y = b;

	return (CMP(x->rsb_boff, y->rsb_boff));
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
    sl_bmapno_t *bact, sl_bmapno_t *both)
{
	sl_bmapno_t n;
	int off;

	off = iosidx * SL_BITS_PER_REPLICA + SL_NBITS_REPLST_BHDR;
	for (n = 0; n < rsb->rsb_nbmaps; n++,
	    off += SL_BITS_PER_REPLICA * current_mrs.mrs_nios +
	    SL_NBITS_REPLST_BHDR) {
		switch (SL_REPL_GET_BMAP_IOS_STAT(rsb->rsb_data, off)) {
		case BREPLST_VALID:
			++*bact;
			break;
		default:
			++*both;
			break;
		}
	}
}

void
packshow_conns(__unusedx char *conn)
{
	psc_ctlmsg_push(MSCMT_GETCONNS, sizeof(struct slctlmsg_conn));
}

void
packshow_fcmhs(__unusedx char *fid)
{
	struct slctlmsg_fcmh *scf;

	scf = psc_ctlmsg_push(MSCMT_GETFCMH,
	    sizeof(struct slctlmsg_fcmh));
	scf->scf_fg.fg_fid = FID_ANY;
}

void
pack_replst(const char *fn, __unusedx const struct stat *stb,
    __unusedx void *arg)
{
	struct msctlmsg_replst *mrs;

	mrs = psc_ctlmsg_push(MSCMT_GETREPLST,
	    sizeof(struct msctlmsg_replst));
	mrs->mrs_fid = fn2fid(fn);
}

void
pack_replrq(const char *fn, const struct stat *stb, void *arg)
{
	struct msctlmsg_replrq *mrq;
	struct replrq_arg *ra = arg;
	int n;

	if (S_ISDIR(stb->st_mode)) {
		if (!recursive) {
			errno = EISDIR;
			warn("%s", fn);
		}
		return;
	}

	mrq = psc_ctlmsg_push(ra->opcode,
	    sizeof(struct msctlmsg_replrq));
	mrq->mrq_bmapno = ra->bmapno;
	mrq->mrq_nios = ra->nios;
	for (n = 0; n < ra->nios; n++)
		strlcpy(mrq->mrq_iosv[n], ra->iosv[n],
		    sizeof(mrq->mrq_iosv[0]));
	mrq->mrq_fid = fn2fid(fn);
}

void
parse_replrq(int opcode, char *replrqspec,
    void (*packf)(const char *, const struct stat *, void *))
{
	char *files, *endp, *bmapnos, *bmapno, *next, *bend, *iosv, *ios;
	struct replrq_arg ra;
	int bmax;
	long l;

	iosv = replrqspec;
	ra.opcode = opcode;

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

	for (n = 0; n < NBRPOL; n++)
		if (strcmp(name, repl_policies[n]) == 0)
			return (n);
	errx(1, "%s: invalid replication policy", name);
}

void
cmd_new_bmap_repl_policy_one(const char *fn,
    __unusedx const struct stat *stb, void *arg)
{
	struct msctlmsg_newreplpol *mfnrp;
	struct repl_policy_arg *a = arg;

	mfnrp = psc_ctlmsg_push(a->opcode, sizeof(*mfnrp));
	mfnrp->mfnrp_pol = a->replpol;
	mfnrp->mfnrp_fid = fn2fid(fn);
}

void
cmd_new_bmap_repl_policy(int ac, char **av)
{
	struct repl_policy_arg arg;
	const char *s;
	int i;

	s = strchr(av[0], '=');
	if (s) {
		arg.opcode = MSCMT_SET_NEWREPLPOL;
		arg.replpol = lookup_repl_policy(s + 1);
	} else
		arg.opcode = MSCMT_GET_NEWREPLPOL;

	if (ac < 2)
		errx(1, "new-bmap-repl-policy: no file(s) specified");
	for (i = 1; i < ac; i++)
		walk(av[i], cmd_new_bmap_repl_policy_one, &arg);
}

void
cmd_bmap_repl_policy_one(const char *fn,
    __unusedx const struct stat *stb, void *arg)
{
	struct msctlmsg_bmapreplpol *mfbrp;
	struct repl_policy_arg *a = arg;
	struct bmap_range *br;

	PLL_FOREACH(br, &a->branges) {
		mfbrp = psc_ctlmsg_push(a->opcode, sizeof(*mfbrp));
		mfbrp->mfbrp_pol = a->replpol;
		mfbrp->mfbrp_bmapno = br->bmin;
		mfbrp->mfbrp_nbmaps = br->bmax - br->bmin + 1;
		mfbrp->mfbrp_fid = fn2fid(fn);
	}
}

void
cmd_bmap_repl_policy(int ac, char **av)
{
	char *bmapspec, *bmapno, *next, *endp, *bend, *val;
	struct bmap_range *br, *br_next;
	struct repl_policy_arg arg;
	long l;
	int i;

	pll_init(&arg.branges, struct bmap_range, lentry, NULL);

	val = strchr(av[0], '=');

	bmapspec = strchr(av[0], ':');
	if (bmapspec == NULL)
		errx(1, "bmap-repl-policy: no bmapspec specified");
	*bmapspec++ = '\0';

	if (val) {
		arg.replpol = lookup_repl_policy(val + 1);
		arg.opcode = MSCMT_SET_BMAPREPLPOL;
	} else
		arg.opcode = MSCMT_GET_BMAPREPLPOL;

	for (bmapno = bmapspec; bmapno; bmapno = next) {
		if ((next = strchr(bmapno, ',')) != NULL)
			*next++ = '\0';
		l = strtol(bmapno, &endp, 10);
		if (l < 0 || (sl_bmapno_t)l >= UINT32_MAX || endp == bmapno)
			errx(1, "%s: invalid bmap number", bmapno);

		br = PSCALLOC(sizeof(*br));
		INIT_LISTENTRY(&br->lentry);
		br->bmin = br->bmax = l;

		/* parse bmap range */
		if (*endp == '-') {
			endp++;
			l = strtol(endp, &bend, 10);
			if (l < 0 || (sl_bmapno_t)l <= br->bmin ||
			    (sl_bmapno_t)l >= UINT32_MAX ||
			    bend == endp || *bend != '\0')
				errx(1, "%s: invalid bmapspec", endp);
			br->bmax = l;
		} else if (*endp != '\0')
			errx(1, "%s: invalid bmapspec", bmapno);
		pll_add(&arg.branges, br);
	}
	for (i = 1; i < ac; i++)
		walk(av[i], cmd_bmap_repl_policy_one, &arg);
	PLL_FOREACH_SAFE(br, br_next, &arg.branges)
		PSCFREE(br);
}

void
cmd_replrq_one(const char *fn, __unusedx const struct stat *stb,
    void *arg)
{
	struct msctlmsg_replrq *mrq;
	struct replrq_arg *a = arg;

	mrq = psc_ctlmsg_push(a->opcode, sizeof(*mrq));
	mrq->mrq_fid = fn2fid(fn);
}

void
cmd_replrq(int ac, char **av)
{
	struct replrq_arg arg;
	int i;

	if (ac < 2)
		errx(1, "%s: no file(s) specified", av[0]);
	if (strncmp(av[0], "repl-add", strlen("repl-add")) == 0)
		arg.opcode = MSCMT_ADDREPLRQ;
	else
		arg.opcode = MSCMT_DELREPLRQ;
	for (i = 1; i < ac; i++)
		walk(av[i], cmd_replrq_one, &arg);
}

void
cmd_replst_one(const char *fn, __unusedx const struct stat *stb,
    __unusedx void *arg)
{
	struct msctlmsg_replst *mrs;

	mrs = psc_ctlmsg_push(MSCMT_GETREPLST, sizeof(*mrs));
	mrs->mrs_fid = fn2fid(fn);
}

void
cmd_replst(int ac, char **av)
{
	struct replrq_arg arg;
	int i;

	if (ac < 2)
		errx(1, "%s: no file(s) specified", av[0]);
	arg.opcode = MSCMT_GETREPLST;
	for (i = 1; i < ac; i++)
		walk(av[i], cmd_replst_one, &arg);
}

int
replst_slave_check(struct psc_ctlmsghdr *mh, const void *m)
{
	const struct msctlmsg_replst_slave *mrsl = m;
	__unusedx struct srsm_replst_bhdr *srsb;
	struct replst_slave_bdata *rsb;
	uint32_t nbytes, len;

	if (pfl_memchk(&current_mrs, 0, sizeof(current_mrs)))
		errx(1, "received unexpected replication status slave message");

	if (mh->mh_size < sizeof(*mrsl))
		return (sizeof(*mrsl));

	nbytes = howmany((SL_BITS_PER_REPLICA * current_mrs.mrs_nios +
	    SL_NBITS_REPLST_BHDR) * mrsl->mrsl_nbmaps, NBBY);

	len = mh->mh_size - sizeof(*mrsl);
	if (len > SRM_REPLST_PAGESIZ || len != nbytes)
		return (sizeof(*mrsl));

	if (mrsl->mrsl_flags & MRSLF_EOF)
		current_mrs_eof = 1;
	if (mrsl->mrsl_nbmaps) {
		rsb = PSCALLOC(sizeof(*rsb) + nbytes);
		INIT_PSC_LISTENTRY(&rsb->rsb_lentry);
		rsb->rsb_nbmaps = mrsl->mrsl_nbmaps;
		rsb->rsb_boff = mrsl->mrsl_boff;
		memcpy(rsb->rsb_data, mrsl->mrsl_data, nbytes);
		psclist_add_sorted(&current_mrs_bdata,
		    &rsb->rsb_lentry, rsb_cmp,
		    offsetof(struct replst_slave_bdata, rsb_lentry));
	}

	if (!current_mrs_eof || !rsb_isfull())
		return (-1);
	return (0);
}

void
fnstat_prhdr(__unusedx struct psc_ctlmsghdr *mh, __unusedx const void *m)
{
	/* XXX add #repls, #bmaps */
	printf("%-59s %6s %6s %6s\n",
	    "file-replication-status", "#valid", "#bmap", "%prog");
}

void
setcolor(int col)
{
	if (!has_col || col == -1)
		return;
	putp(tparm(enter_bold_mode));
	putp(tparm(set_a_foreground, col));
}

void
uncolor(void)
{
	if (!has_col)
		return;
	putp(tparm(orig_pair));
	putp(tparm(exit_attribute_mode));
}

void
fnstat_prdat(__unusedx const struct psc_ctlmsghdr *mh,
    __unusedx const void *m)
{
	sl_bmapno_t bact, both, nb;
	int val, n, nbw, off, dlen, cmap[NBREPLST], maxwidth;
	char *label, map[NBREPLST], pmap[NBREPLST], rbuf[PSCFMT_RATIO_BUFSIZ];
	struct replst_slave_bdata *rsb, *nrsb;
	struct srsm_replst_bhdr bhdr;
	struct stat stb;
	uint32_t iosidx;

	maxwidth = psc_ctl_get_display_maxwidth();

	map[BREPLST_INVALID] = '-';
	map[BREPLST_REPL_SCHED] = 's';
	map[BREPLST_REPL_QUEUED] = 'q';
	map[BREPLST_VALID] = '+';
	map[BREPLST_TRUNCPNDG] = 't';
	map[BREPLST_TRUNCPNDG_SCHED] = 'p';
	map[BREPLST_GARBAGE] = 'g';
	map[BREPLST_GARBAGE_SCHED] = 'x';

	pmap[BREPLST_INVALID] = '/';
	pmap[BREPLST_REPL_SCHED] = 'S';
	pmap[BREPLST_REPL_QUEUED] = 'Q';
	pmap[BREPLST_VALID] = '*';
	pmap[BREPLST_TRUNCPNDG] = 'T';
	pmap[BREPLST_TRUNCPNDG_SCHED] = 'P';
	pmap[BREPLST_GARBAGE] = 'G';
	pmap[BREPLST_GARBAGE_SCHED] = 'X';

	brepls_init(cmap, -1);
	cmap[BREPLST_INVALID] = COLOR_BLACK;
	cmap[BREPLST_REPL_SCHED] = COLOR_YELLOW;
	cmap[BREPLST_REPL_QUEUED] = -1;
	cmap[BREPLST_VALID] = COLOR_GREEN;
	cmap[BREPLST_TRUNCPNDG_SCHED] = COLOR_BLUE;
	cmap[BREPLST_GARBAGE] = COLOR_BLACK;
	cmap[BREPLST_GARBAGE_SCHED] = COLOR_BLUE;

	n = printf("%s", fid2fn(current_mrs.mrs_fid, &stb));
	if (S_ISDIR(stb.st_mode)) {
		n += printf("/");
		label = " repl-policy: ";
	} else
		label = " new-bmap-repl-policy: ";
	dlen = maxwidth - strlen(label) -
	    strlen(repl_policies[BRPOL_ONETIME]);
	if (n > dlen)
		printf("\n%*s", dlen, "");
	else
		printf("%*s", dlen - n, "");
	printf("%s", label);
	if (current_mrs.mrs_newreplpol >= NBRPOL)
		printf("<unknown: %d>\n", current_mrs.mrs_newreplpol);
	else
		printf("%s\n", repl_policies[current_mrs.mrs_newreplpol]);

	for (iosidx = 0; iosidx < current_mrs.mrs_nios; iosidx++) {
		nbw = 0;
		bact = both = 0;
		psclist_for_each_entry(rsb, &current_mrs_bdata, rsb_lentry)
			rsb_accul_replica_stats(rsb, iosidx, &bact, &both);

		setcolor(COLOR_CYAN);
		printf("  %-57s", current_mrs.mrs_iosv[iosidx]);
		uncolor();

		psc_fmt_ratio(rbuf, bact, bact + both);
		printf(" %6d %6d %6s", bact, bact + both, rbuf);

		psclist_for_each_entry(rsb, &current_mrs_bdata, rsb_lentry) {
			off = SL_BITS_PER_REPLICA * iosidx +
			    SL_NBITS_REPLST_BHDR;
			for (nb = 0; nb < rsb->rsb_nbmaps; nb++, nbw++,
			    off += SL_BITS_PER_REPLICA *
			    current_mrs.mrs_nios + SL_NBITS_REPLST_BHDR) {
				if (nbw >= maxwidth - 4)
					nbw = 0;
				if (nbw == 0)
					printf("\n    ");

				memset(&bhdr, 0, sizeof(bhdr));
				pfl_bitstr_copy(&bhdr, 0, rsb->rsb_data, nb *
				    (SL_NBITS_REPLST_BHDR + SL_BITS_PER_REPLICA *
				     current_mrs.mrs_nios), SL_NBITS_REPLST_BHDR);
				val = SL_REPL_GET_BMAP_IOS_STAT(rsb->rsb_data, off);
				setcolor(cmap[val]);
				putchar((bhdr.srsb_replpol == BRPOL_PERSIST ?
				    pmap : map)[SL_REPL_GET_BMAP_IOS_STAT(
				    rsb->rsb_data, off)]);
				uncolor();
			}
		}
		putchar('\n');
	}

	/* reset current_mrs for next replst */
	psclist_for_each_entry_safe(rsb, nrsb, &current_mrs_bdata, rsb_lentry) {
		psclist_del(&rsb->rsb_lentry, &current_mrs_bdata);
		PSCFREE(rsb);
	}
	memset(&current_mrs, 0, sizeof(current_mrs));
	current_mrs_eof = 0;
}

int
replst_savdat(__unusedx struct psc_ctlmsghdr *mh, const void *m)
{
	const struct msctlmsg_replst *mrs = m;

	if (mh->mh_size != sizeof(*mrs))
		return (sizeof(*mrs));

	if (!pfl_memchk(&current_mrs, 0, sizeof(current_mrs)))
		psc_fatalx("communication error: replication status not completed");

	if (mrs->mrs_nios > SL_MAX_REPLICAS)
		psc_fatalx("communication error: replication status # "
		    "replicas out of range (%u)", mrs->mrs_nios);
	memcpy(&current_mrs, mrs, sizeof(current_mrs));
	return (-1);
}

void
ms_ctlmsg_error_prdat(__unusedx const struct psc_ctlmsghdr *mh,
    const void *m)
{
	const struct psc_ctlmsg_error *pce = m;
	struct fnfidpair *ffp;
	const char *p, *endp;
	slfid_t fid;
	int i;

	if (psc_ctl_lastmsgtype != mh->mh_type &&
	    psc_ctl_lastmsgtype != -1)
		fprintf(stderr, "\n");
	p = pce->pce_errmsg;
	if (*p++ != '0')
		goto out;
	if (*p++ != 'x')
		goto out;
	endp = p;
	for (i = 0; i < 16; i++, endp++)
		if (!isxdigit(*endp))
			goto out;
	if (*endp != ':')
		goto out;
	fid = strtoull(p, NULL, 16);
	ffp = psc_hashtbl_search(&fnfidpairs, NULL, NULL, &fid);
	if (ffp == NULL)
		goto out;
	warnx("%s%s", ffp->ffp_fn, endp);
	return;

 out:
	warnx("%s", pce->pce_errmsg);
}

struct psc_ctlshow_ent psc_ctlshow_tab[] = {
	PSC_CTLSHOW_DEFS,
	{ "connections",	packshow_conns },
	{ "fcmhs",		packshow_fcmhs },

	/* aliases */
	{ "conns",		packshow_conns },
	{ "fidcache",		packshow_fcmhs },
	{ "files",		packshow_fcmhs }
};

#define psc_ctlmsg_error_prdat ms_ctlmsg_error_prdat

struct psc_ctlmsg_prfmt psc_ctlmsg_prfmts[] = {
	PSC_CTLMSG_PRFMT_DEFS,
/* ADDREPLRQ		*/ { NULL,		NULL,		0,				NULL },
/* DELREPLRQ		*/ { NULL,		NULL,		0,				NULL },
/* GETCONNS		*/ { sl_conn_prhdr,	sl_conn_prdat,	sizeof(struct slctlmsg_conn),	NULL },
/* GETFCMH		*/ { sl_fcmh_prhdr,	sl_fcmh_prdat,	sizeof(struct slctlmsg_fcmh),	NULL },
/* GETREPLST		*/ { NULL,		NULL,		0,				replst_savdat },
/* GETREPLST_SLAVE	*/ { fnstat_prhdr,	fnstat_prdat,	0,				replst_slave_check },
/* GET_BMAPREPLPOL	*/ { fnstat_prhdr,	fnstat_prdat,	0,				NULL },
/* GET_NEWREPLPOL	*/ { fnstat_prhdr,	fnstat_prdat,	0,				NULL },
/* SET_BMAPREPLPOL	*/ { NULL,		NULL,		0,				NULL },
/* SET_NEWREPLPOL	*/ { NULL,		NULL,		0,				NULL }
};

psc_ctl_prthr_t psc_ctl_prthrs[] = {
/* BMAPFLSH	*/ NULL,
/* BMAPFLSHRLS	*/ NULL,
/* BMAPFLSHRPC	*/ NULL,
/* BMAPREADAHEAD*/ NULL,
/* CONN		*/ NULL,
/* CTL		*/ psc_ctlthr_pr,
/* CTLAC	*/ psc_ctlacthr_pr,
/* EQPOLL	*/ NULL,
/* FS		*/ NULL,
/* FSMGR	*/ NULL,
/* LNETAC	*/ NULL,
/* NBRQ		*/ NULL,
/* RCM		*/ NULL,
/* TIOS		*/ NULL,
/* USKLNDPL	*/ NULL
};

struct psc_ctlcmd_req psc_ctlcmd_reqs[] = {
	{ "bmap-repl-policy:",		cmd_bmap_repl_policy },
	{ "new-bmap-repl-policy:",	cmd_new_bmap_repl_policy },
//	{ "reconfig",			cmd_reconfig },
	{ "repl-add:",			cmd_replrq },
	{ "repl-remove:",		cmd_replrq },
	{ "repl-status",		cmd_replst }
};

PFLCTL_CLI_DEFS;

const char *progname;
const char *daemon_name = "mount_slash";
int recursive;

void
parse_enqueue(char *arg)
{
	parse_replrq(MSCMT_ADDREPLRQ, arg, pack_replrq);
}

void
parse_replst(char *arg)
{
	if (arg[0] == ':')
		pack_replst("", NULL, NULL);
	else
		walk(arg, pack_replst, NULL);
}

void
parse_dequeue(char *arg)
{
	parse_replrq(MSCMT_DELREPLRQ, arg, pack_replrq);
}

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
	{ 'P', PCOF_FUNC, psc_ctlparse_pool },
	{ 'p', PCOF_FUNC, psc_ctlparse_param },
	{ 'Q', PCOF_FUNC, parse_enqueue },
	{ 'R', PCOF_FLAG, &recursive },
	{ 'r', PCOF_FUNC, parse_replst },
	{ 's', PCOF_FUNC, psc_ctlparse_show },
	{ 'U', PCOF_FUNC, parse_dequeue },
	{ 'v', PCOF_FLAG, &verbose }
};

int
main(int argc, char *argv[])
{
	pfl_init();
	progname = argv[0];
	psc_hashtbl_init(&fnfidpairs, 0, struct fnfidpair, ffp_fid,
	    ffp_hentry, 1024, NULL, "fnfidpairs");

	setupterm(NULL, STDOUT_FILENO, NULL);
	has_col = has_colors() && isatty(STDOUT_FILENO);

	psc_ctlcli_main(SL_PATH_MSCTLSOCK, argc, argv, opts,
	    nitems(opts));
	if (!pfl_memchk(&current_mrs, 0, sizeof(current_mrs)))
		errx(1, "communication error: replication status "
		    "not completed");
	exit(0);
}
