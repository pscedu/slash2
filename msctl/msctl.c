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

#include "pfl/bitflag.h"
#include "pfl/cdefs.h"
#include "pfl/ctl.h"
#include "pfl/ctlcli.h"
#include "pfl/fmt.h"
#include "pfl/log.h"
#include "pfl/pfl.h"
#include "pfl/str.h"
#include "pfl/walk.h"

#include "mount_slash/ctl_cli.h"
#include "mount_slash/pgcache.h"
#include "bmap.h"
#include "ctl.h"
#include "ctlcli.h"
#include "pathnames.h"
#include "slashrpc.h"
#include "slconfig.h"
#include "slerr.h"

#define walk(f, func, arg)						\
	pfl_filewalk((f),						\
	    PFL_FILEWALKF_NOCHDIR |					\
	    (verbose   ? PFL_FILEWALKF_VERBOSE : 0) |			\
	    (recursive ? PFL_FILEWALKF_RECURSIVE : 0), NULL, (func), (arg))

int				 verbose;
int				 recursive;

const char			*daemon_name = "mount_slash";
int				 exit_status;

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
	char			 iosv[SL_MAX_REPLICAS][SITE_NAME_MAX];
	int			 nios;
	int			 opcode;
	sl_bmapno_t		 bmapno;
	sl_bmapno_t		 nbmaps;
	int			 sys_prio;
	int			 usr_prio;
};

struct bmap_range {
	sl_bmapno_t		 bmin;
	sl_bmapno_t		 bmax;
	struct psc_listentry	 lentry;
};

struct fattr_arg {
	int			 opcode;
	int			 attrid;
	int			 val;
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
	struct pfl_hashentry	 ffp_hentry;
};

/* keep in sync with BRPOL_* constants */
const char *replpol_tab[] = {
	"one-time",
	"persist"
};

const char *fattr_tab[] = {
	"ios-aff",
	"repl-pol"
};

const char *bool_tab[] = {
	"off",
	"on"
};

struct psc_hashtbl fnfidpairs;

slfid_t
fn2fid(const char *fn)
{
	struct fnfidpair *ffp;
	struct stat stb;
	slfid_t fid;

	if (lstat(fn, &stb) == -1)
		err(1, "stat %s", fn);

#ifndef HAVE_NO_FUSE_FSID
	{
		struct statvfs sfb;

		if (statvfs(fn, &sfb) == -1)
			err(1, "statvfs %s", fn);
		if (sfb.f_fsid != SLASH_FSID && sfb.f_fsid)
			errx(1, "%s: not in a SLASH2 file system "
			    "(fsid=%lx)", fn, sfb.f_fsid);
	}
#endif

	fid = stb.st_ino;
	ffp = psc_hashtbl_search(&fnfidpairs, &fid);

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

	ffp = psc_hashtbl_search(&fnfidpairs, &fid);
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

	scf = psc_ctlmsg_push(MSCMT_GETFCMH, sizeof(*scf));
	scf->scf_fg.fg_fid = FID_ANY;
}

void
packshow_bmaps(__unusedx char *fid)
{
	struct slctlmsg_bmap *scb;

	scb = psc_ctlmsg_push(MSCMT_GETBMAP, sizeof(*scb));
	scb->scb_fg.fg_fid = FID_ANY;
}

void
packshow_biorqs(__unusedx char *spec)
{
	psc_ctlmsg_push(MSCMT_GETBIORQ, sizeof(struct msctlmsg_biorq));
}

void
packshow_bmpces(__unusedx char *spec)
{
	psc_ctlmsg_push(MSCMT_GETBMPCE, sizeof(struct msctlmsg_bmpce));
}

void
parse_replrq(int opcode, const char *fn, const char *oreplrqspec,
    int (*packf)(FTSENT *, void *))
{
	char *sprio, *uprio, *bmapno, *next, *bend, *iosv, *ios;
	char replrqspec[LINE_MAX], *endp, *bmapnos;
	struct replrq_arg ra;
	sl_bmapno_t bmax;
	long l;

	if (strlcpy(replrqspec, oreplrqspec, sizeof(replrqspec)) >=
	    sizeof(replrqspec)) {
		errno = ENAMETOOLONG;
		errx(1, "%s", oreplrqspec);
	}
	iosv = replrqspec;
	ra.opcode = opcode;

	bmapnos = strchr(replrqspec, ':');
	if (bmapnos == NULL) {
		warnx("%s: no bmaps specified in replication "
		    "request specification", replrqspec);
		return;
	}
	*bmapnos++ = '\0';

	ra.usr_prio = -1;
	ra.sys_prio = -1;

	uprio = strchr(bmapnos, ':');
	if (uprio) {
		*uprio++ = '\0';

		sprio = strchr(uprio, 's');
		if (sprio)
			*sprio++ = '\0';

		if (uprio) {
			l = strtol(uprio, &endp, 10);
			if (l < 0 || l > INT_MAX ||
			    uprio == endp || *endp)
				errx(1, "invalid user priority: %s",
				    uprio);
			else
				ra.usr_prio = l;
		}
		if (sprio) {
			l = strtol(sprio, &endp, 10);
			if (l < 0 || l > INT_MAX ||
			    sprio == endp || *endp)
				errx(1, "invalid user priority: %s",
				    sprio);
			else
				ra.sys_prio = l;
		}
	}

	/* parse I/O systems */
	ra.nios = 0;
	for (ios = iosv; ios; ios = next) {
		next = strchr(ios, ',');
		if (next)
			*next++ = '\0';
		if (ra.nios >= (int)nitems(ra.iosv))
			errx(1, "%s: too many replicas specified",
			    replrqspec);
		if (strchr(ios, '@') == NULL)
			errx(1, "%s: no site specified", ios);
		if (strlcpy(ra.iosv[ra.nios++], ios,
		    sizeof(ra.iosv[0])) >= sizeof(ra.iosv[0]))
			errx(1, "%s: I/O system name too long", ios);
	}

	/* handle special all-bmap case */
	if (strcmp(bmapnos, "*") == 0) {
		ra.bmapno = 0;
		ra.nbmaps = -1;
		walk(fn, packf, &ra);
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
			    bend == endp || *bend != '\0' ||
			    l < ra.bmapno)
				errx(1, "%s: invalid replication request",
				    replrqspec);
			bmax = l;
		} else if (*endp != '\0')
			errx(1, "%s: invalid replication request",
			    replrqspec);
		ra.nbmaps = bmax - ra.bmapno + 1;
		walk(fn, packf, &ra);
	}
}

int
lookup(const char **tbl, int n, const char *name)
{
	int i;

	for (i = 0; i < n; i++)
		if (strcasecmp(name, tbl[i]) == 0)
			return (i);
	return (-1);
}

int
cmd_fattr1(FTSENT *f, void *arg)
{
	struct msctlmsg_fattr *mfa;
	struct fattr_arg *a = arg;

	mfa = psc_ctlmsg_push(a->opcode, sizeof(*mfa));
	mfa->mfa_fid = fn2fid(f->fts_path);
	mfa->mfa_attrid = a->attrid;
	mfa->mfa_val = a->val;
	return (0);
}

void
cmd_fattr(int ac, char **av)
{
	struct fattr_arg arg;
	char *nam, *val;
	int i;

	nam = strchr(av[0], ':');
	if (nam == NULL)
		errx(1, "fattr: no attribute specified");
	nam++;

	val = strchr(nam, '=');
	if (val)
		*val++ = '\0';

	arg.attrid = lookup(fattr_tab, nitems(fattr_tab), nam);
	if (arg.attrid == -1)
		errx(1, "fattr: unknown attribute %s", nam);

	if (val) {
		arg.opcode = MSCMT_SET_FATTR;
		switch (arg.attrid) {
		case SL_FATTR_IOS_AFFINITY:
			arg.val = lookup(bool_tab, nitems(bool_tab),
			    val);
			if (arg.val == -1)
				errx(1, "fattr: %s: invalid value",
				    val);
			break;
		case SL_FATTR_REPLPOL:
			arg.val = lookup(replpol_tab,
			    nitems(replpol_tab), val);
			if (arg.val == -1)
				errx(1, "fattr: %s: invalid value",
				    val);
			break;
		}
	} else
		arg.opcode = MSCMT_GET_FATTR;

	if (ac < 2)
		errx(1, "fattr: no file(s) specified");
	for (i = 1; i < ac; i++)
		walk(av[i], cmd_fattr1, &arg);
}

int
cmd_bmap_repl_policy_one(FTSENT *f, void *arg)
{
	struct msctlmsg_bmapreplpol *mfbrp;
	struct repl_policy_arg *a = arg;
	struct bmap_range *br;

	PLL_FOREACH(br, &a->branges) {
		mfbrp = psc_ctlmsg_push(a->opcode, sizeof(*mfbrp));
		mfbrp->mfbrp_pol = a->replpol;
		mfbrp->mfbrp_bmapno = br->bmin;
		mfbrp->mfbrp_nbmaps = br->bmax - br->bmin + 1;
		mfbrp->mfbrp_fid = fn2fid(f->fts_path);
	}
	return (0);
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

	bmapspec = strchr(av[0], ':');
	if (bmapspec == NULL)
		errx(1, "bmap-repl-policy: no bmapspec specified");
	*bmapspec++ = '\0';

	val = strchr(bmapspec, '=');
	if (val) {
		*val++ = '\0';
		arg.replpol = lookup(replpol_tab, nitems(replpol_tab),
		    val);
		if (arg.replpol == -1)
			errx(1, "bmap-repl-policy: %s: unknown policy",
			    val);
		arg.opcode = MSCMT_SET_BMAPREPLPOL;
	} else
		arg.opcode = MSCMT_GET_BMAPREPLPOL;

	for (bmapno = bmapspec; bmapno; bmapno = next) {
		if ((next = strchr(bmapno, ',')) != NULL)
			*next++ = '\0';
		l = strtol(bmapno, &endp, 10);
		if (l < 0 || (sl_bmapno_t)l >= UINT32_MAX || endp ==
		    bmapno)
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

int
cmd_replrq_one(FTSENT *f, void *arg)
{
	struct msctlmsg_replrq *mrq;
	struct replrq_arg *ra = arg;
	int n;

	if (f->fts_info != FTS_F && f->fts_info != FTS_D) {
		if (!recursive) {
			errno = EINVAL;
			warn("%s", f->fts_path);
		}
		return (0);
	}

	mrq = psc_ctlmsg_push(ra->opcode, sizeof(*mrq));
	mrq->mrq_bmapno = ra->bmapno;
	mrq->mrq_nbmaps = ra->nbmaps;
	mrq->mrq_nios = ra->nios;
	mrq->mrq_sys_prio = ra->sys_prio;
	mrq->mrq_usr_prio = ra->usr_prio;
	for (n = 0; n < ra->nios; n++)
		strlcpy(mrq->mrq_iosv[n], ra->iosv[n],
		    sizeof(mrq->mrq_iosv[0]));
	/*
 	 * If the path points to a different slash2 installation
 	 * on the same machine, we could be rejected with ESTALE.
 	 */
	mrq->mrq_fid = fn2fid(f->fts_path);
	return (0);
}

void
cmd_replrq(int ac, char **av)
{
	int i, opc;
	char *p;

	if (ac < 2)
		errx(1, "%s: no file(s) specified", av[0]);
	if (strncmp(av[0], "repl-add", strlen("repl-add")) == 0)
		opc = MSCMT_ADDREPLRQ;
	else
		opc = MSCMT_DELREPLRQ;
	p = strchr(av[0], ':');
	if (p == NULL)
		errx(1, "%s: no replrqspec specified", av[0]);
	p++;
	for (i = 1; i < ac; i++)
		parse_replrq(opc, av[i], p, cmd_replrq_one);
}

int
cmd_replst_one(FTSENT *f, __unusedx void *arg)
{
	struct msctlmsg_replst *mrs;

	if (f && f->fts_info != FTS_F && f->fts_info != FTS_D) {
		if (!recursive) {
			errno = EINVAL;
			warn("%s", f->fts_path);
		}
		return (0);
	}

	mrs = psc_ctlmsg_push(MSCMT_GETREPLST, sizeof(*mrs));
	if (f)
		mrs->mrs_fid = fn2fid(f->fts_path);
	else
		mrs->mrs_fid = FID_ANY;
	return (0);
}

void
cmd_replst(int ac, char **av)
{
	struct replrq_arg arg;
	int i;

	arg.opcode = MSCMT_GETREPLST;
	for (i = 1; i < ac; i++)
		walk(av[i], cmd_replst_one, &arg);
	if (ac == 1)
		cmd_replst_one(NULL, NULL);
}

int
replst_slave_check(struct psc_ctlmsghdr *mh, const void *m)
{
	const struct msctlmsg_replst_slave *mrsl = m;
	__unusedx struct srt_replst_bhdr *srsb;
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

int
fattr_prhdr(__unusedx struct psc_ctlmsghdr *mh, __unusedx const void *m)
{
	printf("%-59s %9s %6s\n",
	    "file", "attribute", "value");
	return(PSC_CTL_DISPLAY_WIDTH);
}

void
fattr_prdat(__unusedx const struct psc_ctlmsghdr *mh,
    __unusedx const void *m)
{
	const struct msctlmsg_fattr *mfa = m;
	const char *attrname = "?", *val = "?";

	if (mfa->mfa_attrid >= 0 && mfa->mfa_attrid < nitems(fattr_tab))
		attrname = fattr_tab[mfa->mfa_attrid];
	switch (mfa->mfa_attrid) {
	case SL_FATTR_IOS_AFFINITY:
		val = mfa->mfa_val ? "on" : "off";
		break;
	case SL_FATTR_REPLPOL:
		val = mfa->mfa_val >= 0 && mfa->mfa_val <
		    nitems(replpol_tab) ? replpol_tab[mfa->mfa_val] :
		    "?";
		break;
	}
	printf("%-59s %9s %6s\n", fid2fn(mfa->mfa_fid, NULL), attrname,
	    val);
}

int
fnstat_prhdr(__unusedx struct psc_ctlmsghdr *mh, __unusedx const void *m)
{
	/* XXX add #repls, #bmaps */
	printf("%-59s %6s %6s %6s\n",
	    "file-replication-status", "#valid", "#bmap", "%res");
	return(PSC_CTL_DISPLAY_WIDTH);
}

void
fnstat_prdat(__unusedx const struct psc_ctlmsghdr *mh,
    __unusedx const void *m)
{
	sl_bmapno_t bact, both, nb;
	int val, n, nbw, off, dlen, cmap[NBREPLST], maxwidth;
	char *label, map[NBREPLST], pmap[NBREPLST], rbuf[PSCFMT_RATIO_BUFSIZ];
	struct replst_slave_bdata *rsb, *nrsb;
	struct srt_replst_bhdr bhdr;
	struct stat stb;
	uint32_t iosidx;

	maxwidth = psc_ctl_get_display_maxwidth();

	map[BREPLST_INVALID] = '-';
	map[BREPLST_REPL_SCHED] = 's';
	map[BREPLST_REPL_QUEUED] = 'q';
	map[BREPLST_VALID] = '+';
	map[BREPLST_TRUNC_QUEUED] = 't';
	map[BREPLST_TRUNC_SCHED] = 'p';
	map[BREPLST_GARBAGE_QUEUED] = 'g';
	map[BREPLST_GARBAGE_SCHED] = 'x';

	pmap[BREPLST_INVALID] = '/';
	pmap[BREPLST_REPL_SCHED] = 'S';
	pmap[BREPLST_REPL_QUEUED] = 'Q';
	pmap[BREPLST_VALID] = '*';
	pmap[BREPLST_TRUNC_QUEUED] = 'T';
	pmap[BREPLST_TRUNC_SCHED] = 'P';
	pmap[BREPLST_GARBAGE_QUEUED] = 'G';
	pmap[BREPLST_GARBAGE_SCHED] = 'X';

	brepls_init(cmap, -1);
	cmap[BREPLST_REPL_QUEUED] = COLOR_MAGENTA;
	cmap[BREPLST_REPL_SCHED] = COLOR_YELLOW;
	cmap[BREPLST_VALID] = COLOR_GREEN;
	cmap[BREPLST_TRUNC_QUEUED] = COLOR_MAGENTA;
	cmap[BREPLST_TRUNC_SCHED] = COLOR_YELLOW;

	n = printf("%s", fid2fn(current_mrs.mrs_fid, &stb));
	if (S_ISDIR(stb.st_mode)) {
		n += printf("/");
		label = " repl-policy: ";
	} else
		label = " new-bmap-repl-policy: ";
	dlen = PSC_CTL_DISPLAY_WIDTH - strlen(label) -
	    strlen(replpol_tab[BRPOL_ONETIME]);
	if (n + strlen(label) + strlen(replpol_tab[BRPOL_ONETIME]) >
	    PSC_CTL_DISPLAY_WIDTH)
		dlen = maxwidth - strlen(label) -
		    strlen(replpol_tab[BRPOL_ONETIME]);
	if (n > dlen)
		printf("\n%*s", dlen, "");
	else
		printf("%*s", dlen - n, "");
	printf("%s", label);
	if (current_mrs.mrs_newreplpol >= NBRPOL)
		printf("<unknown: %d>\n", current_mrs.mrs_newreplpol);
	else
		printf("%s\n", replpol_tab[current_mrs.mrs_newreplpol]);

	for (iosidx = 0; iosidx < current_mrs.mrs_nios; iosidx++) {
		nbw = 0;
		bact = both = 0;
		psclist_for_each_entry(rsb, &current_mrs_bdata, rsb_lentry)
			rsb_accul_replica_stats(rsb, iosidx, &bact, &both);

		setcolor(COLOR_CYAN);
		printf("  %-57s", current_mrs.mrs_iosv[iosidx]);
		uncolor();

		pfl_fmt_ratio(rbuf, bact, bact + both);
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
				pfl_bitstr_copy(&bhdr, 0, rsb->rsb_data,
				    nb * (SL_NBITS_REPLST_BHDR +
				     SL_BITS_PER_REPLICA *
				     current_mrs.mrs_nios),
				    SL_NBITS_REPLST_BHDR);
				val = SL_REPL_GET_BMAP_IOS_STAT(
				    rsb->rsb_data, off);
				setcolor(cmap[val]);
				putchar((bhdr.srsb_replpol ==
				    BRPOL_PERSIST ?  pmap : map)[
				     SL_REPL_GET_BMAP_IOS_STAT(
				     rsb->rsb_data, off)]);
				uncolor();
			}
		}
		putchar('\n');
	}

	/* reset current_mrs for next replst */
	psclist_for_each_entry_safe(rsb, nrsb, &current_mrs_bdata,
	    rsb_lentry) {
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

int
ms_biorq_prhdr(__unusedx struct psc_ctlmsghdr *mh, __unusedx const void *m)
{
	printf("%16s %5s %3s %10s %10s "
	    "%12s %3s %16s %10s %4s %12s\n",
	    "fid", "bno", "ref", "off", "len",
	    "flags", "try", "sliod", "expire", "np", "addr");
	return(PSC_CTL_DISPLAY_WIDTH+31);
}

void
ms_biorq_prdat(__unusedx const struct psc_ctlmsghdr *mh, const void *m)
{
	const struct msctlmsg_biorq *msr = m;

	printf("%016"SLPRIxFID" %5d %3d %10d %10d  "
	    "%c%c%c%c%c%c%c%c%c%c%c "
	    "%3d %16s %10"PRId64" %4d %lx\n",
	    msr->msr_fid, msr->msr_bno, msr->msr_ref, msr->msr_off,
	    msr->msr_len,
	    msr->msr_flags & BIORQ_READ			? 'r' : '-',
	    msr->msr_flags & BIORQ_WRITE		? 'w' : '-',
	    msr->msr_flags & BIORQ_DIO			? 'd' : '-',
	    msr->msr_flags & BIORQ_EXPIRE		? 'x' : '-',
	    msr->msr_flags & BIORQ_DESTROY		? 'D' : '-',
	    msr->msr_flags & BIORQ_FLUSHRDY		? 'l' : '-',
	    msr->msr_flags & BIORQ_FREEBUF		? 'f' : '-',
	    msr->msr_flags & BIORQ_WAIT			? 'W' : '-',
	    msr->msr_flags & BIORQ_ONTREE		? 't' : '-',
	    msr->msr_flags & BIORQ_READAHEAD		? 'a' : '-',
	    msr->msr_flags & BIORQ_AIOWAKE		? 'k' : '-',
	    msr->msr_retries, msr->msr_last_sliod,
	    msr->msr_expire.tv_sec, msr->msr_npages,
	    msr->msr_addr);
}

int
ms_bmpce_prhdr(__unusedx struct psc_ctlmsghdr *mh, __unusedx const void *m)
{
	printf("%-16s %6s %3s %4s %7s %7s "
	    "%11s %3s %3s %8s\n",
	    "fid", "bno", "ref", "err", "offset", "start",
	    "flags", "nwr", "aio", "lastacc");
	return(PSC_CTL_DISPLAY_WIDTH);
}

void
ms_bmpce_prdat(__unusedx const struct psc_ctlmsghdr *mh, const void *m)
{
	const struct msctlmsg_bmpce *mpce = m;

	printf("%016"SLPRIxFID" %6d %3d "
	    "%4d %7x %7x "
	    "%c%c%c%c%c%c%c%c%c%c%c "
	    "%3d %3d "
	    "%8"PRIx64"\n",
	    mpce->mpce_fid, mpce->mpce_bno, mpce->mpce_ref,
	    mpce->mpce_rc, mpce->mpce_off, mpce->mpce_start,
	    mpce->mpce_flags & BMPCEF_DATARDY	? 'd' : '-',
	    mpce->mpce_flags & BMPCEF_FAULTING	? 'f' : '-',
	    mpce->mpce_flags & BMPCEF_TOFREE	? 't' : '-',
	    mpce->mpce_flags & BMPCEF_EIO	? 'e' : '-',
	    mpce->mpce_flags & BMPCEF_AIOWAIT	? 'w' : '-',
	    mpce->mpce_flags & BMPCEF_DISCARD	? 'D' : '-',
	    mpce->mpce_flags & BMPCEF_READAHEAD	? 'r' : '-',
	    mpce->mpce_flags & BMPCEF_ACCESSED	? 'a' : '-',
	    mpce->mpce_flags & BMPCEF_IDLE	? 'i' : '-',
	    mpce->mpce_flags & BMPCEF_REAPED	? 'X' : '-',
	    mpce->mpce_flags & BMPCEF_READALC	? 'R' : '-',
	    mpce->mpce_nwaiters, mpce->mpce_npndgaios,
	    mpce->mpce_laccess.tv_sec);
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

	exit_status = 1;

	if (psc_ctl_lastmsgtype != mh->mh_type &&
	    psc_ctl_lastmsgtype != -1)
		fprintf(stderr, "\n");

	/*
	 * If the beginning of a message looks like a FID, try translate
	 * it back to full pathname from our cache.
	 */
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
	ffp = psc_hashtbl_search(&fnfidpairs, &fid);
	if (ffp == NULL)
		goto out;
	warnx("%s%s", ffp->ffp_fn, endp);
	return;

 out:
	warnx("%s", pce->pce_errmsg);
}

struct psc_ctlshow_ent psc_ctlshow_tab[] = {
	PSC_CTLSHOW_DEFS,
	{ "bmaps",		packshow_bmaps },
	{ "biorqs",		packshow_biorqs },
	{ "bmpces",		packshow_bmpces },
	{ "connections",	packshow_conns },
	{ "fcmhs",		packshow_fcmhs },

	/* aliases */
	{ "conns",		packshow_conns },
	{ "fidcache",		packshow_fcmhs },
	{ "files",		packshow_fcmhs }
};

#define psc_ctlmsg_error_prdat ms_ctlmsg_error_prdat

struct psc_ctlmsg_prfmt psc_ctlmsg_prfmts[] = {
	PSC_CTLMSG_PRFMT_DEFS
/* ADDREPLRQ		*/ , { NULL,		NULL,		0,				NULL }
/* DELREPLRQ		*/ , { NULL,		NULL,		0,				NULL }
/* GETCONNS		*/ , { sl_conn_prhdr,	sl_conn_prdat,	sizeof(struct slctlmsg_conn),	NULL }
/* GETFCMH		*/ , { sl_fcmh_prhdr,	sl_fcmh_prdat,	sizeof(struct slctlmsg_fcmh),	NULL }
/* GETREPLST		*/ , { NULL,		NULL,		0,				replst_savdat }
/* GETREPLST_SLAVE	*/ , { fnstat_prhdr,	fnstat_prdat,	0,				replst_slave_check }
/* GET_BMAPREPLPOL	*/ , { fnstat_prhdr,	fnstat_prdat,	0,				NULL }
/* GET_FATTR		*/ , { fattr_prhdr,	fattr_prdat,	sizeof(struct msctlmsg_fattr),	NULL }
/* SET_BMAPREPLPOL	*/ , { NULL,		NULL,		0,				NULL }
/* SET_FATTR		*/ , { NULL,		NULL,		0,				NULL }
/* GETBMAP		*/ , { sl_bmap_prhdr,	sl_bmap_prdat,	sizeof(struct slctlmsg_bmap),	NULL }
/* GETBIORQ		*/ , { ms_biorq_prhdr,	ms_biorq_prdat,	sizeof(struct msctlmsg_biorq),	NULL }
/* GETBMPCE		*/ , { ms_bmpce_prhdr,	ms_bmpce_prdat,	sizeof(struct msctlmsg_bmpce),	NULL }
};

struct psc_ctlcmd_req psc_ctlcmd_reqs[] = {
	{ "bmap-repl-policy:",		cmd_bmap_repl_policy },
	{ "fattr:",			cmd_fattr },
//	{ "reconfig",			cmd_reconfig },
	{ "repl-add:",			cmd_replrq },
	{ "repl-remove:",		cmd_replrq },
	{ "repl-status",		cmd_replst }
};

PFLCTL_CLI_DEFS;

void
parse_enqueue(char *arg)
{
	char *fn;

	fn = strchr(arg, ':');
	if (fn == NULL) {
		warnx("%s: no bmaps specified in replication "
		    "request specification", arg);
		return;
	}
	fn = strchr(fn + 1, ':');
	if (fn == NULL) {
		warnx("%s: no file specified in replication "
		    "request specification", arg);
		return;
	}
	*fn++ = '\0';
	parse_replrq(MSCMT_ADDREPLRQ, fn, arg, cmd_replrq_one);
}

void
parse_replst(char *arg)
{
	if (arg[0] == ':')
		cmd_replst_one(NULL, NULL);
	else
		walk(arg, cmd_replst_one, NULL);
}

void
parse_dequeue(char *arg)
{
	char *fn;

	fn = strchr(arg, ':');
	if (fn == NULL) {
		warnx("%s: no bmaps specified in replication "
		    "request specification", arg);
		return;
	}
	fn = strchr(fn + 1, ':');
	if (fn == NULL) {
		warnx("%s: no file specified in replication "
		    "request specification", arg);
		return;
	}
	*fn++ = '\0';
	parse_replrq(MSCMT_DELREPLRQ, fn, arg, cmd_replrq_one);
}

__dead void
usage(void)
{
	extern const char *__progname;

	fprintf(stderr,
	    "usage: %s [-HInRVv] [-p paramspec] [-S socket] [-s value] [cmd arg ...]\n",
	    __progname);
	exit(1);
}

void
msctl_show_version(void)
{
	fprintf(stderr, "%d\n", sl_stk_version);
}

struct psc_ctlopt opts[] = {
	{ 'H', PCOF_FLAG, &psc_ctl_noheader },
	{ 'I', PCOF_FLAG, &psc_ctl_inhuman },
	{ 'n', PCOF_FLAG, &psc_ctl_nodns },
	{ 'p', PCOF_FUNC, psc_ctlparse_param },
	{ 'Q', PCOF_FUNC, parse_enqueue },
	{ 'R', PCOF_FLAG, &recursive },
	{ 'r', PCOF_FUNC, parse_replst },
	{ 's', PCOF_FUNC, psc_ctlparse_show },
	{ 'U', PCOF_FUNC, parse_dequeue },
	{ 'V', PCOF_FLAG, msctl_show_version },
	{ 'v', PCOF_FLAG, &verbose },
};

int
main(int argc, char *argv[])
{
	pfl_init();
	sl_errno_init();
	pscthr_init(0, NULL, 0, "msctl");

	psc_hashtbl_init(&fnfidpairs, 0, struct fnfidpair, ffp_fid,
	    ffp_hentry, 97, NULL, "fnfidpairs");

	psc_ctlcli_main(SL_PATH_MSCTLSOCK, argc, argv, opts,
	    nitems(opts));
	if (!pfl_memchk(&current_mrs, 0, sizeof(current_mrs)))
		errx(1, "communication error: replication status "
		    "not completed");
	exit(exit_status);
}
