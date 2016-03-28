/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright 2009-2016, Pittsburgh Supercomputing Center
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

#include <sys/uio.h>
#include <sys/xattr.h>
#include <sys/wait.h>

#include <err.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "pfl/cdefs.h"
#include "pfl/fmt.h"
#include "pfl/fmtstr.h"
#include "pfl/listcache.h"
#include "pfl/lockedlist.h"
#include "pfl/pfl.h"
#include "pfl/pthrutil.h"
#include "pfl/thread.h"
#include "pfl/walk.h"

#include "slashrpc.h"

#include "slashd/inode.h"
#include "slashd/mdsio.h"

#define df_warnx(msg, ...)						\
	do {								\
		flockfile(stderr);					\
		warnx(msg, ##__VA_ARGS__);				\
		funlockfile(stderr);					\
	} while (0)

#define df_warn(msg, ...)						\
	do {								\
		flockfile(stderr);					\
		warn(msg, ##__VA_ARGS__);				\
		funlockfile(stderr);					\
	} while (0)

struct path {
	const char		*p_fn;
	struct psc_listentry	 p_lentry;
};

struct host {
	const char		*h_hostname;
	const char		*h_path;
	struct psc_listentry	 h_lentry;
};

#define STAT_SZ		(8 + sizeof(struct srt_stat))
#define INOSTAT_SZ	(STAT_SZ + sizeof(struct slash_inode_od) + 8)
#define INOXSTAT_SZ	(INOSTAT_SZ + sizeof(struct slash_inode_extras_od) + 8)

struct file {
	const char		*f_basefn;
	const char		*f_pathfn;
	struct psc_listentry	 f_lentry;
	int			 f_fd;
	uint32_t		 f_nrepls;
	uint64_t		 f_ino_mem_crc;
	uint64_t		 f_inox_mem_crc;
	struct {
		uint64_t		 metasize;
		struct srt_stat		 sstb;
		struct slm_ino_od	 ino;
		uint64_t		 ino_crc;
		struct slm_inox_od	 inox;
		uint64_t		 inox_crc;
	} f_data;
#define f_metasize	f_data.metasize
#define f_sstb		f_data.sstb
#define f_ino		f_data.ino
#define f_ino_od_crc	f_data.ino_crc
#define f_inox		f_data.inox
#define f_inox_od_crc	f_data.inox_crc
};

struct psc_lockedlist		 df_hosts = PLL_INIT(&df_hosts, struct host, h_lentry);
int				 df_rank;
int				 df_nprocs = 1;
int				 df_onlyfiles;
struct psc_lockedlist		 df_excludes = PLL_INIT(&df_excludes, struct path, p_lentry);
const char			*df_outfn;
FILE				*df_outfp;
char				 df_buf[512 * 1024];
const char			*df_dispfmt =
    "%f:\n"
    "  crc %d mem %C od %c\n"
    "  version %v\n"
    "  flags %L\n"
    "  nrepls %n\n"
    "  fsize %s\n"
    "  fid %F\n"
    "  fgen %G\n"
    "  uid %u\n"
    "  gid %g\n"
    "  repls %R\n"
    "  replblks %B\n"
    "  xcrc %y mem %X od %x\n"
    "  bmaps %N\n%M";

const char			*repl_states = "-sq+tpgx";

struct path *
searchpaths(struct psc_lockedlist *pll, const char *fn)
{
	struct path *p;

	/* XXX use bsearch */
	PLL_FOREACH(p, pll)
		if (strcmp(p->p_fn, fn) == 0)
			return (p);
	return (NULL);
}

void
pr_repls(FILE *outfp, struct file *f)
{
	uint32_t i;

	for (i = 0; i < f->f_nrepls; i++)
		fprintf(outfp, "%s%u", i ? "," : "",
		    i < SL_DEF_REPLICAS ?
		    f->f_ino.ino_repls[i].bs_id :
		    f->f_inox.inox_repls[i - SL_DEF_REPLICAS].bs_id);
}

void
pr_times(const char **p, FILE *outfp, struct file *f)
{
	time_t tim;
	struct tm tm;
	struct pfl_timespec *ts;
	char fmt[64], buf[64];
	const char *t = *p;
	int i;

	switch (*++t) {
	case 'a':
		ts = &f->f_sstb.sst_atim;
		break;
	case 'c':
		ts = &f->f_sstb.sst_ctim;
		break;
	case 'm':
		ts = &f->f_sstb.sst_mtim;
		break;
	default:
		errx(1, "invalid %%T times specification: %s", t);
		break;
	}
	if (*++t != '<')
		errx(1, "invalid %%T format: %s", t);
	t++;
	for (i = 0; *t != '\0' && *t != '>' && i < (int)sizeof(fmt); i++)
		fmt[i] = *t++;
	if (i == sizeof(fmt))
		errx(1, "%%T format too long: %s", t);
	fmt[i] = '\0';

	if (f) {
		tim = ts->tv_sec;
		localtime_r(&tim, &tm);

		strftime(buf, sizeof(buf), fmt, &tm);
		fprintf(outfp, "%s", buf);
	}

	*p = t;
}

void
pr_repl_blks(FILE *outfp, struct file *f)
{
	uint32_t i;

	if (S_ISDIR(f->f_sstb.sst_mode))
		return;

	for (i = 0; i < f->f_nrepls; i++)
		fprintf(outfp, "%s%"PRIu64, i ? "," : "",
		    i < SL_DEF_REPLICAS ?
		    f->f_ino.ino_repl_nblks[i] :
		    f->f_inox.inox_repl_nblks[i - SL_DEF_REPLICAS]);
}

void
pr_bmaps(FILE *outfp, struct file *f)
{
	struct {
		struct bmap_ondisk bod;
		uint64_t crc;
	} bd;
	sl_bmapno_t bno;
	int fd, off;
	uint32_t i;
	size_t rc;
	FILE *fp;

	if (S_ISDIR(f->f_sstb.sst_mode))
		return;
	if (f->f_metasize <= SL_BMAP_START_OFF)
		return;

	if (lseek(f->f_fd, SL_BMAP_START_OFF, SEEK_SET) == -1)
		df_warn("seek");
	fd = dup(f->f_fd);
	if (fd == -1) {
		df_warn("dup");
		return;
	}
	fp = fdopen(fd, "r");
	if (fp == NULL) {
		df_warn("fdopen");
		close(fd);
		return;
	}

	for (bno = 0; ; bno++) {
		rc = fread(&bd, 1, sizeof(bd), fp);
		if (rc == 0)
			break;
		if (rc != sizeof(bd)) {
			df_warn("read");
			break;
		}

		fprintf(outfp, "   %5u: gen %5u pol %u res ",
		    bno, bd.bod.bod_gen, bd.bod.bod_replpol);
		for (i = 0, off = 0; i < f->f_nrepls;
		    i++, off += SL_BITS_PER_REPLICA)
			fprintf(outfp, "%c", repl_states[
			    SL_REPL_GET_BMAP_IOS_STAT(
			    bd.bod.bod_repls, off)]);
		fprintf(outfp, "\n");

		fprintf(outfp, "   %5u: crcstates ", bno);
		for (i = 0; i < SLASH_SLVRS_PER_BMAP; i++)
			fprintf(outfp, "%s%d", i ? "," : "",
			    bd.bod.bod_crcstates[i]);
		fprintf(outfp, "\n");
	}

	if (ferror(fp))
		df_warn("%s: read", f->f_pathfn);

	fclose(fp);
}

void
addexclude(const char *fn)
{
	struct path *p;

	p = PSCALLOC(sizeof(*p));
	INIT_PSC_LISTENTRY(&p->p_lentry);
	p->p_fn = fn;
	pll_add(&df_excludes, p);
}

int
load_data_fd(struct file *f, void *buf)
{
	f->f_fd = open(f->f_basefn, O_RDONLY);
	if (f->f_fd == -1)
		return (0);
	if (fgetxattr(f->f_fd, SLXAT_INOXSTAT, buf, INOXSTAT_SZ) !=
	    INOXSTAT_SZ)
		return (0);
	psc_crc64_calc(&f->f_ino_mem_crc, &f->f_ino, sizeof(f->f_ino));
	psc_crc64_calc(&f->f_inox_mem_crc, &f->f_inox,
	    sizeof(f->f_inox));
	return (1);
}

int
load_data_inox(struct file *f, void *buf)
{
	if (getxattr(f->f_basefn, SLXAT_INOXSTAT, buf, INOXSTAT_SZ) !=
	    INOXSTAT_SZ)
		return (0);
	psc_crc64_calc(&f->f_ino_mem_crc, &f->f_ino, sizeof(f->f_ino));
	psc_crc64_calc(&f->f_inox_mem_crc, &f->f_inox,
	    sizeof(f->f_inox));
	return (1);
}

int
load_data_ino(struct file *f, void *buf)
{
	if (getxattr(f->f_basefn, SLXAT_INOSTAT, buf, INOSTAT_SZ) !=
	    INOSTAT_SZ)
		return (0);
	psc_crc64_calc(&f->f_ino_mem_crc, &f->f_ino, sizeof(f->f_ino));
	return (1);
}

int
load_data_stat(struct file *f, void *buf)
{
	if (getxattr(f->f_basefn, SLXAT_STAT, buf, STAT_SZ) !=
	    STAT_SZ)
		return (0);
	return (1);
}

int (*load_data)(struct file *f, void *) = load_data_stat;

int
dumpfid(FTSENT *fe, __unusedx void *arg)
{
	struct file fil, *f = &fil;
	struct slm_ino_od *ino;
	struct srt_stat *sstb;
	struct path *p;
	char modebuf[16];

	p = searchpaths(&df_excludes, fe->fts_path);
	if (p) {
		pll_remove(&df_excludes, p);
		PSCFREE(p);
		pfl_fts_set(fe, FTS_SKIP);
		return (0);
	}

	if (fe->fts_level < 5) {
		if (df_rank != (int)(fe->fts_ino % df_nprocs))
			return (0);
	} else if (fe->fts_level == 5) {
		if (df_rank != (int)(fe->fts_ino % df_nprocs)) {
			pfl_fts_set(fe, FTS_SKIP);
			return (0);
		}
	}

	if (fe->fts_info != FTS_F && (fe->fts_info != FTS_D ||
	    df_onlyfiles))
		return (0);

	memset(f, 0, sizeof(*f));
	f->f_fd = -1;
	INIT_PSC_LISTENTRY(&f->f_lentry);
	f->f_basefn = fe->fts_level ? fe->fts_name : fe->fts_accpath;
	f->f_pathfn = fe->fts_path;

	if (!load_data(f, &f->f_data)) {
		df_warn("%s", f->f_pathfn);
		goto out;
	}
	sstb = &f->f_sstb;
	ino = &f->f_ino;
	f->f_nrepls = MIN(SL_MAX_REPLICAS, ino->ino_nrepls);
	(void)PRFMTSTR(df_outfp, df_dispfmt,
	    PRFMTSTRCASEV('B', pr_repl_blks(_fp, f))
	    PRFMTSTRCASE('b', PRIu64, sstb->sst_blocks)
	    PRFMTSTRCASE('C', PSCPRIxCRC64, f->f_ino_mem_crc)
	    PRFMTSTRCASE('c', PSCPRIxCRC64, f->f_ino_od_crc)
	    PRFMTSTRCASE('d', "s",
		f->f_ino_od_crc == f->f_ino_mem_crc ? "OK" : "BAD")
	    PRFMTSTRCASE('F', PRIx64, sstb->sst_fg.fg_fid)
	    PRFMTSTRCASE('f', "s", f->f_pathfn)
	    PRFMTSTRCASE('G', PRIu64, sstb->sst_fg.fg_gen)
	    PRFMTSTRCASE('g', "u", sstb->sst_gid)
	    PRFMTSTRCASE('L', "#x", ino->ino_flags)
	    PRFMTSTRCASEV('M', pr_bmaps(_fp, f))
	    PRFMTSTRCASE('m', "s", pfl_fmt_mode(sstb->sst_mode,
		modebuf))
	    PRFMTSTRCASE('N', PSCPRIdOFFT,
		(f->f_metasize - SL_BMAP_START_OFF) / BMAP_OD_SZ)
	    PRFMTSTRCASE('n', "u", ino->ino_nrepls)
	    PRFMTSTRCASEV('R', pr_repls(_fp, f))
	    PRFMTSTRCASEV('T', pr_times(&_t, _fp, f))
	    PRFMTSTRCASE('s', PRIu64, sstb->sst_size)
	    PRFMTSTRCASE('u', "u", sstb->sst_uid)
	    PRFMTSTRCASE('v', "u", ino->ino_version)
	    PRFMTSTRCASE('X', PSCPRIxCRC64, f->f_inox_mem_crc)
	    PRFMTSTRCASE('x', PSCPRIxCRC64, f->f_inox_od_crc)
	    PRFMTSTRCASE('y', "s",
		f->f_inox_mem_crc == f->f_inox_od_crc ? "OK" : "BAD")
	);

 out:
	if (f->f_fd != -1)
		close(f->f_fd);
	return (0);
}

void
addhost(char *s)
{
	char *next, *path;
	struct host *h;

	for (; s; s = next) {
		next = strchr(s, ';');
		if (next)
			*next++ = '\0';
		path = strchr(s, ':');
		if (path)
			*path++ = '\0';

		h = PSCALLOC(sizeof(*h));
		INIT_LISTENTRY(&h->h_lentry);
		h->h_hostname = s;
		h->h_path = path;
		pll_add(&df_hosts, h);
	}
}

__dead void
usage(void)
{
	extern const char *__progname;

	fprintf(stderr,
	    "usage: %s [-R] [-C hosts] [-F fmt] [-O file]\n"
	    "\t[-t nthr] [-x exclude] file ...\n",
	    __progname);
	exit(1);
}

void
dumpfids(char **av, int flags)
{
	if (df_outfn) {
		char fn[PATH_MAX];

		(void)FMTSTR(fn, sizeof(fn), df_outfn,
		    FMTSTRCASE('n', "d", df_rank)
		);
		df_outfp = fopen(fn, "w");
		if (df_outfp == NULL)
			err(1, "open %s", fn);
	} else
		df_outfp = stdout;

	for (; *av; av++)
		pfl_filewalk(*av, flags, NULL, dumpfid, NULL);
}

#define NEED_INO	(1 << 0)
#define NEED_INOX	(1 << 1)
#define NEED_FD		(1 << 2)

int
main(int argc, char *argv[])
{
	int i, pid, status, walkflags, c, need = 0;
	char *endp;
	FILE *fp;
	long l;

	pfl_init();
	walkflags = PFL_FILEWALKF_NOSTAT;
	while ((c = getopt(argc, argv, "C:F:fO:Rt:x:")) != -1) {
		switch (c) {
		case 'C':
			addhost(optarg);
			break;
		case 'f':
			df_onlyfiles = 1;
			break;
		case 'F':
			df_dispfmt = optarg;
			break;
		case 'O':
			df_outfn = optarg;
			break;
		case 'R':
			walkflags |= PFL_FILEWALKF_RECURSIVE;
			break;
		case 't':
			l = strtol(optarg, &endp, 10);
			if (l < 1 || l > 256 ||
			    optarg == endp || *endp)
				errx(1, "invalid -t value: %s", optarg);
			df_nprocs = l;
			break;
		case 'x':
			addexclude(optarg);
			break;
		default:
			usage();
		}
	}
	argc -= optind;
	argv += optind;
	if (!argc)
		usage();

	fp = fopen("/dev/null", "w");
	if (fp == NULL)
		err(1, "/dev/null");
	(void)PRFMTSTR(fp, df_dispfmt,
	    PRFMTSTRCASEV('B', need |= NEED_INOX)
	    PRFMTSTRCASEV('b', )
	    PRFMTSTRCASEV('C', need |= NEED_INO)
	    PRFMTSTRCASEV('c', need |= NEED_INO)
	    PRFMTSTRCASEV('d', need |= NEED_INO)
	    PRFMTSTRCASEV('F', )
	    PRFMTSTRCASEV('f', )
	    PRFMTSTRCASEV('G', )
	    PRFMTSTRCASEV('g', )
	    PRFMTSTRCASEV('L', need |= NEED_INO)
	    PRFMTSTRCASEV('M', need |= NEED_FD)
	    PRFMTSTRCASEV('m', )
	    PRFMTSTRCASEV('N', )
	    PRFMTSTRCASEV('n', need |= NEED_INO)
	    PRFMTSTRCASEV('R', need |= NEED_INOX)
	    PRFMTSTRCASEV('s', )
	    PRFMTSTRCASEV('T', pr_times(&_t, NULL, NULL))
	    PRFMTSTRCASEV('u', )
	    PRFMTSTRCASEV('v', need |= NEED_INO)
	    PRFMTSTRCASEV('X', need |= NEED_INOX)
	    PRFMTSTRCASEV('x', need |= NEED_INOX)
	    PRFMTSTRCASEV('y', need |= NEED_INOX)
	);
	fclose(fp);

	if (need & NEED_FD)
		load_data = load_data_fd;
	else if (need & NEED_INOX)
		load_data = load_data_inox;
	else if (need & NEED_INO)
		load_data = load_data_ino;

	for (df_rank = 1; df_rank < df_nprocs; df_rank++) {
		pid = fork();
		switch (pid) {
		case -1:
			err(1, "fork");
		case 0: /* child */
			dumpfids(argv, walkflags);
			exit(0);
			break;
		}
	}
	df_rank = 0;
	dumpfids(argv, walkflags);
	for (i = 1; i < df_nprocs; i++)
		if (wait(&status) == -1)
			err(1, "wait");
	exit(0);

//	PLL_FOREACH(h, &hosts) {
//		it = thrv[i++] = pscthr_init(0, thrmain, NULL,
//		    sizeof(*t), "thr-%s", h->h_hostname);
//		t = it->pscthr_private;
//		t->t_host = h;
//		pscthr_setready(it);
#if 0
		switch (fork()) {
		case -1:
			break;
		case 0:
			//ssh -o 'Compression yes' host
			execve();
			err();
		default:
			break;
		}
#endif
//	}
}
