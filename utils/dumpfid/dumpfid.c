/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2015, Pittsburgh Supercomputing Center (PSC).
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
 *
 * Pittsburgh Supercomputing Center	phone: 412.268.4960  fax: 412.268.5832
 * 300 S. Craig Street			e-mail: remarks@psc.edu
 * Pittsburgh, PA 15213			web: http://www.psc.edu/
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

#include <sys/param.h>
#include <sys/uio.h>
#include <sys/xattr.h>

#include <err.h>
#include <fcntl.h>
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

struct path {
	const char		*p_fn;
	struct psc_listentry	 p_lentry;
};

struct host {
	const char		*h_hostname;
	const char		*h_path;
	struct psc_listentry	 h_lentry;
};

struct thr {
	FILE			*t_fp;
	struct host		*t_host;
	char			 t_buf[512 * 1024];
};

#define STAT_SZ		(8 + sizeof(struct srt_stat))
#define INOSTAT_SZ	(STAT_SZ + sizeof(struct slash_inode_od) + 8)
#define INOXSTAT_SZ	(INOSTAT_SZ + sizeof(struct slash_inode_extras_od) + 8)

struct file {
	char			*f_fn;
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

struct psc_lockedlist		 hosts = PLL_INIT(&hosts, struct host, h_lentry);
int				 recurse;
pthread_barrier_t		 barrier;
struct psc_lockedlist		 excludes = PLL_INIT(&excludes, struct path, p_lentry);
const char			*outfn;
const char			*display_fmt =
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

struct psc_listcache		 files;
struct psc_poolmaster		 files_poolmaster;
struct psc_poolmgr		*files_pool;

const char			*progname;

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
pr_bmaps(struct thr *t, FILE *outfp, struct file *f)
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
		warn("seek");
	fd = dup(f->f_fd);
	if (fd == -1) {
		warn("dup");
		return;
	}
	fp = fdopen(fd, "r");
	if (fp == NULL) {
		warn("fdopen");
		close(fd);
		return;
	}
	setbuffer(fp, t->t_buf, sizeof(t->t_buf));

	for (bno = 0; ; bno++) {
		rc = fread(&bd, 1, sizeof(bd), fp);
		if (rc == 0)
			break;
		if (rc != sizeof(bd)) {
			warn("read");
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
	}

	if (ferror(fp))
		warn("%s: read", f->f_fn);

	fclose(fp);
}

int
queue(const char *fn, __unusedx const struct stat *stb, int ftyp,
    __unusedx int level, __unusedx void *arg)
{
	struct path *p;
	struct file *f;

	p = searchpaths(&excludes, fn);
	if (p) {
		pll_remove(&excludes, p);
		PSCFREE(p);
		return (PFL_FILEWALK_RC_SKIP);
	}

	if (ftyp != PFWT_F &&
	    ftyp != PFWT_D)
		return (0);

	f = psc_pool_get(files_pool);
	memset(f, 0, sizeof(*f));
	f->f_fd = -1;
	INIT_PSC_LISTENTRY(&f->f_lentry);
	f->f_fn = pfl_strdup(fn);
	lc_add(&files, f);
	return (0);
}

void
addexclude(const char *fn)
{
	struct path *p;

	p = PSCALLOC(sizeof(*p));
	INIT_PSC_LISTENTRY(&p->p_lentry);
	p->p_fn = fn;
	pll_add(&excludes, p);
}

int
load_data_fd(struct file *f, void *buf)
{
	f->f_fd = open(f->f_fn, O_RDONLY);
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
	if (getxattr(f->f_fn, SLXAT_INOXSTAT, buf, INOXSTAT_SZ) !=
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
	if (getxattr(f->f_fn, SLXAT_INOSTAT, buf, INOSTAT_SZ) !=
	    INOSTAT_SZ)
		return (0);
	psc_crc64_calc(&f->f_ino_mem_crc, &f->f_ino, sizeof(f->f_ino));
	return (1);
}

int
load_data_stat(struct file *f, void *buf)
{
	if (getxattr(f->f_fn, SLXAT_STAT, buf, STAT_SZ) !=
	    STAT_SZ)
		return (0);
	return (1);
}

int (*load_data)(struct file *f, void *) = load_data_stat;

void
thrmain(struct psc_thread *thr)
{
	char modebuf[16];
	struct slm_ino_od *ino;
	struct srt_stat *sstb;
	struct file *f;
	struct thr *t;

	t = thr->pscthr_private;
	if (t->t_host) {
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
	} else {
		if (outfn) {
			char fn[PATH_MAX];

			(void)FMTSTR(fn, sizeof(fn), outfn,
			    FMTSTRCASE('n', "s", thr->pscthr_name +
				strcspn(thr->pscthr_name, "0123456789"))
			);
			t->t_fp = fopen(fn, "w");
			if (t->t_fp == NULL)
				err(1, "open %s", fn);
		} else
			t->t_fp = stdout;
	}

	pthread_barrier_wait(&barrier);
	while ((f = lc_getwait(&files))) {
		if (!load_data(f, &f->f_data)) {
			warn("%s", f->f_fn);
			goto next;
		}
		sstb = &f->f_sstb;
		ino = &f->f_ino;
		f->f_nrepls = MIN(SL_MAX_REPLICAS, ino->ino_nrepls);
		(void)PRFMTSTR(t->t_fp, display_fmt,
		    PRFMTSTRCASEV('B', pr_repl_blks(_fp, f))
		    PRFMTSTRCASE('b', PRIu64, sstb->sst_blocks)
		    PRFMTSTRCASE('C', PSCPRIxCRC64, f->f_ino_mem_crc)
		    PRFMTSTRCASE('c', PSCPRIxCRC64, f->f_ino_od_crc)
		    PRFMTSTRCASE('d', "s",
			f->f_ino_od_crc == f->f_ino_mem_crc ? "OK" : "BAD")
		    PRFMTSTRCASE('F', PRIx64, sstb->sst_fg.fg_fid)
		    PRFMTSTRCASE('f', "s", f->f_fn)
		    PRFMTSTRCASE('G', PRIu64, sstb->sst_fg.fg_gen)
		    PRFMTSTRCASE('g', "u", sstb->sst_gid)
		    PRFMTSTRCASE('L', "#x", ino->ino_flags)
		    PRFMTSTRCASEV('M', pr_bmaps(t, _fp, f))
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

 next:
		if (f->f_fd != -1)
			close(f->f_fd);
		PSCFREE(f->f_fn);
		psc_pool_return(files_pool, f);
	}
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
		pll_add(&hosts, h);
	}
}

__dead void
usage(void)
{
	fprintf(stderr,
	    "usage: %s [-R] [-C hosts] [-F fmt] [-O file]\n"
	    "\t[-t nthr] [-x exclude] file ...\n",
	    progname);
	exit(1);
}

#define NEED_INO	(1 << 0)
#define NEED_INOX	(1 << 1)
#define NEED_FD		(1 << 2)

int
main(int argc, char *argv[])
{
	int walkflags = PFL_FILEWALKF_RELPATH, c, i, nthr = 1, need = 0;
	struct psc_thread **thrv, *it;
	struct host *h;
	struct thr *t;
	char *endp;
	FILE *fp;
	long l;

	pfl_init();
	progname = argv[0];
	while ((c = getopt(argc, argv, "C:F:O:Rt:x:")) != -1) {
		switch (c) {
		case 'C':
			addhost(optarg);
			break;
		case 'F':
			display_fmt = optarg;
			break;
		case 'O':
			outfn = optarg;
			break;
		case 'R':
			walkflags |= PFL_FILEWALKF_RECURSIVE;
			break;
		case 't':
			l = strtol(optarg, &endp, 10);
			if (l < 1 || l > 256 ||
			    optarg == endp || *endp)
				errx(1, "invalid -t value: %s", optarg);
			nthr = l;
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
	(void)PRFMTSTR(fp, display_fmt,
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

	psc_poolmaster_init(&files_poolmaster, struct file, f_lentry, 0,
	    nthr * 4, nthr * 4, nthr * 4, NULL, NULL, NULL, "files",
	    NULL);
	files_pool = psc_poolmaster_getmgr(&files_poolmaster);

	pthread_barrier_init(&barrier, NULL, nthr + 1);
	lc_init(&files, struct file, f_lentry);
	thrv = PSCALLOC((nthr + pll_nitems(&hosts)) * sizeof(*thrv));
	for (i = 0; i < nthr; i++) {
		it = thrv[i] = pscthr_init(0, thrmain, NULL, sizeof(*t),
		    "thr%d", i);
		pscthr_setready(it);
	}
	PLL_FOREACH(h, &hosts) {
		it = thrv[i++] = pscthr_init(0, thrmain, NULL,
		    sizeof(*t), "thr-%s", h->h_hostname);
		t = it->pscthr_private;
		t->t_host = h;
		pscthr_setready(it);
	}
	pthread_barrier_wait(&barrier);
	for (; *argv; argv++)
		pfl_filewalk(*argv, walkflags, NULL, queue, NULL);
	lc_kill(&files);
	for (i = 0; i < nthr + pll_nitems(&hosts); i++)
		pthread_join(thrv[i]->pscthr_pthread, NULL);
	exit(0);
}
