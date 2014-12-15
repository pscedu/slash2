/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2014, Pittsburgh Supercomputing Center (PSC).
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
		uint64_t			 metasize;
		struct srt_stat			 sstb;
		struct slash_inode_od		 ino;
		uint64_t			 ino_crc;
		struct slash_inode_extras_od	 inox;
		uint64_t			 inox_crc;
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
const char			*fmt;

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
pr_repl_blks(FILE *outfp, struct file *f)
{
	uint32_t i;

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

	if (ftyp != PFWT_F)
		return (0);

	f = psc_pool_get(files_pool);
	memset(f, 0, sizeof(*f));
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
load_data_fd(struct file *f, char *buf)
{
	f->f_fd = open(f->f_fn, O_RDONLY);
	if (f->f_fd == -1)
		return (0);
	if (fgetxattr(f->f_fd, SLXAT_INOXSTAT, buf, INOX_SZ) != INOX_SZ)
		return (0);
	psc_crc64_calc(&f->f_ino_mem_crc, &f->f_ino, sizeof(f->f_ino));
	psc_crc64_calc(&f->f_inox_mem_crc, &f->f_inox,
	    sizeof(f->f_inox));
	return (1);
}

int
load_data_inox(struct file *f, char *buf)
{
	if (getxattr(f->fn, SLXAT_INOXSTAT, buf, INOX_SZ) != INOX_SZ)
		return (0);
	psc_crc64_calc(&f->f_ino_mem_crc, &f->f_ino, sizeof(f->f_ino));
	psc_crc64_calc(&f->f_inox_mem_crc, &f->f_inox,
	    sizeof(f->f_inox));
	return (1);
}

int
load_data_ino(struct file *f, char *buf)
{
	if (getxattr(f->fn, SLXAT_INOSTAT, buf, INO_SZ) != INO_SZ)
		return (0);
	psc_crc64_calc(&f->ino_mem_crc, &ino, sizeof(ino));
	return (1);
}

int
load_data_stat(struct file *f, char *buf)
{
	if (getxattr(f->fn, SLXAT_STAT, buf, STAT_SZ) != STAT_SZ)
		return (0);
	return (1);
}

void
thrmain(struct psc_thread *thr)
{
	struct file *f;
	struct thr *t;

	t = thr->pscthr_private;
	if (t->host) {
#if 0
		switch (fork()) {
		case -1:
			break;
		case 0:
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
				strcspn(thr->pscthr_name, "012345679"))
			);
			t->fp = fopen(fn, "w");
			if (t->fp == NULL)
				err(1, "%s", fn);
		} else
			t->fp = stdout;
	}

	pthread_barrier_wait(&barrier);
	while ((f = lc_getwait(&files))) {
		if (load_data(f, tbuf)) {
			warn("%s", f->fn);
			goto next;
		}
		(void)PRFMTSTR(fp, fmt,
		    PRFMTSTRCASE('B', NULL, pr_repl_blks(fp, f))
		    PRFMTSTRCASE('b', PRIu64, sstb->sst_blocks)
		    PRFMTSTRCASE('c', PSCPRIxCRC64, t->ino_od_crc)
		    PRFMTSTRCASE('C', PSCPRIxCRC64, t->ino_mem_crc)
		    PRFMTSTRCASE('d', "s",
			t->ino_od_crc == t->ino_mem_crc ? "OK" : "BAD")
		    PRFMTSTRCASE('F', PRIx64, sstb->sst_fg.fg_fid)
		    PRFMTSTRCASE('f', "#x", ino->ino_flags)
		    PRFMTSTRCASE('G', PRIu64, sstb->sst_fg.fg_gen)
		    PRFMTSTRCASE('g', "u", sstb->sst_gid)
		    PRFMTSTRCASE('M', NULL, pr_bmaps(fp, f))
		    PRFMTSTRCASE('N', PSCPRIdOFFT,
			(f->f_metasize - SL_BMAP_START_OFF) / BMAP_OD_SZ)
		    PRFMTSTRCASE('n', "u", ino->ino_nrepls)
		    PRFMTSTRCASE('R', NULL, pr_repls(fp, f))
		    PRFMTSTRCASE('s', PRIu64, sstb->sst_size)
		    PRFMTSTRCASE('u', "u", sstb->sst_uid)
		    PRFMTSTRCASE('v', "u", ino->ino_version)
		    PRFMTSTRCASE('x', PSCPRIxCRC64, t->inox_od_crc)
		    PRFMTSTRCASE('X', PSCPRIxCRC64, t->inox_mem_crc)
		    PRFMTSTRCASE('y', PSCPRIxCRC64,
			t->inox_mem_crc == t->inox_od_crc ? "OK" : "BAD")
		);

 next:
		if (f->fd != -1)
			close(f->fd);
		PSCFREE(f->fn);
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

int
main(int argc, char *argv[])
{
	int walkflags = PFL_FILEWALKF_RELPATH, c, i, nthr = 1;
	struct psc_thread **thrv;
	struct thr *t;
	char *endp;
	long l;

	pfl_init();
	progname = argv[0];
	while ((c = getopt(argc, argv, "C:F:O:Rt:x:")) != -1) {
		switch (c) {
		case 'C':
			addhost(optarg);
			break;
		case 'F':
			fmt = optarg;
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

	(void)PRFMTSTR(stdout, fmt,
	    PRFMTSTRCASE('B', NULL, pr_repl_blks(fp, f))
	    PRFMTSTRCASE('b', NULL, sstb->sst_blocks)
	    PRFMTSTRCASE('c', NULL, t->ino_od_crc)
	    PRFMTSTRCASE('C', NULL, t->ino_mem_crc)
	    PRFMTSTRCASE('d', NULL, t->ino_od_crc == t->ino_mem_crc ? "OK" : "BAD")
	    PRFMTSTRCASE('F', NULL, sstb->sst_fg.fg_fid)
	    PRFMTSTRCASE('f', NULL, ino->ino_flags)
	    PRFMTSTRCASE('G', NULL, sstb->sst_fg.fg_gen)
	    PRFMTSTRCASE('g', NULL, need_stat = 1)
	    PRFMTSTRCASE('M', NULL, pr_bmaps(fp, f))
	    PRFMTSTRCASE('N', NULL, (f->f_metasize - SL_BMAP_START_OFF) / BMAP_OD_SZ)
	    PRFMTSTRCASE('n', NULL, need_ino = 1)
	    PRFMTSTRCASE('R', NULL, pr_repls(fp, f))
	    PRFMTSTRCASE('s', NULL, need_stat = 1)
	    PRFMTSTRCASE('u', NULL, need_stat = 1)
	    PRFMTSTRCASE('v', NULL, need_ino = 1)
	    PRFMTSTRCASE('x', NULL, need_inox = 1)
	    PRFMTSTRCASE('X', NULL, need_inox = 1)
	    PRFMTSTRCASE('y', NULL, need_inox = 1)
	);

	ssh -o 'Compression yes' host

	psc_poolmaster_init(&files_poolmaster, );
	files_pool = psc_poolmaster_getmgr();
	while (lc_nitems(&files) > 256)
		usleep(1000);

	pthread_barrier_init(&barrier, NULL, nthr + 1);
	lc_init(&files, struct file, f_lentry);
	thrv = PSCALLOC((nthr + pll_nitems(&hosts)) * sizeof(*thrv));
	for (i = 0; i < nthr; i++) {
		t = thrv[i] = pscthr_init(0, thrmain, NULL, sizeof(*t),
		    "thr%d", i);
		pscthr_setready(t);
	}
	PLL_FOREACH(h, &hosts) {
		t = thrv[i++] = pscthr_init(0, thrmain, NULL,
		    sizeof(*t), "thr-%s", h->h_hostname);
		t->t_host = h;
		pscthr_setready(t);
	}
	pthread_barrier_wait(&barrier);
	for (; *argv; argv++)
		pfl_filewalk(*argv, walkflags, NULL, queue, NULL);
	lc_kill(&files);
	for (i = 0; i < nthr + pll_nitems(&hosts); i++)
		pthread_join(thrv[i]->pscthr_pthread, NULL);
	exit(0);
}
