/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2013, Pittsburgh Supercomputing Center (PSC).
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

#include "slashd/inode.h"
#include "slashd/mdsio.h"

struct path {
	const char		*fn;
	struct psc_listentry	 lentry;
};

struct thr {
	FILE			*fp;
	char			 lastfn[PATH_MAX];
};

int			 resume;
int			 setid;
int			 setsize = 1;
int			 show;
int			 recurse;
pthread_barrier_t	 barrier;
struct psc_dynarray	 checkpoints;
struct psc_lockedlist	 excludes = PLL_INIT(&excludes, struct path, lentry);
struct psc_listcache	 files;
const char		*outfn;

const char *progname;

#define KEYW(w)

#define	K_BMAPS		(1 <<  0)
#define	K_BSZ		(1 <<  1)
#define	K_CRC		(1 <<  2)
#define	K_FID		(1 <<  3)
#define	K_FLAGS		(1 <<  4)
#define	K_FSIZE		(1 <<  5)
#define	K_NBLKS		(1 <<  6)
#define	K_NREPLS	(1 <<  7)
#define	K_REPLPOL	(1 <<  8)
#define	K_REPLS		(1 <<  9)
#define	K_UID		(1 << 10)
#define	K_VERSION	(1 << 11)
#define	K_XCRC		(1 << 12)
#define	K_DEF		((~0) & ~K_BMAPS)

const char *show_keywords[] = {
	"bmaps",
	"bsz",
	"crc",
	"fid",
	"flags",
	"fsize",
	"nblks",
	"nrepls",
	"replpol",
	"repls",
	"uid",
	"version",
	"xcrc"
};

const char *repl_states = "-sq+tpgx";

struct path *
searchpaths(struct psc_lockedlist *pll, const char *fn)
{
	struct path *p;

	/* XXX use bsearch */
	PLL_FOREACH(p, pll)
		if (strcmp(p->fn, fn) == 0)
			return (p);
	return (NULL);
}

struct f {
	char			*fn;
	struct pfl_stat		 stb;
	struct psc_listentry	 lentry;
	int			 ftyp;
};

int
dumpfid(struct f *f)
{
	sl_bmapno_t bno;
	struct psc_thread *thr = pscthr_get();
	struct slash_inode_extras_od inox;
	struct slash_inode_od ino;
	struct iovec iovs[2];
	struct thr *t;
	uint64_t crc, od_crc;
	uint32_t nr, j;
	char tbuf[32];
	int fd = -1;
	ssize_t rc;
	FILE *fp;

	t = thr->pscthr_private;
	fp = t->fp;

	fd = open(f->fn, O_RDONLY);
	if (fd == -1) {
		warn("%s", f->fn);
		goto out;
	}

	iovs[0].iov_base = &ino;
	iovs[0].iov_len = sizeof(ino);
	iovs[1].iov_base = &od_crc;
	iovs[1].iov_len = sizeof(od_crc);
	rc = readv(fd, iovs, nitems(iovs));
	if (rc == -1) {
		warn("%s", f->fn);
		goto out;
	}
	if (rc != sizeof(ino) + sizeof(od_crc)) {
		warnx("%s: short I/O, want %zd got %zd",
		    f->fn, sizeof(ino) + sizeof(od_crc), rc);
		goto out;
	}

	psc_crc64_calc(&crc, &ino, sizeof(ino));
	fprintf(fp, "%s:\n", f->fn);
	if (show & K_CRC)
		fprintf(fp, "  crc: %s %"PSCPRIxCRC64" %"PSCPRIxCRC64"\n",
		    crc == od_crc ? "OK" : "BAD", crc, od_crc);
	if (show & K_VERSION)
		fprintf(fp, "  version %u\n", ino.ino_version);
	if (show & K_FLAGS)
		fprintf(fp, "  flags %#x\n", ino.ino_flags);
	if (show & K_BSZ)
		fprintf(fp, "  bsz %u\n", ino.ino_bsz);
	if (show & K_NREPLS)
		fprintf(fp, "  nrepls %u\n", ino.ino_nrepls);
	if (show & K_REPLPOL)
		fprintf(fp, "  replpol %u\n", ino.ino_replpol);
	if (show & K_FSIZE && rc > 0) {
		rc = fgetxattr(fd, SLXAT_FSIZE, tbuf, sizeof(tbuf) - 1);
		if (rc == -1)
			warn("%s: getxattr %s", f->fn, SLXAT_FSIZE);
		else {
			tbuf[rc] = '\0';
			fprintf(fp, "  fsize %s\n", tbuf);
		}
	}
	if (show & K_FID) {
		rc = fgetxattr(fd, SLXAT_FID, tbuf, sizeof(tbuf) - 1);
		if (rc == -1)
			warn("%s: getxattr %s", f->fn, SLXAT_FID);
		else {
			tbuf[rc] = '\0';
			fprintf(fp, "  fid %s\n", tbuf);
		}
	}
	if (show & K_UID)
		fprintf(fp, "  usr %d\n", f->stb.st_uid);

	nr = ino.ino_nrepls;
	if (nr > SL_DEF_REPLICAS) {
		if (nr > SL_MAX_REPLICAS)
			nr = SL_MAX_REPLICAS;
		lseek(fd, SL_EXTRAS_START_OFF, SEEK_SET);
		iovs[0].iov_base = &inox;
		iovs[0].iov_len = sizeof(inox);
		iovs[1].iov_base = &od_crc;
		iovs[1].iov_len = sizeof(od_crc);
		rc = readv(fd, iovs, nitems(iovs));
		if (rc == -1) {
			warn("%s", f->fn);
			goto out;
		}
		if (rc != sizeof(inox) + sizeof(od_crc)) {
			warnx("%s: short I/O, want %zd got %zd",
			    f->fn, sizeof(inox) + sizeof(od_crc), rc);
			goto out;
		}
		psc_crc64_calc(&crc, &inox, sizeof(inox));
		if (show & K_XCRC)
			fprintf(fp, "  inox crc: %s %"PSCPRIxCRC64" %"PSCPRIxCRC64"\n",
			    crc == od_crc ? "OK" : "BAD", crc, od_crc);
	}
	if (show & K_REPLS) {
		fprintf(fp, "  repls ");
		for (j = 0; j < nr; j++)
			fprintf(fp, "%s%u", j ? "," : "",
			    j < SL_DEF_REPLICAS ?
			    ino.ino_repls[j].bs_id :
			    inox.inox_repls[j - SL_DEF_REPLICAS].bs_id);
		fprintf(fp, "\n");
	}
	if (show & K_NBLKS) {
		fprintf(fp, "  nblks ");
		for (j = 0; j < nr; j++)
			fprintf(fp, "%s%"PRIu64, j ? "," : "",
			    j < SL_DEF_REPLICAS ?
			    ino.ino_repl_nblks[j] :
			    inox.inox_repl_nblks[j - SL_DEF_REPLICAS]);
		fprintf(fp, "\n");
	}
	if (show & K_BMAPS &&
	    f->stb.st_size > SL_BMAP_START_OFF) {
		fprintf(fp, "  bmaps %"PSCPRIdOFFT"\n",
		    (f->stb.st_size - SL_BMAP_START_OFF) / BMAP_OD_SZ);
		if (lseek(fd, SL_BMAP_START_OFF, SEEK_SET) == -1)
			warn("seek");
		for (bno = 0; ; bno++) {
			struct {
				struct bmap_ondisk bod;
				uint64_t crc;
			} bd;
			int off;

			rc = read(fd, &bd, sizeof(bd));
			if (rc == 0)
				break;
			if (rc != sizeof(bd)) {
				warn("read");
				break;
			}
			fprintf(fp, "   %5u: gen %5u pol %u res ",
			    bno, bd.bod.bod_gen, bd.bod.bod_replpol);

			for (j = 0, off = 0; j < nr;
			    j++, off += SL_BITS_PER_REPLICA)
				fprintf(fp, "%c", repl_states[
				    SL_REPL_GET_BMAP_IOS_STAT(
				    bd.bod.bod_repls, off)]);

//			nslvr = SLASH_SLVRS_PER_BMAP;
//			for (j = 0; j < nr; j++)
//				fprintf(fp, "%s%s", j ? "," : "", );

			fprintf(fp, "\n");
		}
	}

 out:
	if (fd != -1)
		close(fd);
	PSCFREE(f->fn);
	PSCFREE(f);

	return (0);
}

int
fcmp(const void *a, const void *b)
{
	char * const *fa = a;
	char * const *fb = b;

	return (strcmp(*fa, *fb));
}

int
fcmp0(const void *a, const void *b)
{
	return (strcmp(a, b));
}

int
queue(const char *fn, const struct pfl_stat *stb, int ftyp,
    __unusedx int level, __unusedx void *arg)
{
	static int cnt;
	struct path *p;
	struct f *f;

	p = searchpaths(&excludes, fn);
	if (p) {
		pll_remove(&excludes, p);
		PSCFREE(p);
		return (PFL_FILEWALK_RC_SKIP);
	}

	if (ftyp != PFWT_F)
		return (0);

	if (cnt >= setsize)
		cnt = 0;
	if (cnt++ != setid)
		return (0);

	if (psc_dynarray_len(&checkpoints)) {
		static struct psc_spinlock lock = SPINLOCK_INIT;
		char *t;
		int n;

		spinlock(&lock);
		n = psc_dynarray_bsearch(&checkpoints, fn, fcmp0);
		if (n >= 0 && n < psc_dynarray_len(&checkpoints)) {
			t = psc_dynarray_getpos(&checkpoints, n);
			if (strcmp(fn, t) == 0) {
				// XXX leak paths
				DYNARRAY_FOREACH(t, n, &checkpoints)
					PSCFREE(t);
				psc_dynarray_reset(&checkpoints);
			}
		}
		freelock(&lock);

		if (psc_dynarray_len(&checkpoints))
			return (0);
	}

	f = PSCALLOC(sizeof(*f));
	INIT_PSC_LISTENTRY(&f->lentry);
	f->fn = pfl_strdup(fn);
	f->stb = *stb;
	f->ftyp = ftyp;
	while (lc_nitems(&files) > 256)
		usleep(1000);
	lc_add(&files, f);
	return (0);
}

void
lookupshow(const char *flg)
{
	const char **p;
	int i;

	for (i = 0, p = show_keywords;
	    i < nitems(show_keywords); i++, p++)
		if (strcmp(flg, *p) == 0) {
			show |= 1 << i;
			return;
		}
	errx(1, "unknown show field: %s", flg);
}

void
addexclude(const char *fn)
{
	struct path *p;

	p = PSCALLOC(sizeof(*p));
	INIT_PSC_LISTENTRY(&p->lentry);
	p->fn = fn;
	pll_add(&excludes, p);
}

void
thrmain(struct psc_thread *thr)
{
	struct thr *t;
	struct f *f;

	t = thr->pscthr_private;
	if (outfn) {
		char fn[PATH_MAX];

		(void)FMTSTR(fn, sizeof(fn), outfn,
		    FMTSTRCASE('n', "s", thr->pscthr_name +
			strcspn(thr->pscthr_name, "012345679"))
		);
		t->fp = fopen(fn, resume ? "r+" : "w");
		if (t->fp == NULL)
			err(1, "%s", fn);

		if (resume) {
			char *p, *s, *end;
			struct stat stb;
			size_t len;

			if (fstat(fileno(t->fp), &stb) == -1)
				err(1, "%s", fn);

			if (stb.st_size == 0)
				goto ready;

			/* find last position */
			p = mmap(NULL, stb.st_size, PROT_READ,
			    MAP_PRIVATE, fileno(t->fp), 0);
			if (p == MAP_FAILED)
				err(1, "%s", fn);
			end = p + stb.st_size - 1;

			for (s = end; s >= p; s--) {
				if (*s == '\n' && s < end &&
				    s[1] != ' ') {
					for (len = 1; s + len < end &&
					    s[len] != '\n'; len++)
						;
					if (s[len] != '\n')
						goto next;
					psc_dynarray_add(&checkpoints,
					    pfl_strndup(s + 1, len - 1));
					break;
				}
 next:
				;
			}

			munmap(p, stb.st_size);
		}
	} else
		t->fp = stdout;

 ready:
	pthread_barrier_wait(&barrier);
	while ((f = lc_getwait(&files)))
		dumpfid(f);
}

__dead void
usage(void)
{
	fprintf(stderr,
	    "usage: %s [-PR] [-O file] [-o keys] [-S id:size] "
	    "[-t nthr] [-x exclude] file ...\n",
	    progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	extern void *cmpf;
	int walkflags = PFL_FILEWALKF_RELPATH, c, n, nthr = 1;
	struct psc_thread **thrv;
	char *endp, *p, *id;
	pthread_t *tid;
	long l;

	pfl_init();
	progname = argv[0];
	while ((c = getopt(argc, argv, "O:o:PRS:t:x:")) != -1) {
		switch (c) {
		case 'O':
			outfn = optarg;
			break;
		case 'o':
			lookupshow(optarg);
			break;
		case 'P':
			resume = 1;
			break;
		case 'R':
			walkflags |= PFL_FILEWALKF_RECURSIVE;
			break;
		case 'S':
			id = optarg;
			p = strchr(optarg, ':');
			if (p == NULL)
				errx(1, "invalid -S format: %s",
				    optarg);
			*p++ = '\0';

			l = strtol(id, &endp, 10);
			if (l < 0 || l > 256 ||
			    id == endp || *endp)
				errx(1, "invalid -S id: %s", id);
			setid = l;

			l = strtol(p, &endp, 10);
			if (l < 1 || l > 256 ||
			    p == endp || *endp)
				errx(1, "invalid -S setsize: %s", p);
			setsize = l;

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
	if (!show)
		show = K_DEF;
	if (resume && outfn == NULL)
		errx(1, "resume specified but not output file to restore state from");

	pthread_barrier_init(&barrier, NULL, nthr + 1);
	lc_init(&files, struct f, lentry);
	thrv = PSCALLOC(nthr * sizeof(*tid));
	for (n = 0; n < nthr; n++) {
		thrv[n] = pscthr_init(0, 0, thrmain, NULL,
		    sizeof(struct thr), "thr%d", n);
		pscthr_setready(thrv[n]);
	}
	pthread_barrier_wait(&barrier);
	psc_dynarray_sort(&checkpoints, qsort, fcmp);
	for (; *argv; argv++)
		pfl_filewalk(*argv, walkflags, cmpf, queue, NULL);
	lc_kill(&files);
	for (n = 0; n < nthr; n++)
		pthread_join(thrv[n]->pscthr_pthread, NULL);
	exit(0);
}
