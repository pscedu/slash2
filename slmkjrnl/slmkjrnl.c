/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright 2006-2016, Pittsburgh Supercomputing Center
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
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "pfl/fcntl.h"
#include "pfl/fs.h"
#include "pfl/journal.h"
#include "pfl/log.h"
#include "pfl/pfl.h"
#include "pfl/str.h"

#include "mkfn.h"
#include "pathnames.h"
#include "slerr.h"

#include "slashd/journal_mds.h"
#include "slashd/mdsio.h"
#include "slashd/namespace.h"
#include "slashd/subsys_mds.h"

int		 format;
int		 query;
int		 verbose;
const char	*datadir = SL_PATH_DATA_DIR;
struct pscfs	 pscfs;
struct mdsio_ops mdsio_ops;

/*
 * Initialize an on-disk journal.
 * @fn: file path to store journal.
 * @nents: number of entries journal may contain if non-zero.
 * @entsz: size of a journal entry.
 * @rs: read size.
 * Returns the number of entries created.
 */
uint32_t
sl_journal_format(const char *fn, uint32_t nents, uint32_t entsz,
    uint32_t rs, uint64_t uuid, int block_dev)
{
	uint32_t i, slot, max_nents;
	struct psc_journal_enthdr *pje;
	struct psc_journal pj;
	struct stat stb;
	unsigned char *jbuf;
	size_t numblocks;
	ssize_t nb;
	int fd;

	memset(&pj, 0, sizeof(pj));

	fd = open(fn, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (fd == -1)
		psc_fatal("%s", fn);

	if (fstat(fd, &stb) == -1)
		psc_fatal("stat %s", fn);

	/*
	 * If the user does not specify nents, either use default or
	 * based on the block device size.
	 */
	if (nents == 0 && !block_dev)
		nents = SLJ_MDS_JNENTS;

	if (block_dev) {
		if (ioctl(fd, BLKGETSIZE, &numblocks) == -1)
			err(1, "BLKGETSIZE: %s", fn);

		/* show progress, it is going to be a while */
		verbose = 1;

		/* deal with large disks */
		max_nents = MIN(numblocks, SLJ_MDS_MAX_JNENTS);

		/* leave room on both ends */
		max_nents -= stb.st_blksize / SLJ_MDS_ENTSIZE + 16;

		/* efficiency */
		max_nents = (max_nents / rs) * rs;
		if (nents)
			nents = MIN(nents, max_nents);
		else
			nents = max_nents;
	}

	if (nents % rs)
		psc_fatalx("number of slots (%u) should be a multiple of "
		    "readsize (%u)", nents, rs);

	pj.pj_fd = fd;
	pj.pj_hdr = PSCALLOC(PSC_ALIGN(sizeof(struct psc_journal_hdr),
	    stb.st_blksize));

	pj.pj_hdr->pjh_entsz = entsz;
	pj.pj_hdr->pjh_nents = nents;
	pj.pj_hdr->pjh_version = PJH_VERSION;
	pj.pj_hdr->pjh_readsize = rs;
	pj.pj_hdr->pjh_iolen = PSC_ALIGN(sizeof(struct psc_journal_hdr),
	    stb.st_blksize);
	pj.pj_hdr->pjh_magic = PJH_MAGIC;
	pj.pj_hdr->pjh_timestamp = time(NULL);
	pj.pj_hdr->pjh_fsuuid = uuid;

	psc_crc64_init(&pj.pj_hdr->pjh_chksum);
	psc_crc64_add(&pj.pj_hdr->pjh_chksum, pj.pj_hdr,
	    offsetof(struct psc_journal_hdr, pjh_chksum));
	psc_crc64_fini(&pj.pj_hdr->pjh_chksum);

	nb = pwrite(pj.pj_fd, pj.pj_hdr, pj.pj_hdr->pjh_iolen, 0);
	if ((size_t)nb != pj.pj_hdr->pjh_iolen)
		psc_fatalx("failed to write journal header: %s",
		    nb == -1 ? strerror(errno) : "short write");

	nb = PJ_PJESZ(&pj) * pj.pj_hdr->pjh_readsize;
	jbuf = psc_alloc(nb, PAF_PAGEALIGN);
	for (i = 0; i < rs; i++) {
		pje = PSC_AGP(jbuf, PJ_PJESZ(&pj) * i);
		pje->pje_magic = PJE_MAGIC;
		pje->pje_type = PJE_FORMAT;
		pje->pje_xid = PJE_XID_NONE;
		pje->pje_len = 0;

		psc_crc64_init(&pje->pje_chksum);
		psc_crc64_add(&pje->pje_chksum, pje,
		    offsetof(struct psc_journal_enthdr, pje_chksum));
		psc_crc64_add(&pje->pje_chksum, pje->pje_data,
		    pje->pje_len);
		psc_crc64_fini(&pje->pje_chksum);
	}

	i = 0;
	/* XXX use an option to write only one entry in fast create mode */
	for (slot = 0; slot < pj.pj_hdr->pjh_nents; slot += rs) {
		nb = pwrite(pj.pj_fd, jbuf, PJ_PJESZ(&pj) * rs,
		    PJ_GETENTOFF(&pj, slot));
		if ((size_t)nb != PJ_PJESZ(&pj) * rs)
			psc_fatal("failed to write slot %u (%zd)",
			    slot, nb);
		if (verbose && slot % 262144 == 0) {
			printf(".");
			fflush(stdout);
			fsync(pj.pj_fd);
			if (++i == 80) {
				printf("\n");
				i = 0;
			}
		}
	}
	if (verbose && i)
		printf("\n");
	if (close(fd) == -1)
		psc_fatal("failed to close journal");
	psc_free(jbuf, PAF_PAGEALIGN, PJ_PJESZ(&pj) * rs);

	return (nents);
}

void
sl_journal_dump_entry(uint32_t slot, struct psc_journal_enthdr *pje)
{
	union {
		struct slmds_jent_assign_rep *logentry;
		struct slmds_jent_bmap_assign *sjba;
		struct slmds_jent_bmap_repls *sjbr;
		struct slmds_jent_ino_repls *sjir;
		struct slmds_jent_namespace *sjnm;
		struct slmds_jent_bmap_crc *sjbc;
		struct slmds_jent_bmapseq *sjsq;
		void *p;
	} u;
	int type, nlen, n2len;
	const char *n, *n2;

	u.p = PJE_DATA(pje);

	type = pje->pje_type & ~(_PJE_FLSHFT - 1);
	printf("%10d  %3x %12"PRId64" %12"PRId64"  ",
	    slot, type, pje->pje_xid, pje->pje_txg);
	switch (type) {
	case MDS_LOG_BMAP_REPLS:
		printf("fid=%016"PRIx64" bmap_repls", u.sjbr->sjbr_fid);
		break;
#if 0
	case MDS_LOG_BMAP_CRC:
		printf("fid=%016"PRIx64" bmap_crc", u.sjbc->sjbc_fid);
		break;
#endif
	case MDS_LOG_BMAP_SEQ:
		printf("lwm=%"PRIu64" hwm=%"PRIu64,
		    u.sjsq->sjbsq_low_wm,
		    u.sjsq->sjbsq_high_wm);
		break;
	case MDS_LOG_INO_REPLS:
		printf("fid=%016"PRIx64" ino_repls", u.sjir->sjir_fid);
		break;
	case MDS_LOG_BMAP_ASSIGN:
		if (u.logentry->sjar_flags & SLJ_ASSIGN_REP_FREE)
			printf("bia item=%d", u.logentry->sjar_item);
		else {
			printf("fid=%016"PRIx64" bia flags=%x",
			    u.logentry->sjar_bmap.sjba_fid,
			    u.logentry->sjar_flags);
		}
		break;
	case MDS_LOG_NAMESPACE:
		printf("fid=%016"PRIx64" ", u.sjnm->sjnm_target_fid);

		n = u.sjnm->sjnm_name;
		nlen = u.sjnm->sjnm_namelen;
		n2len = u.sjnm->sjnm_namelen2;
		n2 = n + nlen;

		switch (u.sjnm->sjnm_op) {
		case NS_OP_RECLAIM:
			printf("op=reclaim");
			break;
		case NS_OP_CREATE:
			printf("op=create name=%.*s", nlen, n);
			break;
		case NS_OP_MKDIR:
			printf("op=mkdir name=%.*s", nlen, n);
			break;
		case NS_OP_LINK:
			printf("op=link name=%.*s", nlen, n);
			break;
		case NS_OP_SYMLINK:
			printf("op=symlink name=%.*s", nlen, n);
			break;
		case NS_OP_RENAME:
			printf("op=rename oldname=%.*s newname=%.*s",
			    nlen, n, n2len, n2);
			break;
		case NS_OP_UNLINK:
			printf("op=unlink name=%.*s", nlen, n);
			break;
		case NS_OP_RMDIR:
			printf("op=rmdir name=%.*s", nlen, n);
			break;
		case NS_OP_SETSIZE:
			printf("op=setsize");
			break;
		case NS_OP_SETATTR:
			printf("op=setattr mask=%#x",
			    u.sjnm->sjnm_mask);
			break;
		default:
			psclog_errorx("op=INVALID (%d)",
			    u.sjnm->sjnm_op);
		}
		break;
	default:
		psclog_errorx("invalid type");
		break;
	}
	printf("\n");
}

/*
 * Dump the contents of a journal file.
 * @fn: journal filename to query.
 *
 * Each time mds restarts, it writes log entries starting from the very
 * first slot of the log.  Anyway, the function dumps all log entries,
 * some of them may be from previous incarnations of the MDS.
 */
void
sl_journal_dump(const char *fn)
{
	int i, ntotal, nmagic, nchksum, nformat, ndump, first = 1;
	uint32_t slot, highest_slot = -1, lowest_slot = -1;
	uint64_t chksum, highest_xid = 0, lowest_xid = 0;
	struct psc_journal_enthdr *pje;
	struct psc_journal_hdr *pjh;
	struct psc_journal *pj;
	struct stat statbuf;
	unsigned char *jbuf;
	ssize_t nb, pjhlen;
	time_t ts;

	ntotal = nmagic = nchksum = nformat = ndump = 0;

	pj = PSCALLOC(sizeof(*pj));

	strlcpy(pj->pj_name, pfl_basename(fn), sizeof(pj->pj_name));

	pj->pj_fd = open(fn, O_RDWR | O_DIRECT);
	if (pj->pj_fd == -1)
		psc_fatal("failed to open journal %s", fn);
	if (fstat(pj->pj_fd, &statbuf) == -1)
		psc_fatal("failed to stat journal %s", fn);

	/*
	 * O_DIRECT may impose alignment restrictions so align the
	 * buffer and perform I/O in multiples of file system block
	 * size.
	 */
	pjhlen = PSC_ALIGN(sizeof(*pjh), statbuf.st_blksize);
	pjh = psc_alloc(pjhlen, PAF_PAGEALIGN);
	nb = pread(pj->pj_fd, pjh, pjhlen, 0);
	if (nb != pjhlen)
		psc_fatal("failed to read journal header");

	pj->pj_hdr = pjh;
	if (pjh->pjh_magic != PJH_MAGIC)
		psc_fatalx("journal header has a bad magic number "
		    "%#"PRIx64, pjh->pjh_magic);

	if (pjh->pjh_version != PJH_VERSION)
		psc_fatalx("journal header has an invalid version "
		    "number %d", pjh->pjh_version);

	psc_crc64_init(&chksum);
	psc_crc64_add(&chksum, pjh, offsetof(struct psc_journal_hdr,
	    pjh_chksum));
	psc_crc64_fini(&chksum);

	if (pjh->pjh_chksum != chksum)
		psc_fatalx("journal header has an invalid checksum "
		    "value %"PSCPRIxCRC64" vs %"PSCPRIxCRC64,
		    pjh->pjh_chksum, chksum);

	if (S_ISREG(statbuf.st_mode) && statbuf.st_size !=
	    (off_t)(pjhlen + pjh->pjh_nents * PJ_PJESZ(pj)))
		psc_fatalx("size of the journal log %"PSCPRIdOFFT"d does "
		    "not match specs in its header", statbuf.st_size);

	if (pjh->pjh_nents % pjh->pjh_readsize)
		psc_fatalx("number of entries %d is not a multiple of the "
		    "readsize %d", pjh->pjh_nents, pjh->pjh_readsize);

	ts = pjh->pjh_timestamp;

	printf("%s:\n"
	    "  version: %u\n"
	    "  entry size: %u\n"
	    "  number of entries: %u\n"
	    "  batch read size: %u\n"
	    "  entry start offset: %"PRId64"\n"
	    "  format time: %s"
	    "  uuid: %"PRIx64"\n"
	    "  %8s %3s %12s %12s  %s\n",
	    fn, pjh->pjh_version, PJ_PJESZ(pj), pjh->pjh_nents,
	    pjh->pjh_readsize, pjh->pjh_start_off,
	    ctime(&ts), pjh->pjh_fsuuid,
	    "idx", "type", "xid", "txg", "details");

	jbuf = psc_alloc(PJ_PJESZ(pj) * pj->pj_hdr->pjh_readsize,
	    PAF_PAGEALIGN);
	for (slot = 0; slot < pjh->pjh_nents;
	    slot += pjh->pjh_readsize) {
		nb = pread(pj->pj_fd, jbuf, PJ_PJESZ(pj) *
		    pjh->pjh_readsize, PJ_GETENTOFF(pj, slot));
		if (nb != PJ_PJESZ(pj) * pjh->pjh_readsize)
			warn("failed to read %d log entries at slot %d",
			    pjh->pjh_readsize, slot);

		for (i = 0; i < pjh->pjh_readsize; i++) {
			ntotal++;
			pje = (void *)&jbuf[PJ_PJESZ(pj) * i];
			if (pje->pje_magic != PJE_MAGIC) {
				nmagic++;
				warnx("journal slot %d has a bad magic"
				    "number", slot + i);
				continue;
			}

			/*
			 * If we hit a new entry that is never used, we
			 * assume that the rest of the journal is never
			 * used.
			 */
			if (pje->pje_type == PJE_FORMAT) {
				nformat = nformat + pjh->pjh_nents -
				    (slot + i);
				goto done;
			}

			psc_crc64_init(&chksum);
			psc_crc64_add(&chksum, pje, offsetof(
			    struct psc_journal_enthdr, pje_chksum));
			psc_crc64_add(&chksum, pje->pje_data,
			    pje->pje_len);
			psc_crc64_fini(&chksum);

			if (pje->pje_chksum != chksum) {
				nchksum++;
				warnx("journal slot %d has a corrupt "
				    "checksum", slot + i);
				goto done;
			}
			ndump++;
			if (verbose)
				sl_journal_dump_entry(slot + i, pje);
			if (first) {
				first = 0;
				highest_xid = lowest_xid = pje->pje_xid;
				highest_slot = lowest_slot = slot + i;
				continue;
			}
			if (highest_xid < pje->pje_xid) {
				highest_xid = pje->pje_xid;
				highest_slot = slot + i;
			}
			if (lowest_xid > pje->pje_xid) {
				lowest_xid = pje->pje_xid;
				lowest_slot = slot + i;
			}
		}

	}

 done:
	if (close(pj->pj_fd) == -1)
		printf("failed closing journal %s", fn);

	psc_free(jbuf, PAF_PAGEALIGN, PJ_PJESZ(pj));
	PSCFREE(pj);

	printf("----------------------------------------------\n"
	    "%8d slot(s) scanned\n"
	    "%8d in use\n"
	    "%8d formatted\n"
	    "%8d bad magic\n"
	    "%8d bad checksum(s)\n"
	    "lowest transaction ID=%#"PRIx64" (slot=%d)\n"
	    "highest transaction ID=%#"PRIx64" (slot=%d)\n",
	    ntotal, ndump, nformat, nmagic, nchksum,
	    lowest_xid, lowest_slot,
	    highest_xid, highest_slot);
}

__dead void
usage(void)
{
	extern const char *__progname;

	fprintf(stderr,
	    "usage: %s [-fqv] [-b block-device] [-D dir] [-n nentries] [-u uuid]\n",
	    __progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	int block_dev = 0;
	ssize_t newnents, nents = 0;
	char *endp, c, fn[PATH_MAX];
	uint64_t uuid = 0;
	long long ll;

	pfl_init();
	sl_errno_init();
	pscthr_init(0, NULL, 0, "slmkjrnl");
	sl_subsys_register();

	fn[0] = '\0';
	while ((c = getopt(argc, argv, "b:D:fn:qu:v")) != -1)
		switch (c) {
		case 'b':
			strlcpy(fn, optarg, sizeof(fn));
			block_dev = 1;
			break;
		case 'D':
			datadir = optarg;
			break;
		case 'f':
			format = 1;
			break;
		case 'n':
			endp = NULL;
			ll = strtoll(optarg, &endp, 10);
			if (ll <= 0 || ll > (long long)SLJ_MDS_MAX_JNENTS ||
			    endp == optarg || *endp)
				errx(1, "invalid -n nentries: %s",
				    optarg);
			nents = (ssize_t)ll;
			break;
		case 'q':
			query = 1;
			break;
		case 'u':
			endp = NULL;
			uuid = (uint64_t)strtoull(optarg, &endp, 16);
			if (endp == optarg || *endp)
				errx(1, "invalid -u fsuuid: %s",
				    optarg);
			break;
		case 'v':
			verbose = 1;
			break;
		default:
			usage();
		}
	argc -= optind;
	if (argc)
		usage();

	if (fn[0] == '\0') {
		if (mkdir(datadir, 0700) == -1)
			if (errno != EEXIST)
				err(1, "mkdir: %s", datadir);

		xmkfn(fn, "%s/%s", datadir, SL_FN_OPJOURNAL);
	}

	if (format) {
		if (!uuid)
			psc_fatalx("no fsuuid specified");
		newnents = sl_journal_format(fn, nents, SLJ_MDS_ENTSIZE,
		    SLJ_MDS_READSZ, uuid, block_dev);
		if (verbose || nents != newnents)
			warnx("created log file %s with %zu %d-byte entries "
			      "(uuid=%"PRIx64")",
			      fn, newnents, SLJ_MDS_ENTSIZE, uuid);
	} else if (query)
		sl_journal_dump(fn);
	else
		usage();
	exit(0);
}
