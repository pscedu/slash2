/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2011, Pittsburgh Supercomputing Center (PSC).
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
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "pfl/fs.h"
#include "pfl/pfl.h"
#include "pfl/str.h"
#include "psc_util/journal.h"
#include "psc_util/log.h"

#include "mkfn.h"
#include "pathnames.h"
#include "slerr.h"
#include "sljournal.h"

#include "slashd/mdsio.h"
#include "slashd/mdslog.h"
#include "slashd/namespace.h"
#include "slashd/subsys_mds.h"

int		 format;
int		 query;
int		 verbose;
const char	*datadir = SL_PATH_DATA_DIR;
const char	*progname;
struct pscfs	 pscfs;
struct mdsio_ops mdsio_ops;

mdsio_fid_t	 mds_fidnsdir_inum;

__dead void
usage(void)
{
	fprintf(stderr,
	    "usage: %s [-fqv] [-b block-device] [-D dir] [-n nentries]\n",
	    progname);
	exit(1);
}

/**
 * pjournal_format - Initialize an on-disk journal.
 * @fn: file path to store journal.
 * @nents: number of entries journal may contain.
 * @entsz: size of a journal entry.
 * Returns 0 on success, errno on error.
 */
void
pjournal_format(const char *fn, uint32_t nents, uint32_t entsz,
    uint32_t rs)
{
	struct psc_journal_enthdr *pje;
	struct psc_journal_hdr pjh;
	struct psc_journal pj;
	struct stat stb;
	unsigned char *jbuf;
	uint32_t i, slot;
	int rc, fd;
	ssize_t nb;

	if (nents % rs)
		psc_fatalx("number of slots (%u) should be a multiple of "
		    "readsize (%u)", nents, rs);

	memset(&pj, 0, sizeof(struct psc_journal));
	pj.pj_hdr = &pjh;

	rc = 0;
	fd = open(fn, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (fd == -1)
		psc_fatal("%s", fn);

	if (fstat(fd, &stb) == -1)
		psc_fatal("stat %s", fn);

	pj.pj_fd = fd;

	pjh.pjh_entsz = entsz;
	pjh.pjh_nents = nents;
	pjh.pjh_version = PJH_VERSION;
	pjh.pjh_readsize = rs;
	pjh.pjh_iolen = PSC_ALIGN(sizeof(pjh), stb.st_blksize);
	pjh.pjh_magic = PJH_MAGIC;
#if 0
	pjh.pjh_timestamp = time(NULL);
#endif

	PSC_CRC64_INIT(&pjh.pjh_chksum);
	psc_crc64_add(&pjh.pjh_chksum, &pjh,
	    offsetof(struct psc_journal_hdr, pjh_chksum));
	PSC_CRC64_FIN(&pjh.pjh_chksum);

	nb = pwrite(pj.pj_fd, &pjh, pjh.pjh_iolen, 0);
	if ((size_t)nb != pjh.pjh_iolen)
		psc_fatal("failed to write journal header");

	jbuf = psc_alloc(PJ_PJESZ(&pj) * pj.pj_hdr->pjh_readsize,
	    PAF_PAGEALIGN | PAF_LOCK);
	for (i = 0; i < rs; i++) {
		pje = PSC_AGP(jbuf, PJ_PJESZ(&pj) * i);
		pje->pje_magic = PJE_MAGIC;
		pje->pje_type = PJE_FORMAT;
		pje->pje_xid = PJE_XID_NONE;
		pje->pje_len = 0;

		PSC_CRC64_INIT(&pje->pje_chksum);
		psc_crc64_add(&pje->pje_chksum, pje,
		    offsetof(struct psc_journal_enthdr, pje_chksum));
		psc_crc64_add(&pje->pje_chksum, pje->pje_data,
		    pje->pje_len);
		PSC_CRC64_FIN(&pje->pje_chksum);
	}

	for (slot = 0; slot < pjh.pjh_nents; slot += rs) {
		nb = pwrite(pj.pj_fd, jbuf, PJ_PJESZ(&pj) * rs,
		    PJ_GETENTOFF(&pj, slot));
		if ((size_t)nb != PJ_PJESZ(&pj) * rs)
			psc_fatal("failed to write slot %u", slot);
	}
	if (close(fd) == -1)
		psc_fatal("failed to close journal");
	psc_free(jbuf, PAF_LOCK | PAF_PAGEALIGN, PJ_PJESZ(&pj) * rs);
	psclog_info("journal %s formatted: %d slots, %d readsize, error=%d",
	    fn, nents, rs, rc);
}

void
pjournal_dump_entry(uint32_t slot, struct psc_journal_enthdr *pje)
{
	char name[SL_NAME_MAX + 1], newname[SL_NAME_MAX + 1];
	struct slmds_jent_assign_rep *logentry;
	struct slmds_jent_ino_addrepl *jrir;
	struct slmds_jent_bmap_assign *jrba;
	struct slmds_jent_namespace *sjnm;
	struct slmds_jent_bmapseq *sjsq;
	struct slmds_jent_repgen *jrpg;
	struct slmds_jent_crc *jcrc;
	int type;

	type = pje->pje_type & ~(_PJE_FLSHFT - 1);
	printf("%6d: ", slot);
	switch (type) {
	    case MDS_LOG_BMAP_REPL:
		jrpg = PJE_DATA(pje);
		printf("type=%3d, xid=%#"PRIx64", txg=%#"PRIx64", fid="SLPRI_FID,
			type, pje->pje_xid, pje->pje_txg, jrpg->sjp_fid);
		break;
	    case MDS_LOG_BMAP_CRC:
		jcrc = PJE_DATA(pje);
		printf("type=%3d, xid=%#"PRIx64", txg=%#"PRIx64", fid="SLPRI_FID,
			type, pje->pje_xid, pje->pje_txg, jcrc->sjc_fid);
		break;
	    case MDS_LOG_BMAP_SEQ:
		sjsq = PJE_DATA(pje);
		printf("type=%3d, xid=%#"PRIx64", txg=%#"PRIx64", "
		       "LWM=%#"PRIx64", HWM=%#"PRIx64,
			type, pje->pje_xid, pje->pje_txg,
			sjsq->sjbsq_low_wm,
			sjsq->sjbsq_high_wm);
		break;
	    case MDS_LOG_INO_ADDREPL:
		jrir = PJE_DATA(pje);
		printf("type=%3d, xid=%#"PRIx64", txg=%#"PRIx64", fid="SLPRI_FID,
			type, pje->pje_xid, pje->pje_txg, jrir->sjir_fid);
		break;
	    case MDS_LOG_BMAP_ASSIGN:
		logentry = PJE_DATA(pje);
		if (logentry->sjar_flags & SLJ_ASSIGN_REP_FREE)
			printf("type=%3d, xid=%#"PRIx64", "
			    "txg=%#"PRIx64", item=%d",
			    type, pje->pje_xid,
			    pje->pje_txg, logentry->sjar_elem);
		else {
			jrba = &logentry->sjar_bmap;
			printf("type=%3d, xid=%#"PRIx64", "
			    "txg=%#"PRIx64", fid="SLPRI_FID", flags=%x",
			    type, pje->pje_xid, pje->pje_txg,
			    jrba->sjba_fid, logentry->sjar_flags);
		}
		break;
	    case MDS_LOG_NAMESPACE:
		sjnm = PJE_DATA(pje);
		printf("type=%3d, xid=%#"PRIx64", txg=%#"PRIx64", "
		    "fid="SLPRI_FID", ",
		    type, pje->pje_xid, pje->pje_txg,
		    sjnm->sjnm_target_fid);

		name[0]='\0';
		if (sjnm->sjnm_namelen) {
			memcpy(name, sjnm->sjnm_name, sjnm->sjnm_namelen);
			name[sjnm->sjnm_namelen] = '\0';
		}
		newname[0]='\0';
		if (sjnm->sjnm_namelen2) {
			memcpy(newname, sjnm->sjnm_name +
			    sjnm->sjnm_namelen,
			    sjnm->sjnm_namelen2);
			newname[sjnm->sjnm_namelen2] = '\0';
		}

		switch (sjnm->sjnm_op) {
		    case NS_OP_RECLAIM:
			printf("op=reclaim");
			break;
		    case NS_OP_CREATE:
			printf("op=create, name=%s", name);
			break;
		    case NS_OP_MKDIR:
			printf("op=mkdir, name=%s", name);
			break;
		    case NS_OP_LINK:
			printf("op=link, name=%s", name);
			break;
		    case NS_OP_SYMLINK:
			printf("op=symlink, name=%s", name);
			break;
		    case NS_OP_RENAME:
			printf("op=rename, old name=%s, new name=%s",
			    name, newname);
			break;
		    case NS_OP_UNLINK:
			printf("op=unlink, name=%s", name);
			break;
		    case NS_OP_RMDIR:
			printf("op=rmdir, name=%s", name);
			break;
		    case NS_OP_SETSIZE:
			printf("op=setsize");
			break;
		    case NS_OP_SETATTR:
			printf("op=setattr, mask=%#x", sjnm->sjnm_mask);
			break;
		    default:
			psc_fatalx("invalid namespace op %d", sjnm->sjnm_op);
		}
		break;
	    default:
		psc_fatalx("invalid type %d", type);
		break;
	}
	printf("\n");
}

/**
 * pjournal_dump - Dump the contents of a journal file.
 * @fn: journal filename to query.
 * @verbose: whether to report stats summary or full dump.
 */
void
pjournal_dump(const char *fn, int verbose)
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
	pjh = psc_alloc(pjhlen, PAF_PAGEALIGN | PAF_LOCK);
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

	PSC_CRC64_INIT(&chksum);
	psc_crc64_add(&chksum, pjh, offsetof(struct psc_journal_hdr,
	    pjh_chksum));
	PSC_CRC64_FIN(&chksum);

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

	printf("Journal header info for %s:\n"
	    "  Version %u\n"
	    "  Entry size %u\n"
	    "  Number of entries %u\n"
	    "  Batch read size %u\n"
	    "  Entry start offset %"PRId64"\n\n",
	    fn, pjh->pjh_version, PJ_PJESZ(pj), pjh->pjh_nents,
	    pjh->pjh_readsize, pjh->pjh_start_off);

#if 0
	printf("This journal was created on %s",
	    ctime((time_t *)&pjh->pjh_timestamp));
#endif

	jbuf = psc_alloc(PJ_PJESZ(pj) * pj->pj_hdr->pjh_readsize,
			 PAF_PAGEALIGN | PAF_LOCK);
	for (slot = 0; slot < pjh->pjh_nents; slot += pjh->pjh_readsize) {

		nb = pread(pj->pj_fd, jbuf, PJ_PJESZ(pj) *
		    pjh->pjh_readsize, PJ_GETENTOFF(pj, slot));
		if (nb !=  PJ_PJESZ(pj) * pjh->pjh_readsize)
			printf("Failed to read %d log entries at slot %d.\n",
				pjh->pjh_readsize, slot);

		for (i = 0; i < pjh->pjh_readsize; i++) {
			ntotal++;
			pje = (void *)&jbuf[PJ_PJESZ(pj) * i];
			if (pje->pje_magic != PJE_MAGIC) {
				nmagic++;
				printf("Journal slot %d has a bad magic number.\n",
					slot + i);
				continue;
			}
			if (pje->pje_type == PJE_FORMAT) {
				nformat++;
				continue;
			}

			PSC_CRC64_INIT(&chksum);
			psc_crc64_add(&chksum, pje,
			    offsetof(struct psc_journal_enthdr, pje_chksum));
			psc_crc64_add(&chksum, pje->pje_data, pje->pje_len);
			PSC_CRC64_FIN(&chksum);

			if (pje->pje_chksum != chksum) {
				nchksum++;
				printf("Journal slot %d has a bad checksum",
					slot + i);
				continue;
			}
			if (verbose) {
				ndump++;
				pjournal_dump_entry(slot+i, pje);
			}
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
	if (close(pj->pj_fd) == -1)
		printf("failed closing journal %s", fn);

	psc_free(jbuf, PAF_LOCK | PAF_PAGEALIGN, PJ_PJESZ(pj));
	PSCFREE(pj);

	printf("\n%d slot(s) total, %d in use, %d formatted, "
	    "%d bad magic, %d bad checksum(s)\n",
	    ntotal, ndump, nformat, nmagic, nchksum);
	printf("\nLowest transaction ID=%#"PRIx64", slot=%d",
	    lowest_xid, lowest_slot);
	printf("\nHighest transaction ID=%#"PRIx64", slot=%d\n",
	    highest_xid, highest_slot);
}

int
main(int argc, char *argv[])
{
	ssize_t nents = SLJ_MDS_JNENTS;
	char *endp, c, fn[PATH_MAX];
	long l;

	pfl_init();
	sl_subsys_register();
	psc_subsys_register(SLMSS_ZFS, "zfs");
	psc_subsys_register(SLMSS_JOURNAL, "jrnl");

	fn[0] = '\0';
	progname = argv[0];
	while ((c = getopt(argc, argv, "b:D:fn:qv")) != -1)
		switch (c) {
		case 'b':
			strlcpy(fn, optarg, sizeof(fn));
			break;
		case 'D':
			datadir = optarg;
			break;
		case 'f':
			format = 1;
			break;
		case 'n':
			endp = NULL;
			l = strtol(optarg, &endp, 10);
			if (l <= 0 || l > INT_MAX ||
			    endp == optarg || *endp != '\0')
				errx(1, "invalid -n nentries: %s",
				    optarg);
			nents = (ssize_t)l;
			break;
		case 'q':
			query = 1;
			break;
		case 'v':
			verbose = 1;
			break;
		default:
			usage();
		}
	argc -= optind;
	argv += optind;
	if (argc)
		usage();

	if (!format && !query)
		usage();

	if (fn[0] == '\0') {
		if (mkdir(datadir, 0700) == -1)
			if (errno != EEXIST)
				err(1, "mkdir: %s", datadir);

		xmkfn(fn, "%s/%s", datadir, SL_FN_OPJOURNAL);
	}

	if (format) {
		pjournal_format(fn, nents, SLJ_MDS_ENTSIZE, SLJ_MDS_READSZ);
		if (verbose)
			warnx("created log file %s with %d %d-byte entries",
			    fn, SLJ_MDS_JNENTS, SLJ_MDS_ENTSIZE);
	}
	if (query)
		pjournal_dump(fn, verbose);
	exit(0);
}
