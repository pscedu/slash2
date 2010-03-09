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

/*
 * fdbuf - file descriptor buffer routines.
 *
 * File descriptor buffers (struct srt_fd_buf and srt_iofd_buf) are used
 * as global file descriptors which identify a FID as they are signed
 * by a shared private key.
 */

#include <sys/types.h>
#include <sys/stat.h>

#include <fcntl.h>
#include <gcrypt.h>

#include "pfl/cdefs.h"
#include "psc_util/atomic.h"
#include "psc_util/base64.h"
#include "psc_util/lock.h"
#include "psc_util/log.h"
#include "psc_util/random.h"

#include "fdbuf.h"
#include "mkfn.h"
#include "pathnames.h"
#include "slashrpc.h"

#define DESCBUF_KEYSIZE	1024
__static unsigned char	descbuf_key[DESCBUF_KEYSIZE];
__static gcry_md_hd_t	descbuf_hd;
__static int		descbuf_alglen;

/*
 * bdbuf_sign - Sign a bmapdesc buf with the private key.
 * @sbdb: the descriptor to sign.
 * @fgp: the file ID and generation.
 * @cli_prid: client address to prevent spoofing.
 * @ion_nid: ION address to prevent spoofing.
 * @ios_id: I/O system slash.conf ID.
 * @bmapno: bmap index number in file.
 */
void
bdbuf_sign(struct srt_bmapdesc_buf *sbdb, const struct slash_fidgen *fgp,
    const lnet_process_id_t *cli_prid, lnet_nid_t ion_nid,
    sl_ios_id_t ios_id, sl_blkno_t bmapno)
{
	static psc_atomic64_t nonce = PSC_ATOMIC64_INIT(0);
	gcry_error_t gerr;
	gcry_md_hd_t hd;

	sbdb->sbdb_secret.sbs_fg = *fgp;
	sbdb->sbdb_secret.sbs_cli_nid = cli_prid->nid;
	sbdb->sbdb_secret.sbs_cli_pid = cli_prid->pid;
	sbdb->sbdb_secret.sbs_ion_nid = ion_nid;
	sbdb->sbdb_secret.sbs_ios_id = ios_id;
	sbdb->sbdb_secret.sbs_bmapno = bmapno;
	sbdb->sbdb_secret.sbs_magic = SBDB_MAGIC;
	sbdb->sbdb_secret.sbs_nonce = psc_atomic64_inc_getnew(&nonce);

	gerr = gcry_md_copy(&hd, descbuf_hd);
	if (gerr)
		psc_fatalx("gcry_md_copy: %d", gerr);
	gcry_md_write(hd, &sbdb->sbdb_secret,
	    sizeof(sbdb->sbdb_secret));
	psc_base64_encode(gcry_md_read(hd, 0),
	    sbdb->sbdb_hash, descbuf_alglen);
	gcry_md_close(hd);
}

/*
 * bdbuf_check - Check signature validity of a bmapdesc buf.
 * @sbdb: the descriptor to check.
 * @fgp: value-result file ID and generation, after validation.
 * @bmapnop: value-result bmap index number in file.
 * @cli_prid: client address to prevent spoofing.
 * @ion_nid: ION address to prevent spoofing.
 * @ios_id: I/O system slash.conf ID to prevent spoofing.
 * @rw: SL_READ or SL_WRITE, indicating type of access for this bmapdesc.
 */
int
bdbuf_check(const struct srt_bmapdesc_buf *sbdb,
    struct slash_fidgen *fgp, sl_blkno_t *bmapnop,
    const lnet_process_id_t *cli_prid, lnet_nid_t ion_nid,
    sl_ios_id_t ios_id, enum rw rw)
{
	char buf[DESCBUF_REPRLEN];
	gcry_error_t gerr;
	gcry_md_hd_t hd;

	if (sbdb->sbdb_secret.sbs_magic != SBDB_MAGIC)
		return (EBADF);
	if (sbdb->sbdb_secret.sbs_cli_nid != cli_prid->nid ||
	    sbdb->sbdb_secret.sbs_cli_pid != cli_prid->pid)
		return (EBADF);
	if (rw == SL_READ) {
		/* Read requests can get by with looser authentication. */
		if ((sbdb->sbdb_secret.sbs_ion_nid != ion_nid) &&
		    (sbdb->sbdb_secret.sbs_ion_nid != LNET_NID_ANY))
			return (EBADF);
		if ((sbdb->sbdb_secret.sbs_ios_id != ios_id) &&
		    (sbdb->sbdb_secret.sbs_ios_id != IOS_ID_ANY))
			return (EBADF);
	} else if (rw == SL_WRITE) {
		if (sbdb->sbdb_secret.sbs_ion_nid != ion_nid)
			return (EBADF);
		if (sbdb->sbdb_secret.sbs_ios_id != ios_id)
			return (EBADF);
	} else {
		psc_errorx("bdbuf_check passed invalid rw mode: %d", rw);
		return (EBADF);
	}

	gerr = gcry_md_copy(&hd, descbuf_hd);
	if (gerr)
		psc_fatalx("gcry_md_copy: %d", gerr);
	gcry_md_write(hd, &sbdb->sbdb_secret,
	    sizeof(sbdb->sbdb_secret));
	psc_base64_encode(gcry_md_read(hd, 0),
	    buf, descbuf_alglen);
	gcry_md_close(hd);
	if (strcmp(buf, sbdb->sbdb_hash))
		return (EBADF);

	*fgp = sbdb->sbdb_secret.sbs_fg;
	*bmapnop = sbdb->sbdb_secret.sbs_bmapno;
	return (0);
}

/*
 * fdbuf_readkeyfile - Read the contents of the key file into memory.
 */
void
fdbuf_readkeyfile(void)
{
	char keyfn[PATH_MAX];
	gcry_error_t gerr;
	struct stat stb;
	int alg, fd;

	xmkfn(keyfn, "%s/%s", sl_datadir, SL_FN_DESCBUFKEY);
	if ((fd = open(keyfn, O_RDONLY)) == -1)
		psc_fatal("open %s", keyfn);
	if (fstat(fd, &stb) == -1)
		psc_fatal("fstat %s", keyfn);
	fdbuf_checkkey(keyfn, &stb);
	if (read(fd, descbuf_key, sizeof(descbuf_key)) !=
	    (ssize_t)sizeof(descbuf_key))
		psc_fatal("read %s", keyfn);
	close(fd);

	alg = GCRY_MD_SHA256;
	gerr = gcry_md_open(&descbuf_hd, alg, 0);
	if (gerr)
		psc_fatalx("gcry_md_open: %d", gerr);
	gcry_md_write(descbuf_hd, descbuf_key, sizeof(descbuf_key));

	/* base64 is len*4/3 + 1 for integer truncation + 1 for NUL byte */
	descbuf_alglen = gcry_md_get_algo_dlen(alg);
	if (descbuf_alglen * 4 / 3 + 2 >= DESCBUF_REPRLEN)
		psc_fatal("bad alg/base64 size: alg=%d need=%d want=%d",
		    descbuf_alglen, descbuf_alglen * 4 / 3 + 2,
		    DESCBUF_REPRLEN);
}

/*
 * fdbuf_checkkeyfile - Perform sanity checks on the key file.
 */
void
fdbuf_checkkeyfile(void)
{
	char keyfn[PATH_MAX];
	struct stat stb;

	xmkfn(keyfn, "%s/%s", sl_datadir, SL_FN_DESCBUFKEY);
	if (stat(keyfn, &stb) == -1)
		psc_fatal("stat %s", keyfn);
	fdbuf_checkkey(keyfn, &stb);
}

/*
 * fdbuf_checkkey - Perform sanity checks on a key file.
 * @fn: the key file name for use in error messages.
 * @stb: stat(2) buffer used to perform checks.
 */
void
fdbuf_checkkey(const char *fn, struct stat *stb)
{
	if (stb->st_size != sizeof(descbuf_key))
		psc_fatalx("key file %s is wrong size, should be %zu",
		    fn, sizeof(descbuf_key));
	if (!S_ISREG(stb->st_mode))
		psc_fatalx("key file %s: not a file", fn);
	if ((stb->st_mode & (S_IRWXU | S_IRWXG | S_IRWXO)) != (S_IRUSR | S_IWUSR))
		psc_fatalx("key file %s has wrong permissions (0%o), "
		    "should be 0600", fn, stb->st_mode);
	if (stb->st_uid != 0)
		psc_fatalx("key file %s has wrong owner, should be root", fn);
}

/*
 * fdbuf_createkey - Generate the key file.
 */
void
fdbuf_createkeyfile(void)
{
	char keyfn[PATH_MAX];
	int i, j, fd;
	uint32_t r;

	xmkfn(keyfn, "%s/%s", sl_datadir, SL_FN_DESCBUFKEY);
	if ((fd = open(keyfn, O_EXCL | O_WRONLY | O_CREAT, 0600)) == -1) {
		if (errno == EEXIST) {
			fdbuf_checkkeyfile();
			return;
		}
		psc_fatal("open %s", keyfn);
	}
	for (i = 0; i < (int)sizeof(descbuf_key); ) {
		r = psc_random32();
		for (j = 0; j < 4 &&
		    i < (int)sizeof(descbuf_key); j++, i++)
			descbuf_key[i] = (r >> (8 * j)) & 0xff;
	}
	if (write(fd, descbuf_key, sizeof(descbuf_key)) !=
	    (ssize_t)sizeof(descbuf_key))
		psc_fatal("write %s", keyfn);
	close(fd);
}
