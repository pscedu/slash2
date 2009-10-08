/* $Id$ */

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

#include "psc_util/atomic.h"
#include "psc_util/base64.h"
#include "psc_util/cdefs.h"
#include "psc_util/lock.h"
#include "psc_util/log.h"
#include "psc_util/random.h"

#include "fdbuf.h"
#include "slashrpc.h"

#define DESCBUF_KEYSIZE	1024
__static unsigned char	descbuf_key[DESCBUF_KEYSIZE];
__static gcry_md_hd_t	descbuf_hd;

/*
 * bdbuf_sign - Sign a bmapdesc buf with the private key.
 * @sbdb: the descriptor to sign; cfd should be filled in.
 * @fgp: the file ID and generation.
 * @cli_prid: client address to prevent spoofing.
 * @ion_nid: ION address to prevent spoofing.
 * @ios_id: I/O system slash.conf ID.
 * @bmapno: bmap index number in file.
 */
void
bdbuf_sign(struct srt_bmapdesc_buf *sbdb,
    const struct slash_fidgen *fgp, lnet_process_id_t cli_prid,
    lnet_nid_t ion_nid, sl_ios_id_t ios_id, sl_blkno_t bmapno)
{
	static psc_atomic64_t nonce = PSC_ATOMIC64_INIT(0);
	unsigned char *buf;
	gcry_error_t gerr;
	gcry_md_hd_t hd;

	sbdb->sbdb_secret.sbs_fg = *fgp;
	sbdb->sbdb_secret.sbs_cli_prid = cli_prid;
	sbdb->sbdb_secret.sbs_ion_nid = ion_nid;
	sbdb->sbdb_secret.sbs_ios_id = ios_id;
	sbdb->sbdb_secret.sbs_bmapno = bmapno;
	sbdb->sbdb_secret.sbs_magic = SBDB_MAGIC;
	sbdb->sbdb_secret.sbs_nonce = psc_atomic64_inc_return(&nonce);

	gerr = gcry_md_copy(&hd, descbuf_hd);
	if (gerr)
		psc_fatalx("gcry_md_copy: %d", gerr);
	gcry_md_write(hd, &sbdb->sbdb_secret,
	    sizeof(sbdb->sbdb_secret));
	buf = gcry_md_read(hd, 0);
	psc_base64_encode(buf, sbdb->sbdb_hash, DESCBUF_REPRLEN);
	gcry_md_close(hd);
}

/*
 * bdbuf_check - Check signature validity of a bmapdesc buf.
 * @sbdb: the descriptor to check.
 * @cfdp: value-result client file descriptor.
 * @fgp: value-result file ID and generation, after validation.
 * @bmapnop: value-result bmap index number in file.
 * @cli_prid: client address to prevent spoofing.
 * @ion_nid: ION address to prevent spoofing.
 * @ios_id: I/O system slash.conf ID to prevent spoofing.
 */
int
bdbuf_check(struct srt_bmapdesc_buf *sbdb, uint64_t *cfdp,
    struct slash_fidgen *fgp, sl_blkno_t *bmapnop,
    lnet_process_id_t cli_prid, lnet_nid_t ion_nid, sl_ios_id_t ios_id)
{
	char tibuf[PSC_NIDSTR_SIZE], tcbuf[PSC_NIDSTR_SIZE],
	     sibuf[PSC_NIDSTR_SIZE], scbuf[PSC_NIDSTR_SIZE],
	     buf[DESCBUF_REPRLEN];
	gcry_error_t gerr;
	gcry_md_hd_t hd;
	int rc;

	rc = 0;
	if (sbdb->sbdb_secret.sbs_magic != SBDB_MAGIC) {
		rc = EINVAL;
		goto out;
	}
	if (memcmp(&sbdb->sbdb_secret.sbs_cli_prid,
	    &cli_prid, sizeof(cli_prid))) {
		rc = EPERM;
		goto out;
	}
	if (sbdb->sbdb_secret.sbs_ion_nid != ion_nid) {
		rc = EPERM;
		goto out;
	}
	if (sbdb->sbdb_secret.sbs_ios_id != ios_id) {
		rc = EPERM;
		goto out;
	}

	gerr = gcry_md_copy(&hd, descbuf_hd);
	if (gerr)
		psc_fatalx("gcry_md_copy: %d", gerr);
	gcry_md_write(hd, &sbdb->sbdb_secret,
	    sizeof(sbdb->sbdb_secret));
	psc_base64_encode(gcry_md_read(hd, 0), buf, DESCBUF_REPRLEN);
	if (strcmp(buf, sbdb->sbdb_hash))
		rc = EBADF;
	gcry_md_close(hd);

	if (rc)
		goto out;

	if (fgp)
		*fgp = sbdb->sbdb_secret.sbs_fg;
	if (cfdp)
		*cfdp = sbdb->sbdb_secret.sbs_cfd;
	if (bmapnop)
		*bmapnop = sbdb->sbdb_secret.sbs_bmapno;

 out:
	if (rc)
		psc_errorx("bdbuf_check(cli=%s:%s,ion=%s:%s,ios=%d:%d): %s",
		    psc_id2str(sbdb->sbdb_secret.sbs_cli_prid, scbuf),
		    psc_id2str(cli_prid, tcbuf),
		    psc_nid2str(sbdb->sbdb_secret.sbs_ion_nid, sibuf),
		    psc_nid2str(ion_nid, tibuf), sbdb->sbdb_secret.sbs_ios_id,
		    ios_id, strerror(rc));
	return (rc);
}

/*
 * fdbuf_sign - Sign an fdbuf with the private key.
 * @sfdb: the srt_fd_buf to sign, cfd should be filled in.
 * @fgp: the file ID and generation.
 * @cli_prid: peer address to prevent spoofing.
 */
void
fdbuf_sign(struct srt_fd_buf *sfdb, const struct slash_fidgen *fgp,
    lnet_process_id_t cli_prid)
{
	static psc_atomic64_t nonce = PSC_ATOMIC64_INIT(0);
	unsigned char *buf;
	gcry_error_t gerr;
	gcry_md_hd_t hd;

	sfdb->sfdb_secret.sfs_fg = *fgp;
	sfdb->sfdb_secret.sfs_cli_prid = cli_prid;
	sfdb->sfdb_secret.sfs_magic = SFDB_MAGIC;
	sfdb->sfdb_secret.sfs_nonce = psc_atomic64_inc_return(&nonce);

	gerr = gcry_md_copy(&hd, descbuf_hd);
	if (gerr)
		psc_fatalx("gcry_md_copy: %d", gerr);
	gcry_md_write(hd, &sfdb->sfdb_secret,
	    sizeof(sfdb->sfdb_secret));
	buf = gcry_md_read(hd, 0);
	psc_base64_encode(buf, sfdb->sfdb_hash, DESCBUF_REPRLEN);
	gcry_md_close(hd);
}

/*
 * fdbuf_check - Check signature validity of an fdbuf.
 * @sfdb: the srt_fd_buf to check.
 * @cfdp: value-result client file descriptor.
 * @fgp: value-result file ID and generation, after validation.
 * @cli_prid: peer address to prevent spoofing.
 */
int
fdbuf_check(struct srt_fd_buf *sfdb, uint64_t *cfdp,
    struct slash_fidgen *fgp, lnet_process_id_t cli_prid)
{
	char buf[DESCBUF_REPRLEN];
	gcry_error_t gerr;
	gcry_md_hd_t hd;
	int rc;

	rc = 0;
	if (sfdb->sfdb_secret.sfs_magic != SFDB_MAGIC)
		return (EINVAL);
	if (memcmp(&sfdb->sfdb_secret.sfs_cli_prid,
	    &cli_prid, sizeof(cli_prid)))
		return (EPERM);

	gerr = gcry_md_copy(&hd, descbuf_hd);
	if (gerr)
		psc_fatalx("gcry_md_copy: %d", gerr);
	gcry_md_write(hd, &sfdb->sfdb_secret,
	    sizeof(sfdb->sfdb_secret));
	psc_base64_encode(gcry_md_read(hd, 0), buf, DESCBUF_REPRLEN);
	if (strcmp(buf, sfdb->sfdb_hash))
		rc = EBADF;
	gcry_md_close(hd);

	if (rc)
		return (rc);

	if (fgp)
		*fgp = sfdb->sfdb_secret.sfs_fg;
	if (cfdp)
		*cfdp = sfdb->sfdb_secret.sfs_cfd;
	return (0);
}

/*
 * fdbuf_readkeyfile - Read the contents of the key file into memory.
 */
void
fdbuf_readkeyfile(void)
{
	gcry_error_t gerr;
	const char *keyfn;
	struct stat stb;
	int alg, fd;

	keyfn = globalConfig.gconf_fdbkeyfn;
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
	if (gcry_md_get_algo_dlen(alg) * 4 / 3 + 2 >= DESCBUF_REPRLEN)
		psc_fatal("bad alg/base64 size: alg=%d need=%d want=%d",
		    gcry_md_get_algo_dlen(alg),
		    gcry_md_get_algo_dlen(alg) * 4 / 3 + 2,
		    DESCBUF_REPRLEN);
}

/*
 * fdbuf_checkkeyfile - Perform sanity checks on the key file.
 */
void
fdbuf_checkkeyfile(void)
{
	const char *keyfn;
	struct stat stb;

	keyfn = globalConfig.gconf_fdbkeyfn;
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
	const char *keyfn;
	int i, j, fd;
	u32 r;

	keyfn = globalConfig.gconf_fdbkeyfn;
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
