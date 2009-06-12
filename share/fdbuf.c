/* $Id$ */

/*
 * fdbuf - file descriptor buffer routines.
 *
 * File descriptor buffers (struct srt_fd_buf and srt_iofd_buf) are used
 * as global file descriptors which identify a FID as they are encrypted
 * by a shared crypto key.
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

__static unsigned char	 fdbuf_key[sizeof(struct srt_fd_buf)];

/*
 * fdbuf_encrypt - Encrypt an fdbuf with the shared key.
 * @sfdb: the srt_fd_buf to encrypt, cfd should be filled in.
 * @fg: the file ID and generation.
 */
void
fdbuf_encrypt(struct srt_fd_buf *sfdb, struct slash_fidgen *fg)
{
	static psc_atomic64_t nonce = PSC_ATOMIC64_INIT(0);
	unsigned char *buf;
	gcry_error_t gerr;
	gcry_md_hd_t hd;
	int alg;

	sfdb->sfdb_secret.sfs_fg = *fg;
	sfdb->sfdb_secret.sfs_magic = SFDB_MAGIC;
	sfdb->sfdb_secret.sfs_nonce = psc_atomic64_inc_return(&nonce);

	alg = GCRY_MD_SHA256;
	gerr = gcry_md_open(&hd, alg, 0);
	if (gerr)
		psc_fatalx("gcry_md_open: %d", gerr);
	gcry_md_write(hd, &sfdb->sfdb_secret,
	    sizeof(sfdb->sfdb_secret));
	gcry_md_write(hd, fdbuf_key, sizeof(fdbuf_key));
	buf = gcry_md_read(hd, 0);
	/* base64 is 4/3 + 1 (for truncation), then 1 for NUL byte */
	if (gcry_md_get_algo_dlen(alg) * 4 / 3 + 2 >=
	    sizeof(sfdb->sfdb_hash))
		psc_fatal("bad base64 size: %d %d %zd",
		    gcry_md_get_algo_dlen(alg),
		    gcry_md_get_algo_dlen(alg) * 4 / 3 + 2,
		    sizeof(sfdb->sfdb_hash));
	psc_base64_encode(buf, sfdb->sfdb_hash,
	    gcry_md_get_algo_dlen(alg));
	gcry_md_close(hd);
}

/*
 * fdbuf_decrypt - Decrypt an fdbuf with the shared key.
 * @sfdb: the srt_fd_buf to decrypt.
 * @cfdp: value-result client file descriptor.
 * @fgp: value-result file ID and generation, after decryption.
 */
int
fdbuf_decrypt(struct srt_fd_buf *sfdb, uint64_t *cfdp,
    struct slash_fidgen *fgp)
{
	if (sfdb->sfdb_secret.sfs_magic != SFDB_MAGIC)
		return (-1);
//	if (sfdb->client != client)
//		return (-1);
//	if (hash(secret) != sfdb->hash)
//		return (-1);
	*fgp = sfdb->sfdb_secret.sfs_fg;
	if (cfdp)
		*cfdp = sfdb->sfdb_secret.sfs_cfd;
	/* XXX do the cfd lookup for the caller */
	return (0);
}

/*
 * fdbuf_readkeyfile - Read the contents of the key file into memory.
 */
void
fdbuf_readkeyfile(void)
{
	const char *keyfn;
	struct stat stb;
	size_t siz;
	int fd;

	siz = sizeof(struct srt_fdb_secret);
	keyfn = globalConfig.gconf_fdbkeyfn;
	if ((fd = open(keyfn, O_RDONLY)) == -1)
		psc_fatal("open %s", keyfn);
	if (fstat(fd, &stb) == -1)
		psc_fatal("fstat %s", keyfn);
	fdbuf_checkkey(keyfn, &stb);
	if (read(fd, fdbuf_key, siz) != (ssize_t)siz)
		psc_fatal("read %s", keyfn);
	close(fd);
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
	if (stb->st_size != sizeof(struct srt_fdb_secret))
		psc_fatalx("key file %s is wrong size, should be %zu",
		    fn, sizeof(struct srt_fdb_secret));
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
	unsigned char secret[sizeof(struct srt_fdb_secret)];
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
	for (i = 0; i < (int)sizeof(struct srt_fdb_secret); ) {
		r = psc_random32();
		for (j = 0; j < 4 &&
		    i < (int)sizeof(struct srt_fdb_secret); j++, i++)
			secret[i] = (r >> (8 * j)) & 255;
	}
	if (write(fd, secret, sizeof(secret)) != (ssize_t)sizeof(secret))
		psc_fatal("write %s", keyfn);
	close(fd);
}
