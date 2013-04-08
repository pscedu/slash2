/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2011, Pittsburgh Supercomputing Center (PSC).
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
 * authbuf_mgt - routines for managing the secret key used to provide
 * in RPC messages sent between hosts in a SLASH network.
 */

#include <sys/types.h>
#include <sys/stat.h>

#include <fcntl.h>
#include <gcrypt.h>
#include <unistd.h>

#include "pfl/cdefs.h"
#include "pfl/stat.h"
#include "psc_util/atomic.h"
#include "psc_util/base64.h"
#include "psc_util/lock.h"
#include "psc_util/log.h"
#include "psc_util/random.h"

#include "authbuf.h"
#include "mkfn.h"
#include "pathnames.h"
#include "slashrpc.h"
#include "slerr.h"

psc_atomic64_t	authbuf_nonce = PSC_ATOMIC64_INIT(0);
unsigned char	authbuf_key[AUTHBUF_KEYSIZE];
gcry_md_hd_t	authbuf_hd;
int		authbuf_alglen;

/*
 * authbuf_readkeyfile - Read the contents of a secret key file into
 *	memory.
 */
void
authbuf_readkeyfile(void)
{
	char keyfn[PATH_MAX];
	gcry_error_t gerr;
	struct stat stb;
	int alg, fd;

	xmkfn(keyfn, "%s/%s", sl_datadir, SL_FN_AUTHBUFKEY);
	if ((fd = open(keyfn, O_RDONLY)) == -1)
		psc_fatal("open %s", keyfn);
	if (fstat(fd, &stb) == -1)
		psc_fatal("fstat %s", keyfn);
	authbuf_checkkey(keyfn, &stb);
	if (read(fd, authbuf_key, sizeof(authbuf_key)) !=
	    (ssize_t)sizeof(authbuf_key))
		psc_fatal("read %s", keyfn);
	close(fd);

	alg = GCRY_MD_SHA256;
	gerr = gcry_md_open(&authbuf_hd, alg, 0);
	if (gerr)
		psc_fatalx("gcry_md_open: %d", gerr);
	gcry_md_write(authbuf_hd, authbuf_key, sizeof(authbuf_key));

	/* base64 is len*4/3 + 1 for integer truncation + 1 for NUL byte */
	authbuf_alglen = gcry_md_get_algo_dlen(alg);
	if (authbuf_alglen * 4 / 3 + 2 >= AUTHBUF_REPRLEN)
		psc_fatal("bad alg/base64 size: alg=%d need=%d want=%d",
		    authbuf_alglen, authbuf_alglen * 4 / 3 + 2,
		    AUTHBUF_REPRLEN);

	psc_atomic64_set(&authbuf_nonce, psc_random64());
}

/*
 * authbuf_checkkeyfile - Perform sanity checks on the secret key file.
 */
void
authbuf_checkkeyfile(void)
{
	char keyfn[PATH_MAX];
	struct stat stb;

	xmkfn(keyfn, "%s/%s", sl_datadir, SL_FN_AUTHBUFKEY);
	if (stat(keyfn, &stb) == -1)
		psc_fatal("stat %s", keyfn);
	authbuf_checkkey(keyfn, &stb);
}

/*
 * authbuf_checkkey - Perform sanity checks on a secret key file.
 * @fn: the key file name for use in error messages.
 * @stb: stat(2) buffer used to perform checks.
 */
void
authbuf_checkkey(const char *fn, struct stat *stb)
{
	if (stb->st_size != sizeof(authbuf_key))
		psc_fatalx("key file %s is wrong size, should be %zu",
		    fn, sizeof(authbuf_key));
	if (!S_ISREG(stb->st_mode))
		psc_fatalx("key file %s: not a file", fn);
	if ((stb->st_mode & (ALLPERMS & ~S_IWUSR)) != S_IRUSR)
		psc_fatalx("key file %s has wrong permissions (0%o), "
		    "should be 0400", fn, stb->st_mode & ALLPERMS);
	if (stb->st_uid != 0)
		psc_fatalx("key file %s has wrong owner, should be root", fn);
}

/*
 * authbuf_createkey - Generate a secret key file.
 */
void
authbuf_createkeyfile(void)
{
	char keyfn[PATH_MAX];
	int i, j, fd;
	uint32_t r;

	xmkfn(keyfn, "%s/%s", sl_datadir, SL_FN_AUTHBUFKEY);
	if ((fd = open(keyfn, O_EXCL | O_WRONLY | O_CREAT, 0600)) == -1) {
		if (errno == EEXIST) {
			authbuf_checkkeyfile();
			return;
		}
		psc_fatal("open %s", keyfn);
	}
	for (i = 0; i < (int)sizeof(authbuf_key); ) {
		r = psc_random32();
		for (j = 0; j < 4 &&
		    i < (int)sizeof(authbuf_key); j++, i++)
			authbuf_key[i] = (r >> (8 * j)) & 0xff;
	}
	if (write(fd, authbuf_key, sizeof(authbuf_key)) !=
	    (ssize_t)sizeof(authbuf_key))
		psc_fatal("write %s", keyfn);
	close(fd);
}
