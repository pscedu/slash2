/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright (c) 2009-2015, Pittsburgh Supercomputing Center (PSC).
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

/*
 * authbuf_mgt - routines for managing the secret key used to provide
 * in RPC messages sent between hosts in a SLASH2 deployment.
 */

#include <sys/types.h>
#include <sys/stat.h>

#include <fcntl.h>
#include <gcrypt.h>
#include <unistd.h>

#include "pfl/atomic.h"
#include "pfl/cdefs.h"
#include "pfl/lock.h"
#include "pfl/log.h"
#include "pfl/random.h"
#include "pfl/stat.h"

#include "authbuf.h"
#include "mkfn.h"
#include "pathnames.h"
#include "slashrpc.h"
#include "slerr.h"

psc_atomic64_t	sl_authbuf_nonce = PSC_ATOMIC64_INIT(0);
unsigned char	sl_authbuf_key[AUTHBUF_KEYSIZE];
gcry_md_hd_t	sl_authbuf_hd;

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

	psc_assert(gcry_md_get_algo_dlen(GCRY_MD_SHA256) ==
	    AUTHBUF_ALGLEN);

	xmkfn(keyfn, "%s/%s", sl_datadir, SL_FN_AUTHBUFKEY);
	if ((fd = open(keyfn, O_RDONLY)) == -1)
		psc_fatal("open %s", keyfn);
	if (fstat(fd, &stb) == -1)
		psc_fatal("fstat %s", keyfn);
	authbuf_checkkey(keyfn, &stb);
	if (read(fd, sl_authbuf_key, sizeof(sl_authbuf_key)) !=
	    (ssize_t)sizeof(sl_authbuf_key))
		psc_fatal("read %s", keyfn);
	close(fd);

	alg = GCRY_MD_SHA256;
	gerr = gcry_md_open(&sl_authbuf_hd, alg, 0);
	if (gerr)
		psc_fatalx("gcry_md_open: %d", gerr);
	gcry_md_write(sl_authbuf_hd, sl_authbuf_key,
	    sizeof(sl_authbuf_key));

	psc_atomic64_set(&sl_authbuf_nonce, psc_random64());
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
	if (stb->st_size != sizeof(sl_authbuf_key))
		psc_fatalx("key file %s is wrong size, should be %zu",
		    fn, sizeof(sl_authbuf_key));
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
	for (i = 0; i < (int)sizeof(sl_authbuf_key); ) {
		r = psc_random32();
		for (j = 0; j < 4 &&
		    i < (int)sizeof(sl_authbuf_key); j++, i++)
			sl_authbuf_key[i] = (r >> (8 * j)) & 0xff;
	}
	if (write(fd, sl_authbuf_key, sizeof(sl_authbuf_key)) !=
	    (ssize_t)sizeof(sl_authbuf_key))
		psc_fatal("write %s", keyfn);
	close(fd);
}
