/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015, Google, Inc.
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
 * authbuf_mgt - routines for managing the secret key used to perform
 * cryptographic digital signatures on RPC messages contents sent
 * between nodes in a SLASH2 deployment.
 *
 * Note that the protocol used is guaranteed secure and shouldn't be
 * relied on as such; it is primarily for memory sanity checking.  If
 * real security is desired, use SSL.
 */

#include <sys/types.h>
#include <sys/stat.h>

#include <fcntl.h>
#include <gcrypt.h>
#include <unistd.h>

#include "pfl/alloc.h"
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

psc_atomic64_t	 sl_authbuf_nonce = PSC_ATOMIC64_INIT(0);
unsigned char	*sl_authbuf_key;
ssize_t		 sl_authbuf_keysize;
gcry_md_hd_t	 sl_authbuf_hd;

/*
 * Read the contents of a secret key file into memory.
 */
void
authbuf_readkeyfile(void)
{
	char keyfn[PATH_MAX];
	gcry_error_t gerr;
	struct stat stb;
	int alg, fd;
	ssize_t rc;

	psc_assert(gcry_md_get_algo_dlen(GCRY_MD_SHA256) ==
	    AUTHBUF_ALGLEN);

	xmkfn(keyfn, "%s/%s", sl_datadir, SL_FN_AUTHBUFKEY);
	fd = open(keyfn, O_RDONLY);
	if (fd == -1)
		psc_fatal("open %s", keyfn);
	if (fstat(fd, &stb) == -1)
		psc_fatal("fstat %s", keyfn);
	authbuf_checkkey(keyfn, &stb);
	sl_authbuf_key = PSCALLOC(sl_authbuf_keysize);
	rc = read(fd, sl_authbuf_key, sl_authbuf_keysize);
	if (rc != sl_authbuf_keysize)
		psc_fatal("read %s; wanted=%zd got=%zd", keyfn,
		    sl_authbuf_keysize, rc);
	close(fd);

	alg = GCRY_MD_SHA256;
	gerr = gcry_md_open(&sl_authbuf_hd, alg, 0);
	if (gerr)
		psc_fatalx("gcry_md_open: %d", gerr);
	gcry_md_write(sl_authbuf_hd, sl_authbuf_key,
	    sl_authbuf_keysize);

	PSCFREE(sl_authbuf_key);

	psc_atomic64_set(&sl_authbuf_nonce, psc_random64());
}

/*
 * Perform sanity checks on the secret key file.
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
 * Perform sanity checks on a secret key file.
 * @fn: the key file name for use in error messages.
 * @stb: stat(2) buffer used to perform checks.
 */
void
authbuf_checkkey(const char *fn, struct stat *stb)
{
	if (stb->st_size < AUTHBUF_MINKEYSIZE)
		psc_fatalx("key file %s is too small; "
		    "should be at least %d bytes",
		    fn, AUTHBUF_MINKEYSIZE);
	if (stb->st_size > AUTHBUF_MAXKEYSIZE)
		psc_fatalx("key file %s is too big; "
		    "should be at least %d bytes",
		    fn, AUTHBUF_MAXKEYSIZE);
	sl_authbuf_keysize = stb->st_size;

	if (!S_ISREG(stb->st_mode))
		psc_fatalx("key file %s: not a file", fn);
#if 0
	if ((dstb->st_mode & (ALLPERMS & ~S_IWUSR)) != S_IRUSR)
		psc_fatalx("data directory %s has wrong permissions "
		    "(0%o), should be 0400", dirfn, dstb->st_mode &
		    ALLPERMS);
#endif
	if ((stb->st_mode & (ALLPERMS & ~S_IWUSR)) != S_IRUSR)
		psc_fatalx("key file %s has wrong permissions (0%o), "
		    "should be 0400", fn, stb->st_mode & ALLPERMS);
#if 0
	/*
 	 * All a regular user to start mount slash2 using setuid trick.
 	 */
	if (stb->st_uid != 0)
		psc_fatalx("key file %s has wrong owner, should be "
		    "root", fn);
#endif
}

/*
 * Generate a secret key file.
 */
void
authbuf_createkeyfile(void)
{
	char keyfn[PATH_MAX];
	int fd;

	xmkfn(keyfn, "%s/%s", sl_datadir, SL_FN_AUTHBUFKEY);
	if ((fd = open(keyfn, O_EXCL | O_WRONLY | O_CREAT, 0600)) == -1) {
		if (errno == EEXIST) {
			authbuf_checkkeyfile();
			return;
		}
		psc_fatal("open %s", keyfn);
	}
	sl_authbuf_keysize = AUTHBUF_MINKEYSIZE	+
	    psc_random32u(AUTHBUF_MAXKEYSIZE - AUTHBUF_MINKEYSIZE + 1);
	sl_authbuf_key = PSCALLOC(sl_authbuf_keysize);
	pfl_random_getbytes(sl_authbuf_key, sl_authbuf_keysize);
	if (write(fd, sl_authbuf_key, sl_authbuf_keysize) !=
	    sl_authbuf_keysize)
		psc_fatal("write %s", keyfn);
	PSCFREE(sl_authbuf_key);
	close(fd);
}
