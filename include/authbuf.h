/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015, Google, Inc.
 * Copyright (c) 2008-2015, Pittsburgh Supercomputing Center (PSC).
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
 * authbuf - routines for managing, signing, and checking the signatures
 * messages sent between hosts in a SLASH2 deployment for integrity with a
 * secret key.
 */

#ifndef _SL_AUTHBUF_H_
#define _SL_AUTHBUF_H_

#include <gcrypt.h>

#include "pfl/atomic.h"

struct stat;
struct pscrpc_request;

#define AUTHBUF_ALGLEN		32
#define AUTHBUF_MINKEYSIZE	1024
#define AUTHBUF_MAXKEYSIZE	(128 * 1024)

int	authbuf_check(struct pscrpc_request *, int, int);
void	authbuf_sign(struct pscrpc_request *, int);

void	authbuf_checkkey(const char *, struct stat *);
void	authbuf_checkkeyfile(void);
void	authbuf_createkeyfile(void);
void	authbuf_readkeyfile(void);

extern psc_atomic64_t	sl_authbuf_nonce;
extern gcry_md_hd_t	sl_authbuf_hd;

#endif /* _SL_AUTHBUF_H_ */
