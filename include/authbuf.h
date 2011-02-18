/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2010, Pittsburgh Supercomputing Center (PSC).
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
 * authbuf - routines for managing, signing, and checking the signatures
 * messages sent between hosts in a SLASH network for integrity with a
 * secret key.
 */

#ifndef _SL_AUTHBUF_H_
#define _SL_AUTHBUF_H_

#include "psc_util/atomic.h"

struct stat;
struct pscrpc_request;

#define AUTHBUF_KEYSIZE	1024

int	authbuf_check(struct pscrpc_request *, int);
void	authbuf_sign(struct pscrpc_request *, int);

void	authbuf_checkkey(const char *, struct stat *);
void	authbuf_checkkeyfile(void);
void	authbuf_createkeyfile(void);
void	authbuf_readkeyfile(void);

extern psc_atomic64_t	authbuf_nonce;
extern unsigned char	authbuf_key[AUTHBUF_KEYSIZE];
extern int		authbuf_alglen;

#endif /* _SL_AUTHBUF_H_ */
