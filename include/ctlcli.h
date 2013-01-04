/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2010, Pittsburgh Supercomputing Center (PSC).
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

#ifndef _SL_CTLCLI_H_
#define _SL_CTLCLI_H_

void sl_conn_prhdr(struct psc_ctlmsghdr *, const void *);
void sl_conn_prdat(const struct psc_ctlmsghdr *, const void *);

void sl_fcmh_prhdr(struct psc_ctlmsghdr *, const void *);
void sl_fcmh_prdat(const struct psc_ctlmsghdr *, const void *);

void sl_bmap_prhdr(struct psc_ctlmsghdr *, const void *);
void sl_bmap_prdat(const struct psc_ctlmsghdr *, const void *);

#endif /* _SL_CTLCLI_H_ */
