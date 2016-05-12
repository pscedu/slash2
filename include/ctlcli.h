/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015, Google, Inc.
 * Copyright (c) 2009-2013, Pittsburgh Supercomputing Center (PSC).
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

#ifndef _SL_CTLCLI_H_
#define _SL_CTLCLI_H_

int  sl_conn_prhdr(struct psc_ctlmsghdr *, const void *);
void sl_conn_prdat(const struct psc_ctlmsghdr *, const void *);

int  sl_fcmh_prhdr(struct psc_ctlmsghdr *, const void *);
void sl_fcmh_prdat(const struct psc_ctlmsghdr *, const void *);

int  sl_bmap_prhdr(struct psc_ctlmsghdr *, const void *);
void sl_bmap_prdat(const struct psc_ctlmsghdr *, const void *);

#endif /* _SL_CTLCLI_H_ */
