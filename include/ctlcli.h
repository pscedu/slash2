/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2013, Pittsburgh Supercomputing Center (PSC).
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
 *
 * Pittsburgh Supercomputing Center	phone: 412.268.4960  fax: 412.268.5832
 * 300 S. Craig Street			e-mail: remarks@psc.edu
 * Pittsburgh, PA 15213			web: http://www.psc.edu/
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
