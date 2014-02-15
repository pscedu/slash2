/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2014, Pittsburgh Supercomputing Center (PSC).
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

#ifndef _SL_CTLSVR_H_
#define _SL_CTLSVR_H_

struct bmap;
struct psc_ctlparam_node;

struct slctl_res_field {
	const char	 *name;
	int		(*cbf)(int, struct psc_ctlmsghdr *,
			    struct psc_ctlmsg_param *, char **, int,
			    int, struct sl_resource *);
};

int slctlrep_getconn(int, struct psc_ctlmsghdr *, void *);
int slctlrep_getfcmh(int, struct psc_ctlmsghdr *, void *);
int slctlrep_getbmap(int, struct psc_ctlmsghdr *, void *);
int slctlmsg_bmap_send(int, struct psc_ctlmsghdr *, struct
    slctlmsg_bmap *, struct bmap *);

int slctlparam_resources(int, struct psc_ctlmsghdr *,
    struct psc_ctlmsg_param *, char **, int,
    struct psc_ctlparam_node *);

void slctlparam_nbrq_outstanding_get(char *);
void slctlparam_uptime_get(char *);
void slctlparam_version_get(char *);

extern const struct slctl_res_field slctl_resmds_fields[];
extern const struct slctl_res_field slctl_resios_fields[];

#endif /* _SL_CTLSVR_H_ */
