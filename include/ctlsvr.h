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

void slctlparam_uptime_get(char *);
void slctlparam_version_get(char *);
void slctlparam_logrotate_get(char *);
int  slctlparam_logrotate_set(const char *);

extern const struct slctl_res_field slctl_resmds_fields[];
extern const struct slctl_res_field slctl_resios_fields[];

#endif /* _SL_CTLSVR_H_ */
