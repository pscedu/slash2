/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2013, Pittsburgh Supercomputing Center (PSC).
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

void slctlparam_version_get(char *);

extern const struct slctl_res_field slctl_resmds_fields[];
extern const struct slctl_res_field slctl_resios_fields[];

#endif /* _SL_CTLSVR_H_ */
