/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2012, Pittsburgh Supercomputing Center (PSC).
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

#ifndef _MDSCOH_H_
#define _MDSCOH_H_

struct pscrpc_async_args;
struct pscrpc_request;
struct bmap_mds_lease;

int	mdscoh_req(struct bmap_mds_lease *);
int	mdscoh_cb(struct pscrpc_request *, struct pscrpc_async_args *);
void	slmcohthr_spawn(void);

#endif /* _MDSCOH_H_ */
