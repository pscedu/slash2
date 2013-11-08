/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2007-2013, Pittsburgh Supercomputing Center (PSC).
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

#ifndef _FIDC_CLI_H_
#define _FIDC_CLI_H_

#include "pfl/list.h"
#include "pfl/lock.h"

#include "sltypes.h"
#include "fidcache.h"
#include "dircache.h"

struct pscfs_clientctx;

struct fidc_membh;

struct fci_finfo {
	int			 nrepls:16;
	int			 newreplpol:16;
	int			 ino_flags;
	sl_replica_t		 reptbl[SL_MAX_REPLICAS];
	uint64_t		 xattrsize;
};

struct fci_dinfo {
	struct psc_lockedlist	 dc_pages;
	uint64_t		 dc_nents;
};

struct fcmh_cli_info {
	struct sl_resm		*fci_resm;
	struct timeval		 fci_age;
	union {
		struct fci_finfo	f;
		struct fci_dinfo	d;
	} u;
#define fci_nrepls	u.f.nrepls
#define fci_reptbl	u.f.reptbl
#define fci_xattrsize	u.f.xattrsize
#define fci_ino_flags	u.f.ino_flags
#define fci_newreplpol	u.f.newreplpol

#define fci_dc_pages	u.d.dc_pages
#define fci_dc_nents	u.d.dc_nents
	struct psclist_head	 fci_lentry;	/* all fcmhs with dirty attributes */
	struct timespec		 fci_etime;	/* attr expire time */
};

static __inline struct fcmh_cli_info *
fcmh_2_fci(struct fidc_membh *f)
{
	return (fcmh_get_pri(f));
}

static __inline struct fidc_membh *
fci_2_fcmh(struct fcmh_cli_info *fci)
{
	struct fidc_membh *fcmh;

	psc_assert(fci);
	fcmh = (void *)fci;
	return (fcmh - 1);
}

/* Client-specific fcmh_flags */
#define FCMH_CLI_HAVEREPLTBL	(_FCMH_FLGSHFT << 0)	/* file replica table present */
#define FCMH_CLI_FETCHREPLTBL	(_FCMH_FLGSHFT << 1)	/* file replica table loading */
#define FCMH_CLI_INITDIRCACHE	(_FCMH_FLGSHFT << 2)	/* dircache initialized */
#define FCMH_CLI_TRUNC		(_FCMH_FLGSHFT << 3)	/* truncate in progress */
#define FCMH_CLI_DIRTY_ATTRS	(_FCMH_FLGSHFT << 4)	/* has dirty attributes */
#define FCMH_CLI_DIRTY_QUEUE	(_FCMH_FLGSHFT << 5)	/* on dirty queue */

int	fcmh_checkcreds(struct fidc_membh *, const struct pscfs_creds *, int);

#define fidc_lookup_load_inode(fid, fcmhp, pfcc)			\
	_fidc_lookup_load_inode(PFL_CALLERINFOSS(SLSS_FCMH), (fid),	\
	    (fcmhp), (pfcc))

/**
 * fidc_lookup_load_inode - Create the inode if it doesn't exist,
 *	loading its attributes from the MDS.
 */
#define _pfl_callerinfo pci
static __inline int
_fidc_lookup_load_inode(const struct pfl_callerinfo *pci, slfid_t fid,
    struct fidc_membh **fcmhp, struct pscfs_clientctx *pfcc)
{
	struct slash_fidgen fg = { fid, FGEN_ANY };

	return (_fidc_lookup(pci, &fg, FIDC_LOOKUP_CREATE |
	    FIDC_LOOKUP_LOAD, NULL, 0, fcmhp, pfcc));
}
#undef _pfl_callerinfo

#endif /* _FIDC_CLI_H_ */
