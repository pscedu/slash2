/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2007-2014, Pittsburgh Supercomputing Center (PSC).
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

/*
 * slfile mount_slash specific data, comes after slfile in memory.
 * @fci_resm: MDS resource who owns this file.
 * @fci_age: when we populated this file in our cache (used for
 *	expiration).
 * @fci_inode: mirror of MDS's inode table grabbed on GETBMAP.
 * @fci_xattrsize: extended attributes size, cached for optimizations
 *	(i.e. return from GETXATTR immediately when size is zero).
 * @fcif_idxmap: priority ordering of inode residency table entries for
 *	quick access.
 * @fcif_mapstircnt: how many times @idxmap has been used since last
 *	stir.
 * @fci_dc_pages: dircache pages.
 * @fcid_lookup_age: second-resolution of last dircache LOOKUP miss.
 * @fcid_lookup_misses: how many LOOKUPs did not hit dircache since @age.
 * @fci_lentry: cache membership.
 * @fci_etime: attribute expiration time.
 */
struct fcmh_cli_info {
	struct sl_resm			*fci_resm;
	struct timeval			 fci_age;
	union {
		struct {
			struct srt_inode inode;
			uint64_t	 xattrsize;
			int		 idxmap[SL_MAX_REPLICAS];
			int		 mapstircnt;
		} f;
#define fci_xattrsize		u.f.xattrsize
#define fci_inode		u.f.inode
#define fcif_idxmap		u.f.idxmap
#define fcif_mapstircnt		u.f.mapstircnt
		struct {
			struct psc_lockedlist
					 pages; // XXX tree?

			/*
			 * Predictive readdir when LOOKUPs aren't
			 * hitting dircache.
			 */
			struct timeval	 lookup_age;	/* async readdir  */
			uint64_t	 lookup_misses;
		} d;
#define fci_dc_pages		u.d.pages
#define fcid_lookup_age		u.d.lookup_age
#define fcid_lookup_misses	u.d.lookup_misses
	} u;
	struct psclist_head		 fci_lentry;	/* all fcmhs with dirty attributes */
	struct timespec			 fci_etime;	/* attr expire time */
};

#define fcmh_2_nrepls(f)	fcmh_2_fci(f)->fci_inode.nrepls

#define MAPSTIR_THRESH			10

#define DIR_LOOKUP_MISSES_INCR		1000
#define DIR_LOOKUP_MISSES_THRES		400001

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
#define FCMH_CLI_HAVEINODE	(_FCMH_FLGSHFT << 0)	/* file inode present */
#define FCMH_CLI_INITDIRCACHE	(_FCMH_FLGSHFT << 1)	/* dircache initialized */
#define FCMH_CLI_TRUNC		(_FCMH_FLGSHFT << 2)	/* truncate in progress */
#define FCMH_CLI_DIRTY_ATTRS	(_FCMH_FLGSHFT << 3)	/* has dirty attributes */
#define FCMH_CLI_DIRTY_QUEUE	(_FCMH_FLGSHFT << 4)	/* on dirty queue */

int	fcmh_checkcreds(struct fidc_membh *, const struct pscfs_creds *, int);

int	slc_fcmh_fetch_inode(struct fidc_membh *);
void	slc_fcmh_load_inode(struct fidc_membh *, struct srt_inode *);

#define fidc_lookup_load(fid, fcmhp, pfcc)				\
	_fidc_lookup_load(PFL_CALLERINFOSS(SLSS_FCMH), (fid),		\
	    (fcmhp), (pfcc))

/**
 * fidc_lookup_load - Create the fcmh if it doesn't exist,
 *	loading its attributes from the MDS.
 */
#define _pfl_callerinfo pci
static __inline int
_fidc_lookup_load(const struct pfl_callerinfo *pci, slfid_t fid,
    struct fidc_membh **fcmhp, struct pscfs_clientctx *pfcc)
{
	struct slash_fidgen fg = { fid, FGEN_ANY };

	return (_fidc_lookup(pci, &fg, FIDC_LOOKUP_CREATE |
	    FIDC_LOOKUP_LOAD, fcmhp, pfcc));
}

static __inline int
_fidc_lookup_peek(const struct pfl_callerinfo *pci, slfid_t fid,
    struct fidc_membh **fcmhp, struct pscfs_clientctx *pfcc)
{
	struct slash_fidgen fg = { fid, FGEN_ANY };

	return (_fidc_lookup(pci, &fg, FIDC_LOOKUP_NONE,
	    fcmhp, pfcc));
}
#undef _pfl_callerinfo

#define fidc_lookup_peek(fid, fcmhp, pfcc)				\
	_fidc_lookup_peek(PFL_CALLERINFOSS(SLSS_FCMH), (fid),		\
	    (fcmhp), (pfcc))

#endif /* _FIDC_CLI_H_ */
