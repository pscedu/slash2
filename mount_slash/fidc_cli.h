/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
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

#ifndef _FIDC_CLI_H_
#define _FIDC_CLI_H_

#include "psc_ds/list.h"
#include "psc_util/lock.h"

#include "fidcache.h"
#include "sltypes.h"

struct fidc_membh;

/*
 * Currently, we don't use reference count to protect the child-parent relationship.
 * This gives the reaper the maximum flexibility to reclaim fcmh.  However, we do 
 * have to follow some rules: (1) the reaper does not choose a non-empty directory
 * as a victim.  This makes sure that the parent pointer of a child is always valid.
 * (2) we should lock a parent when adding or removing a child from its children list.
 */
struct fcmh_cli_info {
	struct fidc_membh	*fci_parent;
	struct psclist_head	 fci_children;
	struct psclist_head	 fci_sibling;
	sl_replica_t		 fci_reptbl[SL_MAX_REPLICAS];
	int			 fci_nrepls;
	int			 fci_hash;
	char			*fci_name;
};

#define fcmh_2_fci(f)		((struct fcmh_cli_info *)fcmh_get_pri(f))

/* client-specific fcmh_state flags */
#define FCMH_CLI_HAVEREPLTBL	(_FCMH_FLGSHFT << 0)	/* file replica table present */
#define FCMH_CLI_FETCHREPLTBL	(_FCMH_FLGSHFT << 1)	/* file replica table loading */
#define FCMH_CLI_APPENDWR	(_FCMH_FLGSHFT << 2)	/* file opened with O_APPEND */

struct fidc_membh *
	fidc_child_lookup(struct fidc_membh *, const char *);
void	fidc_child_add(struct fidc_membh *, struct fidc_membh *, const char *);
int	fidc_child_reap_cb(struct fidc_membh *);
void	fidc_child_rename(struct fidc_membh *, const char *,
	    struct fidc_membh *, const char *);
void	fidc_child_unlink(struct fidc_membh *, const char *);

ssize_t	fcmh_getsize(struct fidc_membh *);
void	fcmh_setlocalsize(struct fidc_membh *, uint64_t);

/**
 * fidc_lookup_load_inode - Create the inode if it doesn't exist loading
 *	its attributes from the network.
 */
static __inline int
fidc_lookup_load_inode(slfid_t fid, const struct slash_creds *crp,
    struct fidc_membh **fcmhp)
{
	struct slash_fidgen fg = { fid, FIDGEN_ANY };

	return (fidc_lookup(&fg, FIDC_LOOKUP_CREATE|FIDC_LOOKUP_LOAD, NULL, 0, crp, fcmhp));
}

#endif /* _FIDC_CLI_H_ */
