/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015, Google, Inc.
 * Copyright (c) 2014-2015, Pittsburgh Supercomputing Center (PSC).
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

#include <acl/libacl.h>

#include "pfl/acl.h"
#include "pfl/cdefs.h"
#include "pfl/fs.h"

#include "fidcache.h"
#include "mount_slash.h"

#define ACL_DEF_SIZE	128
#define ACL_EA_ACCESS	"system.posix_acl_access"

/*
 * Pull POSIX ACLs from an fcmh via RPCs to MDS.
 */
acl_t
slc_acl_get_fcmh(struct pscfs_req *pfr, __unusedx const struct pscfs_creds *pcr,
    struct fidc_membh *f)
{
	char trybuf[ACL_DEF_SIZE] = { 0 };
	void *buf = NULL;
	size_t retsz;
	ssize_t rc;
	int alloc = 0;
	acl_t a = NULL;

	retsz = ACL_DEF_SIZE;
	buf = trybuf;

 again:

	rc = slc_getxattr(pfr, ACL_EA_ACCESS, buf, retsz, f, &retsz);
	if (rc == 0)
		goto out;
	if (rc != ERANGE || alloc)
		goto out;

	retsz = 0;
	rc = slc_getxattr(pfr, ACL_EA_ACCESS, buf, retsz, f, &retsz);
	if (rc) 
		goto out;

	alloc = 1;
	buf = PSCALLOC(retsz);
	goto again;

 out:

	if (!rc)
		a = pfl_acl_from_xattr(buf, retsz);

	if (buf != trybuf)
		PSCFREE(buf);
	return (a);
}

#define ACL_SET_PRECEDENCE(level, prec, e, authz)			\
	if ((level) < (prec)) {						\
		(authz) = (e);						\
		(prec) = (level);					\
	}

#define ACL_PERM(set, perm)	(acl_get_perm(set, perm) < 1)

#define ACL_AUTH(e, mode)						\
	_PFL_RVSTART {							\
		acl_permset_t _set;					\
		int _rv = EACCES;					\
									\
		if (acl_get_permset((e), &_set) == -1) {		\
			psclog_error("acl_get_permset");		\
		} else if (						\
		    (((mode) & R_OK) && ACL_PERM(_set, ACL_READ)) ||	\
		    (((mode) & W_OK) && ACL_PERM(_set, ACL_WRITE)) ||	\
		    (((mode) & X_OK) && ACL_PERM(_set, ACL_EXECUTE))) {	\
		} else							\
			_rv = 0;					\
		(_rv);							\
	} _PFL_RVEND

/* subtle: || 1 is there in case gid is zero to avoid premature exit */
#define FOREACH_GROUP(g, i, pcrp)					\
	for ((i) = 0; (i) <= (pcrp)->pcr_ngid && (((g) = (i) == 0 ?	\
	    (pcrp)->pcr_gid : (pcrp)->pcr_gidv[(i) - 1]) || 1); (i)++)

int
sl_checkacls(acl_t a, struct srt_stat *sstb,
    const struct pscfs_creds *pcrp, int accmode)
{
	int wh, rv = EACCES, i, rc, prec = 6;
	acl_entry_t e, authz = NULL, mask = NULL;
	acl_tag_t tag;
	gid_t *gp, g;
	uid_t *up;

	wh = ACL_FIRST_ENTRY;
	while ((rc = acl_get_entry(a, wh, &e)) == 1) {
		wh = ACL_NEXT_ENTRY;

		rc = acl_get_tag_type(e, &tag);
		switch (tag) {
		case ACL_USER_OBJ:
			if (sstb->sst_uid == pcrp->pcr_uid)
				ACL_SET_PRECEDENCE(1, prec, e, authz);
			break;
		case ACL_USER:
			up = acl_get_qualifier(e);
			if (*up == pcrp->pcr_uid)
				ACL_SET_PRECEDENCE(2, prec, e, authz);
			break;

		case ACL_GROUP_OBJ:
			FOREACH_GROUP(g, i, pcrp)
				if (g == sstb->sst_gid) {
					ACL_SET_PRECEDENCE(3, prec, e,
					    authz);
					break;
				}
			break;
		case ACL_GROUP:
			gp = acl_get_qualifier(e);
			FOREACH_GROUP(g, i, pcrp)
				if (g == *gp) {
					ACL_SET_PRECEDENCE(4, prec, e,
					    authz);
					break;
				}
			break;

		case ACL_OTHER:
			ACL_SET_PRECEDENCE(5, prec, e, authz);
			break;

		case ACL_MASK:
			mask = e;
			break;

		default:
			psclog_error("acl_get_tag_type");
			break;
		}
	}
	if (rc == -1)
		psclog_error("acl_get_entry");
	else if (authz) {
		rv = ACL_AUTH(authz, accmode);
		if (prec != 1 && prec != 5 &&
		    rv == 0 && mask)
			rv = ACL_AUTH(mask, accmode);
	}
#ifdef SLOPT_POSIX_ACLS_REVERT
	else
		rv = checkcreds(sstb, pcrp, accmode);
#endif
	return (rv);
}

int
sl_fcmh_checkacls(struct fidc_membh *f, struct pscfs_req *pfr,
    const struct pscfs_creds *pcrp, int accmode)
{
	int locked, rc;
	acl_t a;

	a = slc_acl_get_fcmh(pfr, pcrp, f);
	/*
	 * If there is no ACL entries, we revert to traditional
	 * Unix mode bits for permission checking.
	 *
	 * I have seen small ACL with 28 bytes that does not
	 * refer to other group or user. Should we consider
	 * such a case as no ACL as well?
	 */
	if (a == NULL) {
#ifdef SLOPT_POSIX_ACLS_REVERT
		locked = FCMH_RLOCK(f);
		rc = checkcreds(&f->fcmh_sstb, pcrp, accmode);
		FCMH_URLOCK(f, locked);
#else
		rc = EACCES;
#endif
		return (rc);
	}
	locked = FCMH_RLOCK(f);
	rc = sl_checkacls(a, &f->fcmh_sstb, pcrp, accmode);
	FCMH_URLOCK(f, locked);
	acl_free(a);
	return (rc);
}

/* acl-2.2.52: setfacl/do_set.c */

static void
set_perm(
        acl_entry_t ent,
        mode_t perm)
{
        acl_permset_t set;

        acl_get_permset(ent, &set);
        if (perm & ACL_READ)
                acl_add_perm(set, ACL_READ);
        else
                acl_delete_perm(set, ACL_READ);
        if (perm & ACL_WRITE)
                acl_add_perm(set, ACL_WRITE);
        else
                acl_delete_perm(set, ACL_WRITE);
        if (perm & ACL_EXECUTE)
                acl_add_perm(set, ACL_EXECUTE);
        else
                acl_delete_perm(set, ACL_EXECUTE);
}


/*
 * Update ACL after a chmod.
 */
int
sl_fcmh_updateacls(struct fidc_membh *f, struct pscfs_req *pfr,
    const struct pscfs_creds *pcrp)
{
	acl_t a;
	int wh, rv = EACCES, rc;
	acl_entry_t e;
	acl_tag_t tag;
	mode_t perm;
	acl_entry_t mask_obj = NULL;
	acl_entry_t group_obj = NULL;
	uint32_t umode = f->fcmh_sstb.sst_mode;

	a = slc_acl_get_fcmh(pfr, pcrp, f);
	if (a == NULL)
		return EACCES;

	wh = ACL_FIRST_ENTRY;
	while ((rc = acl_get_entry(a, wh, &e)) == 1) {
		wh = ACL_NEXT_ENTRY;

		rc = acl_get_tag_type(e, &tag);
		switch (tag) {
		case ACL_USER_OBJ:
			perm = 0;
			if (umode & S_IRUSR)
        			perm |= ACL_READ;
			if (umode & S_IWUSR)
        			perm |= ACL_WRITE;
			if (umode & S_IXUSR)
        			perm |= ACL_EXECUTE;
			set_perm(e, perm);
			break;
		case ACL_OTHER:
			perm = 0;
			if (umode & S_IROTH)
        			perm |= ACL_READ;
			if (umode & S_IWOTH)
        			perm |= ACL_WRITE;
			if (umode & S_IXOTH)
        			perm |= ACL_EXECUTE;
			set_perm(e, perm);
			break;
		case ACL_USER:
		case ACL_GROUP:
			break;
		case ACL_GROUP_OBJ:
			group_obj = e;
			break;
		case ACL_MASK:
			mask_obj = e;
			break;
		default:
			psclog_error("acl_get_tag_type");
			break;
		}
	}
	if (rc == -1)
		psclog_error("acl_get_entry");
	if (mask_obj)
		goto set;
	if (!group_obj) {
		psclog_error("No group obj");
		goto out;
	}

 set:
	perm = 0;
	if (umode & S_IRGRP)
       		perm |= ACL_READ;
	if (umode & S_IWGRP)
       		perm |= ACL_WRITE;
	if (umode & S_IXGRP)
       		perm |= ACL_EXECUTE;
	set_perm(e, perm);

 out:
	acl_free(a);
	return (rv);
}
