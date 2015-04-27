/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2014-2015, Pittsburgh Supercomputing Center (PSC).
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

#include <acl/libacl.h>

#include "pfl/acl.h"
#include "pfl/cdefs.h"
#include "pfl/fs.h"

#include "fidcache.h"
#include "mount_slash.h"

#define ACL_EA_ACCESS "system.posix_acl_access"

/*
 * Pull POSIX ACLs from an fcmh via RPCs to MDS.
 */
acl_t
slc_acl_get_fcmh(const struct pscfs_clientctx *pfcc,
    const struct pscfs_creds *pcr, struct fidc_membh *f)
{
	char trybuf[64] = { 0 };
	void *buf = NULL;
	size_t retsz = 0;
	ssize_t rc;
	acl_t a;

	rc = slc_getxattr(pfcc, pcr, ACL_EA_ACCESS, trybuf,
	    sizeof(trybuf), f, &retsz);
	if (rc == 0) {
		buf = trybuf;
	} else if (rc == ERANGE) {
		buf = PSCALLOC(retsz);
		rc = slc_getxattr(pfcc, pcr, ACL_EA_ACCESS, buf, retsz,
		    f, &retsz);
		if (rc) {
			PSCFREE(buf);
			return (NULL);
		}
	} else
		return (NULL);

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
sl_fcmh_checkacls(struct fidc_membh *f,
    const struct pscfs_clientctx *pfcc, const struct pscfs_creds *pcrp,
    int accmode)
{
	int locked, rv;
	acl_t a;

	a = slc_acl_get_fcmh(pfcc, pcrp, f);
	if (a == NULL) {
		int rc;

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
	rv = sl_checkacls(a, &f->fcmh_sstb, pcrp, accmode);
	FCMH_URLOCK(f, locked);
	acl_free(a);
	return (rv);
}
