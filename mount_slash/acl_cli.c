/* $Id$ */
/* %PSCGPL_COPYRIGHT% */

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
slc_acl_get_fcmh(struct pscfs_req *pfr, struct fidc_membh *f)
{
	void *buf = NULL;
	size_t retsz = 0;
	char trybuf[16];
	ssize_t rc;
	acl_t a;

	rc = slc_getxattr(pfr, ACL_EA_ACCESS, trybuf, sizeof(trybuf), f,
	    &retsz);
	if (rc == 0) {
		buf = trybuf;
	} else if (rc == -ERANGE) {
		buf = PSCALLOC(retsz);
		rc = slc_getxattr(pfr, ACL_EA_ACCESS, buf, retsz, f,
		    &retsz);
	}

	a = pfl_acl_from_xattr(buf, rc);

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
	for ((i) = 0; (i) <= (pcrp)->pcr_ngid && (((g) = (i) ?		\
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
				if (g == sstb->sst_gid && ACL_AUTH(e,
				    accmode)) {
					ACL_SET_PRECEDENCE(3, prec, e,
					    authz);
					break;
				}
			break;
		case ACL_GROUP:
			gp = acl_get_qualifier(e);
			FOREACH_GROUP(g, i, pcrp)
				if (g == *gp && ACL_AUTH(e, accmode)) {
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
sl_fcmh_checkacls(struct fidc_membh *f, const struct pscfs_creds *pcrp,
    int accmode)
{
	acl_t a;
	int rv;

	a = slc_acl_get_fcmh(NULL, f);
	if (a == NULL)
		return (EACCES);
	rv = sl_checkacls(a, &f->fcmh_sstb, pcrp, accmode);
	acl_free(a);
	return (rv);
}
