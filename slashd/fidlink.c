/* $Id$ */

#include <sys/param.h>
#include <sys/stat.h>
#include <sys/xattr.h>

#define __USE_GNU
#include <fcntl.h>
#undef __USE_GNU

#include <errno.h>
#include <stdio.h>
#include <unistd.h>

#include "psc_util/lock.h"
#include "psc_util/log.h"

#include "fid.h"
#include "slconfig.h"
#include "slashd.h"
#include "fidcache.h"
#include "pathnames.h"
#include "creds.h"
#include "inode.h"

/*
 * translate_pathname - rewrite a pathname from a client to the location
 *	it actually correponds with as known to slash in the server file system.
 * @path: client-issued path which will contain the server path on successful return.
 * @must_exist: whether this path must exist or not (e.g. if being created).
 * Returns 0 on success or -1 on error.
 */
int
translate_pathname(char *path, int must_exist)
{
	char *lastsep, buf[PATH_MAX], prefix[PATH_MAX];
	int rc;

	rc = snprintf(prefix, sizeof(prefix), "%s/%s",
	    nodeInfo.node_res->res_fsroot, _PATH_NS);
	if (rc == -1)
		return (-1);
	rc = snprintf(buf, sizeof(buf), "%s/%s", prefix, path);
	if (rc == -1)
		return (-1);
	if (rc >= (int)sizeof(buf)) {
		errno = ENAMETOOLONG;
		return (-1);
	}

	/*
	 * As realpath(3) requires that the resolved pathname must exist,
	 * if we are creating a new pathname, it obviously won't exist,
	 * so trim the last component and append it later on.
	 */
	lastsep = NULL;
	if (must_exist == 0 && (lastsep = strrchr(buf, '/')) != NULL) {
		if (strcmp(lastsep, "/..") == 0 ||
		    strcmp(lastsep, "/.") == 0) {
			errno = -EINVAL;
			return (-1);
		}
		*lastsep = '\0';
	}
	if (realpath(buf, path) == NULL)
		return (-1);
	if (strncmp(path, prefix, strlen(prefix))) {
		/*
		 * If they found some way around
		 * realpath(3), try to catch it...
		 */
		errno = EINVAL;
		return (-1);
	}
	if (lastsep) {
		*lastsep = '/';
		strncat(path, lastsep, PATH_MAX - 1 - strlen(path));
	}
	return (0);
}

/*
 * untranslate_pathname - strip fsroot from a path.
 * @path: path to strip.
 */
int
untranslate_pathname(char *path)
{
	char *t, *p, prefix[PATH_MAX];
	int rc;

	rc = snprintf(prefix, sizeof(prefix), "%s/%s",
	    nodeInfo.node_res->res_fsroot, _PATH_NS);
	if (rc == -1)
		return (-1);
	if (strncmp(path, prefix, strlen(prefix)) == 0) {
		for (t = path, p = path + strlen(prefix);
		    p < path + PATH_MAX && *p != '\0'; p++, t++)
			*t = *p;
		if (t < path + PATH_MAX)
			*t = '\0';
		return (0);
	}
	errno = EINVAL;
	return (-1);
}

int
fid_lookup(const char *fn, struct fidc_memb_handle *fcmh, struct slash_creds *creds)
{
	int fd;
	sl_inodeh_t *inoh = &fcmh->fcmh_memb.fcm_inodeh;

	fd = access_fsop(ACSOP_LOOKUP, creds->uid, creds->gid,fn, O_RDONLY); 
}

/*
 * fid_get - lookup the FID for a pathname.
 * @fidp: value-result FID pointer.
 * @fn: pathname to lookup, should have been passed to translate_pathname().
 * @flags: open(2) flags so we know how to bring this into existence if
 *	it doesn't exist.
 * Notes: FID entry will be created if pathname does not exist.
 */
int
fid_get(const char *fn, struct slash_fidgen *fgp,
    struct slash_creds *creds, int flags, mode_t mode)
{
	static psc_spinlock_t lookuplock = LOCK_INITIALIZER;
	struct slash_inode_store ino;
	ssize_t sz;
	int fd, rc;

	/* Trim off flags that have no business here. */
	flags &= O_RDONLY | O_RDWR | O_WRONLY | O_CREAT | O_EXCL |
	    O_DIRECTORY;

	rc = 0;
	spinlock(&lookuplock);
	if ((flags & (O_CREAT | O_EXCL)) == (O_CREAT | O_EXCL)) {
		/* Must be exclusive CREATE. */
		fd = access_fsop(ACSOP_OPEN, creds->uid, creds->gid,
		    fn, O_CREAT | O_EXCL | O_WRONLY, mode);
		if (fd == -1) {
			rc = -1;
			goto done;
		}
		memset(&ino, 0, sizeof(ino));
		fgp->fg_fid = slmds_get_inum(); // | myfsid()
		fgp->fg_gen = 0;
		ino.ino_fg = *fgp;
		sz = fsetxattr(fd, SFX_INODE, &ino,
		    sizeof(ino), XATTR_CREATE);
		if (sz == -1)
			rc = -1;
		else if (sz != sizeof(ino)) {
			psc_error("short setxattr");
			rc = -1;
		}
		// XXX sync/flush
		close(fd);
	} else if (flags & O_CREAT) {
		/*
		 * May already exist, or come into existence *while*
		 * we are creating it, or may not exist at all.
		 */
		fd = access_fsop(ACSOP_OPEN, creds->uid, creds->gid,
		    fn, O_RDONLY);
		if (fd == -1) {
			/* Doesn't exist, try to create it. */
			fd = access_fsop(ACSOP_OPEN, creds->uid, creds->gid,
			    fn, O_CREAT | O_EXCL | O_WRONLY, 0644);
			if (fd == -1) {
				rc = -1;
				goto done;
			}
			fgp->fg_fid = slmds_get_inum(); // | myfsid()
			fgp->fg_gen = 0;
			ino.ino_fg = *fgp;
			sz = fsetxattr(fd, SFX_INODE, &ino,
			    sizeof(ino), XATTR_CREATE);
			if (sz == -1)
				rc = -1;
			else if (sz != sizeof(ino)) {
				psc_error("short setxattr");
				rc = -1;
			}
			// XXX sync/flush
			close(fd);
		} else {
			sz = fgetxattr(fd, SFX_INODE, &ino, sizeof(ino));
			if (sz == -1)
				rc = -1;
			else if (sz != sizeof(ino)) {
				psc_error("short getxattr");
				rc = -1;
			} else
				*fgp = ino.ino_fg;
			close(fd);
		}
	} else {
		/* Must already exist. */
		fd = access_fsop(ACSOP_OPEN, creds->uid, creds->gid,
		    fn, O_RDONLY);
		if (fd == -1) {
			rc = -1;
			goto done;
		}
		sz = fgetxattr(fd, SFX_INODE, &ino, sizeof(ino));
		if (sz == -1)
			rc = -1;
		else if (sz != sizeof(ino)) {
			psc_error("short getxattr");
			rc = -1;
		} else
			*fgp = ino.ino_fg;
		close(fd);
	}
 done:
	freelock(&lookuplock);
	return (rc);
}
