/* $Id$ */

#include <sys/types.h>
#include <sys/stat.h>

#include <errno.h>
#include <stdio.h>
#include <unistd.h>

#include "psc_types.h"
#include "psc_util/log.h"
#include "psc_util/assert.h"
#include "psc_util/mkdirs.h"

#include "config.h"
#include "fid.h"
#include "slconfig.h"
#include "slashd.h"

#define _PATH_OBJROOT	"fids"
#define _PATH_NS	"ns"

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
 * fid_get - lookup the FID for a pathname.
 * @fidp: value-result FID pointer.
 * @path: pathname to lookup, should have been passed to translate_pathname().
 * @must_exist: whether this entry already exists in the namespace or is
 *	being created.
 * Notes: FID entry will be created if pathname does not exist.
 */
int
fid_get(slash_fid_t *fidp, const char *path, int must_exist)
{
	static psc_spinlock_t createlock = LOCK_INITIALIZER;
	char fn[PATH_MAX];
	struct stat stb;
	ssize_t sz;
	int fd, rc;

	rc = snprintf(fn, sizeof(fn), "%s", path);
	if (rc == -1)
		psc_fatal("snprintf");
	rc = 0;
	if (must_exist) {
		fd = open(fn, O_RDONLY);
		if (fd == -1)
			return (-1);
		sz = read(fd, fidp, sizeof(*fidp));
		if (sz == -1)
			rc = -1;
		else if (sz != sizeof(*fidp)) {
			psc_error("short write");
			rc = -1;
		}
		close(fd);
	} else {
		spinlock(&createlock);
		if (stat(fn, &stb) == -1) {
			if (errno != ENOENT) {
				psc_error("stat %s", fn);
				return (-1);
			}
		} else {
			errno = EEXIST;
			return (-1);
		}
		fd = open(fn, O_CREAT | O_EXCL | O_WRONLY, 0644);
		freelock(&createlock);
		if (fd == -1)
			return (-1);
		fidp->fid_inum = slash_get_inum();
		sz = write(fd, fidp, sizeof(*fidp));
		if (sz == -1)
			rc = -1;
		else if (sz != sizeof(*fidp)) {
			psc_error("short write");
			rc = -1;
		}
		close(fd);
	}
	fidp->fid_gen = 0; /* XXX gen should not be here */
	return (rc);
}
