/* $Id$ */

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statvfs.h>

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>

#include "psc_util/log.h"

#include "creds.h"
#include "pathnames.h"
#include "sltypes.h"
#include "slutil.h"

const char *sl_datadir = SL_PATH_DATADIR;

void
dump_statbuf(int level, const struct stat *stb)
{
	DEBUG_STATBUF(level, stb, "");
}

void
print_flag(const char *str, int *seq)
{
	printf("%s%s", *seq ? "|" : "", str);
	*seq = 1;
}

void
dump_fflags(int fflags)
{
	int seq = 0;

	if (fflags & O_WRONLY)
		print_flag("O_WRONLY", &seq);
	if (fflags & O_RDWR)
		print_flag("O_RDWR", &seq);
	if ((fflags & O_ACCMODE) == O_RDONLY)
		print_flag("O_RDONLY", &seq);

	if (fflags & O_CREAT)
		print_flag("O_CREAT", &seq);
	if (fflags & O_EXCL)
		print_flag("O_EXCL", &seq);
	if (fflags & O_NOCTTY)
		print_flag("O_NOCTTY", &seq);
	if (fflags & O_TRUNC)
		print_flag("O_TRUNC", &seq);
	if (fflags & O_APPEND)
		print_flag("O_APPEND", &seq);
	if (fflags & O_NONBLOCK)
		print_flag("O_NONBLOCK", &seq);
	if (fflags & O_SYNC)
		print_flag("O_SYNC", &seq);
	if (fflags & O_ASYNC)
		print_flag("O_ASYNC", &seq);

	if (fflags & O_DIRECT)
		print_flag("O_DIRECT", &seq);
	if (fflags & O_DIRECTORY)
		print_flag("O_DIRECTORY", &seq);
	if (fflags & O_NOFOLLOW)
		print_flag("O_NOFOLLOW", &seq);
	if (fflags & O_NOATIME)
		print_flag("O_NOATIME", &seq);
	if (fflags & O_LARGEFILE)
		print_flag("O_LARGEFILE", &seq);

	printf("\n");
}

enum rw
fflags_2_rw(int fflags)
{
	if (fflags & (O_WRONLY | O_RDWR))
		return (SL_WRITE);
	return (SL_READ);
}

void
sl_externalize_stat(const struct stat *stb, struct srt_stat *sstb)
{
	sstb->sst_dev		= stb->st_dev;
	sstb->sst_ino		= stb->st_ino;
	sstb->sst_mode		= stb->st_mode;
	sstb->sst_nlink		= stb->st_nlink;
	sstb->sst_uid		= stb->st_uid;
	sstb->sst_gid		= stb->st_gid;
	sstb->sst_rdev		= stb->st_rdev;
	sstb->sst_size		= stb->st_size;
	sstb->sst_blksize	= stb->st_blksize;
	sstb->sst_blocks	= stb->st_blocks;
	sstb->sst_atime		= stb->st_atime;
	sstb->sst_mtime		= stb->st_mtime;
	sstb->sst_ctime		= stb->st_ctime;
}

void
sl_internalize_stat(const struct srt_stat *sstb, struct stat *stb)
{
	stb->st_dev		= sstb->sst_dev;
	stb->st_ino		= sstb->sst_ino;
	stb->st_mode		= sstb->sst_mode;
	stb->st_nlink		= sstb->sst_nlink;
	stb->st_uid		= sstb->sst_uid;
	stb->st_gid		= sstb->sst_gid;
	stb->st_rdev		= sstb->sst_rdev;
	stb->st_size		= sstb->sst_size;
	stb->st_blksize		= sstb->sst_blksize;
	stb->st_blocks		= sstb->sst_blocks;
	stb->st_atime		= sstb->sst_atime;
	stb->st_mtime		= sstb->sst_mtime;
	stb->st_ctime		= sstb->sst_ctime;
}

void
sl_externalize_statfs(const struct statvfs *sfb, struct srt_statfs *ssfb)
{
	ssfb->sf_bsize		= sfb->f_bsize;
	ssfb->sf_frsize		= sfb->f_frsize;
	ssfb->sf_blocks		= sfb->f_blocks;
	ssfb->sf_bfree		= sfb->f_bfree;
	ssfb->sf_bavail		= sfb->f_bavail;
	ssfb->sf_files		= sfb->f_files;
	ssfb->sf_ffree		= sfb->f_ffree;
	ssfb->sf_favail		= sfb->f_favail;
	ssfb->sf_fsid		= sfb->f_fsid;
	ssfb->sf_flag		= sfb->f_flag;
	ssfb->sf_namemax	= sfb->f_namemax;
}

void
sl_internalize_statfs(const struct srt_statfs *ssfb, struct statvfs *sfb)
{
	sfb->f_bsize		= ssfb->sf_bsize;
	sfb->f_frsize		= ssfb->sf_frsize;
	sfb->f_blocks		= ssfb->sf_blocks;
	sfb->f_bfree		= ssfb->sf_bfree;
	sfb->f_bavail		= ssfb->sf_bavail;
	sfb->f_files		= ssfb->sf_files;
	sfb->f_ffree		= ssfb->sf_ffree;
	sfb->f_favail		= ssfb->sf_favail;
	sfb->f_fsid		= ssfb->sf_fsid;
	sfb->f_flag		= ssfb->sf_flag;
	sfb->f_namemax		= ssfb->sf_namemax;
}

/**
 * checkcreds - Perform a classic UNIX permission access check.
 * @sstb: ownership info.
 * @cr: credentials of access.
 * @xmode: type of access.
 * Returns zero on success, errno code on failure.
 */
int
checkcreds(const struct srt_stat *sstb, const struct slash_creds *cr,
    int xmode)
{
	if (sstb->sst_uid == 0)
		return (0);
	if (sstb->sst_uid == cr->uid) {
		if (((xmode & R_OK) && (sstb->sst_mode & S_IRUSR) == 0) ||
		    ((xmode & W_OK) && (sstb->sst_mode & S_IWUSR) == 0) ||
		    ((xmode & X_OK) && (sstb->sst_mode & S_IXUSR) == 0))
			return (EACCES);
		return (0);
	}
	/* XXX check process supplementary group list */
	if (sstb->sst_gid == cr->gid) {
		if (((xmode & R_OK) && (sstb->sst_mode & S_IRGRP) == 0) ||
		    ((xmode & W_OK) && (sstb->sst_mode & S_IWGRP) == 0) ||
		    ((xmode & X_OK) && (sstb->sst_mode & S_IXGRP) == 0))
			return (EACCES);
		return (0);
	}
	if (((xmode & R_OK) && (sstb->sst_mode & S_IROTH) == 0) ||
	    ((xmode & W_OK) && (sstb->sst_mode & S_IWOTH) == 0) ||
	    ((xmode & X_OK) && (sstb->sst_mode & S_IXOTH) == 0))
		return (EACCES);
	return (0);
}
