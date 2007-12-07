/* $Id$ */

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#define FUSE_USE_VERSION 26

#include <fuse.h>

const char *hello_str = "Hello World!\n";
const char *hello_path = "/hello";

int
slash_access(const char *path, int mask)
{
	int res;

	res = access(path, mask);
	if (res == -1)
		return -errno;

	return 0;
}

int
slash_chmod(const char *path, mode_t mode)
{
	int res;

	res = chmod(path, mode);
	if (res == -1)
		return -errno;

	return 0;
}

int
slash_chown(const char *path, uid_t uid, gid_t gid)
{
	int res;

	res = lchown(path, uid, gid);
	if (res == -1)
		return -errno;

	return 0;
}

int
slash_fsync(const char *path, int isdatasync, struct fuse_file_info *fi)
{
    return 0;
}

int
slash_getattr(const char *path, struct stat *stb)
{
	int res = 0;

	memset(stb, 0, sizeof(*stb));
	if (strcmp(path, "/") == 0) {
		stb->st_mode = S_IFDIR | 0755;
		stb->st_nlink = 1;
	} else if (strcmp(path, hello_path) == 0) {
		stb->st_mode = S_IFREG | 0444;
		stb->st_nlink = 1;
		stb->st_size = strlen(hello_str);
	} else
		res = -ENOENT;

	return res;
}

int
slash_link(const char *from, const char *to)
{
	int res;

	res = link(from, to);
	if (res == -1)
		return -errno;

	return 0;
}

int
slash_mkdir(const char *path, mode_t mode)
{
	int res;

	res = mkdir(path, mode);
	if (res == -1)
		return -errno;

	return 0;
}

int
slash_open(const char *path, struct fuse_file_info *fi)
{
	if (strcmp(path, hello_path) != 0)
		return -ENOENT;

	if ((fi->flags & 3) != O_RDONLY)
		return -EACCES;

	return 0;
}

int
slash_read(const char *path, char *buf, size_t size, off_t offset,
    struct fuse_file_info *fi)
{
	size_t len;

	if (strcmp(path, hello_path) != 0)
		return -ENOENT;

	len = strlen(hello_str);
	if (offset < len) {
		if (offset + size > len)
			size = len - offset;
		memcpy(buf, hello_str + offset, size);
	} else
		size = 0;

	return size;
}

int
slash_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
    off_t offset, struct fuse_file_info *fi)
{
	struct stat st;

	if (strcmp(path, "/") != 0)
		return -ENOENT;

	memset(&st, 0, sizeof(st));
	st.st_ino = 2;
	st.st_mode = 0755;
	filler(buf, ".", &st, 0);

	memset(&st, 0, sizeof(st));
	st.st_ino = 1;
	st.st_mode = 0755;
	filler(buf, "..", &st, 0);

	memset(&st, 0, sizeof(st));
	st.st_ino = 3;
	st.st_mode = 0644;
	filler(buf, hello_path + 1, &st, 0);

	return 0;
}

int
slash_readlink(const char *path, char *buf, size_t size)
{
	int res;

	res = readlink(path, buf, size - 1);
	if (res == -1)
		return -errno;

	buf[res] = '\0';
	return 0;
}

int
slash_rename(const char *from, const char *to)
{
	int res;

	res = rename(from, to);
	if (res == -1)
		return -errno;

	return 0;
}

int
slash_rmdir(const char *path)
{
	int res;

	res = rmdir(path);
	if (res == -1)
		return -errno;

	return 0;
}

int
slash_statfs(const char *path, struct statvfs *stbuf)
{
	int res;

	res = statvfs(path, stbuf);
	if (res == -1)
		return -errno;

	return 0;
}

int
slash_symlink(const char *from, const char *to)
{
	int res;

	res = symlink(from, to);
	if (res == -1)
		return -errno;

	return 0;
}

int
slash_truncate(const char *path, off_t size)
{
	int res;

	res = truncate(path, size);
	if (res == -1)
		return -errno;

	return 0;
}

int
slash_unlink(const char *path)
{
	int res;

	res = unlink(path);
	if (res == -1)
		return -errno;

	return 0;
}

int
slash_utime(const char *path, const struct timespec ts[2])
{
	int res;
	struct timeval tv[2];

	tv[0].tv_sec = ts[0].tv_sec;
	tv[0].tv_usec = ts[0].tv_nsec / 1000;
	tv[1].tv_sec = ts[1].tv_sec;
	tv[1].tv_usec = ts[1].tv_nsec / 1000;

	res = utimes(path, tv);
	if (res == -1)
		return -errno;

	return 0;
}

int
slash_write(const char *path, const char *buf, size_t size,
    off_t offset, struct fuse_file_info *fi)
{
	int fd;
	int res;

	fd = open(path, O_WRONLY);
	if (fd == -1)
		return -errno;

	res = pwrite(fd, buf, size, offset);
	if (res == -1)
		res = -errno;

	close(fd);
	return res;
}

struct fuse_operations slashops = {
	.access		= slash_access,
	.chmod		= slash_chmod,
	.chown		= slash_chown,
	.fsync		= slash_fsync,
	.getattr	= slash_getattr,
	.link		= slash_link,
	.mkdir		= slash_mkdir,
	.open		= slash_open,
	.read		= slash_read,
	.readdir	= slash_readdir,
	.readlink	= slash_readlink,
	.rename		= slash_rename,
	.rmdir		= slash_rmdir,
	.statfs		= slash_statfs,
	.symlink	= slash_symlink,
	.truncate	= slash_truncate,
	.unlink		= slash_unlink,
	.utime		= slash_utime,
	.write		= slash_write,
};

int
main(int argc, char *argv[])
{
	if (zclient_services_init())
		pfatalx("zclient_services_init");
	return (fuse_main(argc, argv, &slashops, NULL));
}
