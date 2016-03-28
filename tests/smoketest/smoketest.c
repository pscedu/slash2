/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright 2010-2016, Pittsburgh Supercomputing Center
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


#include <sys/types.h>
#include <sys/stat.h>

#include <dirent.h>
#include <err.h>
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "pfl/cdefs.h"

#define		 DEFAULT_SEED		123456
#define		 TOTAL_WRITES		65537

unsigned int	 seed = DEFAULT_SEED;

char		 workdir[1024];

__dead void
usage(void)
{
	extern const char *__progname;

	fprintf(stderr,
	    "usage: %s [-s seed] [-r test] [-l] [directory]\n",
	    __progname);
	exit(1);
}

struct test_desc {
	char		 *descp;
	int		(*funcp)(void);
};

int
test_basic(void)
{
	int rc, fd;
	char *tmpdir = "basic-test.dir";
	char *tmpfile = "basic-test.file";

	rc = mkdir(tmpdir, S_IRWXU);
	if (rc) {
		printf("Fail to create directory %s, errno = %d at line %d!\n", tmpdir, errno, __LINE__);
		return (1);
	}
	rc = rmdir(tmpdir);
	if (rc) {
		printf("Fail to remove directory %s, errno = %d at line %d!\n", tmpdir, errno, __LINE__);
		return (1);
	}
	fd = open(tmpfile, O_CREAT|O_RDWR, S_IRWXU);
	if (fd < 0) {
		printf("Fail to create file %s, errno = %d at line %d!\n", tmpfile, errno, __LINE__);
		return (1);
	}
	rc = close(fd);
	if (rc < 0) {
		printf("Fail to close file %s, errno = %d at line %d!\n", tmpfile, errno, __LINE__);
		return (1);
	}
	rc = unlink(tmpfile);
	if (rc) {
		printf("Fail to remove file %s, errno = %d at line %d!\n", tmpfile, errno, __LINE__);
		return (1);
	}
	return (0);
}

int
test_rename(void)
{
	int rc;
	DIR *dp;
	struct dirent *dirp;
	char *tmpname1 = "test-rename1.dir";
	char *tmpname2 = "test-rename2.dir";

	rc = mkdir(tmpname1, S_IRWXU);
	if (rc) {
		printf("Fail to create directory %s, errno = %d at line %d!\n", tmpname1, errno, __LINE__);
		return (1);
	}
	/* mimic the tab-completion behavior by reading the current directory */
	dp = opendir(".");
	if (dp == NULL) {
		printf("Fail to open current directory, errno = %d at line %d!\n", errno, __LINE__);
		return (1);
	}
	while ((dirp = readdir(dp)) != NULL);
	rc = closedir(dp);
	if (rc < 0) {
		printf("Fail to close current directory, errno = %d at line %d!\n", errno, __LINE__);
		return (1);
	}
	rc = rename(tmpname1, tmpname2);
	if (rc) {
		printf("Fail to rename directory %s, errno = %d at line %d!\n", tmpname1, errno, __LINE__);
		return (1);
	}
	/* A bug can cause the following to fail with EEXIST */
	rc = mkdir(tmpname1, S_IRWXU);
	if (rc) {
		printf("Fail to create directory %s, errno = %d at line %d!\n", tmpname1, errno, __LINE__);
		return (1);
	}
	rc = rmdir(tmpname1);
	if (rc) {
		printf("Fail to remove directory %s, errno = %d at line %d!\n", tmpname1, errno, __LINE__);
		return (1);
	}
	rc = rmdir(tmpname2);
	if (rc) {
		printf("Fail to remove directory %s, errno = %d at line %d!\n", tmpname2, errno, __LINE__);
		return (1);
	}
	return (0);

}

/*
 * See if we coalesce I/O requests properly.
 */
int
test_random(void)
{
	unsigned long value;
	int fd, rc, ret;
	int i, j, k, idx, diff;
	char *tmpname = "test-random.dat";

	struct writes {
		off_t			offset;
		union {
			unsigned long	value;
			unsigned char   bytes[8];
		};
	} * ptr;

	ptr = malloc(TOTAL_WRITES * sizeof(struct writes));

	ptr[0].value = 0x1234;
	/* make sure we run on a 64-bit little endian machine */
	if (sizeof(unsigned long) != 8 || ptr[0].bytes[0] != 0x34) {
		free(ptr);
		return (2);
	}

	for (i = 0; i < TOTAL_WRITES; i++) {
		ptr[i].offset = (off_t)random();
		ptr[i].value = (unsigned long)random();
	}

	fd = open(tmpname, O_CREAT|O_RDWR, S_IRWXU);
	if (fd < 0) {
		printf("Fail to create file %s, errno = %d at line %d!\n", tmpname, errno, __LINE__);
		return (1);
	}
	for (i = 0; i < TOTAL_WRITES; i++) {
		ret = pwrite(fd, &ptr[i].value, sizeof(ptr[i].value), ptr[i].offset);
		if (ret != sizeof(ptr[i].value)) {
			printf("Fail to write (%lu: %lu), errno = %d at line %d!\n",
				ptr[i].value, ptr[i].offset, errno, __LINE__);
			return (1);
		}
	}
	rc = close(fd);
	if (rc < 0) {
		printf("Fail to close file %s, errno = %d at line %d!\n", tmpname, errno, __LINE__);
		return (1);
	}
	/* handle overwrites */
	for (i = 0; i < TOTAL_WRITES; i++) {
		for (j = i + 1; j < TOTAL_WRITES; j++) {

			if (ptr[j].offset >= ptr[i].offset + 8)
				continue;
			if (ptr[j].offset + 8 <= ptr[i].offset)
				continue;

			if (ptr[i].offset > ptr[j].offset) {
				idx = 0;
				diff = ptr[i].offset - ptr[j].offset;
				for (k = diff ; k < 8; k++, idx++)
					ptr[i].bytes[idx] = ptr[j].bytes[k];
			} else {
				idx = 0;
				diff = ptr[j].offset - ptr[i].offset;
				for (k = diff; k < 8; k++, idx++)
					ptr[i].bytes[k] = ptr[j].bytes[idx];
			}
		}
	}

	fd = open(tmpname, O_RDONLY, S_IRWXU);
	if (fd < 0) {
		printf("Fail to open file %s, errno = %d at line %d!\n", tmpname, errno, __LINE__);
		return (1);
	}
	for (i = 0; i < TOTAL_WRITES; i++) {
		ret = pread(fd, &value, sizeof(ptr[i].value), ptr[i].offset);
		if (ret != sizeof(ptr[i].value)) {
			printf("Fail to read at %lu, errno = %d at line %d!\n",
				ptr[i].offset, errno, __LINE__);
			return (1);
		}
		if (value != ptr[i].value) {
			printf("Fail to read at %lu (%lu versus %lu) at line %d!\n",
				ptr[i].offset, value, ptr[i].value, __LINE__);
			return (1);
		}
	}
	rc = close(fd);
	if (rc < 0) {
		printf("Fail to close file %s, errno = %d at line %d!\n", tmpname, errno, __LINE__);
		return (1);
	}
	rc = unlink(tmpname);
	if (rc) {
		printf("Fail to remove file %s, errno = %d at line %d!\n", tmpname, errno, __LINE__);
		return (1);
	}
	free(ptr);
	return (0);
}

/*
 * See if we handle chown() properly.
 * Reference: pjd-fstest-20080816.tgz at http://www.tuxera.com/community/posix-test-suite/.
 */
int
test_chown(void)
{
	int fd, rc;
	gid_t gidset[1];
	char *tmpname = "test-chmod.dat";

	fd = open(tmpname, O_CREAT|O_RDWR, S_IRWXU);
	if (fd < 0) {
		printf("Fail to create file %s, errno = %d at line %d!\n", tmpname, errno, __LINE__);
		return (1);
	}
	rc = close(fd);
	if (rc < 0) {
		printf("Fail to close file %s, errno = %d at line %d!\n", tmpname, errno, __LINE__);
		return (1);
	}
	rc = chown(tmpname, 65534, 65533);
	if (rc < 0) {
		printf("Fail to chown file %s, errno = %d at line %d!\n", tmpname, errno, __LINE__);
		return (1);
	}
	gidset[0] = 65531;
	rc = setgroups(1, gidset);
	if (rc < 0) {
		printf("Fail to set groups, errno = %d at line %d!\n", errno, __LINE__);
		return (1);
	}
	rc = setegid(gidset[0]);
	if (rc < 0) {
		printf("Fail to set effective groups, errno = %d at line %d!\n", errno, __LINE__);
		return (1);
	}
	rc = setuid(65532);
	if (rc < 0) {
		printf("Fail to set user ID, errno = %d at line %d!\n", errno, __LINE__);
		return (1);
	}
	rc = chown(tmpname, 65535, 65535);
	if (rc < 0) {
		printf("Fail to chown file %s, errno = %d at line %d!\n", tmpname, errno, __LINE__);
		return (1);
	}
	rc = unlink(tmpname);
	if (rc) {
		printf("Fail to remove file %s, errno = %d at line %d!\n", tmpname, errno, __LINE__);
		return (1);
	}
	return (0);
}

/*
 * See if we handle link() properly.
 * Reference: pjd-fstest-20080816.tgz at http://www.tuxera.com/community/posix-test-suite/.
 */
int
test_link(void)
{
	int fd, rc;
	struct stat buf;
	char *tmpname = "test-link.dat";
	char *linkname = "test-link.link";

	fd = open(tmpname, O_CREAT|O_RDWR, S_IRWXU);
	if (fd < 0) {
		printf("Fail to create file %s, errno = %d at line %d!\n", tmpname, errno, __LINE__);
		return (1);
	}
	rc = close(fd);
	if (rc < 0) {
		printf("Fail to close file %s, errno = %d at line %d!\n", tmpname, errno, __LINE__);
		return (1);
	}
	rc = link(tmpname, linkname);
	if (rc < 0) {
		printf("Fail to add a link %s to file %s, errno = %d at line %d!\n", linkname, tmpname, errno, __LINE__);
		return (1);
	}
	rc = stat(tmpname, &buf);
	if (rc < 0) {
		printf("Fail to stat file %s, errno = %d at line %d!\n", tmpname, errno, __LINE__);
		return (1);
	}
	if (buf.st_nlink != 2) {
		printf("Unexpected link count %d, errno = %d at line %d!\n", (int)buf.st_nlink, errno, __LINE__);
		return (1);
	}
	rc = unlink(tmpname);
	if (rc < 0) {
		printf("Fail to unlink %s, errno = %d at line %d!\n", tmpname, errno, __LINE__);
		return (1);
	}
	rc = unlink(linkname);
	if (rc < 0) {
		printf("Fail to unlink %s, errno = %d at line %d!\n", linkname, errno, __LINE__);
		return (1);
	}
	return (0);
}

/*
 * See if we handle truncate() properly.
 * Reference: pjd-fstest-20080816.tgz at http://www.tuxera.com/community/posix-test-suite/.
 */
int
test_truncate(void)
{
	int fd, rc;
	struct stat buf;
	char *tmpname = "test-truncate.dat";

	fd = open(tmpname, O_CREAT|O_RDWR, S_IRWXU);
	if (fd < 0) {
		printf("Fail to create file %s, errno = %d at line %d!\n", tmpname, errno, __LINE__);
		return (1);
	}
	rc = close(fd);
	if (rc < 0) {
		printf("Fail to close file %s, errno = %d at line %d!\n", tmpname, errno, __LINE__);
		return (1);
	}
	rc = truncate(tmpname, 1234567);
	if (rc < 0) {
		printf("Fail to lengthen file %s, errno = %d at line %d!\n", tmpname, errno, __LINE__);
		return (1);
	}
	/*
	 * This following can return 11.  According to /usr/include/asm-generic/errno-base.h, it EAGAIN.
	 */
	rc = truncate(tmpname, 567);
	if (rc < 0) {
		printf("Fail to shorten file %s, errno = %d at line %d!\n", tmpname, errno, __LINE__);
		return (1);
	}
	rc = stat(tmpname, &buf);
	if (rc < 0) {
		printf("Fail to stat file %s, errno = %d at line %d!\n", tmpname, errno, __LINE__);
		return (1);
	}
	if (buf.st_size != 567) {
		printf("Unexpected size %d, errno = %d at line %d!\n", (int)buf.st_nlink, errno, __LINE__);
		return (1);
	}
	rc = unlink(tmpname);
	if (rc < 0) {
		printf("Fail to unlink %s, errno = %d at line %d!\n", tmpname, errno, __LINE__);
		return (1);
	}
	return (0);
}

/*
 * See if we handle open() properly.
 * Reference: pjd-fstest-20080816.tgz at http://www.tuxera.com/community/posix-test-suite/.
 */
int
test_open(void)
{
	int fd, rc;
	ssize_t len;
	struct stat buf;
	time_t ctime1, ctime2;
	time_t mtime1, mtime2;
	char *tmpname = "test-open.dat";

	fd = open(tmpname, O_CREAT|O_RDWR, S_IRWXU);
	if (fd < 0) {
		printf("Fail to create file %s, errno = %d at line %d!\n", tmpname, errno, __LINE__);
		return (1);
	}
	len = write(fd, "hello!\n", 6);
	if (len != 6) {
		printf("Fail to write file %s, errno = %d at line %d!\n", tmpname, errno, __LINE__);
		return (1);
	}
	rc = close(fd);
	if (rc < 0) {
		printf("Fail to close file %s, errno = %d at line %d!\n", tmpname, errno, __LINE__);
		return (1);
	}
	rc = stat(tmpname, &buf);
	if (rc < 0) {
		printf("Fail to stat file %s, errno = %d at line %d!\n", tmpname, errno, __LINE__);
		return (1);
	}
	ctime1 = buf.st_ctime;
	mtime1 = buf.st_mtime;
	sleep(1);
	fd = open(tmpname, O_WRONLY|O_TRUNC);
	if (fd < 0) {
		printf("Fail to create file %s, errno = %d at line %d!\n", tmpname, errno, __LINE__);
		return (1);
	}
	rc = close(fd);
	if (rc < 0) {
		printf("Fail to close file %s, errno = %d at line %d!\n", tmpname, errno, __LINE__);
		return (1);
	}
	rc = stat(tmpname, &buf);
	if (rc < 0) {
		printf("Fail to stat file %s, errno = %d at line %d!\n", tmpname, errno, __LINE__);
		return (1);
	}
	ctime2 = buf.st_ctime;
	mtime2 = buf.st_mtime;
	if (ctime2 - ctime1 < 1) {
		printf("Invalid ctime %ld - %ld at line %d!\n", (long)ctime1, (long)ctime2, __LINE__);
		return (1);
	}
	if (mtime2 - mtime1 < 1) {
		printf("Invalid mtime %ld - %ld at line %d!\n", (long)mtime1, (long)mtime2, __LINE__);
		return (1);
	}
	rc = unlink(tmpname);
	if (rc < 0) {
		printf("Fail to unlink %s, errno = %d at line %d!\n", tmpname, errno, __LINE__);
		return (1);
	}
	return (0);
}

#define	BUF_SIZE	132*1024*1024

static int get_random(char *buf, int size)
{
	int ret;
	FILE *fp;

	fp = fopen("/dev/urandom", "r");
	ret = fread(buf, 1, size, fp);
	fclose(fp);
	return ret;
}

static int test_large()
{
	char *buf;
	int fd, ret;
	size_t i, offset;
	unsigned char ch1, ch2;
	char *filename = "large-file-test.dat";

	buf = malloc(BUF_SIZE);
	if (buf == NULL) {
		printf("Error allocating buffer.\n");
		return (1);
	}

	ret = get_random(buf, BUF_SIZE);
	if (ret != BUF_SIZE) {
		printf("Error reading random data.\n");
		return (1);
	}

#if 0
	printf("%d random bytes have been retrieved...\n", BUF_SIZE);
	for (i = 0; i < 100; i++) {
		printf("0x%02x, ", (unsigned char)buf[i]);
		fflush(stdout);
	}
#endif

	fd = open(filename, O_RDWR | O_CREAT | O_TRUNC, 0600);
	if (fd == -1) {
		printf("Error opening file %s for writing at line %d.\n", filename, __LINE__);
		return (1);
	}

	ret = write(fd, buf, BUF_SIZE);
	if (ret != BUF_SIZE) {
		printf("Error writing last byte of the file.");
		return (1);
	}

	close(fd);

	fd = open(filename, O_RDWR);
	if (fd == -1) {
		printf("Error opening file %s for writing at line %d.\n", filename, __LINE__);
		return (1);
	}

	for (i = 0; i < BUF_SIZE; i++) {
		ret = read(fd, &ch1, 1);
		if (ret != 1) {
			printf("Error reading file %s for writing.", filename);
			return (1);
		}
		ch2 = buf[i];
		if (ch1 != ch2) {
			printf("Unexpected contents: 0x%02x vs 0x%02x", ch1, ch2);
			return (1);
		}
	}
	offset = 128*1024*1024 - 157;
	for (i = offset; i < offset + 834; i++)
		buf[i] = buf[i] - i;

	ret = lseek(fd, offset, SEEK_SET);
	if (ret != (int)offset) {
		printf("Error seeking file %s for writing.", filename);
		return (1);
	}

	for (i = offset; i < offset + 834; i++) {
		ch1 = buf[i];
		ret = write(fd, &ch1, 1);
		if (ret != 1) {
			printf("Error writing file %s for writing.", filename);
			return (1);
		}
	}
	close(fd);

	fd = open(filename, O_RDWR);
	if (fd == -1) {
		printf("Error opening file %s for writing.", filename);
		return (1);
	}

	for (i = 0; i < BUF_SIZE; i++) {
		ret = read(fd, &ch1, 1);
		if (ret != 1) {
			printf("Error reading file %s for writing.", filename);
			return (1);
		}
		ch2 = buf[i];
		if (ch1 != ch2) {
			printf("Unexpected contents: 0x%02x vs 0x%02x", ch1, ch2);
			return (1);
		}
	}
	free(buf);
	close(fd);
	unlink(filename);
	return (0);
}

struct test_desc test_list[] = {

	{
		"Basic file/directory creations and deletions",
		test_basic
	},
	{
		"Create a file with the name that has just been renamed",
		test_rename
	},
	{
		"Random 8-byte writes at random offsets to simulate FUSE I/O",
		test_random
	},
	{
		"A non-owner changes the owner/group of a file to 65535:65535",
		test_chown
	},
	{
		"Test basic link support",
		test_link
	},
	{
		"Test basic truncate support",
		test_truncate
	},
	{
		"Test basic open support",
		test_open
	},
	{
		"Test basic large file I/O",
		test_large
	},
	{
		NULL,
		NULL
	}
};

int
main(int argc, char *argv[])
{
	int total, success;
	int c, rc, pid, index, testindex, listonly;

	listonly = 0;
	testindex = 0;

	if (geteuid() != 0) {
		printf("Please run this program as root.\n");
		exit(0);
	}

	while ((c = getopt(argc, argv, "s:r:l")) != -1) {
		switch (c) {
		    case 's':
			seed = atoi(optarg);
			break;
		    case 'l':
			listonly = 1;
			break;
		    case 'r':
			testindex = atoi(optarg);
			break;
		    default:
			usage();
		}
	}
	argc -= optind;
	argv += optind;
	if (argc != 1 && !listonly)
		usage();

	if (listonly)  {
		index = 0;
		while (1) {
			if (test_list[index].descp == NULL)
				break;
			printf("Test item %d: %s\n",index+1, test_list[index].descp);
			index++;
		}
		exit(0);
	}

	pid = getpid();
	snprintf(workdir, sizeof(workdir), "%s/%d", argv[0], pid);
	rc = mkdir(workdir, S_IRWXU);
	if (rc < 0)
		err(1, "mkdir %s", workdir);
	rc = chdir(workdir);
	if (rc < 0)
		err(1, "chdir %s", workdir);

	warnx("seed = %u, workdir = %s", seed, workdir);
	srandom(seed);

	index = 0;
	total = success = 0;
	while (1) {
		if (test_list[index].descp == NULL)
			break;
		if (testindex && index + 1 != testindex) {
			index++;
			continue;
		}
		total++;
		printf("Running test %d: %s...\n",index+1, test_list[index].descp);
		rc = (*test_list[index].funcp)();
		if (!rc)
			success++;
		index++;
	}
	printf("Total number of tests = %d, number of successes = %d\n", total, success);
	exit(0);
}
