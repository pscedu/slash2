/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2010, Pittsburgh Supercomputing Center (PSC).
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


#include <sys/types.h>
#include <sys/stat.h>

#include <err.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <unistd.h>

#include "pfl/cdefs.h"

#define		 DEFAULT_SEED		123456
#define 	 TOTAL_WRITES		65537

unsigned int	 seed = DEFAULT_SEED;
char		*progname;

char		 workdir[1024];

__dead void
usage(void)
{
	fprintf(stderr,
	    "usage: %s [-s seed] [-l] [directory]\n",
	    progname);
	exit(1);
}

struct bug_history {
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
	/* the bug causes the following to fail with EEXIST */
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

struct bug_history bug_list[] = {

	{
		"Basic file/directory creations and deletions",
		test_basic
	},
	{
		"Create a file with the name that has just been renamed",
		test_rename
	},
	{
		"Random writes and random offset to simulate FUSE I/O",
		test_random
	},
	{
		NULL,
		NULL
	}
};

int
main(int argc, char *argv[])
{
	int c, rc, pid, index, listonly;

	listonly = 0;
	progname = strrchr(argv[0], '/');
	if (progname == NULL)
		progname = argv[0];
	else
		progname++;

	while ((c = getopt(argc, argv, "s:l")) != -1) {
		switch (c) {
		    case 's':
			seed = atoi(optarg);
			break;
		    case 'l':
			listonly = 1;
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
			if (bug_list[index].descp == NULL)
				break;
			printf("Test item %d: %s\n",index, bug_list[index].descp);
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

	printf("program = %s, seed = %u, workdir = %s\n", progname, seed, workdir);
	srandom(seed);

	index = 0;
	while (1) {
		if (bug_list[index].descp == NULL)
			break;
		printf("Checking item %d: %s\n",index, bug_list[index].descp);
		rc = (*bug_list[index].funcp)();
		if (rc)
			break;
		index++;
	}
	exit(0);
}
