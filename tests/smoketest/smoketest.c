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

unsigned int	 seed = DEFAULT_SEED;
char		*progname;

char		 workdir[1024];

__dead void
usage(void)
{
	fprintf(stderr,
	    "usage: %s [-s seed] directory\n",
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
		NULL,
		NULL
	}
};

int
main(int argc, char *argv[])
{
	int c, rc, pid, index;

	progname = strrchr(argv[0], '/');
	if (progname == NULL)
		progname = argv[0];
	else
		progname++;

	while ((c = getopt(argc, argv, "s:")) != -1) {
		switch (c) {
		case 's':
			seed = atoi(optarg);
			break;
		default:
			usage();
		}
	}
	argc -= optind;
	argv += optind;
	if (argc != 1)
		usage();

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
