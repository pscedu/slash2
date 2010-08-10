/* $Id: namespace.c 11864 2010-05-25 16:47:12Z yanovich $ */

#include <sys/types.h>
#include <sys/stat.h>

#include <dirent.h>
#include <err.h>
#include <time.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "pfl/cdefs.h"
#include "psc_ds/queue.h"

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
test_rename(void)
{
	int rc;
	char *tmpname1 = "test-rename1.dir";
	char *tmpname2 = "test-rename2.dir";

	rc = mkdir(tmpname1, S_IRWXU);
	if (rc) {
		printf("Fail to create directory %s, errno = %d at line %d!\n", tmpname1, errno, __LINE__);
		return (1);
	}
	rc = rename(tmpname1, tmpname2);
	if (rc) {
		printf("Fail to rename directory %s, errno = %d at line %d!\n", tmpname1, errno, __LINE__);
		return (1);
	}
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
		printf("Fail to remove directory %s, errno = %d at line %d!\n", tmpname1, errno, __LINE__);
		return (1);
	}
	return (0);

}

struct bug_history bug_list[] = {

	{
		"Create a file with the name that has just been renamed",
		test_rename
	},
	{
		NULL,
		NULL
	}
};

int main(int argc, char *argv[])
{
	int c, rc, pid, index;

	progname = strrchr(argv[0], '/');
	if (progname == NULL)
		progname = argv[0];
	else
		progname++;

	while ((c = getopt (argc, argv, "s:")) != -1) {
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
	sprintf(workdir, "%s/%d", argv[0], pid);
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
		printf("Checking bug: %s\n", bug_list[index].descp);
		rc = (*bug_list[index].funcp)();
		if (rc)
			break;
		index++;
	}
	exit(0);
}

