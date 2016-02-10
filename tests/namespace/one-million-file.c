/* $Id$ */
/*
 * Create a large amount of files while crashing the MDS from time to time.
 * Filenames are made unique by permutation.
 */

#include <sys/types.h>
#include <sys/stat.h>

#include <err.h>
#include <time.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* more than 10 is possible, I can use symbols other than digits to represent them */

#define		 NAME_LENGTH 10		/* 10! = 3628800 */

time_t		 time1;
time_t		 time2;

int		 action = 0;
int		 verbose = 0;
long		 file_count = 0;
long		 total_files = 1000000;

char		 filename[NAME_LENGTH+1];

void
usage(void)
{
	extern const char *__progname;

	fprintf(stderr,
	    "usage: %s [-n total files] [-v] empty-directory\n",
	    __progname);
	exit(1);
}


void doit(const int *values, const int size)
{
	int i, fd;
	struct stat sb;

	if (file_count >= total_files)
		return;

	for (i = 0; i < size; i++) {
		if (values[i] == 10)
			filename[i] = 0x30;
		else
			filename[i] = values[i] + 0x30;
	}
	filename[i] = '\0';

	file_count++;
	if (verbose)
		printf("%7d: ", file_count);

	switch (action) {
	    case 0:
		if (verbose)
			printf("Creating filename: %s\n", filename);
		fd = creat(filename, S_IRWXU);
		if (fd < 0)
			err(1, "Failed to create file %s", filename);
		close(fd);
		break;
	    case 1:
		if (verbose)
			printf("Stating filename: %s\n", filename);
		fd = stat(filename, &sb);
		if (fd < 0)
			err(1, "Failed to stat file %s", filename);
		break;
	    case 2:
		if (verbose)
			printf("Deleting filename: %s\n", filename);
		fd = unlink(filename);
		if (fd < 0)
			err(1, "Failed to delete file %s", filename);
		break;
	    case 3:
		/* dry run */
		if (verbose)
			printf("Generating filename: %s\n", filename);
		break;
	    default:
		printf("invalid action %d\n", action);
		break;
	}
}

void visit(int *values, int n, int k)
{
	int i;
	static int level = -1;

	level = level + 1;
	values[k] = level;

	if (level == n)
		doit(values, n);
	else
		for (i = 0; i < n; i++)
			if (values[i] == 0)
				visit(values, n, i);
	level = level-1;
	values[k] = 0;
}

int
main(int argc, char *argv[])
{
	int i;
	char ch;
	int *values;
	double elapsetime;

	while ((ch = getopt(argc, argv, "n:sdvt")) != -1) {
		switch (ch) {
		case 'n':
			total_files = atol(optarg);
			break;
		case 's':
			action = 1;
			break;
		case 'd':
			action = 2;
			break;
		case 't':
			action = 3;
			break;
		case 'v':
			verbose = 1;
			break;
		default:
			usage();
		}
	}
	argc -= optind;
	argv += optind;
	if (argc != 1)
		usage();

	if (chdir(argv[0]) == -1)
		err(1, "chdir %s", argv[0]);

	values = (int *)malloc(sizeof(int) * NAME_LENGTH);

	for (i = 0; i < NAME_LENGTH; i++) {
		values[i] = 0;
	}

	time(&time1);
	visit(values, NAME_LENGTH, 0);
	time(&time2);

	elapsetime = difftime(time2, time1);
	printf("Time used is %f seconds, total files = %d.\n", elapsetime, file_count);
	exit(0);
}
