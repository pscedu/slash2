/* $Id$ */

#include <sys/types.h>

#include <fcntl.h>
#include <stdio.h>

#include "slutil.h"

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
