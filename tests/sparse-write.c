/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2017, Pittsburgh Supercomputing Center
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
/*
 * 04/26/2017: force bmap allocation to test MDS's ability to round robin.
 */
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>

int
main(int argc, char **argv)
{
	off_t offset;
	int fd, ch, rc;
	char *filename;
	long i, j, val, seed, size, count;

	if (argc != 3) {
		printf("a.out filename count\n");
		exit(0);
	}
	filename = argv[1];
	count = atol(argv[2]);

	i = 0;
	seed = 1234;
	srandom(seed);

	fd = open(filename, O_CREAT|O_WRONLY|O_TRUNC, 0600);
	if (fd < 0) {
		printf("Fail to open file %s, errno = %d.\n", filename, errno);
		exit(0);
	}
	while (i < count) {
		for (j = 0; j < 128; j++) {
			val = random();
			ch = val;
			size = write(fd, &ch, 1);
			if (size != 1) {
				printf("Fail to write file, offset = %ld, errno = %d.\n", i, errno);
				exit(0);
			}
		}
		i++;
		offset = i * 128*1024*1024;
		if (lseek(fd, offset, SEEK_SET) < 0) {
			printf("Fail to seek file, offset = %ld, errno = %d.\n", i, errno);
			exit(0);
		}
	}
	close(fd);
}
