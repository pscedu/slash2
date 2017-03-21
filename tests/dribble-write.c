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
 * 03/21/2017: dribble write a file for testing purposes (kill IOS).
 */
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>

int
main(int argc, char **argv)
{
	int fd, ch, rc;
	char *filename;
	long i, j, val, seed, size, length;

	if (argc != 3) {
		printf("a.out filename length\n");
		exit(0);
	}
	filename = argv[1];
	length = atol(argv[2]);

	i = 0;
	j = 0;
	seed = 1234;
	srandom(seed);

	fd = open(filename, O_CREAT|O_WRONLY, 0600);
	if (fd < 0) {
		printf("Fail to open file %s, errno = %d.\n", filename, errno);
		exit(0);
	}
	while (i < length) {
		val = random();
		ch = val;
		size = write(fd, &ch, 1);
		if (size != 1) {
			printf("Fail to write file, offset = %ld, errno = %d.\n", i, errno);
			exit(0);
		}
		i++;
		/* sync from time to time */
		if ((val & 0xffff) == 0xffff) {
			rc = fsync(fd);
			printf("%2ld: Sync offset = %8ld, rc = %d.\n", j, i, rc);
			if (rc)
				break;
			j++;
		}
	}
	close(fd);
}
