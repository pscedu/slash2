/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015, Google, Inc.
 * Copyright (c) 2010-2013, Pittsburgh Supercomputing Center (PSC).
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
#include <sys/mman.h>
#include <sys/stat.h>

#include <err.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#define	FILE_SIZE	4978

int
main(int argc, char *argv[])
{
	char ch;
	char *map;
	int i, fd, ret, choice = 0;
	char *filename = "mmap-test.dat";

	if (argc == 2) 
		choice = atoi(argv[1]);

	fd = open(filename, O_RDWR | O_CREAT | O_TRUNC, 0600);
	if (fd == -1)
		err(1, "Error opening file %s for writing.", filename);
	ret  = lseek(fd, FILE_SIZE - 1, SEEK_SET);
	if (ret == -1)
		err(1, "Error lseeking to the end of the file.");
	ch = 0x30;
	ret = write(fd, &ch, 1);
	if (ret != 1)
		err(1, "Error writing last byte of the file.");
	if (choice) {
		/*
 		 * This should fail because we turn on direct_io on SLASH2.
 		 */
		printf("Using shared mapping\n");
		map = mmap(0, FILE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	} else {
		printf("Using private mapping\n");
		map = mmap(0, FILE_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);
	}
	if (map == MAP_FAILED)
		err(1, "Error mmapping the file.");
	for (i = 0; i < FILE_SIZE; i++) {
		map[i] = (i % 10) + 0x30;
	}
	if (munmap(map, FILE_SIZE) == -1)
		err(1, "Error un-mmapping the file.");
	close(fd);
	exit(0);
}
