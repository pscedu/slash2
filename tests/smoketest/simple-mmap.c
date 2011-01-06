/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2010-2011, Pittsburgh Supercomputing Center (PSC).
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
	int i, fd, ret;
	char *filename = "mmap-test.dat";

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
	map = mmap(0, FILE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
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
