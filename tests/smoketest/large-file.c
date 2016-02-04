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

#include <err.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include <sys/syscall.h>

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

int
main(int argc, char *argv[])
{
	char *buf;
	size_t offset;
	int i, fd, ret;
	unsigned char ch1, ch2;
	char *filename = "large-file-test.dat";

	buf = malloc(BUF_SIZE);
	if (buf == NULL)
		err(1, "Error allocating buffer.");

	ret = get_random(buf, BUF_SIZE);
	if (ret != BUF_SIZE)
		err(1, "Error reading random data.");

#if 0
	printf("%d random bytes have been retrieved...\n", BUF_SIZE);
	for (i = 0; i < 100; i++) {
		printf("0x%02x, ", (unsigned char)buf[i]);
		fflush(stdout);
	}
#endif

	fd = open(filename, O_RDWR | O_CREAT | O_TRUNC, 0600);
	if (fd == -1)
		err(1, "Error opening file %s for writing.", filename);

	ret = write(fd, buf, BUF_SIZE);
	if (ret != BUF_SIZE)
		err(1, "Error writing last byte of the file.");

	close(fd);

	fd = open(filename, O_RDWR);
	if (fd == -1)
		err(1, "Error opening file %s for writing.", filename);

	for (i = 0; i < BUF_SIZE; i++) {
		ret = read(fd, &ch1, 1);
		if (ret != 1)
			err(1, "Error reading file %s for writing.", filename);
		ch2 = buf[i];
		if (ch1 != ch2) 
			err(1, "Unexpected contents: 0x%02x vs 0x%02x", ch1, ch2);
	}
	offset = 128*1024*1024 - 157;
	for (i = offset; i < offset + 834; i++)
		buf[i] = buf[i] - i;

	ret = lseek(fd, offset, SEEK_SET);
	if (ret != offset)
		err(1, "Error seeking file %s for writing.", filename);

	for (i = offset; i < offset + 834; i++) {
		ch1 = buf[i];
		ret = write(fd, &ch1, 1);
		if (ret != 1)
			err(1, "Error writing file %s for writing.", filename);
	}
	close(fd);

	fd = open(filename, O_RDWR);
	if (fd == -1)
		err(1, "Error opening file %s for writing.", filename);

	for (i = 0; i < BUF_SIZE; i++) {
		ret = read(fd, &ch1, 1);
		if (ret != 1)
			err(1, "Error reading file %s for writing.", filename);
		ch2 = buf[i];
		if (ch1 != ch2) 
			err(1, "Unexpected contents: 0x%02x vs 0x%02x", ch1, ch2);
	}
	close(fd);

	exit(0);
}
