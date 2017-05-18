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
 * 01/22/2017: bigfile4.c, truncate a file repeatedly.
 */
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <pthread.h>

int main(int argc, char *argv[])
{
	char *filename;
	int i, fd, ret;
	unsigned char *buf, val;
	struct timeval t1, t2, t3;
	size_t ch, off, seed, size;

	seed = 35438;
	size = 12345678;
	gettimeofday(&t1, NULL);

	while ((ch = getopt(argc, argv, "s:S:")) != -1) {
		switch (ch) {
			case 's':
				seed = atoi(optarg);
				break;
                        case 'S':
				size = atol(optarg);
				break;
		}   
	}
	if (optind != argc - 1) {
		printf("Usage: a.out [-s seed] [-S size] filename\n");
		exit(1);
	} 
	buf = malloc(size);
	if (buf == NULL) {
		printf("Allocation failed with errno = %d\n", errno);
		exit(1);
	}
	filename = argv[optind];
	printf("seed = %d, size = %ld, file name = %s.\n\n", seed, size, filename);

	fflush(stdout);

       	fd = open(filename, O_RDWR | O_CREAT | O_TRUNC, 0600);
	if (fd < 0) {
		printf("Fail to open file, errno = %d\n", errno);
		exit(1);
	}
	ret = truncate(filename, size);
	if (ret < 0) {
		printf("Fail to truncate file, errno = %d\n", errno);
		exit(1);
	}
	ret = read(fd, buf, size);
	if (ret != size) {
		printf("Fail to read file, errno = %d\n", errno);
		exit(1);
	}
	srandom(seed);
	for (i = 0; i < size; i++) {
		if (buf[i]) {
			printf("File content is corrupted (0 versus %x)\n", buf[i]);
			exit(1);
		}
		buf[i] = random();
	}

	ret = lseek(fd, 0, SEEK_SET);
	if (ret < 0) {
		printf("Fail to lseek file, errno = %d\n", errno);
		exit(1);
	}

	ret = write(fd, buf, size);
	if (ret != size) {
		printf("Fail to write file, errno = %d\n", errno);
		exit(1);
	}
        close(fd);

	/* file has been filled with random contents, now open it again */

       	fd = open(filename, O_RDWR, 0600);
	off = size * 3/4;

	printf("Truncate file to %d bytes ...\n", off);
	ret = truncate(filename, off);
	if (ret < 0) {
		printf("Fail to truncate file, errno = %d\n", errno);
		exit(1);
	}

	printf("Truncate file to %d bytes ...\n", size);
	ret = truncate(filename, size);
	if (ret < 0) {
		printf("Fail to truncate file, errno = %d\n", errno);
		exit(1);
	}
	srandom(seed);
	ret = read(fd, buf, size);
	if (ret != size) {
		printf("Fail to read file, errno = %d\n", errno);
		exit(1);
	}
	for (i = 0; i < size; i++) {
		val = random();
		if (i <  off && buf[i] != val) {
			printf("File content is corrupted (%#x versus %#x)\n", buf[i], val);
			exit(1);
		}
		if (i >= off && buf[i]) {
			printf("File content is corrupted (%#x versus %#x)\n", buf[i], 0);
			exit(1);
		}
	}

	gettimeofday(&t2, NULL);

	if (t2.tv_usec < t1.tv_usec) {
		t2.tv_usec += 1000000;
		t2.tv_sec--;
	}

	t3.tv_sec = t2.tv_sec - t1.tv_sec;
	t3.tv_usec = t2.tv_usec - t1.tv_usec;

	printf("\nTotal elapsed time is %02d:%02d:%02d.\n", t3.tv_sec / 3600, (t3.tv_sec % 3600) / 60, t3.tv_sec % 60);
	exit(0);
}
