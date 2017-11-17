/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2016, Pittsburgh Supercomputing Center
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
 * 03/07/2013: bigfile1.c, read and write random regions within a file.
 */
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#define	MAX_BUF_LEN		8192

#define	BASE_NAME_MAX		128
#define BASE_NAME_SUFFIX	10

int value;
int dryrun;
int verbose;
int setvalue;

char scratch[MAX_BUF_LEN];

struct testfile {
	size_t size;
	int    fd;
	char  *buf;
	char   name[BASE_NAME_MAX+BASE_NAME_SUFFIX];
};

#define	TOTAL_NUM_FILES		9

struct testfile files[TOTAL_NUM_FILES] = {
	{ 
		123,
	},
	{ 
		24789,
	},
	{ 
		770924789,
	},
	{ 
		524789,
	},
	{ 
		22524789,
	},
	{ 
		57824789,
	},
	{ 
		52478900,
	},
	{ 
		2111524789,
	},
	{ 
		3111520000,
	}
};

create_file(int i)
{
	int j = 0;
	off_t offset = 0;
	size_t tmp1, tmp2;

	tmp1 = files[i].size;

	if (dryrun)
		return;

	while (j < 20) {
		tmp2 = write(files[i].fd, files[i].buf + offset, tmp1);
		if (tmp2 < 0) {
			printf("Fail to write file %s, errno = %d\n", files[i].name, errno);
			exit (1);
		}
		j++;
		if (tmp1 == tmp2) {
			printf("File %s has been created with %d attempts\n", files[i].name, j);
			return;
		}
		offset += tmp2;
		tmp1 = tmp1 - tmp2;	
	}
	printf("Can't finish creating file %s within 20 attempts\n", files[i].name);
	exit (1);
}

read_file(int i)
{
	off_t offset;
	size_t j, k, size, tmp1, tmp2;

	offset = random();
	offset = (1.0 * offset / RAND_MAX) * files[i].size;
	if (offset == files[i].size)
		offset--;
	
	if (!dryrun) {
		tmp1 = lseek(files[i].fd, offset, SEEK_SET);
		if (tmp1 != offset) {
			printf("Seek fail: file = %d, offset = %d\n", i, j);
			exit (1);
		}
	}

	if (files[i].size - offset > MAX_BUF_LEN) {
		size = random();
		size = (1.0 * size / RAND_MAX) * MAX_BUF_LEN;
	} else {
		size = random();
		size = (1.0 * size / RAND_MAX) * (files[i].size - offset);
	}

	tmp1 = size;
	if (verbose || dryrun)
		printf("Read  %6d bytes from file %s at offset %12ld\n", tmp1, files[i].name, offset);

	if (dryrun)
		return;

	tmp2 = read(files[i].fd, scratch, tmp1);
	if (tmp1 != tmp2) {
		printf("Read fail: file = %d, offset = %d, errno = %d\n", i, offset, errno);
		exit (1);
	}

	for (j = 0; j < size; j++) {
		if (scratch[j] != files[i].buf[offset + j]) {
			printf("Data mismatch: file = %s, offset = %d, size = %d\n\n", 
				files[i].name, offset, size);
			tmp1 = 0;
			for (k = j; k < size; k++) {
				if (tmp1++ > 512)
					break;
				printf("%5d: %#02x - %#02x\n", offset + k, 
					(unsigned char)scratch[k], 
					(unsigned char)files[i].buf[offset + k]);
			}
			exit (1);
		}
	}
}

write_file(int i)
{
	off_t offset;
	char *buf, ch;
	size_t j, size, tmp1, tmp2;

	offset = random();
	offset = (1.0 * offset / RAND_MAX) * files[i].size;
	
	if (!dryrun) {
		tmp1 = lseek(files[i].fd, offset, SEEK_SET);
		if (tmp1 != offset) {
			printf("Seek fail: file = %d, offset = %d\n", i, offset);
			exit (1);
		}
	}

	size = random();
	size = (1.0 * size / RAND_MAX) * (files[i].size - offset);

	assert(size >= 0);

	while (size) {
		if (size <= 512) 
			tmp1 = size;
		else if (size >= MAX_BUF_LEN) {
			tmp1 = random();
			tmp1 = (1.0 * tmp1 / RAND_MAX) * MAX_BUF_LEN;
		} else {
			tmp1 = random();
			tmp1 = (1.0 * tmp1 / RAND_MAX) * size;
		}
		if (tmp1 == 0)
			tmp1 = 1;

		if (verbose || dryrun)
			printf("Write %6d bytes to file %s at offset %12ld\n", tmp1, files[i].name, offset);

		/* always tweak some data on each write */
		buf = files[i].buf + offset;
		for (j = 0; j < tmp1; j++) {
			if (setvalue)
				ch = (char)value;
			else
				ch = (char)random();

#ifdef NOZERO
			if (ch == 0)
				ch = 0x55;
#endif

			if (!dryrun)
				buf[j] = ch;
		}

		if (dryrun)
			goto skip;

		tmp2 = write(files[i].fd, files[i].buf + offset, tmp1);
		if (tmp1 != tmp2) {
			printf("Write fail: file = %d, offset = %d, errno = %d\n", i, offset, errno);
			exit (1);
		}
			
 skip:
		offset += tmp1;
		size -= tmp1;
	}
}

int main(int argc, char *argv[])
{
	char *name, ch;
	size_t tmp;
	int rc, times = 10;
	unsigned int seed = 1234;
	size_t i, j, c, fd, nfile;
	struct timeval t1, t2, t3;

	gettimeofday(&t1, NULL);
	while ((c = getopt(argc, argv, "ds:n:vV:")) != -1) {
		switch (c) {
			case 's':
				seed = atoi(optarg);
				break;
                        case 'n':
				times = atoi(optarg);
				break;
                        case 'v':
				verbose = 1;
				break;
                        case 'V':
				setvalue = 1;
				value = atoi(optarg);
                        case 'd':
				dryrun = 1;
				break;
		}   
	}
	if (optind > argc - 1) {
#if 0
		printf("optind = %d, argc - 1 = %d\n", optind, argc - 1);
#endif
		printf("Usage: a.out [-v] [-s seed] [-V value ] [-n count] name\n");
		exit (1);
	}   

	if (strlen(argv[optind]) > BASE_NAME_MAX) {
		printf("Base name is too long\n");
		exit (1);
	}

	srandom(seed);
	nfile = sizeof(files)/sizeof(struct testfile);

	if (setvalue)
		printf("Base name = %s, file count = %d, seed = %u, value = %02x, loop = %d.\n\n", 
			argv[optind], nfile, seed, value, times);
	else
		printf("Base name = %s, file count = %d, seed = %u, loop = %d.\n\n", 
			argv[optind], nfile, seed, times);

	for (i = 0; i < nfile; i++) {

		snprintf(files[i].name, 128, "%s.%02d", argv[optind], i);
		printf("Try to allocate %12ld bytes of working memory for file %s\n", 
			files[i].size, files[i].name); 


		fflush(stdout);
		if (!dryrun) {
			files[i].buf = malloc(files[i].size);
			if (!files[i].buf) {
				printf("Fail to allocate memory, errno = %d\n", errno);
				exit (1);
			}
		}

		for (j = 0; j < files[i].size; j++) {
			if (setvalue)
				ch = (char)value;
			else
				ch = (char)random();

#ifdef NOZERO
			if (ch == 0)
				ch = 0x55;
#endif

			if (!dryrun)
				files[i].buf[j] = ch;
		}
	}
	printf("\nMemory for %d files have been allocated/initialized successfully.\n\n", nfile);
	fflush(stdout);

	for (i = 0; i < nfile; i++) {

		if (dryrun)
			continue;

	        files[i].fd = open(files[i].name, O_RDWR | O_CREAT | O_TRUNC, 0600);
		if (files[i].fd < 0) {
			printf("Fail to create file %s, errno = %d\n", files[i].name, errno);
			exit (1);
		}
		create_file(i);

	        close(files[i].fd);
	}
	printf("\nInitial %d files have been created successfully.\n\n", nfile);
	fflush(stdout);

	for (i = 0; i < nfile; i++) {

		if (dryrun)
			continue;

        	files[i].fd = open(files[i].name, O_RDWR);
		if (files[i].fd < 0) {
			printf("Fail to open file %s, errno = %d\n", files[i].name, errno);
			exit (1);
		}
	}

	for (j = 0; j < times; j++) {
		for (i = 0; i < nfile; i++) {
			read_file(i);
		}
		for (i = 0; i < nfile; i++) {
			write_file(i);
		}
		printf("Loop %2d on %d files is done successfully.\n", j, nfile);
		fflush(stdout);
	}

	printf("\n");
	for (i = 0; i < nfile; i++) {
	        close(files[i].fd);
        	rc = unlink(files[i].name);
		if (rc < 0) {
			printf("Fail to unlink file %s, errno = %d.\n", files[i].name, errno);
			exit (1);
		}
		printf("Test file %s has been removed successfully!\n", files[i].name);
	}
	gettimeofday(&t2, NULL);

	if (t2.tv_usec < t1.tv_usec) {
		t2.tv_usec += 1000000;
		t2.tv_sec--;
	}

	t3.tv_sec = t2.tv_sec - t1.tv_sec;
	t3.tv_usec = t2.tv_usec - t1.tv_usec;

	printf("\nTotal elapsed time is %02d:%02d:%02d.\n", t3.tv_sec / 3600, (t3.tv_sec % 3600) / 60, t3.tv_sec % 60);
	exit (0);
}
