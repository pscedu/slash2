/*  %GPL_START_LICENSE%
/*  ---------------------------------------------------------------------
/*  Copyright 2016, Pittsburgh Supercomputing Center
/*  All rights reserved.
/* 
/*  This program is free software; you can redistribute it and/or modify
/*  it under the terms of the GNU General Public License as published by
/*  the Free Software Foundation; either version 2 of the License, or (at
/*  your option) any later version.
/* 
/*  This program is distributed WITHOUT ANY WARRANTY; without even the
/*  implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
/*  PURPOSE.  See the GNU General Public License contained in the file
/*  `COPYING-GPL' at the top of this distribution or at
/*  https://www.gnu.org/licenses/gpl-2.0.html for more details.
/*  ---------------------------------------------------------------------
/*  %END_LICENSE%
/*
 * 06/21/2016: bigfile2.c, read and write a file with different block sizes.
 */
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>

main(int argc, char *argv[])
{
	long val, fsize;
	char *filename;
	struct stat stbuf;
	unsigned char *buf;
	int fd, error = 0, readonly = 0;
	size_t i, j, c, seed, size, bsize, nblocks, remainder = 0;

	bsize = 5678;
	nblocks = 12345;
	seed = getpid();
	while ((c = getopt(argc, argv, "b:s:n:r")) != -1) {
		switch (c) {
			case 'b':
				bsize = atoi(optarg);
				break;
			case 's':
				seed = atoi(optarg);
				break;
                        case 'n':
				nblocks = atoi(optarg);
				break;
                        case 'r':
				readonly = 1;
				break;
		}   
	}
	if (optind != argc - 1) {
		printf("Usage: a.out [-s seed] [-b bsize] [-n nblocks ] filename\n");
		exit(0);
	}   
	filename = argv[optind];
	if (readonly)
        	fd = open(filename, O_RDONLY);
	else
        	fd = open(filename, O_RDWR | O_CREAT | O_TRUNC, 0600);
	if (fd < 0) {
		printf("Fail to open file, errno = %d\n", errno);
		exit(0);
	}
	if (fstat(fd, &stbuf) < 0) {
		printf("Failed to stat file %s, errno = %d\n", filename, errno);
		exit(0);
	}
	if (readonly) {
		fsize = stbuf.st_size;
		nblocks = fsize / bsize;
	} else {
		assert(!stbuf.st_size);
		fsize = (long)nblocks * bsize;
	}

	buf = malloc(bsize);
	if (buf == NULL) {
		printf("Fail to allocate memory, errno = %d\n", errno);
		exit(0);
	}
	srandom(seed);
	printf("Seed = %d, file name = %s, bsize = %d, blocks = %d, size = %ld\n", 
		seed, filename, bsize, readonly ? nblocks + 1: nblocks, fsize);

	for (i = 0; i < nblocks; i++) {
		if (readonly)
			size = read(fd, buf, bsize);
		else {
			for (j = 0; j < bsize; j++) {
				val = random();
				//printf("%lx\n", val);
				buf[j] = (unsigned char)val & 0xff;
			}
			size = write(fd, buf, bsize);
		}
		if (size != bsize) {
			printf("Fail to %s file, errno = %d\n",
				readonly ? "read" : "write", errno);
			break;
		}
		if (!readonly)
			continue;

		for (j = 0; j < bsize; j++) {
			val = random();
			//printf("%lx\n", val);
			if (buf[j] != (unsigned char)val & 0xff) {
				printf("Unexpected data at offset = %ld\n",
					i * nblocks * j);
				error++;
				break;
			}
		}
		if (error > 10)
			break;
	}
	if (error || !readonly || nblocks * bsize == fsize)
		goto done;

	remainder = fsize - (long)nblocks * bsize;
	size = read(fd, buf, remainder);
	if (size != remainder) {
		printf("Fail to %s file, errno = %d\n",
			readonly ? "read" : "write", errno);
		goto done;
	}
	for (i = 0; i < remainder; i++) {
		val = random();
		//printf("%lx\n", val);
		if (buf[i] != (unsigned char)val & 0xff) {
			printf("Unexpected data at offset = %ld\n",
				bsize * nblocks * i);
			error++;
		}
	}

 done:
	if (!error && readonly)
		printf("No corruption has been found (last block has %d bytes)\n", remainder);
	if (!error && !readonly)
		printf("File has been created successfully.\007\n");
        close(fd);
}
