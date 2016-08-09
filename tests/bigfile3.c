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
 * 06/24/2016: bigfile3.c, write a file concurrently by multiple threads.
 */
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <pthread.h>

#define	MAX_THREADS	128

struct arg_and_ret {
	int     id;
	int	fd;
	int	ret;
	int 	seed;
	int	bsize;
	int	nblocks;
	int	nthreads;
} args[MAX_THREADS];

pthread_t threads[MAX_THREADS];

static void* thread_worker(void *arg)
{
	int i, j, ret;
	int32_t result;
	unsigned char *buf;
	char rand_statebuf[32];
	struct random_data rand_state;
	struct arg_and_ret *myarg = (struct arg_and_ret *)arg;

	memset(rand_statebuf, 0, sizeof(rand_statebuf));
	memset(&rand_state, 0, sizeof(rand_state));
	initstate_r(myarg->seed, rand_statebuf, sizeof(rand_statebuf), &rand_state);

	buf = malloc(myarg->bsize);
	if (buf == NULL) {
		myarg->ret = errno;
		pthread_exit(NULL);
	}
	for (i = 0; i < myarg->id * myarg->bsize; i++)
		random_r(&rand_state, &result);

	lseek(myarg->fd, i, SEEK_SET);
	for (i = 0; i < myarg->nblocks; i++) {
		for (j = 0; j < myarg->bsize; j++) {
			random_r(&rand_state, &result);
			buf[j] = (unsigned char)result & 0xff;
		}
		ret = write(myarg->fd, buf, myarg->bsize);
		if (ret != myarg->bsize) {
			myarg->ret = errno;
			break;
		}
		for (j = 0; j < myarg->bsize * (myarg->nthreads - 1); j++)
			random_r(&rand_state, &result);
		lseek(myarg->fd, j, SEEK_CUR);
	}
	free(buf);
	pthread_exit(NULL);
}

int main(int argc, char *argv[])
{
	char *filename;
	int32_t result;
	unsigned char *buf;
	char rand_statebuf[32];
	int i, j, fd, ret, nthreads;
	struct random_data rand_state;
	size_t c, seed, size, bsize, nblocks;

	bsize = 7178;
	nthreads = 5;
	nblocks = 14345;
	seed = getpid();
	while ((c = getopt(argc, argv, "b:s:n:t:")) != -1) {
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
                        case 't':
				nthreads = atoi(optarg);
				break;
		}   
	}
	if (optind != argc - 1 || nthreads < 1 || nthreads > MAX_THREADS) {
		printf("Usage: a.out [-s seed] [-b bsize] [-n nblocks ] [-t nthreads (1-128)] filename\n");
		exit(0);
	} 
	buf = malloc(bsize);
	if (buf == NULL) {
		printf("Allocation failed with errno = %d\n", errno);
		exit(0);
	}
	filename = argv[optind];
       	fd = open(filename, O_RDWR | O_CREAT | O_TRUNC, 0600);
	if (fd < 0) {
		printf("Fail to open file, errno = %d\n", errno);
		exit(0);
	}
	printf("seed = %d, # of threads = %d, block size = %d, nblocks = %d, file size = %ld.\n\n", 
		seed, nthreads, bsize, nblocks, (long)nthreads * (long)nblocks * bsize);
	for (i = 0; i < nthreads; i++) {
		args[i].id = i;
		args[i].fd = open(filename, O_RDWR, 0600);
		if (args[i].fd < 0) {
			printf("Fail to open file, errno = %d\n", errno);
			exit(0);
		}
		args[i].ret = 0;
		args[i].seed = seed;
		args[i].bsize = bsize;
		args[i].nblocks = nblocks;
		args[i].nthreads = nthreads;
		ret = pthread_create(&threads[i], NULL, &thread_worker, &args[i]);
		if (ret < 0) {
			printf("pthread_create failed with %d (%s)\n", ret, strerror(ret));
			exit(0);
		}
	}
	for (i = 0; i < nthreads; i++) {
		pthread_join(threads[i], NULL);
		printf("Thread %3d is done with errno = %d, fd = %3d\n", 
			i, args[i].ret, args[i].fd);
		close(args[i].fd);
	}
        close(fd);

	printf("\nAll threads has exited. Now verifying file contents ... \n");
	memset(rand_statebuf, 0, sizeof(rand_statebuf));
	memset(&rand_state, 0, sizeof(rand_state));
	initstate_r(seed, rand_statebuf, sizeof(rand_statebuf), &rand_state);

       	fd = open(filename, O_RDONLY);
	for (i = 0; i < nblocks; i++) {
		ret = read(fd, buf, bsize);
		if (ret != bsize) {
			printf("Read file failed with errno = %d.\n", errno);
			exit(0);
		}
		for (j = 0; j < bsize; j++) {
			random_r(&rand_state, &result);
			if (buf[j] != (unsigned char)result & 0xff) {
				printf("File corrupted: %2x vs %2x\n", buf[j], result & 0xff);
				exit(0);
			}
		}
	}
	close(fd);
}
