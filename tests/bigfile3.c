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
	off_t j;
	int i, ret;
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
	struct stat stb;
	char rand_statebuf[32];
	struct timeval t1, t2, t3;
	int i, j, k, fd, ret, error, nthreads, readonly;
	struct random_data rand_state;
	size_t c, off, seed, size, bsize, nblocks;

	error = 0;
	readonly = 0;
	bsize = 71781;
	nthreads = 5;
	nblocks = 243456;
	seed = 35438;
	gettimeofday(&t1, NULL);

	while ((c = getopt(argc, argv, "rb:s:n:t:")) != -1) {
		switch (c) {
			case 'r':
				readonly = 1;
				break;
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
		exit(1);
	} 
	buf = malloc(bsize);
	if (buf == NULL) {
		printf("Allocation failed with errno = %d\n", errno);
		exit(1);
	}
	filename = argv[optind];
	printf("seed = %6d, # of threads = %4d, block size = %8d, nblocks = %8d, file size = %12ld.\n\n", 
		seed, nthreads, bsize, nblocks, (long)nthreads * (long)nblocks * bsize);
	fflush(stdout);

	if (readonly)
		goto verify;

       	fd = open(filename, O_RDWR | O_CREAT | O_TRUNC, 0600);
	if (fd < 0) {
		printf("Fail to open file, errno = %d\n", errno);
		exit(1);
	}
        close(fd);

	for (i = 0; i < nthreads; i++) {
		args[i].id = i;
		args[i].fd = open(filename, O_RDWR, 0600);
		if (args[i].fd < 0) {
			printf("Fail to open file, errno = %d\n", errno);
			exit(1);
		}
		args[i].ret = 0;
		args[i].seed = seed;
		args[i].bsize = bsize;
		args[i].nblocks = nblocks;
		args[i].nthreads = nthreads;
		ret = pthread_create(&threads[i], NULL, &thread_worker, &args[i]);
		if (ret < 0) {
			printf("pthread_create failed with %d (%s)\n", ret, strerror(ret));
			exit(1);
		}
	}
	for (i = 0; i < nthreads; i++) {
		pthread_join(threads[i], NULL);
		printf("Thread %3d is done with errno = %3d, fd = %3d\n", 
			i, args[i].ret, args[i].fd);
		error += args[i].ret;
		close(args[i].fd);
		fflush(stdout);
	}

	if (error)
		goto out;

 verify:

	printf("\nAll threads have exited. Now verifying file contents ... \n\n");
	memset(rand_statebuf, 0, sizeof(rand_statebuf));
	memset(&rand_state, 0, sizeof(rand_state));
	initstate_r(seed, rand_statebuf, sizeof(rand_statebuf), &rand_state);
	fflush(stdout);

       	fd = open(filename, O_RDONLY);
	ret = fstat(fd, &stb);
	if (ret < 0) {
		printf("fstat failed with %d (%s)\n", ret, strerror(ret));
		exit(1);
	}
	if (stb.st_size != (off_t)nthreads * nblocks * bsize) {
		printf("File size mismatch: %ld vs %ld\n",stb.st_size,  
			(off_t)nthreads * nblocks * bsize);
		exit(1);
	}
	
	off = 0;
	for (i = 0; i < nthreads; i++) {
		for (j = 0; j < nblocks; j++) {
			ret = read(fd, buf, bsize);
			if (ret != bsize) {
				printf("Read file failed with errno = %d.\n", errno);
				exit(1);
			}
			for (k = 0; k < bsize; k++) {
				random_r(&rand_state, &result);
				if (buf[k] != (unsigned char)result & 0xff) {
					error++;
					printf("%4d: File corrupted offset = %ld (%d:%d:%d): %02x vs %02x\n", 
						error, off+k, i, j, k, buf[k], result & 0xff);
					fflush(stdout);
					if (error > 2048)
						goto out;
				}
			}
			off += bsize;
		}
	}
	close(fd);

 out:
	if (!error) {
		error = unlink(filename);
		if (error)
			printf("Fail to delete file %s, errno = %d.\n", filename, errno);
		else
			printf("File %s has been deleted successfully!\n", filename);
	}

	gettimeofday(&t2, NULL);

	if (t2.tv_usec < t1.tv_usec) {
		t2.tv_usec += 1000000;
		t2.tv_sec--;
	}

	t3.tv_sec = t2.tv_sec - t1.tv_sec;
	t3.tv_usec = t2.tv_usec - t1.tv_usec;

	printf("\n\n%s: Total elapsed time is %02d:%02d:%02d.\n", 
		error ? "Test failed" : "Test succeeded", 
		t3.tv_sec / 3600, (t3.tv_sec % 3600) / 60, t3.tv_sec % 60);

	if (error) {
		printf("Please check file %s.\n", filename);
		exit(1);
	}
	exit(0);
}
