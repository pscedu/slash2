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
	char *buf;
	int i, j, ret;
	int32_t result;
	char rand_statebuf[32];
	struct random_data rand_state;
	struct arg_and_ret *myarg = (struct arg_and_ret *)arg;

	memset(rand_statebuf, 0, sizeof(rand_statebuf));
	memset(&rand_state, 0, sizeof(rand_state));
	initstate_r(myarg->seed, rand_statebuf, sizeof(rand_statebuf), &rand_state);

	buf = malloc(myarg->bsize);
	for (i = 0; i < myarg->id * myarg->bsize; i++)
		random_r(&rand_state, &result);
	lseek(myarg->fd, i, SEEK_SET);

	myarg->ret = 0;
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
		for (j = 0; j < myarg->bsize * myarg->nthreads; j++)
			random_r(&rand_state, &result);
		lseek(myarg->fd, j, SEEK_CUR);
	}
	pthread_exit(NULL);
}

int main(int argc, char *argv[])
{
	char *filename;
	int fd, ret, nthreads;
	size_t i, c, seed, size, bsize, nblocks, remainder = 0;

	bsize = 5678;
	nthreads = 5;
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
                        case 't':
				nthreads = atoi(optarg);
				break;
		}   
	}
	if (optind != argc - 1 || nthreads > MAX_THREADS) {
		printf("Usage: a.out [-s seed] [-b bsize] [-n nblocks ] [-t nthreads (1-128)] filename\n");
		exit(0);
	} 
	filename = argv[optind];
       	fd = open(filename, O_RDWR | O_CREAT | O_TRUNC, 0600);
	if (fd < 0) {
		printf("Fail to open file, errno = %d\n", errno);
		exit(0);
	}

	for (i = 0; i < nthreads; i++) {
		args[i].id = i;
		args[i].fd = fd;
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
	}

        close(fd);
}
