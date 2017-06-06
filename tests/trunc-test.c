/*
 * 06/06/2017: trunc-test.c
 */
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

int
main(int argc, char **argv)
{
	long size;
	int i, fd, ret;
	struct stat stbuf;
	char *buf, *filename;

	if (argc != 3) {
		printf("Usage: a.out filename size\n");
		exit (0);
	}

	filename = argv[1];
	size = atol(argv[2]);
	if (size < 4096) {
		printf("The size of the file is too small!\n");
		exit (0);
	}

	srandom(1234);
	buf = malloc(size);

	for (i = 0; i < size; i++)
		buf[i] = random();

	fd = open(filename, O_RDWR|O_TRUNC|O_EXCL|O_CREAT, 0600);
	if (fd < 0) {
		printf("Open fails with errno = %d\n", errno);
		exit (0);
	}
	ret = ftruncate(fd, size);
	if (ret < 0) {
		printf("Truncate fails with errno = %d\n", errno);
		exit (0);
	}
	ret = lseek(fd, size - 1234, SEEK_SET);
	if (ret < 0) {
		printf("Seek fails with errno = %d\n", errno);
		exit (0);
	}

	ret = write(fd, buf, 4096);
	if (ret != 4096) {
		printf("Write fails with errno = %d\n", errno);
		exit (0);
	}

	ret = stat(filename, &stbuf);
	if (ret < 0) {
		printf("Stat fails with errno = %d\n", errno);
		exit (0);
	}
	if (stbuf.st_size != size - 1234 + 4096) {
		printf("Unexpected file size = %ld\n", stbuf.st_size);
		exit (0);
	}

	ret = ftruncate(fd, 1024);
	if (ret < 0) {
		printf("Truncate fails with errno = %d\n", errno);
		exit (0);
	}
	ret = lseek(fd, 0, SEEK_SET);
	if (ret < 0) {
		printf("Seek fails with errno = %d\n", errno);
		exit (0);
	}

	ret = read(fd, buf, 10240);
	if (ret < 0) {
		printf("Read fails with errno = %d\n", errno);
		exit (0);
	}
	if (ret != 1024) {
		printf("Unexpected bytes returned = %d\n", ret);
		exit (0);
	}
	for (i = 0; i < ret; i++) {
		if (buf[i]) {
			printf("Unexpected file size detected.\n");
			exit (0);
		}
	}
	printf("All tests has passed successfully.\n");
	close(fd);
}
