/*
 * 06/06/2017: trunc-test.c, perform a series of truncation and other file system
 *             operations on a file to test partial truncation support.
 */
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#define	 BUF_SIZE	128*1024*1024

int
main(int argc, char **argv)
{
	long size;
	int i, fd, ret, total = 0;
	struct stat stbuf;
	char *buf, *filename;
	struct timeval t1, t2;

	if (argc != 3) {
		printf("Usage: a.out filename initial-size\n");
		exit (0);
	}

	filename = argv[1];
	size = atol(argv[2]);
	if (size < 4096) {
		printf("The initial size of the file is too small (< 4096)!\n");
		exit (0);
	}

	gettimeofday(&t1, NULL);

	srandom(1234);
	buf = malloc(BUF_SIZE);

	for (i = 0; i < BUF_SIZE; i++)
		buf[i] = random();

	/* ETIMEDOUT = 110 */
	fd = open(filename, O_RDWR|O_TRUNC|O_EXCL|O_CREAT, 0600);
	if (fd < 0) {
		printf("Create fails with errno = %d at line %d\n", errno, __LINE__);
		exit (0);
	}
	total++;
	printf("File %s has been created successfully at line %d.\n", filename, __LINE__);

	ret = ftruncate(fd, size);
	if (ret < 0) {
		printf("Truncate fails with errno = %d at line %d\n", errno, __LINE__);
		exit (0);
	}
	total++;

	ret = lseek(fd, size - 1234, SEEK_SET);
	if (ret < 0) {
		printf("Seek fails with errno = %d at line %d\n", errno, __LINE__);
		exit (0);
	}
	total++;

	ret = write(fd, buf, 4096);
	if (ret != 4096) {
		printf("Write fails with errno = %d at line %d\n", errno, __LINE__);
		exit (0);
	}
	total++;

	/*
 	 * Flush so that the MDS has the latest attributes.
 	 */
	ret = close(fd);
	if (ret < 0) {
		printf("Close fails with errno = %d at line %d\n", errno, __LINE__);
		exit (0);
	}
	total++;

	ret = stat(filename, &stbuf);
	if (ret < 0) {
		printf("Stat fails with errno = %d at line %d\n", errno, __LINE__);
		exit (0);
	}
	if (stbuf.st_size != size - 1234 + 4096) {
		printf("Unexpected file size = %ld at line %d\n", stbuf.st_size, __LINE__);
		exit (0);
	}
	total++;

	fd = open(filename, O_RDWR, 0600);
	if (fd < 0) {
		printf("Open fails with errno = %d at line %d\n", errno, __LINE__);
		exit (0);
	}
	total++;
	printf("File %s has been opened successfully at line %d.\n", filename, __LINE__);

	ret = fstat(fd, &stbuf);
	if (fd < 0) {
		printf("Stat fails with errno = %d at line %d\n", errno, __LINE__);
		exit (0);
	}
	if (stbuf.st_size != size - 1234 + 4096) {
		printf("Unexpected file size = %ld at line %d\n", stbuf.st_size, __LINE__);
		exit (0);
	}
	total++;

	/* EOPNOTSUPP = 95, EBADF = 9 */
	ret = ftruncate(fd, 1024);
	if (ret < 0) {
		printf("Truncate fails with errno = %d at line %d\n", errno, __LINE__);
		exit (0);
	}
	total++;
	printf("File %s has been truncated successfully at line %d.\n", filename, __LINE__);

	ret = lseek(fd, 0, SEEK_SET);
	if (ret < 0) {
		printf("Seek fails with errno = %d at line %d\n", errno, __LINE__);
		exit (0);
	}
	total++;

	ret = read(fd, buf, 10240);
	if (ret < 0) {
		printf("Read fails with errno = %d at line %d\n", errno, __LINE__);
		exit (0);
	}
	if (ret != 1024) {
		printf("Unexpected bytes returned = %d at line %d\n", ret, __LINE__);
		exit (0);
	}
	total++;

	for (i = 0; i < ret; i++) {
		if (buf[i]) {
			printf("Unexpected file size detected at line %d.\n", __LINE__);
			exit (0);
		}
	}
	printf("File %s has been read successfully at line %d.\n", filename, __LINE__);

	ret = ftruncate(fd, 1000);
	if (ret < 0) {
		printf("Truncate fails with errno = %d at line %d\n", errno, __LINE__);
		exit (0);
	}
	total++;
	printf("File %s has been truncated successfully at line %d.\n", filename, __LINE__);

	ret = stat(filename, &stbuf);
	if (ret < 0) {
		printf("Stat fails with errno = %d at line %d\n", errno, __LINE__);
		exit (0);
	}
	total++;

	if (stbuf.st_size != 1000) {
		printf("Unexpected file size = %ld at line %d\n", stbuf.st_size, __LINE__);
		exit (0);
	}


	/* EAGAIN  = 11 */
	ret = ftruncate(fd, 234);
	if (ret < 0) {
		printf("Truncate fails with errno = %d at line %d\n", errno, __LINE__);
		exit (0);
	}
	total++;

	ret = stat(filename, &stbuf);
	if (ret < 0) {
		printf("Stat fails with errno = %d at line %d\n", errno, __LINE__);
		exit (0);
	}
	if (stbuf.st_size != 234) {
		printf("Unexpected file size = %ld at line %d\n", stbuf.st_size, __LINE__);
		exit (0);
	}
	total++;

	ret = ftruncate(fd, 23400);
	if (ret < 0) {
		printf("Truncate fails with errno = %d at line %d\n", errno, __LINE__);
		exit (0);
	}
	total++;

	ret = stat(filename, &stbuf);
	if (ret < 0) {
		printf("Stat fails with errno = %d at line %d\n", errno, __LINE__);
		exit (0);
	}
	if (stbuf.st_size != 23400) {
		printf("Unexpected file size = %ld at line %d\n", stbuf.st_size, __LINE__);
		exit (0);
	}
	total++;

	ret = lseek(fd, 0, SEEK_SET);
	if (ret < 0) {
		printf("Seek fails with errno = %d at line %d\n", errno, __LINE__);
		exit (0);
	}
	total++;

	ret = write(fd, buf, 23400);
	if (ret != 23400) {
		printf("Write fails with errno = %d at line %d\n", errno, __LINE__);
		exit (0);
	}
	total++;

	ret = write(fd, buf, BUF_SIZE);
	if (ret != BUF_SIZE) {
		printf("Write fails with errno = %d at line %d\n", errno, __LINE__);
		exit (0);
	}
	total++;

	ret = write(fd, buf, BUF_SIZE);
	if (ret != BUF_SIZE) {
		printf("Write fails with errno = %d at line %d\n", errno, __LINE__);
		exit (0);
	}
	total++;

	ret = fstat(fd, &stbuf);
	if (ret < 0) {
		printf("Stat fails with errno = %d at line %d\n", errno, __LINE__);
		exit (0);
	}
	if (stbuf.st_size != 23400 + 2 * BUF_SIZE) {
		printf("Unexpected file size = %ld at line %d\n", stbuf.st_size, __LINE__);
		exit (0);
	}
	total++;
	printf("File %s has been written successfully at line %d.\n", filename, __LINE__);

	ret = close(fd);
	if (ret < 0) {
		printf("Close fails with errno = %d at line %d\n", errno, __LINE__);
		exit (0);
	}
	total++;

	printf("Now perform a series of truncations to shorten the file %s...\n", filename);
	for (i = 23400; i >= 0; i-=2749) {
		ret = truncate(filename, i);
		if (ret < 0) {
			printf("Truncate fails with errno = %d at line %d\n", errno, __LINE__);
			exit (0);
		}
		total++;

		fd = open(filename, O_RDWR, 0600);
		if (fd < 0) {
			printf("Create fails with errno = %d at line %d\n", errno, __LINE__);
			exit (0);
		}
		total++;

		ret = write(fd, buf, 12345);
		if (ret != 12345) {
			printf("Write fails with errno = %d at line %d\n", errno, __LINE__);
			exit (0);
		}
		total++;

		ret = fsync(fd);
		if (ret < 0) {
			printf("fsync fails with errno = %d at line %d\n", errno, __LINE__);
			exit (0);
		}
		total++;

		ret = close(fd);
		if (ret < 0) {
			printf("Close fails with errno = %d at line %d\n", errno, __LINE__);
			exit (0);
		}
		total++;
	}

	printf("Now perform a series of truncations to length the file %s...\n", filename);
	for (i = 0; i <= 123456789; i+=54301) {
		ret = truncate(filename, i);
		if (ret < 0) {
			printf("Truncate fails with errno = %d at line %d\n", errno, __LINE__);
			exit (0);
		}
		total++;

		fd = open(filename, O_RDWR, 0600);
		if (fd < 0) {
			printf("Create fails with errno = %d at line %d\n", errno, __LINE__);
			exit (0);
		}
		total++;

		ret = write(fd, buf, 76543);
		if (ret != 76543) {
			printf("Write fails with errno = %d at line %d\n", errno, __LINE__);
			exit (0);
		}
		total++;

		ret = fsync(fd);
		if (ret < 0) {
			printf("fsync fails with errno = %d at line %d\n", errno, __LINE__);
			exit (0);
		}
		total++;

		ret = close(fd);
		if (ret < 0) {
			printf("Close fails with errno = %d at line %d\n", errno, __LINE__);
			exit (0);
		}
		total++;
	}

	gettimeofday(&t2, NULL);

	if (t2.tv_usec < t1.tv_usec) {
		t2.tv_usec += 1000000;
		t2.tv_sec--;
        }

	printf("\nAll %d tests has passed successfully in %ld seconds!\n", 
		total, t2.tv_sec - t1.tv_sec);
}
