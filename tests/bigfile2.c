/*
 * 06/21/2016: bigfile2.c
 */
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>

main(int argc, char *argv[])
{
	long val;
	char *filename;
	unsigned char *buf;
	int error = 0, readonly = 0;
	size_t i, j, c, fd, seed, size, bsize, nblocks;

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
	if (optind > argc - 1) {
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

	buf = malloc(bsize);
	if (buf == NULL) {
		printf("Fail to allocate memory, errno = %d\n", errno);
		exit(0);
	}
	srandom(seed);
	printf("Seed = %d, file name = %s, size = %ld\n", seed, filename, nblocks * bsize);
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
		if (error > 20)
			break;
	}
        close(fd);
}
