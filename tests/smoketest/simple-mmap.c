#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#define	FILE_SIZE	4978

int main(int argc, char *argv[])
{
    char ch;
    char *map;
    int i, fd, ret;
    char *filename = "mmap-test.dat";

    fd = open(filename, O_RDWR | O_CREAT | O_TRUNC, (mode_t)0600);
    if (fd == -1) {
	printf("Error opening file for writing.\n");
	exit(1);
    }
    ret  = lseek(fd, FILE_SIZE - 1, SEEK_SET);
    if (ret == -1) {
	printf("Error lseeking to the end of the file.\n");
	exit(1);
    }
    ch = 0x30;
    ret = write(fd, &ch, 1);
    if (ret != 1) {
	printf("Error writing last byte of the file.\n");
	exit(1);
    }
    map = mmap(0, FILE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (map == MAP_FAILED) {
	printf("Error mmapping the file.\n");
	exit(1);
    }
    for (i = 0; i < FILE_SIZE; i++) {
	map[i] = (i % 10) + 0x30; 
    }
    if (munmap(map, FILE_SIZE) == -1) {
	printf("Error un-mmapping the file.\n");
        exit(1);
    }
    close(fd);
    return 0;
}
