/* $Id$ */

#include <sys/types.h>
#include <sys/stat.h>

#include <dirent.h>
#include <time.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "psc_ds/queue.h"

/*
 * I know that the max filename length is something like 1024.
 * But, hey, most filenames are short.  This makes "ls" easier to read.
 */

#define MaxNameLen		32

#define	INITIAL_SEED		123
#define	FILE_PER_DIRECTORY	150
#define	TOTAL_OPERATIONS	1000

unsigned int			seed = INITIAL_SEED;
long				total_operations = TOTAL_OPERATIONS;
int				file_per_directory = FILE_PER_DIRECTORY;

/*
 * If you include '-' in the following list, you can have a little
 * trouble deleting the files whose name begins with '-'.
 */
char random_chars[] = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

struct dir_item {
	TAILQ_ENTRY(dir_item) list;
	long count;		/* How many entries in this directory */
	/*
	 * Inside gdb, use command "print &currentdir->name[0]" to
	 * show its entirety.
	 */
	char name[1];		/* This is the complete path */
};

/*
 * A directory entry.
 */
struct dir_entry {
	struct dir_entry * next;
	unsigned char type;
	char name[1];
};

TAILQ_HEAD(, dir_item) dirlist;
struct dir_item * currentdir;

long totaldirs;
long totalfiles;
long delete_dir_count;
long delete_file_count;
long create_dir_count;
long create_file_count;
long operation_count;

char * make_name(void);
void delete_create_file(void);
void delete_random_file(void);
void create_random_file(void);
void choose_working_directory(void);

void sigcatch(int);
void print_statistics(void);

time_t time1, time2;

long file_count_16384 = 0;

int main(int argc, char * argv[])
{
	int c;
	int size;
	int ret, print;
	struct dir_item * thisdir;
	char *buf;
	char *ptr;

	while ((c = getopt (argc, argv, "o:s:")) != -1) {
		switch (c) {
			case 's':
				seed = atoi(optarg);
				break;
			case 'o':
				total_operations = atol(optarg);
				break;
			case 'f':
				file_per_directory = atoi(optarg);
				break;
			default:
				break;
		}
	}
	if (argc <= optind) {
		printf("Usage: a.out directory.\n");
		exit(1);
	}
	ret = chdir(argv[optind]);
	if (ret < 0) {
		printf("Cannot set working directory to %s!\n", argv[1]);
		exit(1);
	}
	size = pathconf(".", _PC_PATH_MAX);
	if ((buf = (char *)malloc((size_t)size)) != NULL)
		 ptr = getcwd(buf, (size_t)size);

	printf("Seed = %u, total operation = %ld\n", seed, total_operations);

	srandom(seed);

	/*
	 * I assume that the root directory is empty.
	 */
	thisdir = malloc(sizeof(struct dir_item) + size);
	thisdir->count = 0;
	strcpy(thisdir->name, ptr);

	TAILQ_INIT(&dirlist);
	TAILQ_INSERT_HEAD(&dirlist, thisdir, list);
	totaldirs = 1;

	signal(SIGINT, sigcatch);

	delete_dir_count = 0;
	delete_file_count = 0;
	create_dir_count = 0;
	create_file_count = 0;
	operation_count = 0;

	time(&time1);

	print = 0;

	while (operation_count < total_operations) {
		choose_working_directory();
		delete_create_file();
		if ((print ++ % 5) == 0) {
			printf("Files = %08ld, dirs = %06ld, ops = %08ld\n",
				totalfiles, totaldirs, operation_count);
		}
	}
	time(&time2);
	print_statistics();

	exit(0);

} /* end of main() */

/*
 * Before each operation, we choose a directory randomly with uniform
 * distribution.  All the directories are maintained in a linked list
 * structure.
 */
void choose_working_directory(void)
{
	int i, ret;
	long whichdir;

	/*
	 * This function returns a number between 0 and totaldirs - 1
	 * inclusive.
	 */
	whichdir = (totaldirs / RAND_MAX) * random();

	currentdir = TAILQ_FIRST(&dirlist);
	for (i = 0; i < whichdir; i++) {
		currentdir = TAILQ_NEXT(currentdir, list);
	}
	ret = chdir(currentdir->name);
	if (ret != 0) {
		printf("Cannot change directory to %s, errno = %d.\n",
				currentdir->name, errno);
		exit(1);
	}

} /* end of choose_working_directory() */

/*
 * First decide whether we are going to create a file or delete one.
 * If we are going to create one, we then decide if we are going to
 * create a directory.  If not, we are going to create a regular file.
 */
void delete_create_file(void)
{
	long x;

	x = random() / RAND_MAX ;
	if (x >= 0.7 * RAND_MAX) {
		delete_random_file();
	} else {
		create_random_file();
	}

}

/*
 * Randomly select a regular file from the current directory and delete it.
 * A directory can only be deleted if it is empty.
 */
void delete_random_file(void)
{
	DIR * dp;
	struct stat sb;
	struct dirent * dirp;
	struct dir_item * tmpdir;
	char * olddirname, * tmpdirname;
	int ret, len, whichfile, totalentry;
	struct dir_entry * listhead, * entry, * preventry, * tempentry;

	if (currentdir->count == 0)
		return;

	whichfile = random();
	dp = opendir(currentdir->name);
	if (dp == NULL) {
		printf("Fail to open directory %s, errno = %d!\n",
				currentdir->name, errno);
		exit(1);
	}

	totalentry = 0;
	listhead = NULL;
	while ((dirp = readdir(dp)) != NULL) {
		/* skip special files: dot, dotdot, and others */
		if (dirp->d_name[0] == '.')
			continue;
		entry = malloc(sizeof(struct dir_entry) + strlen(dirp->d_name));
		if (entry == NULL) {
			printf("Not enough memory to continue...\n");
			time(&time2);
			print_statistics();
			exit(0);
		}

		bcopy(dirp->d_name, entry->name, strlen(dirp->d_name)+1);
		entry->type = dirp->d_type;
		preventry = NULL;
		tempentry = listhead;
		while (tempentry) {
			if (strcmp(tempentry->name, entry->name) > 0)
				break;
			preventry = tempentry;
			tempentry = tempentry->next;
		}
		if (preventry != NULL) {
			entry->next = preventry->next;
			preventry->next = entry;
		} else {
			entry->next = listhead;
			listhead = entry;
		}
		totalentry ++;
	}
	if (currentdir->count != totalentry) {
		/*
		 * This must be a file system code bug.
		 */
		printf("Directory: %s, ", currentdir->name);
		printf("%ld entries expected, %d found!\007\n",
				currentdir->count, totalentry);
		time(&time2);
		print_statistics();
		exit(0);
	}
	entry = listhead;
	while (whichfile) {
		entry = entry->next;
		whichfile --;
	}
	if (entry->type == DT_DIR) {
		ret = rmdir(entry->name);
		if (ret < 0) {
			if (errno == ENOTEMPTY)
				goto out;

			printf("Fail to delete directory %s, errno = %d\n",
				entry->name, errno);
			exit(1);
		}
		delete_dir_count ++;
		operation_count ++;

		len = strlen(currentdir->name) + 1 + strlen(entry->name);
		tmpdirname = olddirname = malloc(len + 1);
		if (olddirname == NULL) {
			printf("Out of memory!\n");
			exit(1);
		}

		memcpy(olddirname, currentdir->name, strlen(currentdir->name));
		olddirname += strlen(currentdir->name);
		* olddirname ++ = '/';
		memcpy(olddirname, entry->name, strlen(entry->name));
		olddirname += strlen(entry->name);
		* olddirname = '\0';

		olddirname = tmpdirname;
		tmpdir = TAILQ_FIRST(&dirlist);
		while (tmpdir) {
			if (strcmp(olddirname, tmpdir->name) == 0)
				break;
			tmpdir = TAILQ_NEXT(tmpdir, list);
		}
		if (tmpdir == NULL) {
			printf("Directory list is broken!\n");
			exit(1);
		}
		TAILQ_REMOVE(&dirlist, tmpdir, list);
		free(tmpdir);
		totaldirs --;
	} else {
		ret = stat(entry->name, &sb);
		if (ret < 0) {
			printf("Fail to stat file %s, ret = %d, errno = %d\n",
				entry->name, ret, errno);
			exit(1);
		}
		ret = unlink(entry->name);
		if (ret < 0) {
			printf("Fail to delete file %s, errno = %d\n",
				entry->name, errno);
			exit(1);
		}
		totalfiles --;
		delete_file_count ++;
		operation_count ++;
	}
	currentdir->count --;
out:
	entry = listhead;
	while (entry) {
		preventry = entry;
		entry = entry->next;
		free(preventry);
	}
	closedir(dp);

} /* end of delete_random_file() */

void create_random_file(void)
{
	int fd, ret, len;
	struct dir_item * tmpdir;
	char * filename, * newdirname;
	double x, dirprob;

	dirprob = 1.0 / file_per_directory;
	x = (1.0 * random()) / RAND_MAX;

	filename = make_name();

	if (x < dirprob) {
		/*
		 * Using strcat() is dangerous because I must make sure that
		 * the destination string has enough memory.  Otherwise,
		 * the memory is corrupted silently.  This was the reason
		 * I dumped the strcat() and strdup() stuff.
		 */
		len = strlen(currentdir->name) + 1 + strlen(filename);
		tmpdir = malloc(sizeof(struct dir_item) + len);

		tmpdir->count = 0;
		newdirname = tmpdir->name;
		memcpy(newdirname, currentdir->name, strlen(currentdir->name));
		newdirname += strlen(currentdir->name);
		* newdirname ++ = '/';
		memcpy(newdirname, filename, strlen(filename));
		newdirname += strlen(filename);
		* newdirname = '\0';

		newdirname = tmpdir->name;
		ret = mkdir(newdirname, S_IRWXU);
		if (ret < 0) {
			printf("Fail to create directory %s, errno = %d!\n",
					newdirname, errno);
			exit(1);
		}

		TAILQ_INSERT_HEAD(&dirlist, tmpdir, list);

		currentdir->count ++;
		free(filename);
		totaldirs ++;
		create_dir_count ++;
		operation_count ++;
		return;
	}
	fd = creat(filename, S_IRWXU);
	if (fd < 0) {
		printf("Fail to create file %s, errno = %d!\n",
				filename, errno);
		exit(1);
	}
	close(fd);
	free(filename);
	totalfiles ++;
	create_file_count ++;
	operation_count ++;

	currentdir->count ++;

} /* end of create_random_file() */

/*
 * Create a random file name of random length.  If this name is for
 * a regular file, it will be freed up immediately after use.
 */
char * make_name(void)
{
	char * namebuf;
	struct stat sb;
	int i, len, ret;

again:

	len = random() % MaxNameLen + 1;	/* make sure len >= 1 */
	namebuf = malloc(len + 1);

	for (i = 0; i < len; i++) {
		namebuf[i] = random_chars[random() % strlen(random_chars)];
	}
	namebuf[i] = '\0';

	ret = stat(namebuf, &sb);
	if (ret == 0) {
		printf("Filename %s was used before, try again...\n", namebuf);
		free(namebuf);
		goto again;
	}
	if (errno == ENOENT)
		return (namebuf);
	printf("Can't detect filename %s, errno = %d.\n", namebuf, errno);
	exit(1);

} /* end of make_name() */

void sigcatch(int sig)
{
	time(&time2);
	print_statistics();
	exit(1);

} /* end of sigcatch() */

void print_statistics(void)
{
	double elapsetime;

	printf("\n");
	printf("Delete dir operations: %ld\n", delete_dir_count);
	printf("Delete file operations: %ld\n", delete_file_count);
	printf("Create dir operations: %ld\n", create_dir_count);
	printf("Create file operations: %ld\n", create_file_count);
	printf("\nTotal files: %ld, dirs: %ld, ops: %lu\n",
			totalfiles, totaldirs, operation_count);
	/*
	 * lib/libc/stdtime/difftime.c.
	 */
	elapsetime = difftime(time2, time1);
	printf("Time used to age the directory is %f\n", elapsetime);

} /* end of print_statistics() */
