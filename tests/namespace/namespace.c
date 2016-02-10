/* $Id$ */

#include <sys/types.h>
#include <sys/stat.h>

#include <dirent.h>
#include <err.h>
#include <time.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "pfl/cdefs.h"
#include "pfl/queue.h"

/*
 * This tester does not extensively test boundary conditions on maximum
 * basename sizes (255), which makes debugging with ls(1) easier to read.
 */

#define FILENAME_LEN		32

#define	INITIAL_SEED		123
#define	FILE_PER_DIRECTORY	150
#define	TOTAL_OPERATIONS	1000

struct dir_item {
	TAILQ_ENTRY(dir_item)	list;
	long			count;		/* How many entries in this directory */
	char			name[0];	/* This is the complete path */
};

/*
 * A directory entry.
 */
struct dir_entry {
	struct dir_entry	*next;
	unsigned char		type;
	char			name[0];
};

char *make_name(int);
void delete_create_file(void);
void delete_random_file(void);
void create_random_file(void);
void choose_working_directory(void);

void sigcatch(int);

void print_statistics(void);

unsigned int		seed = INITIAL_SEED;
long			total_operations = TOTAL_OPERATIONS;
int			file_per_directory = FILE_PER_DIRECTORY;

/*
 * Avoid `-' in this list to ease shell commands (`--' to mark end of args).
 */
char random_chars[] = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

TAILQ_HEAD(, dir_item)	 dirlist;
struct dir_item		*currentdir;

long			 totaldirs;
long			 totalfiles;
long			 delete_dir_count;
long			 delete_file_count;
long			 create_dir_count;
long			 create_file_count;
long			 operation_count;

time_t			 time1;
time_t			 time2;

volatile sig_atomic_t	 caughtint;

__dead void
usage(void)
{
	extern const char *__progname;

	fprintf(stderr,
	    "usage: %s [-s seed] [-o operations] [-f fileperdir] empty-directory\n",
	    __progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	int c, size, rc, print;
	struct dir_item *thisdir;
	char *buf;
	char *ptr;

	while ((c = getopt (argc, argv, "f:o:s:")) != -1) {
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
			usage();
		}
	}
	argc -= optind;
	argv += optind;
	if (argc != 1)
		usage();

	if (chdir(argv[0]) == -1)
		err(1, "chdir %s", argv[0]);
	size = pathconf(".", _PC_PATH_MAX);
	buf = malloc((size_t)size);
	if (buf == NULL)
		err(1, NULL);
	ptr = getcwd(buf, (size_t)size);

	printf("Seed = %u, total operation = %ld\n", seed, total_operations);

	srandom(seed);

	/*
	 * I assume that the root directory is empty.
	 */
	thisdir = malloc(sizeof(struct dir_item) + strlen(ptr) + 1);
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
		if (caughtint)
			break;
		choose_working_directory();
		delete_create_file();
		if ((print++ % 5) == 0)
			printf("Files = %08ld, dirs = %06ld, ops = %08ld\n",
				totalfiles, totaldirs, operation_count);
	}
	print_statistics();

	exit(0);

} /* end of main() */

/*
 * Before each operation, we choose a directory randomly with uniform
 * distribution.  All the directories are maintained in a linked list
 * structure.
 */
void
choose_working_directory(void)
{
	long whichdir;
	int i, rc;

	/*
	 * This function returns a number between 0 and totaldirs - 1
	 * inclusive.
	 */
	whichdir = totaldirs  * (1.0 * random() / RAND_MAX);

	currentdir = TAILQ_FIRST(&dirlist);
	for (i = 0; i < whichdir; i++)
		currentdir = TAILQ_NEXT(currentdir, list);
	if (chdir(currentdir->name) == -1)
		err(1, "Cannot change directory to %s",
		    currentdir->name);

} /* end of choose_working_directory() */

/*
 * First decide whether we are going to create a file or delete one.
 * If we are going to create one, we then decide if we are going to
 * create a directory.  If not, we are going to create a regular file.
 */
void
delete_create_file(void)
{
	double x;

	x = (1.0 * random()) / RAND_MAX;
	if (x >= 0.7)
		delete_random_file();
	else
		create_random_file();
}

/*
 * Randomly select a regular file from the current directory and delete it.
 * A directory can only be deleted if it is empty.
 */
void
delete_random_file(void)
{
	struct dir_entry *listhead, *entry, *preventry, *tempentry;
	int rc, len, whichfile, totalentry;
	char *olddirname, *tmpdirname;
	struct dir_item *tmpdir;
	struct dirent *dirp;
	DIR *dp;

	if (currentdir->count == 0)
		return;

	dp = opendir(currentdir->name);
	if (dp == NULL)
		err(1, "Fail to open directory %s", currentdir->name);

	totalentry = 0;
	listhead = NULL;
	while ((dirp = readdir(dp)) != NULL) {
		/* skip special files: dot, dotdot, and others begin with a dot. */
		if (dirp->d_name[0] == '.')
			continue;
		entry = malloc(sizeof(struct dir_entry) + strlen(dirp->d_name));
		if (entry == NULL) {
			printf("Not enough memory to continue...\n");
			print_statistics();
			exit(0);
		}

		bcopy(dirp->d_name, entry->name, strlen(dirp->d_name)+1);
		/* /usr/include/dirent.h: DT_DIR = 4, DT_REG = 8, DT_UNKNOWN = 0 */
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
		totalentry++;
	}
	if (currentdir->count != totalentry) {
		/*
		 * This may be a file system code bug if the test directory
		 * is empty when the test started.
		 */
		printf("Directory: %s, ", currentdir->name);
		printf("%ld entries expected, %d found!\007\n",
				currentdir->count, totalentry);
		print_statistics();
		exit(0);
	}
	whichfile = totalentry * (1.0 * random() / RAND_MAX);
	entry = listhead;
	while (whichfile) {
		entry = entry->next;
		whichfile--;
	}
	/*
	 * Looks like d_type is not supported by FUSE/slash2.  Instead of
	 * doing a stat() to find out the file type, I use the first
	 * letter of the names to distinguish them.
	 */
	switch (entry->name[0]) {
	    case 'd':
		/* Hmm, looks like directory almost will never be removed */
		if (rmdir(entry->name) == -1) {
			if (errno == ENOTEMPTY)
				goto out;

			err(1, "Failed to delete directory %s", entry->name);
		}

		len = strlen(currentdir->name) + 1 + strlen(entry->name);
		tmpdirname = olddirname = malloc(len + 1);
		if (olddirname == NULL)
			err(1, NULL);

		memcpy(olddirname, currentdir->name, strlen(currentdir->name));
		olddirname += strlen(currentdir->name);
		*olddirname++ = '/';
		memcpy(olddirname, entry->name, strlen(entry->name));
		olddirname += strlen(entry->name);
		*olddirname = '\0';

		olddirname = tmpdirname;
		tmpdir = TAILQ_FIRST(&dirlist);
		while (tmpdir) {
			if (strcmp(olddirname, tmpdir->name) == 0)
				break;
			tmpdir = TAILQ_NEXT(tmpdir, list);
		}
		if (tmpdir == NULL)
			errx(1, "Directory list is broken!");
		TAILQ_REMOVE(&dirlist, tmpdir, list);
		free(tmpdir);
		totaldirs--;
		delete_dir_count++;
		break;
	    case 'f':
		if (unlink(entry->name) == -1)
			err(1, "Failed to delete file %s", entry->name);
		totalfiles--;
		delete_file_count++;
		break;
	    default:
		errx(1, "Unexpected directory entry type %c", entry->name[0]);
	}
	operation_count++;
	currentdir->count--;
out:
	entry = listhead;
	while (entry) {
		preventry = entry;
		entry = entry->next;
		free(preventry);
	}
	closedir(dp);

} /* end of delete_random_file() */

void
create_random_file(void)
{
	struct dir_item *tmpdir;
	char *filename, *newdirname;
	double x, dirprob;
	int fd, rc, len;

	dirprob = 1.0 / file_per_directory;
	x = (1.0 * random()) / RAND_MAX;

	if (x < dirprob) {
		filename = make_name(1);
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
		*newdirname++ = '/';
		memcpy(newdirname, filename, strlen(filename));
		newdirname += strlen(filename);
		*newdirname = '\0';

		newdirname = tmpdir->name;
		if (mkdir(newdirname, S_IRWXU) == -1)
			err(1, "Fail to create directory %s", newdirname);
		TAILQ_INSERT_HEAD(&dirlist, tmpdir, list);

		totaldirs++;
		create_dir_count++;
	} else {
		filename = make_name(0);
		fd = creat(filename, S_IRWXU);
		if (fd < 0) {
			err("Failed to create file %s", filename);
		close(fd);
		totalfiles++;
		create_file_count++;
	}

	free(filename);
	operation_count++;
	currentdir->count++;

} /* end of create_random_file() */

/*
 * Create a random file name of random length.  If this name is for
 * a regular file, it will be freed up immediately after use.
 */
char *
make_name(int dir)
{
	int i, len, rc;
	struct stat sb;
	char *namebuf;

 again:
	len = random() % FILENAME_LEN + 2;	/* make sure len >= 2, because we now reserve the first character */
	namebuf = malloc(len + 1);

	for (i = 1; i < len; i++)
		namebuf[i] = random_chars[random() % strlen(random_chars)];
	namebuf[i] = '\0';
	if (dir)
		namebuf[0] = 'd';
	else
		namebuf[0] = 'f';

	/* this slows things down for each operation */
	if (stat(namebuf, &sb) == 0) {
		printf("Filename %s was used before, try again...\n", namebuf);
		free(namebuf);
		goto again;
	}
	if (errno == ENOENT)
		return (namebuf);
	err(1, "Can't detect filename %s", namebuf);

} /* end of make_name() */

void
sigcatch(__unusedx int sig)
{
	caughtint = 1;
} /* end of sigcatch() */

void
print_statistics(void)
{
	double elapsetime;

	time(&time2);
	printf("\n");
	printf("Delete dir operations: %ld\n", delete_dir_count);
	printf("Delete file operations: %ld\n", delete_file_count);
	printf("Create dir operations: %ld\n", create_dir_count);
	printf("Create file operations: %ld\n", create_file_count);
	printf("\nTotal files: %ld, dirs: %ld, ops: %lu\n",
			totalfiles, totaldirs, operation_count);

	elapsetime = difftime(time2, time1);
	printf("Time used to age the directory is %f seconds.\n", elapsetime);

} /* end of print_statistics() */
