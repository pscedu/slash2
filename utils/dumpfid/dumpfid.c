/* $Id$ */

#include <sys/param.h>

#include <err.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "pfl/cdefs.h"

#include "inodeh.h"

const char *progname;

__dead void
usage(void)
{
	fprintf(stderr, "usage: %s file ...\n", progname);
	exit(1);
}

void
dumpfid(const char *fn)
{
	struct slash_inode_extras_od inox;
	struct slash_inode_od ino;
	char buf[BUFSIZ];
	psc_crc_t crc;
	int fd, j, nr;
	ssize_t rc;

	fd = open(fn, O_RDONLY);
	if (fd == -1) {
		warn("%s", fn);
		return;
	}
	rc = pread(fd, &ino, INO_OD_SZ, SL_INODE_START_OFF);
	if (rc == -1) {
		warn("%s", fn);
		goto out;
	}
	if (rc != INO_OD_SZ) {
		warnx("%s: short I/O, want %ld got %ld",
		    fn, INO_OD_SZ, rc);
		goto out;
	}
	psc_crc_calc(&crc, &ino, INO_OD_CRCSZ);
	_debug_ino(buf, sizeof(buf), &ino);
	printf("%s\t%s %s\n", fn, buf, crc == ino.ino_crc ? "OK" : "BAD");

	rc = pread(fd, &inox, INOX_OD_SZ, SL_EXTRAS_START_OFF);
	if (rc == -1) {
		warn("%s", fn);
		goto out;
	}
	if (rc != INOX_OD_SZ) {
		warnx("%s: short I/O, want %ld got %ld",
		    fn, INOX_OD_SZ, rc);
		goto out;
	}
	psc_crc_calc(&crc, &inox, INOX_OD_CRCSZ);
	printf("\tcrc: %s xrepls:", crc == inox.inox_crc ? "OK" : "BAD");
	nr = ino.ino_nrepls;
	if (nr < INO_DEF_NREPLS)
		nr = 0;
	else if (nr > SL_MAX_REPLICAS)
		nr = SL_MAX_REPLICAS;
	for (j = 0; j + INO_DEF_NREPLS < nr; j++)
		printf("%s%u", j ? "," : "", inox.inox_repls[j].bs_id);
	printf(" nbp:%d\n", inox.inox_newbmap_policy);
 out:
	close(fd);
}

int
main(int argc, char *argv[])
{
	progname = argv[0];
	if (getopt(argc, argv, "") != -1)
		usage();
	argc -= optind;
	argv += optind;
	if (!argc)
		usage();
	for (; *argv; argv++)
		dumpfid(*argv);
	exit(0);
}
