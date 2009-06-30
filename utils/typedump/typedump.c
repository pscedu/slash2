/* $Id: typedump.c 5698 2009-03-27 14:42:49Z zhihui $ */

#include <sys/param.h>

#include <err.h>
#include <stdio.h>
#include <unistd.h>

#include "psc_util/cdefs.h"

#include "slashrpc.h"

const char *progname;

__dead void
usage(void)
{
	fprintf(stderr, "usage: %s\n", progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	int c;

	progname = argv[0];
	while ((c = getopt(argc, argv, "")) != -1)
		switch (c) {
		default:
			usage();
		}
	argc -= optind;
	if (argc)
		usage();

#define PRTYPE(type) \
	printf("%-32s %zu\n", #type, sizeof(type))

#define PRVAL(val) \
	printf("%-32s %lu\n", #val, (unsigned long)(val))

	PRTYPE(struct slashrpc_cservice);

	PRTYPE(struct srm_access_req);
	PRTYPE(struct srm_bmap_crcup);
	PRTYPE(struct srm_bmap_crcwire);
	PRTYPE(struct srm_bmap_crcwrt_req);
	PRTYPE(struct srm_bmap_dio_req);
	PRTYPE(struct srm_bmap_mode_req);
	PRTYPE(struct srm_bmap_rep);
	PRTYPE(struct srm_bmap_req);
	PRTYPE(struct srm_connect_req);
	PRTYPE(struct srm_create_req);
	PRTYPE(struct srm_destroy_req);
	PRTYPE(struct srm_generic_rep);
	PRTYPE(struct srm_getattr_rep);
	PRTYPE(struct srm_getattr_req);
	PRTYPE(struct srm_ic_connect_req);
	PRTYPE(struct srm_ic_connect_secret);
	PRTYPE(struct srm_io_rep);
	PRTYPE(struct srm_io_req);
	PRTYPE(struct srm_link_rep);
	PRTYPE(struct srm_link_req);
	PRTYPE(struct srm_lookup_rep);
	PRTYPE(struct srm_lookup_req);
	PRTYPE(struct srm_mkdir_rep);
	PRTYPE(struct srm_mkdir_req);
	PRTYPE(struct srm_mknod_req);
	PRTYPE(struct srm_open_req);
	PRTYPE(struct srm_opencreate_rep);
	PRTYPE(struct srm_opendir_req);
	PRTYPE(struct srm_readdir_rep);
	PRTYPE(struct srm_readdir_req);
	PRTYPE(struct srm_readlink_rep);
	PRTYPE(struct srm_readlink_req);
	PRTYPE(struct srm_release_req);
	PRTYPE(struct srm_releasebmap_req);
	PRTYPE(struct srm_rename_req);
	PRTYPE(struct srm_setattr_req);
	PRTYPE(struct srm_statfs_rep);
	PRTYPE(struct srm_statfs_req);
	PRTYPE(struct srm_symlink_rep);
	PRTYPE(struct srm_symlink_req);
	PRTYPE(struct srm_unlink_req);

	PRTYPE(struct srt_fd_buf);
	PRTYPE(struct srt_fdb_secret);

	exit(0);
}
