/* $Id$ */

#include <sys/param.h>

#include <err.h>
#include <stdio.h>
#include <unistd.h>

#include "psc_util/cdefs.h"

/* start includes */
#include <bmap.h>
#include <buffer.h>
#include <cache_params.h>
#include <creds.h>
#include <fdbuf.h>
#include <fid.h>
#include <fidcache.h>
#include <inode.h>
#include <inodeh.h>
#include <jflush.h>
#include <offtree.h>
#include <pathnames.h>
#include <slashexport.h>
#include <slashrpc.h>
#include <slconfig.h>
#include <sljournal.h>
#include <mount_slash/cli_bmap.h>
#include <mount_slash/control.h>
#include <mount_slash/fidc_client.h>
#include <mount_slash/fuse_listener.h>
#include <mount_slash/mount_slash.h>
#include <mount_slash/msl_fuse.h>
#include <slashd/cfd.h>
#include <slashd/control.h>
#include <slashd/fidc_mds.h>
#include <slashd/mds_bmap.h>
#include <slashd/mds_repl.h>
#include <slashd/mdscoh.h>
#include <slashd/mdsexpc.h>
#include <slashd/mdsio_zfs.h>
#include <slashd/mdslog.h>
#include <slashd/mdsrpc.h>
#include <slashd/rpc.h>
#include <slashd/sb.h>
#include <slashd/slashdthr.h>
#include <sliod/control.h>
#include <sliod/fidc_iod.h>
#include <sliod/iod_bmap.h>
#include <sliod/rpc.h>
#include <sliod/sliod.h>
#include <sliod/slvr.h>
/* end includes */

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

	/* start structs */
	PRTYPE(cred_t);
	PRTYPE(sl_inum_t);
	PRTYPE(sl_mds_id_t);
	PRTYPE(struct biod_crcup_ref);
	PRTYPE(struct biod_infslvr_tree);
	PRTYPE(struct bmap_info_cli);
	PRTYPE(struct bmap_iod_info);
	PRTYPE(struct bmap_mds_info);
	PRTYPE(struct bmap_refresh);
	PRTYPE(struct bmapc_memb);
	PRTYPE(struct bmi_assign);
	PRTYPE(struct cfdent);
	PRTYPE(struct cfdops);
	PRTYPE(struct fidc_child);
	PRTYPE(struct fidc_iod_info);
	PRTYPE(struct fidc_mds_info);
	PRTYPE(struct fidc_memb);
	PRTYPE(struct fidc_membh);
	PRTYPE(struct fidc_open_obj);
	PRTYPE(struct io_server_conn);
	PRTYPE(struct jflush_item);
	PRTYPE(struct mexp_cli);
	PRTYPE(struct mexp_ion);
	PRTYPE(struct mexpbcm);
	PRTYPE(struct mexpfcm);
	PRTYPE(struct msbmap_data);
	PRTYPE(struct msfs_thread);
	PRTYPE(struct msl_fbr);
	PRTYPE(struct msl_fhent);
	PRTYPE(struct msrcm_thread);
	PRTYPE(struct offtree_fill);
	PRTYPE(struct offtree_iov);
	PRTYPE(struct offtree_memb);
	PRTYPE(struct offtree_req);
	PRTYPE(struct offtree_root);
	PRTYPE(struct resprof_mds_info);
	PRTYPE(struct sl_buffer);
	PRTYPE(struct sl_buffer_iovref);
	PRTYPE(struct sl_finfo);
	PRTYPE(struct sl_fsops);
	PRTYPE(struct slash_bmap_cli_wire);
	PRTYPE(struct slash_bmap_od);
	PRTYPE(struct slash_creds);
	PRTYPE(struct slash_fidgen);
	PRTYPE(struct slash_inode_extras_od);
	PRTYPE(struct slash_inode_handle);
	PRTYPE(struct slash_inode_od);
	PRTYPE(struct slash_ricthr);
	PRTYPE(struct slash_riithr);
	PRTYPE(struct slash_rimthr);
	PRTYPE(struct slash_rmcthr);
	PRTYPE(struct slash_rmithr);
	PRTYPE(struct slash_rmmthr);
	PRTYPE(struct slash_sb_mem);
	PRTYPE(struct slash_sb_store);
	PRTYPE(struct slashrpc_cservice);
	PRTYPE(struct slashrpc_export);
	PRTYPE(struct slmds_jent_crc);
	PRTYPE(struct slmds_jent_ino_addrepl);
	PRTYPE(struct slmds_jent_repgen);
	PRTYPE(struct slmds_jents);
	PRTYPE(struct slvr_ref);
	PRTYPE(struct srm_access_req);
	PRTYPE(struct srm_bmap_chmode_rep);
	PRTYPE(struct srm_bmap_chmode_req);
	PRTYPE(struct srm_bmap_crcup);
	PRTYPE(struct srm_bmap_crcwire);
	PRTYPE(struct srm_bmap_crcwrt_req);
	PRTYPE(struct srm_bmap_dio_req);
	PRTYPE(struct srm_bmap_iod_get);
	PRTYPE(struct srm_bmap_rep);
	PRTYPE(struct srm_bmap_req);
	PRTYPE(struct srm_bmap_wire_rep);
	PRTYPE(struct srm_bmap_wire_req);
	PRTYPE(struct srm_connect_req);
	PRTYPE(struct srm_create_req);
	PRTYPE(struct srm_destroy_req);
	PRTYPE(struct srm_generic_rep);
	PRTYPE(struct srm_getattr_rep);
	PRTYPE(struct srm_getattr_req);
	PRTYPE(struct srm_io_rep);
	PRTYPE(struct srm_io_req);
	PRTYPE(struct srm_link_rep);
	PRTYPE(struct srm_link_req);
	PRTYPE(struct srm_lookup_rep);
	PRTYPE(struct srm_lookup_req);
	PRTYPE(struct srm_mkdir_rep);
	PRTYPE(struct srm_mkdir_req);
	PRTYPE(struct srm_mknod_req);
	PRTYPE(struct srm_open_rep);
	PRTYPE(struct srm_open_req);
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
	PRTYPE(struct srt_bdb_secret);
	PRTYPE(struct srt_bmapdesc_buf);
	PRTYPE(struct srt_fd_buf);
	PRTYPE(struct srt_fdb_secret);
/* end structs */

	exit(0);
}
