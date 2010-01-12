/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2009, Pittsburgh Supercomputing Center (PSC).
 *
 * Permission to use, copy, and modify this software and its documentation
 * without fee for personal use or non-commercial use within your organization
 * is hereby granted, provided that the above copyright notice is preserved in
 * all copies and that the copyright and this permission notice appear in
 * supporting documentation.  Permission to redistribute this software to other
 * organizations or individuals is not permitted without the written permission
 * of the Pittsburgh Supercomputing Center.  PSC makes no representations about
 * the suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

#include <sys/param.h>

#include <err.h>
#include <stdio.h>
#include <unistd.h>

#include "pfl/cdefs.h"
#include "psc_util/ctl.h"

/* start includes */
#include "bmap.h"
#include "buffer.h"
#include "cache_params.h"
#include "creds.h"
#include "fdbuf.h"
#include "fid.h"
#include "fidcache.h"
#include "inode.h"
#include "inodeh.h"
#include "jflush.h"
#include "mkfn.h"
#include "pathnames.h"
#include "slashrpc.h"
#include "slconfig.h"
#include "slerr.h"
#include "sljournal.h"
#include "sltypes.h"
#include "mount_slash/bmap_cli.h"
#include "mount_slash/bmpc.h"
#include "mount_slash/ctl_cli.h"
#include "mount_slash/ctlsvr_cli.h"
#include "mount_slash/fidc_cli.h"
#include "mount_slash/fuse_listener.h"
#include "mount_slash/mount_slash.h"
#include "mount_slash/msl_fuse.h"
#include "mount_slash/rpc_cli.h"
#include "msctl/msctl.h"
#include "slashd/bmap_mds.h"
#include "slashd/cfd.h"
#include "slashd/ctl_mds.h"
#include "slashd/fidc_mds.h"
#include "slashd/mdscoh.h"
#include "slashd/mdsexpc.h"
#include "slashd/mdsio_zfs.h"
#include "slashd/mdslog.h"
#include "slashd/repl_mds.h"
#include "slashd/rpc_mds.h"
#include "slashd/slashd.h"
#include "sliod/bmap_iod.h"
#include "sliod/ctl_iod.h"
#include "sliod/fidc_iod.h"
#include "sliod/repl_iod.h"
#include "sliod/rpc_iod.h"
#include "sliod/sliod.h"
#include "sliod/slvr.h"
/* end includes */

struct slash_bmap_od bmapod;
char buf[1024 * 1024];
const char *progname;

void
pr(const char *name, uint64_t value)
{
	static int i;
	int n;

	if (i++ % 2) {
		n = printf("%s ", name);
		while (n++ <= 50)
			putchar('-');
		if (n < 53)
			printf("> ");
		printf("%zu\n", value);
	} else
		printf("%-52s %zu\n", name, value);
}

__dead void
usage(void)
{
	fprintf(stderr, "usage: %s\n", progname);
	exit(1);
}
int
main(int argc, char *argv[])
{
	psc_crc64_t crc;
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

#define PRTYPE(type)	pr(#type, sizeof(type))
#define PRVAL(val)	pr(#val, (unsigned long)(val))

	/* start structs */
	PRTYPE(cred_t);
	PRTYPE(sl_blkgen_t);
	PRTYPE(sl_blkno_t);
	PRTYPE(sl_ino_t);
	PRTYPE(sl_ios_id_t);
	PRTYPE(sl_siteid_t);
	PRTYPE(struct biod_crcup_ref);
	PRTYPE(struct biod_infl_crcs);
	PRTYPE(struct bmap_cli_info);
	PRTYPE(struct bmap_iod_info);
	PRTYPE(struct bmap_mds_info);
	PRTYPE(struct bmap_pagecache);
	PRTYPE(struct bmap_pagecache_entry);
	PRTYPE(struct bmap_refresh);
	PRTYPE(struct bmapc_memb);
	PRTYPE(struct bmi_assign);
	PRTYPE(struct bmpc_ioreq);
	PRTYPE(struct bmpc_mem_slbs);
	PRTYPE(struct cfdent);
	PRTYPE(struct cfdops);
	PRTYPE(struct cli_resm_info);
	PRTYPE(struct fidc_iod_info);
	PRTYPE(struct fidc_mds_info);
	PRTYPE(struct fidc_membh);
	PRTYPE(struct fidc_nameinfo);
	PRTYPE(struct fidc_open_obj);
	PRTYPE(struct iod_resm_info);
	PRTYPE(struct jflush_item);
	PRTYPE(struct mds_resm_info);
	PRTYPE(struct mds_resprof_info);
	PRTYPE(struct mds_site_info);
	PRTYPE(struct mexp_cli);
	PRTYPE(struct mexpbcm);
	PRTYPE(struct mexpfcm);
	PRTYPE(struct msbmap_crcrepl_states);
	PRTYPE(struct msctl_replst_cont);
	PRTYPE(struct msctl_replst_slave_cont);
	PRTYPE(struct msctl_replstq);
	PRTYPE(struct msctlmsg_fncmd_bmapreplpol);
	PRTYPE(struct msctlmsg_fncmd_newreplpol);
	PRTYPE(struct msctlmsg_replrq);
	PRTYPE(struct msctlmsg_replst);
	PRTYPE(struct msctlmsg_replst_slave);
	PRTYPE(struct msfs_thread);
	PRTYPE(struct msl_fbr);
	PRTYPE(struct msl_fcoo_data);
	PRTYPE(struct msl_fhent);
	PRTYPE(struct msrcm_thread);
	PRTYPE(struct sl_buffer);
	PRTYPE(struct sl_buffer_iovref);
	PRTYPE(struct sl_fsops);
	PRTYPE(struct sl_gconf);
	PRTYPE(struct sl_replrq);
	PRTYPE(struct sl_resm);
	PRTYPE(struct sl_resource);
	PRTYPE(struct sl_site);
	PRTYPE(struct slash_bmap_cli_wire);
	PRTYPE(struct slash_bmap_od);
	PRTYPE(struct slash_creds);
	PRTYPE(struct slash_fidgen);
	PRTYPE(struct slash_inode_extras_od);
	PRTYPE(struct slash_inode_handle);
	PRTYPE(struct slash_inode_od);
	PRTYPE(struct slashrpc_cservice);
	PRTYPE(struct slashrpc_export);
	PRTYPE(struct sli_repl_workrq);
	PRTYPE(struct slictlmsg_replwkst);
	PRTYPE(struct sliric_thread);
	PRTYPE(struct slirii_thread);
	PRTYPE(struct slirim_thread);
	PRTYPE(struct slm_rmi_expdata);
	PRTYPE(struct slmds_jent_crc);
	PRTYPE(struct slmds_jent_ino_addrepl);
	PRTYPE(struct slmds_jent_repgen);
	PRTYPE(struct slmds_jents);
	PRTYPE(struct slmrcm_thread);
	PRTYPE(struct slmreplq_thread);
	PRTYPE(struct slmrmc_thread);
	PRTYPE(struct slmrmi_thread);
	PRTYPE(struct slmrmm_thread);
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
	PRTYPE(struct srm_ping_req);
	PRTYPE(struct srm_readdir_rep);
	PRTYPE(struct srm_readdir_req);
	PRTYPE(struct srm_readlink_rep);
	PRTYPE(struct srm_readlink_req);
	PRTYPE(struct srm_release_req);
	PRTYPE(struct srm_releasebmap_req);
	PRTYPE(struct srm_rename_req);
	PRTYPE(struct srm_repl_read_req);
	PRTYPE(struct srm_repl_schedwk_req);
	PRTYPE(struct srm_replrq_req);
	PRTYPE(struct srm_replst_master_req);
	PRTYPE(struct srm_replst_slave_req);
	PRTYPE(struct srm_set_bmapreplpol_req);
	PRTYPE(struct srm_set_newreplpol_req);
	PRTYPE(struct srm_setattr_req);
	PRTYPE(struct srm_statfs_rep);
	PRTYPE(struct srm_statfs_req);
	PRTYPE(struct srm_symlink_rep);
	PRTYPE(struct srm_symlink_req);
	PRTYPE(struct srm_unlink_req);
	PRTYPE(struct srsm_replst_bhdr);
	PRTYPE(struct srt_bdb_secret);
	PRTYPE(struct srt_bmapdesc_buf);
	PRTYPE(struct srt_fd_buf);
	PRTYPE(struct srt_fdb_secret);
	/* end structs */

	/* start constants */
	PRVAL(MSCMT_ADDREPLRQ);
	PRVAL(MSCMT_DELREPLRQ);
	PRVAL(MSCMT_GETREPLST);
	PRVAL(MSCMT_GETREPLST_SLAVE);
	PRVAL(MSCMT_SET_BMAPREPLPOL);
	PRVAL(MSCMT_SET_NEWREPLPOL);
	PRVAL(SRMT_ACCESS);
	PRVAL(SRMT_BMAPCHMODE);
	PRVAL(SRMT_BMAPCRCWRT);
	PRVAL(SRMT_BMAPDIO);
	PRVAL(SRMT_CHMOD);
	PRVAL(SRMT_CHOWN);
	PRVAL(SRMT_CONNECT);
	PRVAL(SRMT_CREATE);
	PRVAL(SRMT_DESTROY);
	PRVAL(SRMT_DISCONNECT);
	PRVAL(SRMT_FGETATTR);
	PRVAL(SRMT_FTRUNCATE);
	PRVAL(SRMT_GETATTR);
	PRVAL(SRMT_GETBMAP);
	PRVAL(SRMT_GETBMAPCRCS);
	PRVAL(SRMT_GETREPTBL);
	PRVAL(SRMT_LINK);
	PRVAL(SRMT_LOCK);
	PRVAL(SRMT_LOOKUP);
	PRVAL(SRMT_MKDIR);
	PRVAL(SRMT_MKNOD);
	PRVAL(SRMT_OPEN);
	PRVAL(SRMT_OPENDIR);
	PRVAL(SRMT_PING);
	PRVAL(SRMT_READ);
	PRVAL(SRMT_READDIR);
	PRVAL(SRMT_READLINK);
	PRVAL(SRMT_RELEASE);
	PRVAL(SRMT_RELEASEBMAP);
	PRVAL(SRMT_RELEASEDIR);
	PRVAL(SRMT_RENAME);
	PRVAL(SRMT_REPL_ADDRQ);
	PRVAL(SRMT_REPL_DELRQ);
	PRVAL(SRMT_REPL_GETST);
	PRVAL(SRMT_REPL_GETST_SLAVE);
	PRVAL(SRMT_REPL_READ);
	PRVAL(SRMT_REPL_SCHEDWK);
	PRVAL(SRMT_RMDIR);
	PRVAL(SRMT_SETATTR);
	PRVAL(SRMT_SET_BMAPREPLPOL);
	PRVAL(SRMT_SET_NEWREPLPOL);
	PRVAL(SRMT_STATFS);
	PRVAL(SRMT_SYMLINK);
	PRVAL(SRMT_TRUNCATE);
	PRVAL(SRMT_UNLINK);
	PRVAL(SRMT_UTIMES);
	PRVAL(SRMT_WRITE);
	/* end constants */

	PRVAL(INOX_OD_SZ);
	PRVAL(INOX_OD_CRCSZ);

	PRVAL(SLASH_BMAP_SIZE);

	psc_crc64_calc(&crc, buf, sizeof(buf));
	printf("NULL 1MB buf CRC is %#"PSCPRIxCRC64"\n", crc);

	psc_crc64_calc(&crc, &bmapod, sizeof(bmapod));
	printf("NULL sl_blkh_t CRC is %#"PRIx64"\n", crc);

	exit(0);
}
