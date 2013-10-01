/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2013, Pittsburgh Supercomputing Center (PSC).
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or (at
 * your option) any later version.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License contained in the file
 * `COPYING-GPL' at the top of this distribution or at
 * https://www.gnu.org/licenses/gpl-2.0.html for more details.
 *
 * Pittsburgh Supercomputing Center	phone: 412.268.4960  fax: 412.268.5832
 * 300 S. Craig Street			e-mail: remarks@psc.edu
 * Pittsburgh, PA 15213			web: http://www.psc.edu/
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
#include "authbuf.h"
#include "bmap.h"
#include "cache_params.h"
#include "creds.h"
#include "ctl.h"
#include "ctlcli.h"
#include "ctlsvr.h"
#include "fid.h"
#include "fidcache.h"
#include "mkfn.h"
#include "pathnames.h"
#include "slashrpc.h"
#include "slconfig.h"
#include "slconn.h"
#include "slerr.h"
#include "slsubsys.h"
#include "sltypes.h"
#include "slutil.h"
#include "mount_slash/bmap_cli.h"
#include "mount_slash/ctl_cli.h"
#include "mount_slash/ctlsvr_cli.h"
#include "mount_slash/dircache.h"
#include "mount_slash/fidc_cli.h"
#include "mount_slash/mount_slash.h"
#include "mount_slash/pgcache.h"
#include "mount_slash/rpc_cli.h"
#include "mount_slash/subsys_cli.h"
#include "slashd/bmap_mds.h"
#include "slashd/ctl_mds.h"
#include "slashd/fidc_mds.h"
#include "slashd/inode.h"
#include "slashd/journal_mds.h"
#include "slashd/mdscoh.h"
#include "slashd/mdsio.h"
#include "slashd/mdslog.h"
#include "slashd/namespace.h"
#include "slashd/odtable_mds.h"
#include "slashd/repl_mds.h"
#include "slashd/rpc_mds.h"
#include "slashd/slashd.h"
#include "slashd/subsys_mds.h"
#include "slashd/up_sched_res.h"
#include "slashd/worker.h"
#include "sliod/bmap_iod.h"
#include "sliod/ctl_iod.h"
#include "sliod/fidc_iod.h"
#include "sliod/repl_iod.h"
#include "sliod/rpc_iod.h"
#include "sliod/slab.h"
#include "sliod/sliod.h"
#include "sliod/slvr.h"
#include "sliod/subsys_iod.h"
/* end includes */

struct bmap_ondisk bmapod;
char buf[1024 * 1024];
const char *progname;

void
pr(const char *name, uint64_t value, int hex)
{
	static int i;
	int n;

	if (i++ % 2) {
		n = printf("%s ", name);
		while (n++ <= 50)
			putchar('-');
		if (n < 53)
			printf("> ");
		if (hex)
			printf("%"PRIx64"\n", value);
		else
			printf("%"PRIu64"\n", value);
	} else {
		if (hex)
			printf("%-52s %"PRIx64"\n", name, value);
		else
			printf("%-52s %"PRIu64"\n", name, value);
	}
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
	uint64_t crc;
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

#define PRTYPE(type)	pr(#type, sizeof(type), 0)
#define PRVAL(val)	pr(#val, (unsigned long)(val), 0)
#define PRVALX(val)	pr(#val, (unsigned long)(val), 1)

	/* start structs */
	printf("structures:\n");
	PRTYPE(mdsio_fid_t);
	PRTYPE(sl_bmapgen_t);
	PRTYPE(sl_bmapno_t);
	PRTYPE(sl_ios_id_t);
	PRTYPE(sl_siteid_t);
	PRTYPE(slfgen_t);
	PRTYPE(slfid_t);
	PRTYPE(struct batchrq);
	PRTYPE(struct bcrcupd);
	PRTYPE(struct bmap);
	PRTYPE(struct bmap_cli_info);
	PRTYPE(struct bmap_core_state);
	PRTYPE(struct bmap_extra_state);
	PRTYPE(struct bmap_iod_info);
	PRTYPE(struct bmap_iod_minseq);
	PRTYPE(struct bmap_iod_rls);
	PRTYPE(struct bmap_ios_assign);
	PRTYPE(struct bmap_mds_info);
	PRTYPE(struct bmap_mds_lease);
	PRTYPE(struct bmap_ondisk);
	PRTYPE(struct bmap_ops);
	PRTYPE(struct bmap_pagecache);
	PRTYPE(struct bmap_pagecache_entry);
	PRTYPE(struct bmap_timeo_table);
	PRTYPE(struct bmpc_ioreq);
	PRTYPE(struct bmpc_write_coalescer);
	PRTYPE(struct dircache_ent);
	PRTYPE(struct dircache_page);
	PRTYPE(struct fci_dinfo);
	PRTYPE(struct fci_finfo);
	PRTYPE(struct fcmh_cli_info);
	PRTYPE(struct fcmh_iod_info);
	PRTYPE(struct fcmh_mds_info);
	PRTYPE(struct fidc_membh);
	PRTYPE(struct lnetif_pair);
	PRTYPE(struct mdsio_fh);
	PRTYPE(struct mdsio_ops);
	PRTYPE(struct msattrfl_thread);
	PRTYPE(struct msbmfl_thread);
	PRTYPE(struct msbmflra_thread);
	PRTYPE(struct msbmflrls_thread);
	PRTYPE(struct msbmflrpc_thread);
	PRTYPE(struct msbmflwatcher_thread);
	PRTYPE(struct msctl_replstq);
	PRTYPE(struct msctlmsg_biorq);
	PRTYPE(struct msctlmsg_bmapreplpol);
	PRTYPE(struct msctlmsg_fncmd);
	PRTYPE(struct msctlmsg_newreplpol);
	PRTYPE(struct msctlmsg_replrq);
	PRTYPE(struct msctlmsg_replst);
	PRTYPE(struct msctlmsg_replst_slave);
	PRTYPE(struct msfs_thread);
	PRTYPE(struct msl_fhent);
	PRTYPE(struct msl_fsrqinfo);
	PRTYPE(struct msl_ra);
	PRTYPE(struct msrci_thread);
	PRTYPE(struct msrcm_thread);
	PRTYPE(struct pfl_workrq);
	PRTYPE(struct resm_cli_info);
	PRTYPE(struct resm_iod_info);
	PRTYPE(struct resm_mds_info);
	PRTYPE(struct resprof_cli_info);
	PRTYPE(struct resprof_mds_info);
	PRTYPE(struct rootNames);
	PRTYPE(struct site_mds_info);
	PRTYPE(struct site_progress);
	PRTYPE(struct sl_buffer);
	PRTYPE(struct sl_config);
	PRTYPE(struct sl_expcli_ops);
	PRTYPE(struct sl_fcmh_ops);
	PRTYPE(struct sl_ino_compat);
	PRTYPE(struct sl_lnetrt);
	PRTYPE(struct sl_mds_crc_log);
	PRTYPE(struct sl_mds_iosinfo);
	PRTYPE(struct sl_mds_nsstats);
	PRTYPE(struct sl_mds_peerinfo);
	PRTYPE(struct sl_resm);
	PRTYPE(struct sl_resm_nid);
	PRTYPE(struct sl_resource);
	PRTYPE(struct sl_site);
	PRTYPE(struct slash_creds);
	PRTYPE(struct slash_fidgen);
	PRTYPE(struct slash_inode_extras_od);
	PRTYPE(struct slash_inode_handle);
	PRTYPE(struct slash_inode_od);
	PRTYPE(struct slashrpc_cservice);
	PRTYPE(struct slc_async_req);
	PRTYPE(struct slconn_params);
	PRTYPE(struct slconn_thread);
	PRTYPE(struct slctl_res_field);
	PRTYPE(struct slctlmsg_bmap);
	PRTYPE(struct slctlmsg_conn);
	PRTYPE(struct slctlmsg_fcmh);
	PRTYPE(struct sli_aiocb_reply);
	PRTYPE(struct sli_exp_cli);
	PRTYPE(struct sli_iocb);
	PRTYPE(struct sli_repl_workrq);
	PRTYPE(struct slictlmsg_fileop);
	PRTYPE(struct slictlmsg_replwkst);
	PRTYPE(struct sliric_thread);
	PRTYPE(struct slirii_thread);
	PRTYPE(struct slirim_thread);
	PRTYPE(struct slm_exp_cli);
	PRTYPE(struct slm_replst_workreq);
	PRTYPE(struct slm_resmlink);
	PRTYPE(struct slm_sth);
	PRTYPE(struct slm_update_data);
	PRTYPE(struct slm_update_generic);
	PRTYPE(struct slm_wkdata_batchrq_cb);
	PRTYPE(struct slm_wkdata_ptrunc);
	PRTYPE(struct slm_wkdata_upsch_cb);
	PRTYPE(struct slm_wkdata_upsch_purge);
	PRTYPE(struct slm_wkdata_upschq);
	PRTYPE(struct slm_wkdata_wr_brepl);
	PRTYPE(struct slmctl_thread);
	PRTYPE(struct slmctlmsg_bml);
	PRTYPE(struct slmctlmsg_replpair);
	PRTYPE(struct slmctlmsg_statfs);
	PRTYPE(struct slmds_jent_assign_rep);
	PRTYPE(struct slmds_jent_bmap_assign);
	PRTYPE(struct slmds_jent_bmap_crc);
	PRTYPE(struct slmds_jent_bmap_repls);
	PRTYPE(struct slmds_jent_bmapseq);
	PRTYPE(struct slmds_jent_ino_repls);
	PRTYPE(struct slmds_jent_namespace);
	PRTYPE(struct slmrcm_thread);
	PRTYPE(struct slmrmc_thread);
	PRTYPE(struct slmrmi_thread);
	PRTYPE(struct slmrmm_thread);
	PRTYPE(struct slmthr_dbh);
	PRTYPE(struct slmupsch_thread);
	PRTYPE(struct slmwk_thread);
	PRTYPE(struct slvr);
	PRTYPE(struct srm_batch_req);
	PRTYPE(struct srm_bmap_chwrmode_rep);
	PRTYPE(struct srm_bmap_chwrmode_req);
	PRTYPE(struct srm_bmap_crcup);
	PRTYPE(struct srm_bmap_crcwrt_rep);
	PRTYPE(struct srm_bmap_crcwrt_req);
	PRTYPE(struct srm_bmap_dio_req);
	PRTYPE(struct srm_bmap_iod_get);
	PRTYPE(struct srm_bmap_ptrunc_req);
	PRTYPE(struct srm_bmap_release_rep);
	PRTYPE(struct srm_bmap_release_req);
	PRTYPE(struct srm_bmap_wake_req);
	PRTYPE(struct srm_connect_rep);
	PRTYPE(struct srm_connect_req);
	PRTYPE(struct srm_create_rep);
	PRTYPE(struct srm_create_req);
	PRTYPE(struct srm_ctl_req);
	PRTYPE(struct srm_destroy_req);
	PRTYPE(struct srm_forward_rep);
	PRTYPE(struct srm_forward_req);
	PRTYPE(struct srm_generic_rep);
	PRTYPE(struct srm_getattr2_rep);
	PRTYPE(struct srm_getattr_rep);
	PRTYPE(struct srm_getattr_req);
	PRTYPE(struct srm_getbmap_full_rep);
	PRTYPE(struct srm_getbmap_full_req);
	PRTYPE(struct srm_getbmapminseq_rep);
	PRTYPE(struct srm_getbmapminseq_req);
	PRTYPE(struct srm_getxattr_rep);
	PRTYPE(struct srm_getxattr_req);
	PRTYPE(struct srm_import_rep);
	PRTYPE(struct srm_import_req);
	PRTYPE(struct srm_io_rep);
	PRTYPE(struct srm_io_req);
	PRTYPE(struct srm_leasebmap_rep);
	PRTYPE(struct srm_leasebmap_req);
	PRTYPE(struct srm_leasebmapext_rep);
	PRTYPE(struct srm_leasebmapext_req);
	PRTYPE(struct srm_link_req);
	PRTYPE(struct srm_listxattr_rep);
	PRTYPE(struct srm_listxattr_req);
	PRTYPE(struct srm_lookup_req);
	PRTYPE(struct srm_mkdir_req);
	PRTYPE(struct srm_mknod_req);
	PRTYPE(struct srm_ping_req);
	PRTYPE(struct srm_readdir_rep);
	PRTYPE(struct srm_readdir_req);
	PRTYPE(struct srm_readlink_rep);
	PRTYPE(struct srm_readlink_req);
	PRTYPE(struct srm_reassignbmap_req);
	PRTYPE(struct srm_reclaim_rep);
	PRTYPE(struct srm_reclaim_req);
	PRTYPE(struct srm_removexattr_rep);
	PRTYPE(struct srm_removexattr_req);
	PRTYPE(struct srm_rename_rep);
	PRTYPE(struct srm_rename_req);
	PRTYPE(struct srm_repl_read_req);
	PRTYPE(struct srm_repl_schedwk_req);
	PRTYPE(struct srm_replrq_req);
	PRTYPE(struct srm_replst_master_req);
	PRTYPE(struct srm_replst_slave_req);
	PRTYPE(struct srm_set_bmapreplpol_req);
	PRTYPE(struct srm_set_newreplpol_req);
	PRTYPE(struct srm_setattr_req);
	PRTYPE(struct srm_setxattr_rep);
	PRTYPE(struct srm_setxattr_req);
	PRTYPE(struct srm_statfs_rep);
	PRTYPE(struct srm_statfs_req);
	PRTYPE(struct srm_symlink_req);
	PRTYPE(struct srm_unlink_req);
	PRTYPE(struct srm_update_rep);
	PRTYPE(struct srm_update_req);
	PRTYPE(struct srsm_replst_bhdr);
	PRTYPE(struct srt_authbuf_footer);
	PRTYPE(struct srt_authbuf_secret);
	PRTYPE(struct srt_bmap_crcwire);
	PRTYPE(struct srt_bmapdesc);
	PRTYPE(struct srt_bmapminseq);
	PRTYPE(struct srt_creds);
	PRTYPE(struct srt_ctlsetopt);
	PRTYPE(struct srt_preclaim_ent);
	PRTYPE(struct srt_readdir_ent);
	PRTYPE(struct srt_reclaim_entry);
	PRTYPE(struct srt_stat);
	PRTYPE(struct srt_statfs);
	PRTYPE(struct srt_update_entry);
	/* end structs */

	/* start constants */
	printf("\nvalues:\n");
	PRVAL(AUTHBUF_ALGLEN);
	PRVAL(AUTHBUF_KEYSIZE);
	PRVAL(BCR_MAX_AGE);
	PRVAL(BCR_MIN_AGE);
	PRVAL(BIM_MINAGE);
	PRVAL(BIM_RETRIEVE_SEQ);
	PRVAL(BMAP_CLI_EXTREQSECS);
	PRVAL(BMAP_CLI_MAX_LEASE);
	PRVAL(BMAP_CLI_TIMEO_INC);
	PRVAL(BMAP_SEQLOG_FACTOR);
	PRVAL(BMAP_TIMEO_MAX);
	PRVAL(BMPC_IOMAXBLKS);
	PRVAL(BPHXC);
	PRVAL(BREPLST_GARBAGE);
	PRVAL(BREPLST_GARBAGE_SCHED);
	PRVAL(BREPLST_INVALID);
	PRVAL(BREPLST_REPL_QUEUED);
	PRVAL(BREPLST_REPL_SCHED);
	PRVAL(BREPLST_TRUNCPNDG);
	PRVAL(BREPLST_TRUNCPNDG_SCHED);
	PRVAL(BREPLST_VALID);
	PRVAL(BRPOL_ONETIME);
	PRVAL(BRPOL_PERSIST);
	PRVAL(CSVC_PING_INTV);
	PRVAL(CSVC_RECONNECT_INTV);
	PRVAL(DEF_READDIR_NENTS);
	PRVAL(DIRENT_TIMEO);
	PRVAL(FCMH_ATTR_TIMEO);
	PRVAL(FCMH_SETATTRF_NONE);
	PRVAL(FIDC_CLI_DEFSZ);
	PRVAL(FIDC_LOOKUP_NONE);
	PRVAL(FID_PATH_DEPTH);
	PRVAL(FID_PATH_START);
	PRVAL(FSID_LEN);
	PRVAL(INTRES_NAME_MAX);
	PRVAL(LNET_NAME_MAX);
	PRVAL(MAX_BMAPS_REQ);
	PRVAL(MAX_BMAP_INODE_PAIRS);
	PRVAL(MAX_BMAP_NCRC_UPDATES);
	PRVAL(MAX_BMAP_RELEASE);
	PRVAL(MAX_READDIR_NENTS);
	PRVAL(MDSIO_FID_ROOT);
	PRVAL(MRSLF_EOF);
	PRVAL(MSCMT_ADDREPLRQ);
	PRVAL(MSCMT_DELREPLRQ);
	PRVAL(MSCMT_GETBIORQ);
	PRVAL(MSCMT_GETBMAP);
	PRVAL(MSCMT_GETCONNS);
	PRVAL(MSCMT_GETFCMH);
	PRVAL(MSCMT_GETREPLST);
	PRVAL(MSCMT_GETREPLST_SLAVE);
	PRVAL(MSCMT_GET_BMAPREPLPOL);
	PRVAL(MSCMT_GET_NEWREPLPOL);
	PRVAL(MSCMT_SET_BMAPREPLPOL);
	PRVAL(MSCMT_SET_NEWREPLPOL);
	PRVAL(MSL_CBARG_BIORQ);
	PRVAL(MSL_CBARG_BIORQS);
	PRVAL(MSL_CBARG_BMAP);
	PRVAL(MSL_CBARG_BMPC);
	PRVAL(MSL_CBARG_BMPCE);
	PRVAL(MSL_CBARG_CSVC);
	PRVAL(MSL_CBARG_RESM);
	PRVAL(MSL_READDIR_CBARG_CSVC);
	PRVAL(MSL_READDIR_CBARG_FCMH);
	PRVAL(MSL_READDIR_CBARG_IOV);
	PRVAL(MSL_READDIR_CBARG_PAGE);
	PRVAL(MS_READAHEAD_MAXPGS);
	PRVAL(MS_READAHEAD_MINSEQ);
	PRVAL(NBREPLST);
	PRVAL(NBRPOL);
	PRVAL(NPREFIOS);
	PRVAL(NSLVRCRC_THRS);
	PRVAL(NUM_BMAP_FLUSH_THREADS);
	PRVAL(REPL_MAX_INFLIGHT_SLVRS);
	PRVAL(RIC_MAX_SLVRS_PER_IO);
	PRVAL(SITE_NAME_MAX);
	PRVAL(SLASH_BLKS_PER_SLVR);
	PRVAL(SLASH_BMAP_CRCSIZE);
	PRVAL(SLASH_BMAP_SIZE);
	PRVAL(SLASH_CRCS_PER_BMAP);
	PRVAL(SLASH_FID_CYCLE_BITS);
	PRVAL(SLASH_FID_CYCLE_BITS);
	PRVAL(SLASH_FID_CYCLE_SHFT);
	PRVAL(SLASH_FID_FLAG_BITS);
	PRVAL(SLASH_FID_FLAG_BITS);
	PRVAL(SLASH_FID_FLAG_SHFT);
	PRVAL(SLASH_FID_INUM_BITS);
	PRVAL(SLASH_FID_INUM_BITS);
	PRVAL(SLASH_FID_INUM_SHFT);
	PRVAL(SLASH_FID_INUM_SHFT);
	PRVAL(SLASH_FID_SITE_BITS);
	PRVAL(SLASH_FID_SITE_BITS);
	PRVAL(SLASH_FID_SITE_SHFT);
	PRVAL(SLASH_FSID);
	PRVAL(SLASH_SLVRS_PER_BMAP);
	PRVAL(SLASH_SLVRS_PER_BMAP);
	PRVAL(SLASH_SLVR_BLKMASK);
	PRVAL(SLASH_SLVR_BLKSZ);
	PRVAL(SLASH_SLVR_BLKSZ);
	PRVAL(SLASH_SLVR_SIZE);
	PRVAL(SLB_MAX);
	PRVAL(SLB_MIN);
	PRVAL(SLB_NBLK);
	PRVAL(SLB_NDEF);
	PRVAL(SLCTL_FCL_ALL);
	PRVAL(SLCTL_FCL_BUSY);
	PRVAL(SLCTL_REST_CLI);
	PRVAL(SLFID_MIN);
	PRVAL(SLFID_NS);
	PRVAL(SLFID_ROOT);
	PRVAL(SLIOD_BMAP_RLS_WAIT_SECS);
	PRVAL(SLI_RIC_BUFSZ);
	PRVAL(SLI_RIC_NBUFS);
	PRVAL(SLI_RIC_NTHREADS);
	PRVAL(SLI_RIC_REPSZ);
	PRVAL(SLI_RII_BUFSZ);
	PRVAL(SLI_RII_NBUFS);
	PRVAL(SLI_RII_NTHREADS);
	PRVAL(SLI_RII_REPSZ);
	PRVAL(SLI_RIM_BUFSZ);
	PRVAL(SLI_RIM_NBUFS);
	PRVAL(SLI_RIM_NTHREADS);
	PRVAL(SLI_RIM_REPSZ);
	PRVAL(SLJ_MDS_ENTSIZE);
	PRVAL(SLJ_MDS_READSZ);
	PRVAL(SLM_NWORKER_THREADS);
	PRVAL(SLM_RECLAIM_BATCH);
	PRVAL(SLM_RMC_BUFSZ);
	PRVAL(SLM_RMC_NBUFS);
	PRVAL(SLM_RMC_NTHREADS);
	PRVAL(SLM_RMC_REPSZ);
	PRVAL(SLM_RMI_BUFSZ);
	PRVAL(SLM_RMI_NBUFS);
	PRVAL(SLM_RMI_NTHREADS);
	PRVAL(SLM_RMI_REPSZ);
	PRVAL(SLM_RMM_BUFSZ);
	PRVAL(SLM_RMM_NBUFS);
	PRVAL(SLM_RMM_NTHREADS);
	PRVAL(SLM_RMM_REPSZ);
	PRVAL(SLM_UPDATE_BATCH);
	PRVAL(SL_BITS_PER_REPLICA);
	PRVAL(SL_DEF_REPLICAS);
	PRVAL(SL_DEF_SNAPSHOTS);
	PRVAL(SL_MAX_BMAPFLSH_RETRIES);
	PRVAL(SL_MAX_IOSREASSIGN);
	PRVAL(SL_MAX_REPLICAS);
	PRVAL(SL_NAME_MAX);
	PRVAL(SL_NBITS_REPLST_BHDR);
	PRVAL(SL_PATH_MAX);
	PRVAL(SL_RES_BITS);
	PRVAL(SL_SITE_BITS);
	PRVAL(SL_TWO_NAME_MAX);
	PRVAL(SRCI_BUFSZ);
	PRVAL(SRCI_BULK_PORTAL);
	PRVAL(SRCI_CTL_PORTAL);
	PRVAL(SRCI_NBUFS);
	PRVAL(SRCI_NTHREADS);
	PRVAL(SRCI_REPSZ);
	PRVAL(SRCI_REP_PORTAL);
	PRVAL(SRCI_REQ_PORTAL);
	PRVAL(SRCI_VERSION);
	PRVAL(SRCM_BUFSZ);
	PRVAL(SRCM_BULK_PORTAL);
	PRVAL(SRCM_CTL_PORTAL);
	PRVAL(SRCM_NBUFS);
	PRVAL(SRCM_NTHREADS);
	PRVAL(SRCM_REPSZ);
	PRVAL(SRCM_REP_PORTAL);
	PRVAL(SRCM_REQ_PORTAL);
	PRVAL(SRCM_VERSION);
	PRVAL(SRIC_BULK_PORTAL);
	PRVAL(SRIC_CTL_PORTAL);
	PRVAL(SRIC_REP_PORTAL);
	PRVAL(SRIC_REQ_PORTAL);
	PRVAL(SRIC_VERSION);
	PRVAL(SRII_BULK_PORTAL);
	PRVAL(SRII_CTL_PORTAL);
	PRVAL(SRII_REP_PORTAL);
	PRVAL(SRII_REQ_PORTAL);
	PRVAL(SRII_VERSION);
	PRVAL(SRIM_BULK_PORTAL);
	PRVAL(SRIM_CTL_PORTAL);
	PRVAL(SRIM_REP_PORTAL);
	PRVAL(SRIM_REQ_PORTAL);
	PRVAL(SRIM_VERSION);
	PRVAL(SRMCTL_OPT_HEALTH);
	PRVAL(SRMC_BULK_PORTAL);
	PRVAL(SRMC_CTL_PORTAL);
	PRVAL(SRMC_REP_PORTAL);
	PRVAL(SRMC_REQ_PORTAL);
	PRVAL(SRMC_VERSION);
	PRVAL(SRMIOP_RD);
	PRVAL(SRMIOP_WR);
	PRVAL(SRMI_BULK_PORTAL);
	PRVAL(SRMI_CTL_PORTAL);
	PRVAL(SRMI_REP_PORTAL);
	PRVAL(SRMI_REQ_PORTAL);
	PRVAL(SRMI_VERSION);
	PRVAL(SRMM_BULK_PORTAL);
	PRVAL(SRMM_CTL_PORTAL);
	PRVAL(SRMM_REP_PORTAL);
	PRVAL(SRMM_REQ_PORTAL);
	PRVAL(SRMM_VERSION);
	PRVAL(SRM_CTLOP_SETOPT);
	PRVAL(SRM_RENAME_NAMEMAX);
	PRVAL(UPSCH_MAX_ITEMS);
	PRVAL(UPSCH_MAX_ITEMS_RES);
	PRVAL(_SLERR_START);
	/* end constants */

	/* start enums */
	printf("\nenums:\n");
	PRVAL(BMAP_OPCNT_BCRSCHED);
	PRVAL(BMAP_OPCNT_BIORQ);
	PRVAL(BMAP_OPCNT_LEASE);
	PRVAL(BMAP_OPCNT_LEASEEXT);
	PRVAL(BMAP_OPCNT_LOOKUP);
	PRVAL(BMAP_OPCNT_READA);
	PRVAL(BMAP_OPCNT_REAPER);
	PRVAL(BMAP_OPCNT_REASSIGN);
	PRVAL(BMAP_OPCNT_REPLWK);
	PRVAL(BMAP_OPCNT_RLSSCHED);
	PRVAL(BMAP_OPCNT_SLVR);
	PRVAL(BMAP_OPCNT_TRUNCWAIT);
	PRVAL(BMAP_OPCNT_UPSCH);
	PRVAL(BMAP_OPCNT_WORK);
	PRVAL(MSTHRT_ATTRFLSH);
	PRVAL(MSTHRT_BMAPFLSH);
	PRVAL(MSTHRT_BMAPFLSHRLS);
	PRVAL(MSTHRT_BMAPFLSHRPC);
	PRVAL(MSTHRT_BMAPLSWATCHER);
	PRVAL(MSTHRT_BMAPREADAHEAD);
	PRVAL(MSTHRT_CONN);
	PRVAL(MSTHRT_CTL);
	PRVAL(MSTHRT_CTLAC);
	PRVAL(MSTHRT_EQPOLL);
	PRVAL(MSTHRT_FS);
	PRVAL(MSTHRT_FSMGR);
	PRVAL(MSTHRT_NBRQ);
	PRVAL(MSTHRT_RCI);
	PRVAL(MSTHRT_RCM);
	PRVAL(MSTHRT_TIOS);
	PRVAL(MSTHRT_USKLNDPL);
	PRVAL(NS_DIR_RECV);
	PRVAL(NS_DIR_SEND);
	PRVAL(NS_NDIRS);
	PRVAL(NS_NOPS);
	PRVAL(NS_NSUMS);
	PRVAL(NS_OP_CREATE);
	PRVAL(NS_OP_LINK);
	PRVAL(NS_OP_MKDIR);
	PRVAL(NS_OP_RECLAIM);
	PRVAL(NS_OP_RENAME);
	PRVAL(NS_OP_RMDIR);
	PRVAL(NS_OP_SETATTR);
	PRVAL(NS_OP_SETSIZE);
	PRVAL(NS_OP_SYMLINK);
	PRVAL(NS_OP_UNLINK);
	PRVAL(NS_SUM_FAIL);
	PRVAL(NS_SUM_PEND);
	PRVAL(NS_SUM_SUCC);
	PRVAL(SLCONNT_CLI);
	PRVAL(SLCONNT_IOD);
	PRVAL(SLCONNT_MDS);
	PRVAL(SLC_FAULT_READAHEAD_CB_EIO);
	PRVAL(SLC_FAULT_READRPC_OFFLINE);
	PRVAL(SLC_FAULT_READ_CB_EIO);
	PRVAL(SLC_FAULT_REQUEST_TIMEOUT);
	PRVAL(SLC_OPST_AIO_PLACED);
	PRVAL(SLC_OPST_BIORQ_ALLOC);
	PRVAL(SLC_OPST_BIORQ_DESTROY);
	PRVAL(SLC_OPST_BIORQ_MAX);
	PRVAL(SLC_OPST_BIORQ_RESTART);
	PRVAL(SLC_OPST_BMAP_DIO);
	PRVAL(SLC_OPST_BMAP_FLUSH);
	PRVAL(SLC_OPST_BMAP_FLUSH_RESCHED);
	PRVAL(SLC_OPST_BMAP_FLUSH_RPCWAIT);
	PRVAL(SLC_OPST_BMAP_LEASE_EXT_ABRT);
	PRVAL(SLC_OPST_BMAP_LEASE_EXT_DONE);
	PRVAL(SLC_OPST_BMAP_LEASE_EXT_FAIL);
	PRVAL(SLC_OPST_BMAP_LEASE_EXT_SEND);
	PRVAL(SLC_OPST_BMAP_REASSIGN_ABRT);
	PRVAL(SLC_OPST_BMAP_REASSIGN_BAIL);
	PRVAL(SLC_OPST_BMAP_REASSIGN_DONE);
	PRVAL(SLC_OPST_BMAP_REASSIGN_FAIL);
	PRVAL(SLC_OPST_BMAP_REASSIGN_SEND);
	PRVAL(SLC_OPST_BMAP_RELEASE);
	PRVAL(SLC_OPST_BMAP_RETRIEVE);
	PRVAL(SLC_OPST_BMPCE_BMAP_REAP);
	PRVAL(SLC_OPST_BMPCE_EIO);
	PRVAL(SLC_OPST_BMPCE_GET);
	PRVAL(SLC_OPST_BMPCE_HIT);
	PRVAL(SLC_OPST_BMPCE_INSERT);
	PRVAL(SLC_OPST_BMPCE_PUT);
	PRVAL(SLC_OPST_BMPCE_REAP);
	PRVAL(SLC_OPST_CLOSE);
	PRVAL(SLC_OPST_CLOSE_DONE);
	PRVAL(SLC_OPST_CREAT);
	PRVAL(SLC_OPST_CREAT_DONE);
	PRVAL(SLC_OPST_DIO_CB0);
	PRVAL(SLC_OPST_DIO_CB_ADD);
	PRVAL(SLC_OPST_DIO_CB_READ);
	PRVAL(SLC_OPST_DIO_CB_WRITE);
	PRVAL(SLC_OPST_DIRCACHE_HIT);
	PRVAL(SLC_OPST_DIRCACHE_HIT_EOF);
	PRVAL(SLC_OPST_DIRCACHE_ISSUE);
	PRVAL(SLC_OPST_DIRCACHE_LOOKUP_HIT);
	PRVAL(SLC_OPST_DIRCACHE_RACE);
	PRVAL(SLC_OPST_DIRCACHE_RAISSUE);
	PRVAL(SLC_OPST_DIRCACHE_REG_ENTRY);
	PRVAL(SLC_OPST_DIRCACHE_REL_ENTRY);
	PRVAL(SLC_OPST_DIRCACHE_WAIT);
	PRVAL(SLC_OPST_FLUSH);
	PRVAL(SLC_OPST_FLUSH_ATTR);
	PRVAL(SLC_OPST_FLUSH_ATTR_WAIT);
	PRVAL(SLC_OPST_FLUSH_DONE);
	PRVAL(SLC_OPST_FSRQ_READ);
	PRVAL(SLC_OPST_FSRQ_READ_FREE);
	PRVAL(SLC_OPST_FSRQ_WRITE);
	PRVAL(SLC_OPST_FSRQ_WRITE_FREE);
	PRVAL(SLC_OPST_FSYNC);
	PRVAL(SLC_OPST_FSYNC_DONE);
	PRVAL(SLC_OPST_GETATTR);
	PRVAL(SLC_OPST_GETXATTR);
	PRVAL(SLC_OPST_GETXATTR_NOSYS);
	PRVAL(SLC_OPST_LEASE_REFRESH);
	PRVAL(SLC_OPST_LINK);
	PRVAL(SLC_OPST_LISTXATTR);
	PRVAL(SLC_OPST_MKDIR);
	PRVAL(SLC_OPST_MKNOD);
	PRVAL(SLC_OPST_OFFLINE_NO_RETRY);
	PRVAL(SLC_OPST_OFFLINE_RETRY);
	PRVAL(SLC_OPST_OFFLINE_RETRY_CLEAR_ERR);
	PRVAL(SLC_OPST_PREFETCH);
	PRVAL(SLC_OPST_READ);
	PRVAL(SLC_OPST_READDIR);
	PRVAL(SLC_OPST_READ_AHEAD);
	PRVAL(SLC_OPST_READ_AHEAD_CB);
	PRVAL(SLC_OPST_READ_AHEAD_CB_ADD);
	PRVAL(SLC_OPST_READ_AHEAD_CB_PAGES);
	PRVAL(SLC_OPST_READ_AIO_NOT_FOUND);
	PRVAL(SLC_OPST_READ_AIO_WAIT);
	PRVAL(SLC_OPST_READ_AIO_WAIT_MAX);
	PRVAL(SLC_OPST_READ_CB);
	PRVAL(SLC_OPST_READ_CB_ADD);
	PRVAL(SLC_OPST_READ_DONE);
	PRVAL(SLC_OPST_READ_RPC_LAUNCH);
	PRVAL(SLC_OPST_REMOVEXATTR);
	PRVAL(SLC_OPST_RENAME);
	PRVAL(SLC_OPST_RENAME_DONE);
	PRVAL(SLC_OPST_RMDIR);
	PRVAL(SLC_OPST_RMDIR_DONE);
	PRVAL(SLC_OPST_RPC_PUSH_REQ_FAIL);
	PRVAL(SLC_OPST_SETATTR);
	PRVAL(SLC_OPST_SETATTR_DONE);
	PRVAL(SLC_OPST_SETXATTR);
	PRVAL(SLC_OPST_SLC_FCMH_CTOR);
	PRVAL(SLC_OPST_SLC_FCMH_DTOR);
	PRVAL(SLC_OPST_SRMT_WRITE);
	PRVAL(SLC_OPST_SRMT_WRITE_CALLBACK);
	PRVAL(SLC_OPST_SYMLINK);
	PRVAL(SLC_OPST_TRUNCATE_FULL);
	PRVAL(SLC_OPST_TRUNCATE_PART);
	PRVAL(SLC_OPST_UNLINK);
	PRVAL(SLC_OPST_UNLINK_DONE);
	PRVAL(SLC_OPST_VERSION);
	PRVAL(SLC_OPST_WRITE);
	PRVAL(SLC_OPST_WRITE_COALESCE);
	PRVAL(SLC_OPST_WRITE_COALESCE_MAX);
	PRVAL(SLC_OPST_WRITE_DONE);
	PRVAL(SLITHRT_ASYNC_IO);
	PRVAL(SLITHRT_BMAPRLS);
	PRVAL(SLITHRT_BREAP);
	PRVAL(SLITHRT_CONN);
	PRVAL(SLITHRT_CTL);
	PRVAL(SLITHRT_CTLAC);
	PRVAL(SLITHRT_HEALTH);
	PRVAL(SLITHRT_LNETAC);
	PRVAL(SLITHRT_NBRQ);
	PRVAL(SLITHRT_REPLPND);
	PRVAL(SLITHRT_RIC);
	PRVAL(SLITHRT_RII);
	PRVAL(SLITHRT_RIM);
	PRVAL(SLITHRT_SLVR_CRC);
	PRVAL(SLITHRT_STATFS);
	PRVAL(SLITHRT_TIOS);
	PRVAL(SLITHRT_USKLNDPL);
	PRVAL(SLI_FAULT_AIO_FAIL);
	PRVAL(SLI_FAULT_CRCUP_FAIL);
	PRVAL(SLI_FAULT_FSIO_READ_FAIL);
	PRVAL(SLI_OPST_AIO_INSERT);
	PRVAL(SLI_OPST_CLOSE_FAIL);
	PRVAL(SLI_OPST_CLOSE_SUCCEED);
	PRVAL(SLI_OPST_CRC_UPDATE);
	PRVAL(SLI_OPST_CRC_UPDATE_BACKLOG);
	PRVAL(SLI_OPST_CRC_UPDATE_BACKLOG_CLEAR);
	PRVAL(SLI_OPST_CRC_UPDATE_CB);
	PRVAL(SLI_OPST_CRC_UPDATE_CB_FAILURE);
	PRVAL(SLI_OPST_FSIO_READ);
	PRVAL(SLI_OPST_FSIO_READ_FAIL);
	PRVAL(SLI_OPST_FSIO_WRITE);
	PRVAL(SLI_OPST_FSIO_WRITE_FAIL);
	PRVAL(SLI_OPST_GET_CUR_SEQ);
	PRVAL(SLI_OPST_GET_CUR_SEQ_RPC);
	PRVAL(SLI_OPST_HANDLE_IO);
	PRVAL(SLI_OPST_HANDLE_PRECLAIM);
	PRVAL(SLI_OPST_HANDLE_REPLREAD);
	PRVAL(SLI_OPST_HANDLE_REPLREAD_AIO);
	PRVAL(SLI_OPST_HANDLE_REPLREAD_REMOVE);
	PRVAL(SLI_OPST_HANDLE_REPL_SCHED);
	PRVAL(SLI_OPST_IOCB_FREE);
	PRVAL(SLI_OPST_IOCB_GET);
	PRVAL(SLI_OPST_IO_PREP_RMW);
	PRVAL(SLI_OPST_ISSUE_REPLREAD);
	PRVAL(SLI_OPST_ISSUE_REPLREAD_CB);
	PRVAL(SLI_OPST_ISSUE_REPLREAD_CB_AIO);
	PRVAL(SLI_OPST_ISSUE_REPLREAD_ERROR);
	PRVAL(SLI_OPST_MIN_SEQNO);
	PRVAL(SLI_OPST_OPEN_FAIL);
	PRVAL(SLI_OPST_OPEN_SUCCEED);
	PRVAL(SLI_OPST_RECLAIM);
	PRVAL(SLI_OPST_RECLAIM_FILE);
	PRVAL(SLI_OPST_RELEASE_BMAP);
	PRVAL(SLI_OPST_REOPEN);
	PRVAL(SLI_OPST_REPL_READAIO);
	PRVAL(SLI_OPST_SEQNO_REDUCE);
	PRVAL(SLI_OPST_SLVR_AIO_REPLY);
	PRVAL(SLI_OPST_SRMT_RELEASE);
	PRVAL(SLI_REPLWKOP_PTRUNC);
	PRVAL(SLI_REPLWKOP_REPL);
	PRVAL(SLMTHRT_BATCHRQ);
	PRVAL(SLMTHRT_BKDB);
	PRVAL(SLMTHRT_BMAPTIMEO);
	PRVAL(SLMTHRT_COH);
	PRVAL(SLMTHRT_CONN);
	PRVAL(SLMTHRT_CTL);
	PRVAL(SLMTHRT_CTLAC);
	PRVAL(SLMTHRT_CURSOR);
	PRVAL(SLMTHRT_JNAMESPACE);
	PRVAL(SLMTHRT_JRECLAIM);
	PRVAL(SLMTHRT_JRNL);
	PRVAL(SLMTHRT_LNETAC);
	PRVAL(SLMTHRT_NBRQ);
	PRVAL(SLMTHRT_RCM);
	PRVAL(SLMTHRT_RMC);
	PRVAL(SLMTHRT_RMI);
	PRVAL(SLMTHRT_RMM);
	PRVAL(SLMTHRT_TIOS);
	PRVAL(SLMTHRT_UPSCHED);
	PRVAL(SLMTHRT_USKLNDPL);
	PRVAL(SLMTHRT_WORKER);
	PRVAL(SLMTHRT_ZFS_KSTAT);
	PRVAL(SLM_FORWARD_CREATE);
	PRVAL(SLM_FORWARD_MKDIR);
	PRVAL(SLM_FORWARD_RENAME);
	PRVAL(SLM_FORWARD_RMDIR);
	PRVAL(SLM_FORWARD_SETATTR);
	PRVAL(SLM_FORWARD_SYMLINK);
	PRVAL(SLM_FORWARD_UNLINK);
	PRVAL(SLM_OPSTATE_INIT);
	PRVAL(SLM_OPSTATE_NORMAL);
	PRVAL(SLM_OPSTATE_REPLAY);
	PRVAL(SLM_OPST_BMAP_CHWRMODE);
	PRVAL(SLM_OPST_BMAP_CHWRMODE_DONE);
	PRVAL(SLM_OPST_BMAP_DIO_CLR);
	PRVAL(SLM_OPST_BMAP_DIO_SET);
	PRVAL(SLM_OPST_BMAP_RELEASE);
	PRVAL(SLM_OPST_COHERENT_CB);
	PRVAL(SLM_OPST_COHERENT_REQ);
	PRVAL(SLM_OPST_CREATE);
	PRVAL(SLM_OPST_EXTEND_BMAP_LEASE);
	PRVAL(SLM_OPST_GETATTR);
	PRVAL(SLM_OPST_GETXATTR);
	PRVAL(SLM_OPST_GET_BMAP_LEASE_READ);
	PRVAL(SLM_OPST_GET_BMAP_LEASE_WRITE);
	PRVAL(SLM_OPST_LINK);
	PRVAL(SLM_OPST_LOGFILE_CREATE);
	PRVAL(SLM_OPST_LOGFILE_REMOVE);
	PRVAL(SLM_OPST_LOOKUP);
	PRVAL(SLM_OPST_MAX_SEQNO);
	PRVAL(SLM_OPST_MIN_SEQNO);
	PRVAL(SLM_OPST_MKDIR);
	PRVAL(SLM_OPST_MKNOD);
	PRVAL(SLM_OPST_ODTABLE_EXTEND);
	PRVAL(SLM_OPST_ODTABLE_FREE);
	PRVAL(SLM_OPST_ODTABLE_FULL);
	PRVAL(SLM_OPST_ODTABLE_REPLACE);
	PRVAL(SLM_OPST_READDIR);
	PRVAL(SLM_OPST_READLINK);
	PRVAL(SLM_OPST_REASSIGN_BMAP_LEASE);
	PRVAL(SLM_OPST_RECLAIM_BATCHNO);
	PRVAL(SLM_OPST_RECLAIM_CURSOR);
	PRVAL(SLM_OPST_RECLAIM_RPC_FAIL);
	PRVAL(SLM_OPST_RECLAIM_RPC_SEND);
	PRVAL(SLM_OPST_RECLAIM_XID);
	PRVAL(SLM_OPST_RENAME);
	PRVAL(SLM_OPST_REPL_SCHEDWK);
	PRVAL(SLM_OPST_SETATTR);
	PRVAL(SLM_OPST_SETXATTR);
	PRVAL(SLM_OPST_STATFS);
	PRVAL(SLM_OPST_SYMLINK);
	PRVAL(SLM_OPST_UNLINK);
	PRVAL(SLREST_ARCHIVAL_FS);
	PRVAL(SLREST_CLUSTER_NOSHARE_LFS);
	PRVAL(SLREST_MDS);
	PRVAL(SLREST_NONE);
	PRVAL(SLREST_PARALLEL_COMPNT);
	PRVAL(SLREST_PARALLEL_LFS);
	PRVAL(SLREST_STANDALONE_FS);
	PRVAL(SLXCTLOP_SET_BMAP_REPLPOL);
	PRVAL(SLXCTLOP_SET_FILE_NEWBMAP_REPLPOL);
	PRVAL(SL_READ);
	PRVAL(SL_WRITE);
	PRVAL(SRMT_BATCH_RP);
	PRVAL(SRMT_BATCH_RQ);
	PRVAL(SRMT_BMAPCHWRMODE);
	PRVAL(SRMT_BMAPCRCWRT);
	PRVAL(SRMT_BMAPDIO);
	PRVAL(SRMT_BMAP_PTRUNC);
	PRVAL(SRMT_BMAP_WAKE);
	PRVAL(SRMT_CONNECT);
	PRVAL(SRMT_CREATE);
	PRVAL(SRMT_CTL);
	PRVAL(SRMT_EXTENDBMAPLS);
	PRVAL(SRMT_GETATTR);
	PRVAL(SRMT_GETBMAP);
	PRVAL(SRMT_GETBMAPCRCS);
	PRVAL(SRMT_GETBMAPMINSEQ);
	PRVAL(SRMT_GETXATTR);
	PRVAL(SRMT_IMPORT);
	PRVAL(SRMT_LINK);
	PRVAL(SRMT_LISTXATTR);
	PRVAL(SRMT_LOOKUP);
	PRVAL(SRMT_MKDIR);
	PRVAL(SRMT_MKNOD);
	PRVAL(SRMT_NAMESPACE_FORWARD);
	PRVAL(SRMT_NAMESPACE_UPDATE);
	PRVAL(SRMT_PING);
	PRVAL(SRMT_PRECLAIM);
	PRVAL(SRMT_READ);
	PRVAL(SRMT_READDIR);
	PRVAL(SRMT_READLINK);
	PRVAL(SRMT_REASSIGNBMAPLS);
	PRVAL(SRMT_RECLAIM);
	PRVAL(SRMT_RELEASEBMAP);
	PRVAL(SRMT_REMOVEXATTR);
	PRVAL(SRMT_RENAME);
	PRVAL(SRMT_REPL_ADDRQ);
	PRVAL(SRMT_REPL_DELRQ);
	PRVAL(SRMT_REPL_GETST);
	PRVAL(SRMT_REPL_GETST_SLAVE);
	PRVAL(SRMT_REPL_READ);
	PRVAL(SRMT_REPL_READAIO);
	PRVAL(SRMT_REPL_SCHEDWK);
	PRVAL(SRMT_RMDIR);
	PRVAL(SRMT_SETATTR);
	PRVAL(SRMT_SETXATTR);
	PRVAL(SRMT_SET_BMAPREPLPOL);
	PRVAL(SRMT_SET_NEWREPLPOL);
	PRVAL(SRMT_STATFS);
	PRVAL(SRMT_SYMLINK);
	PRVAL(SRMT_UNLINK);
	PRVAL(SRMT_WRITE);
	PRVAL(UPDT_BMAP);
	PRVAL(UPDT_HLDROP);
	PRVAL(UPDT_PAGEIN);
	PRVAL(UPDT_PAGEIN_UNIT);
	/* end enums */

	PRVAL(SL_REPLICA_NBYTES);

	PRVAL(LNET_MTU);
	PRVAL(BMAP_OD_SZ);
	PRVAL(BMAP_OD_CRCSZ);

	PRVALX(FID_ANY);

	psc_crc64_calc(&crc, buf, sizeof(buf));
	printf("NULL 1MB buf CRC is %"PSCPRIxCRC64"\n", crc);

	psc_crc64_calc(&crc, &bmapod, sizeof(bmapod));
	printf("NULL sl_blkh_t CRC is %#"PRIx64"\n", crc);

	PRVAL(offsetof(struct bmap_mds_info, bmi_upd));

	PRTYPE(struct pscfs_dirent);

	exit(0);
}
