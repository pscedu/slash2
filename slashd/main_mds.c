/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2011, Pittsburgh Supercomputing Center (PSC).
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

#include <err.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <gcrypt.h>

#include "pfl/fs.h"
#include "pfl/pfl.h"
#include "pfl/str.h"
#include "psc_util/alloc.h"
#include "psc_util/ctlsvr.h"
#include "psc_util/log.h"
#include "psc_util/odtable.h"
#include "psc_util/thread.h"
#include "psc_util/usklndthr.h"

#include "authbuf.h"
#include "bmap_mds.h"
#include "ctl_mds.h"
#include "fidcache.h"
#include "mdscoh.h"
#include "mdsio.h"
#include "mkfn.h"
#include "odtable_mds.h"
#include "pathnames.h"
#include "repl_mds.h"
#include "rpc_mds.h"
#include "slashd.h"
#include "slconfig.h"
#include "slerr.h"
#include "slsubsys.h"
#include "subsys_mds.h"
#include "up_sched_res.h"

#include "zfs-fuse/zfs_slashlib.h"

GCRY_THREAD_OPTION_PTHREAD_IMPL;

int			 allow_root_uid = 1;
int			 disable_propagation = 0;

const char		*progname;

struct psc_poolmaster	 upsched_poolmaster;

struct slash_creds	 rootcreds = { 0, 0 };
struct pscfs		 pscfs;

int
psc_usklndthr_get_type(const char *namefmt)
{
	if (strstr(namefmt, "lnacthr"))
		return (SLMTHRT_LNETAC);
	return (SLMTHRT_USKLNDPL);
}

void
psc_usklndthr_get_namev(char buf[PSC_THRNAME_MAX], const char *namefmt,
    va_list ap)
{
	size_t n;

	n = strlcpy(buf, "slm", PSC_THRNAME_MAX);
	if (n < PSC_THRNAME_MAX)
		vsnprintf(buf + n, PSC_THRNAME_MAX - n, namefmt, ap);
}

void
append_path(const char *newpath)
{
	const char *path;
	size_t len;
	char *p;
	int rc;

	path = getenv("PATH");
	len = (path ? strlen(path) + 1 : 0) + strlen(newpath) + 1;
	p = PSCALLOC(len);
	rc = snprintf(p, len, "%s%s%s", path ? path : "",
	    path && path[0] != '\0' ? ":" : "", newpath);
	if (rc == -1)
		err(1, "%s", newpath);
	else if (rc >= (int)len)
		errx(1, "impossible");
	if (setenv("PATH", p, 1) == -1)
		err(1, "setenv");
	PSCFREE(p);
}

void
import_zpool(const char *zpoolname, const char *zfspoolcf)
{
	char cmdbuf[BUFSIZ];
	int rc;

	rc = snprintf(cmdbuf, sizeof(cmdbuf), "zpool export '%s'",
	    zpoolname);
	if (rc == -1)
		psc_fatal("%s", zpoolname);
	else if (rc >= (int)sizeof(cmdbuf))
		psc_fatalx("pool name too long: %s", zpoolname);
	rc = system(cmdbuf);
	if (rc == -1)
		psc_error("zpool export failed, will continue");

	if (zfspoolcf)
		rc = snprintf(cmdbuf, sizeof(cmdbuf),
		    "zpool import -f -c '%s' '%s'", zfspoolcf, zpoolname);
	else
		rc = snprintf(cmdbuf, sizeof(cmdbuf),
		    "zpool import -f '%s'", zpoolname);
	if (rc == -1)
		psc_fatal("%s", zpoolname);
	else if (rc >= (int)sizeof(cmdbuf))
		psc_fatalx("pool name too long: %s", zpoolname);
	rc = system(cmdbuf);
	if (rc == -1)
		psc_fatal("zpool import");
	else if (rc)
		psc_fatalx("zpool import: returned %d", rc);
}

__dead void
usage(void)
{
	fprintf(stderr,
	    "usage: %s [-D datadir] [-f slashconf] [-p zpoolcache] [-S socket]\n"
	    "\t[zpoolname]\n",
	    progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	char *zpcachefn = NULL, *zpname;
	const char *cfn, *sfn;
	int rc, c;

	/* gcrypt must be initialized very early on */
	gcry_control(GCRYCTL_SET_THREAD_CBS, &gcry_threads_pthread);
	if (!gcry_check_version(GCRYPT_VERSION))
		errx(1, "libgcrypt version mismatch");

	pfl_init();
	sl_subsys_register();
	psc_subsys_register(SLMSS_ZFS, "zfs");
	psc_subsys_register(SLMSS_JOURNAL, "log");

#ifdef ZPOOL_PATH
	append_path(ZPOOL_PATH);
#endif

	progname = argv[0];
	cfn = SL_PATH_CONF;
	sfn = SL_PATH_SLMCTLSOCK;
	while ((c = getopt(argc, argv, "D:f:p:S:X:Y")) != -1)
		switch (c) {
		case 'D':
			sl_datadir = optarg;
			break;
		case 'f':
			cfn = optarg;
			break;
		case 'p':
			zpcachefn = optarg;
			break;
		case 'S':
			sfn = optarg;
			break;
		case 'X': /* undocumented, developer only */
			allow_root_uid = 1;
			break;
		case 'Y': /* undocumented, developer only */
			disable_propagation = 1;
			break;
		default:
			usage();
		}
	argc -= optind;
	argv += optind;
	if (argc != 1 && argc != 0)
		usage();

	pscthr_init(SLMTHRT_CTL, 0, NULL, NULL,
	    sizeof(struct psc_ctlthr), "slmctlthr");

	slcfg_parse(cfn);

	if (zpcachefn == NULL && globalConfig.gconf_zpcachefn[0])
		zpcachefn = globalConfig.gconf_zpcachefn;
	if (argc)
		zpname = argv[0];
	else if (globalConfig.gconf_zpname[0])
		zpname = globalConfig.gconf_zpname;
	else {
		warn("no ZFS pool specified");
		usage();
	}

	fidc_init(sizeof(struct fcmh_mds_info), FIDC_MDS_DEFSZ);
	bmap_cache_init(sizeof(struct bmap_mds_info));

	/*
	 * Initialize the mdsio layer.  There is where ZFS threads
	 * are started and the given ZFS pool is imported.
	 */
	mdsio_init();
	import_zpool(zpname, zpcachefn);

	rc = mdsio_lookup(MDSIO_FID_ROOT, SL_RPATH_META_DIR,
	    &mds_metadir_inum, &rootcreds, NULL);
	if (rc)
		psc_fatalx("lookup .slmd metadir: %s", slstrerror(rc));

	rc = mdsio_lookup(mds_metadir_inum, SL_RPATH_UPSCH_DIR,
	    &mds_upschdir_inum, &rootcreds, NULL);
	if (rc)
		psc_fatalx("lookup %s/%s dir: %s", SL_RPATH_META_DIR,
		    SL_RPATH_UPSCH_DIR, slstrerror(rc));

	rc = mdsio_lookup(mds_metadir_inum, SL_RPATH_FIDNS_DIR,
	    &mds_fidnsdir_inum, &rootcreds, NULL);
	if (rc)
		psc_fatalx("lookup %s/%s dir: %s", SL_RPATH_META_DIR,
		    SL_RPATH_FIDNS_DIR, slstrerror(rc));

	rc = mdsio_lookup(mds_metadir_inum, SL_RPATH_TMP_DIR,
	    &mds_tmpdir_inum, &rootcreds, NULL);
	if (rc)
		psc_fatalx("lookup %s/%s dir: %s", SL_RPATH_META_DIR,
		    SL_RPATH_TMP_DIR, slstrerror(rc));

	zfsslash2_build_immns_cache();

	authbuf_createkeyfile();
	authbuf_readkeyfile();

	sl_drop_privs(allow_root_uid);

	libsl_init(PSCNET_SERVER, 1);

	slm_workq_init();

	psc_poolmaster_init(&upsched_poolmaster,
	    struct up_sched_work_item, uswi_lentry, PPMF_AUTO, 256, 256,
	    0, NULL, NULL, NULL, "upschwk");
	upsched_pool = psc_poolmaster_getmgr(&upsched_poolmaster);

	psc_poolmaster_init(&bmapMdsLeasePoolMaster,
	    struct bmap_mds_lease, bml_bmdsi_lentry, PPMF_AUTO, 256,
	    256, 0, NULL, NULL, NULL, "bmplease");
	bmapMdsLeasePool = psc_poolmaster_getmgr(&bmapMdsLeasePoolMaster);

	lc_reginit(&slm_replst_workq, struct slm_replst_workreq,
	    rsw_lentry, "replstwkq");

	lc_reginit(&pndgBmapCbs, struct bmap_mds_lease, bml_coh_lentry,
	    "pendingbml");

	lc_reginit(&inflBmapCbs, struct bmap_mds_lease, bml_coh_lentry,
	    "inflightbml");

	sl_nbrqthr_spawn(SLMTHRT_NBRQ, "slmnbrqthr");
	mds_journal_init(disable_propagation);

	mds_odtable_load(&mdsBmapAssignTable, SL_FN_BMAP_ODTAB, "bmapassign");

	mds_bmap_timeotbl_init();

	mds_odtable_scan(mdsBmapAssignTable, mds_bia_odtable_startup_cb);

	slm_workers_spawn();
	slmcohthr_spawn();
	slmbmaptimeothr_spawn();
	slm_rpc_initsvc();

	/* start an update scheduler thread for each site */
	slmupschedthr_spawnall();

	mds_repl_init();
	slmtimerthr_spawn();
	slmctlthr_main(sfn);
	/* NOTREACHED */
}
