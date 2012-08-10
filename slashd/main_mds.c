/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2012, Pittsburgh Supercomputing Center (PSC).
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
#include <sqlite3.h>

#include "pfl/fs.h"
#include "pfl/pfl.h"
#include "pfl/str.h"
#include "psc_util/alloc.h"
#include "psc_util/ctlsvr.h"
#include "psc_util/log.h"
#include "psc_util/odtable.h"
#include "psc_util/random.h"
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
#include "worker.h"

#include "zfs-fuse/zfs_slashlib.h"


/* this table is immutable, at least for now */
struct psc_hashtbl		 rootHtable;

GCRY_THREAD_OPTION_PTHREAD_IMPL;

int			 allow_root_uid = 1;
int			 disable_propagation = 0;

int			 current_vfsid;

const char		*progname;

struct slash_creds	 rootcreds = { 0, 0 };
struct pscfs		 pscfs;
uint64_t		 slm_fsuuid[MAX_FILESYSTEMS];
struct psc_thread	*slmconnthr;
uint32_t		 sys_upnonce;

struct odtable		*slm_repl_odt;
struct odtable		*slm_ptrunc_odt;

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
		psclog_error("zpool export failed, will continue");

	if (zfspoolcf)
		rc = snprintf(cmdbuf, sizeof(cmdbuf),
		    "zpool import -f -c '%s' '%s'", zfspoolcf,
		    zpoolname);
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
		psc_fatalx("zpool import: returned %d\n"
		    "check if mount point is empty", WEXITSTATUS(rc));
}

__dead void
usage(void)
{
	fprintf(stderr,
	    "usage: %s [-V] [-D datadir] [-f slashconf] [-p zpoolcache] [-S socket]\n"
	    "\t[zpoolname]\n",
	    progname);
	exit(1);
}

void
slmconnthr_spawn(void)
{
	struct sl_resource *r;
	struct sl_site *s;
	struct sl_resm *m;
	int i, j;

	slmconnthr = slconnthr_spawn(SLMTHRT_CONN, "slm", NULL, NULL);
	CONF_FOREACH_RESM(s, r, i, m, j)
		if (r->res_type == SLREST_MDS)
			slm_getmcsvcf(m, CSVCF_NORECON);
		else if (RES_ISFS(r))
			slm_geticsvcf(m, CSVCF_NORECON);
}

/*
 * Note: The root file system must have fsid of zero, so that a client can
 * see all the file systems in the pool.
 */
int
read_vfsid(int vfsid, char *fn, uint64_t *id)
{
	int rc;
	void *h;
	char *endp;
	size_t nb;
	mdsio_fid_t mf;
	char buf[30];
	
	rc = mdsio_lookup(vfsid, mds_metadir_inum[vfsid], fn, &mf,
	    &rootcreds, NULL);

	/* backward compatibility, assuming one default file system in the pool */
	if (rc == ENOENT && !strcmp(fn, SL_FN_SITEID)) {
		*id = nodeSite->site_id;
		return (0);
	}

	if (rc) {
		psclog_errorx("lookup %s/%s: %s", SL_RPATH_META_DIR,
		    fn, slstrerror(rc));
		goto out;
	}
	rc = mdsio_opencreate(vfsid, mf, &rootcreds, O_RDONLY, 0, NULL,
	    NULL, NULL, &h, NULL, NULL, 0);
	if (rc) {
		psclog_errorx("open %s/%s: %s", SL_RPATH_META_DIR,
		    fn, slstrerror(rc));
		goto out;
	}
	rc = mdsio_read(vfsid, &rootcreds, buf, sizeof(buf), &nb, 0, h);
	mdsio_release(vfsid, &rootcreds, h);

	if (rc) {
		psclog_errorx("read %s/%s: %s", SL_RPATH_META_DIR,
		    fn, slstrerror(rc));
		goto out;
	}

	buf[nb - 1] = '\0';
	* id = strtoull(buf, &endp, 16);
	if (*endp || endp == buf) {
		rc = EINVAL;
		psclog_errorx("read %s/%s: %s", SL_RPATH_META_DIR,
		    fn, slstrerror(rc));
	}
out:
	return (rc);
}


/*
 * XXX Allow an empty file system to register and fill in contents later.
 *     Or use slmctl to register a new file system when it is ready.
 */
void
psc_register_filesystem(int vfsid)
{
	int i, rc, found1, found2;
	uint64_t siteid;
	uint64_t uuid;
	struct psc_hashbkt *b;
	struct rootNames *entry;
	int root_vfsid;
	char *fsname;
    	mdsio_fid_t mfp;
	struct srt_stat sstb;

	psclog_warnx("Checking file system %s\n", zfsMount[vfsid].name);
	if (zfsMount[vfsid].name[0] != '/') {
		psclog_warnx("Bogus file system name: %s", zfsMount[vfsid].name);
		return;
	}

	/*
 	 * Mimic the behaviour of lib/libzfs/libzfs_mount.c.  Because we are not
 	 * fuse mounted, we can't rely on the zfs utility to do this for us.
 	 *
 	 * libzfs_mount.c actually does a mkdirp().  Since we only do one mkdir,
 	 * any new file system must mount directly under the root.
 	 */
	fsname = strchr(&zfsMount[vfsid].name[1], '/');
	if (!(zfsMount[vfsid].flag & ZFS_SLASH2_MKDIR) && fsname) {
		fsname++;
		/* make sure that the newly mounted file system has an entry */
		mdsio_fid_to_vfsid(SLFID_ROOT, &root_vfsid);
		rc = mdsio_lookup(root_vfsid, MDSIO_FID_ROOT, fsname,
		        &mfp, &rootcreds, NULL);
		if (rc == ENOENT) {
			sstb.sst_mode = 0755;
			sstb.sst_uid = rootcreds.scr_uid;
			sstb.sst_gid = rootcreds.scr_gid;
			rc = mdsio_mkdir(root_vfsid, MDSIO_FID_ROOT, fsname, &sstb,
	    			0, MDSIO_OPENCRF_NOLINK, NULL, NULL, NULL, NULL, 0);
		}
		if (rc) {
			psclog_warnx("Verify %s entry: %s", fsname, slstrerror(rc));
			goto out;
		}
	}
	zfsMount[vfsid].flag |= ZFS_SLASH2_MKDIR;

	rc = mdsio_lookup(vfsid, MDSIO_FID_ROOT, SL_RPATH_META_DIR,
	    &mds_metadir_inum[vfsid], &rootcreds, NULL);
	if (rc) {
		psclog_warnx("lookup .slmd metadir: %s", slstrerror(rc));
		goto out;
	}

	rc = mdsio_lookup(vfsid, mds_metadir_inum[vfsid], SL_RPATH_FIDNS_DIR,
		&mds_fidnsdir_inum[vfsid], &rootcreds, NULL);
	if (rc) {
		psclog_warnx("lookup %s/%s dir: %s", SL_RPATH_META_DIR,
			SL_RPATH_FIDNS_DIR, slstrerror(rc));
		goto out;
	}

	rc = mdsio_lookup(vfsid, mds_metadir_inum[vfsid], SL_RPATH_TMP_DIR,
		&mds_tmpdir_inum[vfsid], &rootcreds, NULL);
	if (rc) {
		psclog_warnx("lookup %s/%s dir: %s", SL_RPATH_META_DIR,
			SL_RPATH_TMP_DIR, slstrerror(rc));
		goto out;
	}

	rc = read_vfsid(vfsid, SL_FN_SITEID, &zfsMount[vfsid].siteid);
	if (rc)
		goto out;
	rc = read_vfsid(vfsid, SL_FN_FSUUID, &zfsMount[vfsid].uuid);
	if (rc)
		goto out;

	found1 = 0;
	found2 = 0;
	uuid = zfsMount[vfsid].uuid;
	siteid = zfsMount[vfsid].siteid;
	for (i = 0; i < mount_index; i++) {
		if (i == vfsid)
			continue;
		if (zfsMount[i].siteid == siteid)
			found1++;
		if (zfsMount[i].uuid == uuid)
			found2++;
	}
	if (found1) {
		psclog_warnx("Duplicate SITEID found: %"PRIx64"\n", siteid);
		goto out;
	}
	if (found2) {
		psclog_warnx("Duplicate UUID found: %"PRIx64"\n", uuid);
		goto out;
	}
	rc = zfsslash2_build_immns_cache(vfsid);
	if (rc) {
		psclog_warnx("Fail to create cache for file system %s\n", 
			basename(zfsMount[vfsid].name));
		goto out;
	}

	entry = PSCALLOC(sizeof(struct rootNames));
	if (!entry) {
		psclog_warnx("Fail to allocate memory to register %s\n", 
			basename(zfsMount[vfsid].name));
		goto out;
	}

	strcpy(entry->rn_name, basename(zfsMount[vfsid].name));
	entry->rn_vfsid = vfsid;
	psc_hashent_init(&rootHtable, entry);
	b = psc_hashbkt_get(&rootHtable, entry->rn_name);
	psc_hashbkt_add_item(&rootHtable, b, entry);

	zfsMount[vfsid].flag |= ZFS_SLASH2_READY;
	psclog_warnx("File system %s registered (%"PRIx64":%"PRIx64").\n", 
		basename(zfsMount[vfsid].name), siteid, uuid);
 out:
	return;
}

psc_spinlock_t  scan_lock = SPINLOCK_INIT;

/*
 * Scan for newly added file systems in the pool
 */
void
psc_scan_filesystems(void)
{
	int i;
	spinlock(&scan_lock);
	for (i = 0; i < mount_index; i++) {
		if (!(zfsMount[i].flag & ZFS_SLASH2_READY))
			psc_register_filesystem(i);
	}
	freelock(&scan_lock);
}

int
main(int argc, char *argv[])
{
	char *zpcachefn = NULL, *zpname, fn[PATH_MAX];
	const char *cfn, *sfn, *p;
	int rc, c, found, nofsuuid = 0;

	int vfsid;

	/* gcrypt must be initialized very early on */
	gcry_control(GCRYCTL_SET_THREAD_CBS, &gcry_threads_pthread);
	if (!gcry_check_version(GCRYPT_VERSION))
		errx(1, "libgcrypt version mismatch");

	pfl_init();
	sl_subsys_register();
	psc_subsys_register(SLMSS_ZFS, "zfs");
	psc_subsys_register(SLMSS_JOURNAL, "log");
	psc_subsys_register(SLMSS_UPSCH, "upsch");

#ifdef ZPOOL_PATH
	append_path(ZPOOL_PATH);
#endif

	cfn = SL_PATH_CONF;
	p = getenv("CONFIG_FILE");
	if (p)
		cfn = p;

	progname = argv[0];
	sfn = SL_PATH_SLMCTLSOCK;
	while ((c = getopt(argc, argv, "D:f:p:S:X:YUV")) != -1)
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
		case 'U':
			nofsuuid = 1;
			break;
		case 'V':
			errx(0, "revision is %d", SL_STK_VERSION);
		default:
			usage();
		}

	argc -= optind;
	argv += optind;
	if (argc != 1 && argc != 0)
		usage();

	pscthr_init(SLMTHRT_CTL, 0, NULL, NULL,
	    sizeof(struct psc_ctlthr), "slmctlthr");

	sys_upnonce = psc_random32();

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

	psc_hashtbl_init(&rootHtable, PHTF_STR, struct rootNames,
		rn_name, rn_hentry, 1024, NULL, "rootnames");

	/* using hook can cause layer violation */
	zfsslash2_register_hook(psc_register_filesystem);

	authbuf_createkeyfile();
	authbuf_readkeyfile();

	sl_drop_privs(allow_root_uid);

	libsl_init(2 * (SLM_RMM_NBUFS + SLM_RMI_NBUFS + SLM_RMC_NBUFS));

	for (vfsid = 0; vfsid < mount_index; vfsid++)
		psc_register_filesystem(vfsid);

	found = 0;
	for (vfsid = 0; vfsid < mount_index; vfsid++) {
		if (nodeSite->site_id == zfsMount[vfsid].siteid) {
			psc_assert(!found);
			found = 1;
			current_vfsid = vfsid;
			psclog_warnx("File system %s (id = %d) matches site ID %d",
			    zfsMount[vfsid].name, vfsid, nodeSite->site_id);
		}
	}
	if (!found)
		psc_fatalx("Site ID=%d doesn't match any file system",
		    nodeSite->site_id);

	xmkfn(fn, "%s/%s", sl_datadir, SL_FN_UPSCHDB);
	rc = sqlite3_open(fn, &slm_dbh);
	if (rc)
		psc_fatal("%s: %s", fn, sqlite3_errmsg(slm_dbh));

	lc_reginit(&slm_replst_workq, struct slm_replst_workreq,
	    rsw_lentry, "replstwkq");
	pfl_workq_init(128);
	slm_upsch_init();

	psc_poolmaster_init(&bmapMdsLeasePoolMaster,
	    struct bmap_mds_lease, bml_bmdsi_lentry, PPMF_AUTO, 256,
	    256, 0, NULL, NULL, NULL, "bmplease");
	bmapMdsLeasePool = psc_poolmaster_getmgr(&bmapMdsLeasePoolMaster);

	sl_nbrqset = pscrpc_nbreqset_init(NULL, NULL);
	pscrpc_nbreapthr_spawn(sl_nbrqset, SLMTHRT_NBRQ, "slmnbrqthr");

	psclog_info("SLASH2 metadata daemon (mds) revision is %d",
	    SL_STK_VERSION);

	dbdo(NULL, NULL,
	    " UPDATE	upsch"
	    " SET	status = 'Q'"
	    " WHERE	status = 'S'");

	mds_odtable_load(&mdsBmapAssignTable, SL_FN_BMAP_ODTAB,
	    "bmapassign");
	mds_odtable_load(&slm_repl_odt, SL_FN_REPL_ODTAB, "repl");
	mds_odtable_load(&slm_ptrunc_odt, SL_FN_PTRUNC_ODTAB, "ptrunc");

	mds_journal_init(disable_propagation, zfsMount[current_vfsid].uuid);
	mds_bmap_timeotbl_init();
	mds_odtable_scan(mdsBmapAssignTable, mds_bia_odtable_startup_cb, NULL);
	mds_odtable_scan(slm_repl_odt, slm_repl_odt_startup_cb, NULL);
	mds_odtable_scan(slm_ptrunc_odt, slm_ptrunc_odt_startup_cb, NULL);

	pfl_workq_lock();
	pfl_wkthr_spawn(SLMTHRT_WORKER, SLM_NWORKER_THREADS,
	    "slmwkthr%d");
	pfl_workq_waitempty();

	slmcohthr_spawn();
	slmbmaptimeothr_spawn();
	slmconnthr_spawn();
	slmupschedthr_spawn();
	slmtimerthr_spawn();
	slm_rpc_initsvc();
	slmctlthr_main(sfn);
	/* NOTREACHED */
}
