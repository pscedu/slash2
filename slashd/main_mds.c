/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2008-2018, Pittsburgh Supercomputing Center
 * All rights reserved.
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
 * ---------------------------------------------------------------------
 * %END_LICENSE%
 */

#include <sys/types.h>

#include <dirent.h>
#include <err.h>
#include <libgen.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <gcrypt.h>

#include "pfl/alloc.h"
#include "pfl/ctlsvr.h"
#include "pfl/fault.h"
#include "pfl/fs.h"
#include "pfl/log.h"
#include "pfl/odtable.h"
#include "pfl/pfl.h"
#include "pfl/random.h"
#include "pfl/str.h"
#include "pfl/sys.h"
#include "pfl/thread.h"
#include "pfl/timerthr.h"
#include "pfl/usklndthr.h"
#include "pfl/workthr.h"

#include "authbuf.h"
#include "bmap_mds.h"
#include "ctl_mds.h"
#include "fidcache.h"
#include "mdsio.h"
#include "mkfn.h"
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

extern const char *__progname;

sqlite3                 *db_handle;

int			 current_vfsid;

struct slash_creds	 rootcreds = { 0, 0 };
struct pscfs		 pscfs;
struct psc_thread	*slmconnthr;
uint32_t		 sl_sys_upnonce;

/* this table is immutable, at least for now */
struct psc_hashtbl	 slm_roots;

struct psc_listcache	 slm_db_hipri_workq;
struct psc_listcache	 slm_db_lopri_workq;

int			 slm_opstate;

struct psc_poolmaster	 slm_bml_poolmaster;
struct psc_poolmgr	*slm_bml_pool;

struct psc_poolmaster	 slm_repl_status_poolmaster;

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

/*
 * Use system() calls to import pool and mount file systems.  Note that
 * the paths needed by the system() are compiled in to potentially avoid
 * system binaries (e.g. ZFSOnLinux).
 *
 * We don't check WEXITSTATUS(rc) after a system() call because
 * sometimes the ZFS tool can return an error (e.g. EEXIST) even if the
 * pool is otherwise healthy.
 */
void
import_zpool(const char *zpoolname, const char *zfspoolcf)
{
	char scratchbuf[BUFSIZ];
	struct dirent *d;
	int i, rc;
	DIR *dir;

	rc = snprintf(scratchbuf, sizeof(scratchbuf), "/%s", zpoolname);
	psc_assert(rc < (int)sizeof(scratchbuf) && rc >= 0);

	/*
	 * ZFS fuse can create the mount point automatically if it does
	 * not exist.  However, if the mount point exists and is not
	 * empty, it does not mount the default file system in the pool
	 * for some reason.
	 */
	dir = opendir(scratchbuf);
	if (dir) {
		i = 0;
		while ((d = readdir(dir)) != NULL) {
			if (i++ < 2)
				continue;
			errno = ENOTEMPTY;
			psc_fatal("Please clean up directory %s before mount",
			    scratchbuf);
		}
		closedir(dir);
	}

#if 0
	/*
 	 *  The following message during start up should be harmless:
 	 *
	 * cannot import XXX: a pool with that name is already created/imported,
	 * and no additional pools with that name were found
	 * cannot mount XXX: mountpoint or dataset is busy
	 */
	rc = pfl_systemf("zpool import -f %s%s%s '%s'",
	    zfspoolcf ? "-c '" : "",
	    zfspoolcf ? zfspoolcf : "",
	    zfspoolcf ? "'" : "",
	    zpoolname);
	if (rc == -1)
		psc_fatal("failed to execute command to import zpool "
		    "%s: %s", zpoolname, scratchbuf);
#endif

#if 0
	/* mount the default file system in the pool */
	rc = pfl_systemf("zfs mount %s", zpoolname);
	if (rc == -1)
		psc_fatal("failed to execute command to mount %s",
		    zpoolname);
#endif

#if 1
	/* mount the other MDS file systems from the pool */
	rc = system("zfs mount -a");
	if (rc == -1)
		psc_fatal("failed to execute command to mount file systems");
#endif

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
			; // slm_getmcsvcf(m, CSVCF_NORECON);
		else if (RES_ISFS(r))
			slm_geticsvcf(m, CSVCF_NORECON, 0);
}

/*
 * Note: The root file system must have fsid of zero, so that a client
 * can see all the file systems in the pool.
 */
int
read_vfsid(int vfsid, char *fn, uint64_t *field)
{
	int rc;
	void *h;
	char *endp;
	size_t nb;
	mdsio_fid_t mf;
	char buf[32];

	rc = mdsio_lookup(vfsid, mds_metadir_inum[vfsid], fn, &mf,
	    &rootcreds, NULL);
	if (rc) {
		psclog_errorx("lookup %s/%s: %s", SL_RPATH_META_DIR,
		    fn, sl_strerror(rc));
		goto out;
	}
	rc = mdsio_opencreate(vfsid, mf, &rootcreds, O_RDONLY, 0, NULL,
	    NULL, NULL, &h, NULL, NULL, 0);
	if (rc) {
		psclog_errorx("open %s/%s: %s", SL_RPATH_META_DIR,
		    fn, sl_strerror(rc));
		goto out;
	}
	rc = mdsio_read(vfsid, &rootcreds, buf, sizeof(buf), &nb, 0, h);
	mdsio_release(vfsid, &rootcreds, h);

	if (rc) {
		psclog_errorx("read %s/%s: %s", SL_RPATH_META_DIR,
		    fn, sl_strerror(rc));
		goto out;
	}

	buf[nb - 1] = '\0';
	*field = strtoull(buf, &endp, 16);
	if (*endp || endp == buf) {
		rc = EINVAL;
		psclog_errorx("read %s/%s: %s", SL_RPATH_META_DIR,
		    fn, sl_strerror(rc));
	}
 out:
	return (rc);
}

/*
 * XXX Allow an empty file system to register and fill in contents
 * later.  Or use slmctl to register a new file system when it is ready.
 */
void
slm_mdfs_register(int vfsid)
{
	int i, rc, found1, found2, root_vfsid;
	uint64_t resid, uuid;
	struct mio_rootnames *rn;
	struct srt_stat sstb;
	sl_siteid_t siteid;
	mdsio_fid_t mfp;
	char *fsname;

	psclog_info("Checking file system %s", zfs_mounts[vfsid].zm_name);
	if (zfs_mounts[vfsid].zm_name[0] != '/') {
		psclog_warnx("Bogus file system name: %s",
		    zfs_mounts[vfsid].zm_name);
		return;
	}

	/*
	 * Mimic the behaviour of lib/libzfs/libzfs_mount.c.  Because we
	 * are not fuse mounted, we can't rely on the zfs utility to do
	 * this for us.
	 *
	 * libzfs_mount.c actually does a mkdirp().  Since we only do
	 * one mkdir, any new file system must mount directly under the
	 * root.
	 */
	fsname = strchr(&zfs_mounts[vfsid].zm_name[1], '/');
	if (!(zfs_mounts[vfsid].zm_flags & ZFS_SLASH2_MKDIR) && fsname) {
		fsname++;
		/*
		 * Make sure that the newly mounted file system has an
		 * entry.
		 */
		slfid_to_vfsid(SLFID_ROOT, &root_vfsid);
		rc = mdsio_lookup(root_vfsid, MDSIO_FID_ROOT, fsname,
		    &mfp, &rootcreds, NULL);
		if (rc == ENOENT) {
			sstb.sst_mode = 0755;
			sstb.sst_uid = rootcreds.scr_uid;
			sstb.sst_gid = rootcreds.scr_gid;
			rc = mdsio_mkdir(root_vfsid, MDSIO_FID_ROOT,
			    fsname, &sstb, 0, MDSIO_OPENCRF_NOLINK,
			    NULL, NULL, NULL, NULL, 0);
		}
		if (rc) {
			psclog_warnx("Verify %s entry: %s", fsname,
			    sl_strerror(rc));
			return;
		}
	}
	zfs_mounts[vfsid].zm_flags |= ZFS_SLASH2_MKDIR;

	rc = mdsio_lookup(vfsid, MDSIO_FID_ROOT, SL_RPATH_META_DIR,
	    &mds_metadir_inum[vfsid], &rootcreds, NULL);
	if (rc) {
		psclog_warnx("lookup .slmd metadir: %s", sl_strerror(rc));
		return;
	}

	rc = mdsio_lookup(vfsid, mds_metadir_inum[vfsid],
	    SL_RPATH_FIDNS_DIR, &mds_fidnsdir_inum[vfsid], &rootcreds,
	    NULL);
	if (rc) {
		psclog_warnx("lookup %s/%s dir: %s", SL_RPATH_META_DIR,
		    SL_RPATH_FIDNS_DIR, sl_strerror(rc));
		return;
	}

	rc = mdsio_lookup(vfsid, mds_metadir_inum[vfsid],
	    SL_RPATH_TMP_DIR, &mds_tmpdir_inum[vfsid], &rootcreds,
	    NULL);
	if (rc) {
		psclog_warnx("lookup %s/%s dir: %s", SL_RPATH_META_DIR,
		    SL_RPATH_TMP_DIR, sl_strerror(rc));
		return;
	}

	rc = read_vfsid(vfsid, SL_FN_RESID, &resid);
	if (rc)
		return;
	zfs_mounts[vfsid].zm_siteid = sl_resid_to_siteid(resid);
	rc = read_vfsid(vfsid, SL_FN_FSUUID, &zfs_mounts[vfsid].zm_uuid);
	if (rc)
		return;

	found1 = 0;
	found2 = 0;
	uuid = zfs_mounts[vfsid].zm_uuid;
	siteid = zfs_mounts[vfsid].zm_siteid;
	for (i = 0; i < zfs_nmounts; i++) {
		if (i == vfsid)
			continue;
		if (zfs_mounts[i].zm_siteid == siteid)
			found1++;
		if (zfs_mounts[i].zm_uuid == uuid)
			found2++;
	}
	if (found1) {
		psclog_warnx("duplicate SITEID found: %hu", siteid);
		return;
	}
	if (found2) {
		psclog_warnx("duplicate UUID found: %"PRIx64, uuid);
		return;
	}
	rc = zfsslash2_build_immns_cache(vfsid);
	if (rc) {
		psclog_warnx("failed to create cache for file system %s",
		    pfl_basename(zfs_mounts[vfsid].zm_name));
		return;
	}

	rn = PSCALLOC(sizeof(*rn));
	strlcpy(rn->rn_name, pfl_basename(zfs_mounts[vfsid].zm_name),
	    sizeof(rn->rn_name));
	rn->rn_vfsid = vfsid;
	psc_hashent_init(&slm_roots, rn);
	psc_hashtbl_add_item(&slm_roots, rn);

	zfs_mounts[vfsid].zm_flags |= ZFS_SLASH2_READY;
	psclog_info("file system %s registered (site_id=%hu "
	    "uuid=%"PRIx64")",
	    pfl_basename(zfs_mounts[vfsid].zm_name), siteid, uuid);
}

/*
 * Scan for newly added file systems in the pool.
 */
void
slm_mdfs_scan(void)
{
	static psc_spinlock_t scan_lock = SPINLOCK_INIT;
	int i;

	spinlock(&scan_lock);
	for (i = 0; i < zfs_nmounts; i++)
		if (!(zfs_mounts[i].zm_flags & ZFS_SLASH2_READY))
			slm_mdfs_register(i);
	freelock(&scan_lock);
}

__dead void
usage(void)
{
	fprintf(stderr,
	    "usage: %s [-V] [-D datadir] [-f slashconf] [-p zpoolcache] [-S socket]\n"
	    "\t[zpoolname]\n",
	    __progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	size_t size;
	char *path_env, *zpcachefn = NULL, *zpname, *estr;
	const char *cfn, *sfn, *p;
	int i, c, rc, vfsid, found, total;
	struct psc_thread *thr;
	time_t now;
	struct psc_thread *me;
	char dbfn[PATH_MAX];

	/* gcrypt must be initialized very early on */
	gcry_control(GCRYCTL_SET_THREAD_CBS, &gcry_threads_pthread);
	if (!gcry_check_version(GCRYPT_VERSION))
		errx(1, "libgcrypt version mismatch");

	pfl_init();
	sl_subsys_register();
	pfl_subsys_register(SLMSS_ZFS, "zfs");
	pfl_subsys_register(SLMSS_JOURNAL, "log");
	pfl_subsys_register(SLMSS_UPSCH, "upsch");
	pfl_subsys_register(SLMSS_INFO, "info");

	rc = pfl_asprintf(&path_env, "%s:%s:%s", ZFS_BIN_PATH,
	    ZPOOL_PATH, getenv("PATH"));
	psc_assert(rc != -1);
	setenv("PATH", path_env, 1);

	sfn = SL_PATH_SLMCTLSOCK;
	p = getenv("CTL_SOCK_FILE");
	if (p)
		sfn = p;

	cfn = SL_PATH_CONF;
	p = getenv("CONFIG_FILE");
	if (p)
		cfn = p;

	while ((c = getopt(argc, argv, "D:f:p:S:V")) != -1)
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
		case 'V':
			errx(0, "version is %d", sl_stk_version);
		default:
			usage();
		}

	argc -= optind;
	argv += optind;
	if (argc != 1 && argc != 0)
		usage();

	pscthr_init(SLMTHRT_CTL, NULL, 
	    sizeof(struct psc_ctlthr), "slmctlthr0");

	sl_sys_upnonce = psc_random32();

	slcfg_local->cfg_fidcachesz = MDS_FIDCACHE_SIZE;
	slcfg_parse(cfn);

	libsl_init(2 * (SLM_RMM_NBUFS + SLM_RMI_NBUFS + SLM_RMC_NBUFS));

	/* override global defaults with cfg settings */
	if (zpcachefn == NULL && slcfg_local->cfg_zpcachefn)
		zpcachefn = slcfg_local->cfg_zpcachefn;
	if (argc)
		zpname = argv[0];
	else if (slcfg_local->cfg_zpname[0])
		zpname = slcfg_local->cfg_zpname;
	else {
		warnx("no ZFS pool specified");
		usage();
	}

	fidc_init(sizeof(struct fcmh_mds_info));
	bmap_cache_init(sizeof(struct bmap_mds_info), MDS_BMAP_COUNT, NULL);

	/*
	 * Start up ZFS threads and import the MDS zpool.  Also, make
	 * sure ARC max size is finalized before calling arc_init().
	 */
	arc_set_slashd();
	if (slcfg_local->cfg_arc_max)
		arc_set_maxsize(slcfg_local->cfg_arc_max);

	rc = mdsio_init();
	if (rc) {
		/* 08/03/2016: saw this today and the mds is still up */
		psc_fatalx("failed to initialize ZFS, rc= %d", rc);
	}
	import_zpool(zpname, zpcachefn);

	psc_hashtbl_init(&slm_roots, PHTF_STR, struct mio_rootnames,
	    rn_name, rn_hentry, 97, NULL, "rootnames");

	authbuf_createkeyfile();
	authbuf_readkeyfile();

	sl_drop_privs(1);

	/* startup meter */
	pfl_meter_destroy(&res2mdsinfo(sl_resprof)->sp_batchmeter);

	for (vfsid = 0; vfsid < zfs_nmounts; vfsid++)
		slm_mdfs_register(vfsid);

	if (!zfs_nmounts)
		/* 
		 * Sometimes you need to do an export/import cycle
		 * or just run zfs mount -a.
		 */
		errx(1, "No ZFS file system found! Try zpool import or zfs mount "
			"with zfs-fuse first.");

	found = 0;
	for (vfsid = 0; vfsid < zfs_nmounts; vfsid++) {
		/* nodeSite->site_id is nodeResm->resm_res->res_site */
		if (nodeSite->site_id == zfs_mounts[vfsid].zm_siteid) {
			psc_assert(!found);
			found = 1;
			current_vfsid = vfsid;
			psclog_info("file system %s (id=%d) "
			    "matches site ID %d",
			    zfs_mounts[vfsid].zm_name, vfsid,
			    nodeSite->site_id);
		}
	}
	if (!found) {
		fprintf(stderr,
		    "------------------------------------------------\n"
		    "file systems available:\n");
		for (vfsid = 0; vfsid < zfs_nmounts; vfsid++)
			fprintf(stderr,
			    "  file system %3d: %s\tid=%hu\n",
			    vfsid, zfs_mounts[vfsid].zm_name,
			    zfs_mounts[vfsid].zm_siteid);
		errx(1, "site id=%d doesn't match any file system",
		    nodeSite->site_id);
	}

	if (zfs_mounts[current_vfsid].zm_uuid !=
	    globalConfig.gconf_fsuuid)
		psc_fatalx("FSUUID do not match; "
		    "ZFS=%"PRIx64" slcfg=%"PRIx64,
		    zfs_mounts[current_vfsid].zm_uuid,
		    globalConfig.gconf_fsuuid);

	psc_poolmaster_init(&slm_repl_status_poolmaster,
	    struct slm_replst_workreq, rsw_lentry, PPMF_AUTO, 64,
	    64, 0, NULL, "replst");
	slm_repl_status_pool = psc_poolmaster_getmgr(
	    &slm_repl_status_poolmaster);

	lc_reginit(&slm_replst_workq, struct slm_replst_workreq,
	    rsw_lentry, "replstwkq");

	size = sizeof(struct slm_wkdata_wr_brepl);
	if (size < sizeof(struct slm_wkdata_upsch_purge))
		size = sizeof(struct slm_wkdata_upsch_purge);
	if (size < sizeof(struct slm_wkdata_upschq))
		size = sizeof(struct slm_wkdata_upschq);
	pfl_workq_init(size, 1024, 2048);

	slm_upsch_init();

	psc_poolmaster_init(&slm_bml_poolmaster,
	    struct bmap_mds_lease, bml_bmi_lentry, PPMF_AUTO, 2048,
	    2048, 0, NULL, "bmplease");
	slm_bml_pool = psc_poolmaster_getmgr(&slm_bml_poolmaster);

	sl_nbrqset = pscrpc_prep_set();
	pscrpc_nbreapthr_spawn(sl_nbrqset, SLMTHRT_NBRQ, 8,
	    "slmnbrqthr");

	slm_opstate = SLM_OPSTATE_REPLAY;

	pfl_odt_load(&slm_bia_odt, &slm_odtops, 0, SL_FN_BMAP_ODTAB,
	    "bmapassign");

	mds_bmap_timeotbl_init();

	slrpc_initcli();
	mds_update_boot_file();

	rc = sqlite3_threadsafe();
	if (rc == SQLITE_CONFIG_SINGLETHREAD)
		psclog_warnx("SQLite is configured in single-threaded mode.");

	sqlite3_enable_shared_cache(1);

	xmkfn(dbfn, "%s/%s", SL_PATH_DEV_SHM, SL_FN_UPSCHDB);
	rc = sqlite3_open_v2(dbfn, &db_handle, 
		SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);

	if (rc != SQLITE_OK)
		psc_fatalx("Fail to open/create SQLite data base %s", dbfn);

	rc = sqlite3_exec(db_handle,
		"PRAGMA integrity_check", NULL, NULL, &estr);
	if (rc != SQLITE_OK)
		psc_fatalx("SQLite data base %s is corrupted", dbfn);

	dbdo(NULL, NULL, "PRAGMA synchronous=OFF");
	dbdo(NULL, NULL, "PRAGMA journal_mode=WAL");

	total = 0;
	dbdo(slm_upsch_tally_cb, &total,
		"SELECT count(*) "
		"FROM sqlite_master WHERE type = 'table'");

	if (!total) {
		psclog_warnx("Creating a new upsch table for replication.");

		dbdo(NULL, NULL,
		    "CREATE TABLE upsch ("
		    "	id		INT PRIMARY KEY,"
		    "	resid		UNSIGNED INT,"
		    "	fid		UNSIGNED BIGINT,"
		    "	uid		UNSIGNED INT,"
		    "	gid		UNSIGNED INT,"
		    "	bno		UNSIGNED INT,"
		    "	status		CHAR(1),"
		    "	sys_prio	INT,"
		    "	usr_prio	INT,"
		    "	nonce		UNSIGNED INT,"
		    "	UNIQUE(resid, fid, bno)"
		    ")");

		dbdo(NULL, NULL,
		    "CREATE INDEX 'upsch_resid_idx'"
		    " ON 'upsch' ('resid')");
		dbdo(NULL, NULL,
		    "CREATE INDEX 'upsch_fid_idx'"
		    " ON 'upsch' ('fid')");
		dbdo(NULL, NULL,
		    "CREATE INDEX 'upsch_bno_idx'"
		    " ON 'upsch' ('bno')");
#if 0
		dbdo(NULL, NULL,
		    "CREATE INDEX 'upsch_uid_idx'"
		    " ON 'upsch' ('uid')");
		dbdo(NULL, NULL,
		    "CREATE INDEX 'upsch_gid_idx'"
		    " ON 'upsch' ('gid')");

		dbdo(NULL, NULL,
		    "CREATE VIEW gsort AS"
		    " SELECT	gid,"
		    "		RANDOM() AS rnd"
		    " FROM	upsch"
		    " GROUP BY	gid");
		dbdo(NULL, NULL,
		    "CREATE VIEW usort AS"
		    " SELECT	uid,"
		    "		RANDOM() AS rnd"
		    " FROM	upsch"
		    " GROUP BY uid");
#endif
	} else {
		total = 0;
		dbdo(slm_upsch_tally_cb, &total,
		    "SELECT count(*) "
		    "FROM sqlite_master "
		    "WHERE type = 'table' AND name = 'upsch'");
		if (total != 1)
			psc_fatalx("SQLite data base %s is problematic", dbfn);
		total = 0;
		dbdo(slm_upsch_tally_cb, &total,
		    "SELECT count (*)"
	    	    "FROM upsch");
		psclog_warnx("Reusing existing table (%d rows) for replication.", 
		    total);
	}

	dbdo(NULL, NULL, "BEGIN TRANSACTION");

	lc_reginit(&slm_db_hipri_workq, struct pfl_workrq, wkrq_lentry,
	    "db-hipri-workq");
	lc_reginit(&slm_db_lopri_workq, struct pfl_workrq, wkrq_lentry,
	    "db-lopri-workq");

	mds_journal_init(zfs_mounts[current_vfsid].zm_uuid);
	dbdo(NULL, NULL, "COMMIT");

	dbdo(slm_upsch_requeue_cb, NULL,
	    " SELECT	fid,"
	    "		bno"
	    " FROM	upsch"
	    " WHERE	status = 'S'");

	dbdo(NULL, NULL,
	    " UPDATE	upsch"
	    " SET	status = 'Q'"
	    " WHERE	status = 'S'");

	slmctlthr_spawn(sfn);
	pfl_opstimerthr_spawn(SLMTHRT_OPSTIMER, "slmopstimerthr");
	time(&now);
	psclog_max("SLASH2 utility slmctl is now ready at %s", ctime(&now));

	pfl_odt_check(slm_bia_odt, mds_bia_odtable_startup_cb, NULL);

	/*
	 * As soon as log replay is over, we should be able to set the
	 * state to NORMAL.  However, we had issues when trying to write
	 * new log entries while replaying odtable.  So keep it this way
	 * for now.
	 */
	slm_opstate = SLM_OPSTATE_NORMAL;

	pfl_workq_lock();
	pfl_wkthr_spawn(SLMTHRT_WORKER, SLM_NWORKER_THREADS, 0, "slmwkthr%d");
	pfl_workq_waitempty();

	for (i = 0; i < 4; i++) {
		thr = pscthr_init(SLMTHRT_DBWORKER, pfl_wkthr_main,
		    sizeof(struct slmdbwk_thread), "slmdbhiwkthr%d", i);
		slmdbwkthr(thr)->smdw_wkthr.wkt_workq = &slm_db_hipri_workq;
		pscthr_setready(thr);
	}

	thr = pscthr_init(SLMTHRT_DBWORKER, pfl_wkthr_main,
	    sizeof(struct slmdbwk_thread), "slmdblowkthr");
	slmdbwkthr(thr)->smdw_wkthr.wkt_workq = &slm_db_lopri_workq;
	pscthr_setready(thr);

	slmbmaptimeothr_spawn();
	slmconnthr_spawn();
	slm_rpc_initsvc();
	slmbchrqthr_spawn();
	slmupschthr_spawn();
	sl_freapthr_spawn(SLMTHRT_FREAP, "slmfreapthr");

	time(&now);
	psclogs_info(SLMSS_INFO, "SLASH2 %s version %d started at %s",
	    __progname, sl_stk_version, ctime(&now));
	psclogs_info(SLMSS_INFO, "Max ARC caching size is %"PRIu64" bytes",
	    arc_get_maxsize());

	pfl_fault_register(RMC_HANDLE_FAULT);

	me = pscthr_get();
	psc_ctlthr_mainloop(me);
	exit(0);
}
