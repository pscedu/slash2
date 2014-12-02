/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2014, Pittsburgh Supercomputing Center (PSC).
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
#include "pfl/fs.h"
#include "pfl/log.h"
#include "pfl/odtable.h"
#include "pfl/pfl.h"
#include "pfl/random.h"
#include "pfl/str.h"
#include "pfl/thread.h"
#include "pfl/usklndthr.h"
#include "pfl/workthr.h"

#include "authbuf.h"
#include "bmap_mds.h"
#include "ctl_mds.h"
#include "fidcache.h"
#include "mdscoh.h"
#include "mdsio.h"
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

int			 current_vfsid;

const char		*progname;

struct slash_creds	 rootcreds = { 0, 0 };
struct pscfs		 pscfs;
struct psc_thread	*slmconnthr;
uint32_t		 sl_sys_upnonce;

struct pfl_odt		*slm_ptrunc_odt;

/* this table is immutable, at least for now */
struct psc_hashtbl	 rootHtable;

struct psc_listcache	 slm_db_workq;
int			 slm_opstate;

struct psc_poolmaster	 slm_bml_poolmaster;
struct psc_poolmgr	*slm_bml_pool;

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

/*
 * Use system() calls to import pool and mount file systems.  Note that
 * the paths needed by the system() is built in at compile time and
 * added by append_path() at run time.
 */
void
import_zpool(const char *zpoolname, const char *zfspoolcf)
{
	char cmdbuf[BUFSIZ], mountpoint[BUFSIZ];
	struct dirent *d;
	int i, rc;
	DIR *dir;

	rc = snprintf(mountpoint, sizeof(mountpoint), "/%s", zpoolname);
	psc_assert(rc < (int)sizeof(mountpoint) && rc >= 0);

	/*
	 * ZFS fuse can create the mount point automatically if it does
	 * not exist.  However, if the mount point exists and is not
	 * empty, it does not mount the default file system in the pool
	 * for some reason.
	 */
	dir = opendir(mountpoint);
	if (dir != NULL) {
		i = 0;
		while ((d = readdir(dir)) != NULL) {
			if (i++ < 2)
				continue;
			errno = ENOTEMPTY;
			psc_fatal("Please clean up directory %s before mount",
			    mountpoint);
		}
		closedir(dir);
	}

	rc = snprintf(cmdbuf, sizeof(cmdbuf),
	    "zpool import -f -c '%s' '%s'", zfspoolcf ? zfspoolcf : "",
	    zpoolname);
	if (rc >= (int)sizeof(cmdbuf) || rc < 0)
		psc_fatal("Fail to construct command to import %s",
		    zpoolname);
	rc = system(cmdbuf);
	if (rc == -1)
		psc_fatal("Fail to execute command to import %s",
		    zpoolname);

	/* mount the default file system in the pool */
	rc = snprintf(cmdbuf, sizeof(cmdbuf), "zfs mount %s", zpoolname);
	if (rc >= (int)sizeof(cmdbuf) || rc < 0)
		psc_fatal("Fail to construct command to mount %s",
		    zpoolname);
	rc = system(cmdbuf);
	if (rc == -1)
		psc_fatal("Fail to execute command to mount %s",
		    zpoolname);

	/* mount the rest file systems in the pool */
	rc = system("zfs mount -a");
	if (rc == -1)
		psc_fatal("Fail to execute command to mount file systems");
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

/**
 * read_vfsid -
 *
 * Note: The root file system must have fsid of zero, so that a client
 * can see all the file systems in the pool.
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

	/*
	 * backward compatibility: assuming one default file system in
	 * the pool
	 */
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

void
psc_suspend_filesystem(void)
{
}

void
psc_resume_filesystem(void)
{
}

/**
 * psc_register_filesystem -
 *
 * XXX Allow an empty file system to register and fill in contents later.
 *     Or use slmctl to register a new file system when it is ready.
 */
void
psc_register_filesystem(int vfsid)
{
	int i, rc, found1, found2, root_vfsid;
	uint64_t siteid, uuid;
	struct mio_rootnames *rn;
	struct srt_stat sstb;
	mdsio_fid_t mfp;
	char *fsname;

	psclog_info("Checking file system %s", zfsMount[vfsid].name);
	if (zfsMount[vfsid].name[0] != '/') {
		psclog_warnx("Bogus file system name: %s",
		    zfsMount[vfsid].name);
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
	fsname = strchr(&zfsMount[vfsid].name[1], '/');
	if (!(zfsMount[vfsid].flag & ZFS_SLASH2_MKDIR) && fsname) {
		fsname++;
		/*
		 * make sure that the newly mounted file system has an
		 * entry
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
			    slstrerror(rc));
			return;
		}
	}
	zfsMount[vfsid].flag |= ZFS_SLASH2_MKDIR;

	rc = mdsio_lookup(vfsid, MDSIO_FID_ROOT, SL_RPATH_META_DIR,
	    &mds_metadir_inum[vfsid], &rootcreds, NULL);
	if (rc) {
		psclog_warnx("lookup .slmd metadir: %s", slstrerror(rc));
		return;
	}

	rc = mdsio_lookup(vfsid, mds_metadir_inum[vfsid],
	    SL_RPATH_FIDNS_DIR, &mds_fidnsdir_inum[vfsid], &rootcreds,
	    NULL);
	if (rc) {
		psclog_warnx("lookup %s/%s dir: %s", SL_RPATH_META_DIR,
		    SL_RPATH_FIDNS_DIR, slstrerror(rc));
		return;
	}

	rc = mdsio_lookup(vfsid, mds_metadir_inum[vfsid],
	    SL_RPATH_TMP_DIR, &mds_tmpdir_inum[vfsid], &rootcreds,
	    NULL);
	if (rc) {
		psclog_warnx("lookup %s/%s dir: %s", SL_RPATH_META_DIR,
		    SL_RPATH_TMP_DIR, slstrerror(rc));
		return;
	}

	rc = read_vfsid(vfsid, SL_FN_SITEID, &zfsMount[vfsid].siteid);
	if (rc)
		return;
	rc = read_vfsid(vfsid, SL_FN_FSUUID, &zfsMount[vfsid].uuid);
	if (rc)
		return;

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
		psclog_warnx("duplicate SITEID found: %"PRIx64, siteid);
		return;
	}
	if (found2) {
		psclog_warnx("duplicate UUID found: %"PRIx64, uuid);
		return;
	}
	rc = zfsslash2_build_immns_cache(vfsid);
	if (rc) {
		psclog_warnx("failed to create cache for file system %s",
		    basename(zfsMount[vfsid].name));
		return;
	}

	rn = PSCALLOC(sizeof(*rn));

	strlcpy(rn->rn_name, basename(zfsMount[vfsid].name),
	    sizeof(rn->rn_name));
	rn->rn_vfsid = vfsid;
	psc_hashent_init(&rootHtable, rn);
	psc_hashtbl_add_item(&rootHtable, rn);

	zfsMount[vfsid].flag |= ZFS_SLASH2_READY;
	psclog_info("file system %s registered (site=%"PRIx64" uuid=%"PRIx64")",
	    basename(zfsMount[vfsid].name), siteid, uuid);
}

/**
 * psc_scan_filesystems - Scan for newly added file systems in the pool.
 */
void
psc_scan_filesystems(void)
{
	static psc_spinlock_t scan_lock = SPINLOCK_INIT;
	int i;

	spinlock(&scan_lock);
	for (i = 0; i < mount_index; i++)
		if (!(zfsMount[i].flag & ZFS_SLASH2_READY))
			psc_register_filesystem(i);
	freelock(&scan_lock);
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

int
main(int argc, char *argv[])
{
	char *zpcachefn = NULL, *zpname, *estr;
	const char *cfn, *sfn, *p;
	int rc, vfsid, c, found;
	struct psc_thread *thr;

	/* gcrypt must be initialized very early on */
	gcry_control(GCRYCTL_SET_THREAD_CBS, &gcry_threads_pthread);
	if (!gcry_check_version(GCRYPT_VERSION))
		errx(1, "libgcrypt version mismatch");

	progname = argv[0];
	pfl_init();
	sl_subsys_register();
	psc_subsys_register(SLMSS_ZFS, "zfs");
	psc_subsys_register(SLMSS_JOURNAL, "log");
	psc_subsys_register(SLMSS_UPSCH, "upsch");
	psc_subsys_register(SLMSS_INFO, "info");

	append_path(ZFS_BIN_PATH);
	append_path(ZPOOL_PATH);

	sfn = SL_PATH_SLMCTLSOCK;
	p = getenv("CTL_SOCK_FILE");
	if (p)
		sfn = p;

	cfn = SL_PATH_CONF;
	p = getenv("CONFIG_FILE");
	if (p)
		cfn = p;

	while ((c = getopt(argc, argv, "D:f:p:S:VX:Y")) != -1)
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
			errx(0, "revision is %d", SL_STK_VERSION);
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

	pscthr_init(SLMTHRT_CTL, NULL, NULL,
	    sizeof(struct psc_ctlthr) +
	    sizeof(struct slmctl_thread), "slmctlthr0");

	sl_sys_upnonce = psc_random32();

	slcfg_parse(cfn);

	/* override global defaults with cfg settings */
	if (zpcachefn == NULL && globalConfig.gconf_zpcachefn[0])
		zpcachefn = globalConfig.gconf_zpcachefn;
	if (argc)
		zpname = argv[0];
	else if (globalConfig.gconf_zpname[0])
		zpname = globalConfig.gconf_zpname;
	else {
		warnx("no ZFS pool specified");
		usage();
	}

	psclog_info("%s: revision is %d", progname, SL_STK_VERSION);

	fidc_init(sizeof(struct fcmh_mds_info), FIDC_MDS_DEFSZ);
	bmap_cache_init(sizeof(struct bmap_mds_info));

	/* Start up ZFS threads and import the MDS zpool. */
	mdsio_init();
	import_zpool(zpname, zpcachefn);

	psc_hashtbl_init(&rootHtable, PHTF_STR, struct mio_rootnames,
	    rn_name, rn_hentry, 97, NULL, "rootnames");

	/* using hook can cause layer violation */
	zfsslash2_register_hook(psc_register_filesystem);

	zfsslash2_register_resume_hook(psc_resume_filesystem);
	zfsslash2_register_suspend_hook(psc_suspend_filesystem);

	authbuf_createkeyfile();
	authbuf_readkeyfile();

	sl_drop_privs(allow_root_uid);

	libsl_init(2 * (SLM_RMM_NBUFS + SLM_RMI_NBUFS + SLM_RMC_NBUFS));

	/* startup meter */
	psc_meter_destroy(&res2mdsinfo(nodeResProf)->sp_batchmeter);

	if (nodeResm->resm_res->res_arc_max) {
		void arc_set_maxsize(uint64_t);

		arc_set_maxsize(nodeResm->resm_res->res_arc_max);
	}

	for (vfsid = 0; vfsid < mount_index; vfsid++)
		psc_register_filesystem(vfsid);

	found = 0;
	for (vfsid = 0; vfsid < mount_index; vfsid++) {
		/* nodeSite->site_id is nodeResm->resm_res->res_site */
		if (nodeSite->site_id == zfsMount[vfsid].siteid) {
			psc_assert(!found);
			found = 1;
			current_vfsid = vfsid;
			psclog_info("file system %s (id=%d) "
			    "matches site ID %d",
			    zfsMount[vfsid].name, vfsid,
			    nodeSite->site_id);
		}
	}
	if (!found) {
		fprintf(stderr,
		    "------------------------------------------------\n"
		    "file systems available:\n");
		for (vfsid = 0; vfsid < mount_index; vfsid++)
			fprintf(stderr,
			    "  file system %3d: %s\tid=%"PRId64"\n",
			    vfsid, zfsMount[vfsid].name,
			    zfsMount[vfsid].siteid);
		errx(1, "site id=%d doesn't match any file system",
		    nodeSite->site_id);
	}

	if (zfsMount[current_vfsid].uuid != globalConfig.gconf_fsuuid)
		psc_fatalx("FSUUID do not match; "
		    "ZFS=%"PRIx64" slcfg=%"PRIx64,
		    zfsMount[current_vfsid].uuid,
		    globalConfig.gconf_fsuuid);

	lc_reginit(&slm_replst_workq, struct slm_replst_workreq,
	    rsw_lentry, "replstwkq");
	pfl_workq_init(128);
	slm_upsch_init();

	psc_poolmaster_init(&slm_bml_poolmaster,
	    struct bmap_mds_lease, bml_bmi_lentry, PPMF_AUTO, 256,
	    256, 0, NULL, NULL, NULL, "bmplease");
	slm_bml_pool = psc_poolmaster_getmgr(&slm_bml_poolmaster);

	sl_nbrqset = pscrpc_nbreqset_init(NULL);
	pscrpc_nbreapthr_spawn(sl_nbrqset, SLMTHRT_NBRQ, 1, "slmnbrqthr");

	slm_opstate = SLM_OPSTATE_REPLAY;

	pfl_odt_load(&slm_bia_odt, &slm_odtops, 0, SL_FN_BMAP_ODTAB,
	    "bmapassign");
	pfl_odt_load(&slm_ptrunc_odt, &slm_odtops, 0,
	    SL_FN_PTRUNC_ODTAB, "ptrunc");

	mds_bmap_timeotbl_init();

	sqlite3_enable_shared_cache(1);
	//dbdo(NULL, NULL, "PRAGMA page_size=");
	dbdo(NULL, NULL, "PRAGMA synchronous=OFF");
	dbdo(NULL, NULL, "PRAGMA journal_mode=WAL");

	/* no-op to test integrity */
	rc = sqlite3_exec(slmctlthr_getpri(pscthr_get())->smct_dbh.dbh,
	    " UPDATE	upsch"
	    "	SET	id=0"
	    " WHERE	id=0", NULL,
	    NULL, &estr);
	if (rc == SQLITE_ERROR) {
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
	}

	slrpc_initcli();

	dbdo(NULL, NULL, "BEGIN TRANSACTION");
	mds_journal_init(disable_propagation,
	    zfsMount[current_vfsid].uuid);
	dbdo(NULL, NULL, "COMMIT");

	pfl_workq_lock();
	pfl_wkthr_spawn(SLMTHRT_WORKER, SLM_NWORKER_THREADS,
	    "slmwkthr%d");
	pfl_workq_waitempty();

	lc_reginit(&slm_db_workq, struct pfl_workrq, wkrq_lentry,
	    "dbworkq");
	LIST_CACHE_LOCK(&slm_db_workq);
	thr = pscthr_init(SLMTHRT_DBWORKER, pfl_wkthr_main, NULL,
	    sizeof(struct slmdbwk_thread), "slmdbwkthr");
	slmdbwkthr(thr)->smdw_wkthr.wkt_workq = &slm_db_workq;
	pscthr_setready(thr);
	psc_waitq_wait(&slm_db_workq.plc_wq_want,
	    &slm_db_workq.plc_lock);

	dbdo(slm_upsch_revert_cb, NULL,
	    " SELECT	fid,"
	    "		bno"
	    " FROM	upsch"
	    " WHERE	status = 'S'");

	dbdo(NULL, NULL,
	    " UPDATE	upsch"
	    " SET	status = 'Q'"
	    " WHERE	status = 'S'");

	pscthr_init(SLMTHRT_BKDB, slmbkdbthr_main, NULL, 0,
	    "slmbkdbthr");

	pfl_odt_check(slm_bia_odt, mds_bia_odtable_startup_cb, NULL);
	pfl_odt_check(slm_ptrunc_odt, slm_ptrunc_odt_startup_cb, NULL);

	slm_opstate = SLM_OPSTATE_NORMAL;

	slmbmaptimeothr_spawn();
	slmtimerthr_spawn();
	slmconnthr_spawn();
	slm_rpc_initsvc();
	slmbchrqthr_spawn();
	slmupschthr_spawn();
	sl_freapthr_spawn(SLMTHRT_FREAP, "slmfreapthr");

	slmctlthr_main(sfn);
	exit(0);
}
