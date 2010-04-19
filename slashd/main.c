/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
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

#include "pfl/pfl.h"
#include "pfl/str.h"
#include "psc_util/alloc.h"
#include "psc_util/ctlsvr.h"
#include "psc_util/log.h"
#include "psc_util/odtable.h"
#include "psc_util/thread.h"
#include "psc_util/usklndthr.h"

#include "bmap_mds.h"
#include "ctl_mds.h"
#include "fdbuf.h"
#include "fidcache.h"
#include "mdscoh.h"
#include "mdsio.h"
#include "mkfn.h"
#include "pathnames.h"
#include "repl_mds.h"
#include "rpc_mds.h"
#include "slashd.h"
#include "slconfig.h"

GCRY_THREAD_OPTION_PTHREAD_IMPL;

const char		*progname;

struct psc_poolmaster	 replrq_poolmaster;

struct slash_creds	 rootcreds = { 0, 0 };

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
	free(p);
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
		psc_fatal("zpool export");

	if (zfspoolcf)
		rc = snprintf(cmdbuf, sizeof(cmdbuf),
		    "zpool import -c '%s' '%s'", zfspoolcf, zpoolname);
	else
		rc = snprintf(cmdbuf, sizeof(cmdbuf),
		    "zpool import '%s'", zpoolname);
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

void
slm_init(void)
{
	char fn[PATH_MAX];

	psc_poolmaster_init(&replrq_poolmaster, struct sl_replrq,
	    rrq_lentry, PPMF_AUTO, 256, 256, 0, NULL, NULL, NULL,
	    "replrq");
	replrq_pool = psc_poolmaster_getmgr(&replrq_poolmaster);

	psc_poolmaster_init(&bmapMdsLeasePoolMaster, struct bmap_mds_lease,
	    bml_bmdsi_lentry, PPMF_AUTO, 256, 256, 0, NULL, NULL, NULL,
	    "bmap_leases");
	bmapMdsLeasePool = psc_poolmaster_getmgr(&bmapMdsLeasePoolMaster);
       
	lc_reginit(&slm_replst_workq, struct slm_replst_workreq,
	    rsw_lentry, "replstwkq");

	lc_reginit(&pndgBmapCbs, struct bmap_mds_lease, bml_coh_lentry,
	    "pendingBmapCbs");

	lc_reginit(&inflBmapCbs, struct bmap_mds_lease, bml_coh_lentry,
	    "inflBmapCbs");

	bmap_cache_init(sizeof(struct bmap_mds_info));

	mds_journal_init();

	xmkfn(fn, "%s/%s", sl_datadir, SL_FN_IONBMAPS_ODT);
	psc_assert(!odtable_load(fn, &mdsBmapAssignTable));
	odtable_scan(mdsBmapAssignTable, mds_bmi_odtable_startup_cb);
}

__dead void
usage(void)
{
	fprintf(stderr,
	    "usage: %s [-f slashconf] [-p zpoolcache] [-S socket] zpoolname\n",
	    progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	const char *cfn, *sfn;
	char *zfspoolcf=NULL;
	int c;

	/* gcrypt must be initialized very early on */
	gcry_control(GCRYCTL_SET_THREAD_CBS, &gcry_threads_pthread);
	if (!gcry_check_version(GCRYPT_VERSION))
		errx(1, "libgcrypt version mismatch");

	pfl_init();

#ifdef ZPOOL_PATH
	append_path(ZPOOL_PATH);
#endif

	progname = argv[0];
	cfn = SL_PATH_CONF;
	sfn = SL_PATH_SLMCTLSOCK;
	while ((c = getopt(argc, argv, "D:f:p:S:")) != -1)
		switch (c) {
		case 'D':
			sl_datadir = optarg;
			break;
		case 'f':
			cfn = optarg;
			break;
		case 'p':
			zfspoolcf = optarg;
			break;
		case 'S':
			sfn = optarg;
			break;
		default:
			usage();
		}
	argc -= optind;
	argv += optind;
	if (argc != 1)
		usage();

	pscthr_init(SLMTHRT_CTL, 0, NULL, NULL,
	    sizeof(struct psc_ctlthr), "slmctlthr");

	slcfg_parse(cfn);

	/* Initialize the mdsio layer. */
	mdsio_init();
	import_zpool(argv[0], zfspoolcf);

	authbuf_createkeyfile();
	authbuf_readkeyfile();
	fidc_init(sizeof(struct fcmh_mds_info), FIDC_MDS_DEFSZ,
	    FIDC_MDS_MAXSZ, NULL, FIDC_MDS);
	libsl_init(PSCNET_SERVER, 1);

	slm_init();

	slmfssyncthr_spawn();
	slmcohthr_spawn();
	slmbmaptimeothr_spawn();
	slm_rpc_initsvc();
	slmreplqthr_spawnall();
	mds_repl_init();
	slmtimerthr_spawn();
	slmctlthr_main(sfn);
	/* NOTREACHED */
}
