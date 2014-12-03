/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2014, Pittsburgh Supercomputing Center (PSC).
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
#include <sys/statvfs.h>
#include <sys/mount.h>
#include <sys/wait.h>
#include <sys/time.h>

#include <err.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <gcrypt.h>

#include "pfl/alloc.h"
#include "pfl/ctlsvr.h"
#include "pfl/fault.h"
#include "pfl/iostats.h"
#include "pfl/listcache.h"
#include "pfl/pfl.h"
#include "pfl/random.h"
#include "pfl/str.h"
#include "pfl/thread.h"
#include "pfl/timerthr.h"
#include "pfl/usklndthr.h"

#include "authbuf.h"
#include "bmap_iod.h"
#include "fidc_iod.h"
#include "fidcache.h"
#include "mkfn.h"
#include "pathnames.h"
#include "repl_iod.h"
#include "rpc_iod.h"
#include "slab.h"
#include "slconfig.h"
#include "slerr.h"
#include "sliod.h"
#include "slsubsys.h"
#include "slutil.h"
#include "slvr.h"

GCRY_THREAD_OPTION_PTHREAD_IMPL;

int			 sli_selftest_rc;
struct srt_statfs	 sli_ssfb;
psc_spinlock_t		 sli_ssfb_lock = SPINLOCK_INIT;
struct psc_thread	*sliconnthr;

uint32_t		 sl_sys_upnonce;
const char		*progname;

struct sli_rdwrstats	 sli_rdwrstats[8];

int
psc_usklndthr_get_type(const char *namefmt)
{
	if (strstr(namefmt, "lnacthr"))
		return (SLITHRT_LNETAC);
	return (SLITHRT_USKLNDPL);
}

void
psc_usklndthr_get_namev(char buf[PSC_THRNAME_MAX], const char *namefmt,
    va_list ap)
{
	size_t n;

	n = strlcpy(buf, "sli", PSC_THRNAME_MAX);
	if (n < PSC_THRNAME_MAX)
		vsnprintf(buf + n, PSC_THRNAME_MAX - n, namefmt, ap);
}

void
slistatfsthr_main(struct psc_thread *thr)
{
	struct statvfs sfb;
	int rc;

	while (pscthr_run(thr)) {
#ifdef HAVE_STATFS_FSTYPE
		struct statfs b;

		rc = statfs(slcfg_local->cfg_fsroot, &b);
		if (rc == -1)
			psclog_error("statfs %s",
			    slcfg_local->cfg_fsroot);
		statfs_2_statvfs(&b, &sfb);
#else
		rc = statvfs(slcfg_local->cfg_fsroot, &sfb);
		if (rc == -1)
			psclog_error("statvfs %s",
			    slcfg_local->cfg_fsroot);
#endif

		if (rc == 0) {
			spinlock(&sli_ssfb_lock);
			sl_externalize_statfs(&sfb, &sli_ssfb);
#ifdef HAVE_STATFS_FSTYPE
			strlcpy(sli_ssfb.sf_type, b.f_fstypename,
			    sizeof(sli_ssfb.sf_type));
#endif
			freelock(&sli_ssfb_lock);
		}
		sleep(30);
	}
}

/**
 * slihealththr_main - Occassionally run the self health test.
 * @thr: our thread.
 */
void
slihealththr_main(struct psc_thread *thr)
{
	struct psc_waitq dummy = PSC_WAITQ_INIT;
	struct slashrpc_cservice *csvc;
	struct timespec ts;
	int rc;

	signal(SIGALRM, SIG_IGN);
	PFL_GETTIMESPEC(&ts);
	while (pscthr_run(thr)) {
		ts.tv_sec += 60;
		psc_waitq_waitabs(&dummy, NULL, &ts);
		errno = 0;
		rc = system(slcfg_local->cfg_selftest);

		/*
		 * Code		Description
		 * ---------------------------------------
		 * 0		OK
		 * 1		serious error: do not use
		 * 2		degraded: avoid
		 */
		if (rc == -1)
			rc = -errno;
		else if (WIFEXITED(rc))
			rc = WEXITSTATUS(rc);
		if (sli_selftest_rc != rc) {
			psclog_notice("health changed from %d to %d "
			    "(error=%d)", sli_selftest_rc, rc, errno);

			sli_selftest_rc = rc;

			PLL_LOCK(&sl_clients);
			PLL_FOREACH(csvc, &sl_clients) {
				CSVC_LOCK(csvc);
				sl_csvc_incref(csvc);
				CSVC_ULOCK(csvc);
				sli_rci_ctl_health_send(csvc);
			}
			PLL_ULOCK(&sl_clients);
		}
	}
}

int
slirmiconnthr_upcall(__unusedx void *arg)
{
	return (sli_selftest_rc);
}

__dead void
usage(void)
{
	fprintf(stderr,
	    "usage: %s [-V] [-D datadir] [-f cfgfile] [-S socket] [mds-resource]\n",
	    progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	const char *cfn, *sfn, *p, *prefmds;
	sigset_t signal_set;
	int sz, nsz, i, rc, c;
	struct stat stb;

	/* gcrypt must be initialized very early on */
	gcry_control(GCRYCTL_SET_THREAD_CBS, &gcry_threads_pthread);
	if (!gcry_check_version(GCRYPT_VERSION))
		errx(1, "libgcrypt version mismatch");

	progname = argv[0];
	pfl_init();
	sl_subsys_register();
	psc_subsys_register(SLISS_SLVR, "slvr");

	psc_fault_register(SLI_FAULT_AIO_FAIL);
	psc_fault_register(SLI_FAULT_CRCUP_FAIL);
	psc_fault_register(SLI_FAULT_FSIO_READ_FAIL);

	sfn = SL_PATH_SLICTLSOCK;
	p = getenv("CTL_SOCK_FILE");
	if (p)
		sfn = p;

	cfn = SL_PATH_CONF;
	p = getenv("CONFIG_FILE");
	if (p)
		cfn = p;

	while ((c = getopt(argc, argv, "D:f:S:V")) != -1)
		switch (c) {
		case 'D':
			sl_datadir = optarg;
			break;
		case 'f':
			cfn = optarg;
			break;
		case 'S':
			sfn = optarg;
			break;
		case 'V':
			errx(0, "revision is %d", SL_STK_VERSION);
		default:
			usage();
		}
	argc -= optind;
	argv += optind;
	if (argc > 1)
		usage();

	sigemptyset(&signal_set);
	sigaddset(&signal_set, SIGIO);
	sigprocmask(SIG_BLOCK, &signal_set, NULL);

	pscthr_init(SLITHRT_CTL, NULL, NULL, sizeof(struct psc_ctlthr),
	    "slictlthr0");

	sl_sys_upnonce = psc_random32();

	slcfg_local->cfg_fidcachesz = 4096;
	slcfg_parse(cfn);
	authbuf_checkkeyfile();
	authbuf_readkeyfile();

	libsl_init((SLI_RIM_NBUFS + SLI_RIC_NBUFS + SLI_RII_NBUFS) * 2);

	if (stat(slcfg_local->cfg_fsroot, &stb) == -1)
		psc_fatal("%s", slcfg_local->cfg_fsroot);

	bmap_cache_init(sizeof(struct bmap_iod_info));
	fidc_init(sizeof(struct fcmh_iod_info));
	bim_init();
	sl_nbrqset = pscrpc_nbreqset_init(NULL);
	slvr_cache_init();

	sli_rdwrstats[0].size =        1024;
	sli_rdwrstats[1].size =    4 * 1024;
	sli_rdwrstats[2].size =   16 * 1024;
	sli_rdwrstats[3].size =   64 * 1024;
	sli_rdwrstats[4].size =  128 * 1024;
	sli_rdwrstats[5].size =  512 * 1024;
	sli_rdwrstats[6].size = 1024 * 1024;
	sli_rdwrstats[7].size = 0;

	for (sz = i = 0; sli_rdwrstats[i].size; i++, sz = nsz) {
		nsz = sli_rdwrstats[i].size / 1024;
		psc_iostats_init(&sli_rdwrstats[i].rd, "rd:%dk-%dk", sz, nsz);
		psc_iostats_init(&sli_rdwrstats[i].wr, "wr:%dk-%dk", sz, nsz);
	}

	psc_poolmaster_init(&bmap_rls_poolmaster, struct bmap_iod_rls,
	    bir_lentry, PPMF_AUTO, 64, 64, 0, NULL, NULL, NULL,
	    "bmaprls");
	bmap_rls_pool = psc_poolmaster_getmgr(&bmap_rls_poolmaster);

	sli_repl_init();
	pscthr_init(SLITHRT_STATFS, slistatfsthr_main, NULL, 0,
	    "slistatfsthr");

	if (slcfg_local->cfg_selftest)
		pscthr_init(SLITHRT_HEALTH, slihealththr_main, NULL, 0,
		    "slihealththr");

	slrpc_initcli();

	sl_drop_privs(1);

	sliconnthr = slconnthr_spawn(SLITHRT_CONN, "sli",
	    slcfg_local->cfg_selftest ?
	    slirmiconnthr_upcall : NULL, NULL);

	prefmds = slcfg_local->cfg_prefmds;
	if (argc)
		prefmds = argv[0];
	rc = sli_rmi_setmds(prefmds);
	if (rc)
		psc_fatalx("invalid MDS %s: %s", prefmds,
		    slstrerror(rc));

	psc_assert(globalConfig.gconf_fsuuid);
	psclog_info("gconf_fsuuid=%"PRIx64, globalConfig.gconf_fsuuid);

	pscrpc_nbreapthr_spawn(sl_nbrqset, SLITHRT_NBRQ, 1, "slinbrqthr");

	slibmaprlsthr_spawn();
	sli_rpc_initsvc();
	psc_tiosthr_spawn(SLITHRT_TIOS, "slitiosthr");
	sl_freapthr_spawn(SLITHRT_FREAP, "slifreapthr");

	OPSTAT_INCR(SLI_OPST_MIN_SEQNO);

	slictlthr_main(sfn);
	exit(0);
}
