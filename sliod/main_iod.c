/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright 2006-2016, Pittsburgh Supercomputing Center
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
#include "pfl/fmtstr.h"
#include "pfl/listcache.h"
#include "pfl/opstats.h"
#include "pfl/pfl.h"
#include "pfl/random.h"
#include "pfl/rlimit.h"
#include "pfl/str.h"
#include "pfl/sys.h"
#include "pfl/thread.h"
#include "pfl/timerthr.h"
#include "pfl/usklndthr.h"
#include "pfl/workthr.h"

#include "authbuf.h"
#include "batchrpc.h"
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

extern const char *__progname;

int			 sli_selftest_result;
int			 sli_selftest_enable = 1;

/*
 * Do not write to this I/O server unless absolutely necessary.
 */
int			 sli_disable_write = 0;

struct srt_bwqueued	 sli_bwqueued;
psc_spinlock_t		 sli_bwqueued_lock = SPINLOCK_INIT;
struct srt_statfs	 sli_ssfb;
psc_spinlock_t		 sli_ssfb_lock = SPINLOCK_INIT;
struct timespec		 sli_ssfb_send;
struct statvfs		 sli_statvfs_buf;

struct pfl_opstats_grad	 sli_iorpc_iostats_rd;
struct pfl_opstats_grad	 sli_iorpc_iostats_wr;
struct pfl_iostats_rw	 sli_backingstore_iostats;
struct psc_thread	*sliconnthr;

uint32_t		 sl_sys_upnonce;

int64_t sli_io_grad_sizes[] = {
		0,
	     1024,
	 4 * 1024,
	16 * 1024,
	64 * 1024,
       128 * 1024,
       512 * 1024,
      1024 * 1024,
};

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
	char type[LINE_MAX];
	int rc;

	pfl_getfstype(slcfg_local->cfg_fsroot, type, sizeof(type));

	while (pscthr_run(thr)) {
		rc = statvfs(slcfg_local->cfg_fsroot, &sli_statvfs_buf);
		if (rc == -1)
			psclog_error("statvfs %s", slcfg_local->cfg_fsroot);

		if (rc == 0) {
			spinlock(&sli_ssfb_lock);
			sl_externalize_statfs(&sli_statvfs_buf,
			    &sli_ssfb);
			strlcpy(sli_ssfb.sf_type, type,
			    sizeof(sli_ssfb.sf_type));
			freelock(&sli_ssfb_lock);
		}
		thr->pscthr_waitq = "sleep 60";
		sleep(60);
		thr->pscthr_waitq = NULL;
	}
}

/*
 * Occasionally run the self health test.
 *
 * @thr: our thread.
 */
void
slihealththr_main(struct psc_thread *thr)
{
	struct psc_waitq dummy = PSC_WAITQ_INIT("health");
	struct slrpc_cservice *csvc;
	struct timespec ts;
	char cmdbuf[BUFSIZ];
	struct psc_dynarray a = DYNARRAY_INIT;
	int i, rc;

	/* See ../../slash2/utils/fshealthtest for an example */
	if (slcfg_local->cfg_selftest) {
		FMTSTR(cmdbuf, sizeof(cmdbuf), slcfg_local->cfg_selftest,
		    FMTSTRCASE('r', "s", slcfg_local->cfg_fsroot)
		);
		psclog_warnx("self-health check command is %s", cmdbuf);
	}

	signal(SIGALRM, SIG_IGN);
	while (pscthr_run(thr)) {
		PFL_GETTIMESPEC(&ts);
		ts.tv_sec += 60;
		psc_waitq_waitabs(&dummy, NULL, &ts);
		errno = 0;

		rc = 0;
		/*
 		 * sli_selftest_enable can be used to disable the selftest 
 		 * script when it is buggy or does not apply to the local
 		 * environment.
 		 */
		if (slcfg_local->cfg_selftest && sli_selftest_enable) {
			rc = system(cmdbuf);

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
		}
		if (!rc)
			rc = !sli_has_enough_space(NULL, 0, 0, 0);
		if (!rc)
			rc = sli_disable_write;
		if (sli_selftest_result != rc) {

			/* The result is sent to MDS by slconnthr_main() */
			psclog_warnx("health will be changed from %d to %d "
			    "(errno=%d)", sli_selftest_result, rc, errno);
			sli_selftest_result = rc;

			PLL_LOCK(&sl_clients);
			PLL_FOREACH(csvc, &sl_clients) {
				CSVC_LOCK(csvc);
				if (csvc->csvc_flags & CSVCF_TOFREE) {
					OPSTAT_INCR("health-tofree");
					CSVC_ULOCK(csvc);
					continue;
				}
				sl_csvc_incref(csvc);
				CSVC_ULOCK(csvc);
				psc_dynarray_add(&a, csvc);
			}
			PLL_ULOCK(&sl_clients);
			DYNARRAY_FOREACH(csvc, i, &a) {
				OPSTAT_INCR("health-report");
				sli_rci_ctl_health_send(csvc);
			}
			psc_dynarray_reset(&a);
		}
	}
	psc_dynarray_free(&a);
}

int
slirmiconnthr_upcall(__unusedx void *arg)
{
	return (sli_selftest_result);
}

__dead void
usage(void)
{
	fprintf(stderr,
	    "usage: %s [-V] [-D datadir] [-f cfgfile] [-S socket] [mds-resource]\n",
	    __progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	const char *cfn, *sfn, *p, *prefmds;
	sigset_t signal_set;
	time_t now;
	int c;
	struct psc_thread *me;

	/* gcrypt must be initialized very early on */
	gcry_control(GCRYCTL_SET_THREAD_CBS, &gcry_threads_pthread);
	if (!gcry_check_version(GCRYPT_VERSION))
		errx(1, "libgcrypt version mismatch");

	pfl_init();
	sl_subsys_register();
	pfl_subsys_register(SLISS_SLVR, "slvr");
	pfl_subsys_register(SLISS_INFO, "info");

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
			errx(0, "version is %d", sl_stk_version);
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

	pscthr_init(SLITHRT_CTL, NULL, sizeof(struct psc_ctlthr),
	    "slictlthr0");

	sl_sys_upnonce = psc_random32();

	slcfg_local->cfg_fidcachesz = IOS_FIDCACHE_SIZE;
	slcfg_local->cfg_slab_cache_size = SLAB_DEF_CACHE;
	slcfg_parse(cfn);
	authbuf_checkkeyfile();
	authbuf_readkeyfile();

	libsl_init((SLI_RIM_NBUFS + SLI_RIC_NBUFS + SLI_RII_NBUFS) * 2);

	/*
	 * Make sure our root is workable and initialize our statvfs
	 * buffer.
	 */
	if (statvfs(slcfg_local->cfg_fsroot, &sli_statvfs_buf) == -1)
		psc_fatal("root directory %s", slcfg_local->cfg_fsroot);

	bmap_cache_init(sizeof(struct bmap_iod_info), SLI_BMAP_COUNT, NULL);
	fidc_init(sizeof(struct fcmh_iod_info));
	bim_init();
	sl_nbrqset = pscrpc_prep_set();
	slvr_cache_init();

	pfl_opstats_grad_init(&sli_iorpc_iostats_rd, 0,
	    sli_io_grad_sizes, nitems(sli_io_grad_sizes),
	    "iorpc-rd:%s");
	pfl_opstats_grad_init(&sli_iorpc_iostats_wr, 0,
	    sli_io_grad_sizes, nitems(sli_io_grad_sizes),
	    "iorpc-wr:%s");

	sli_backingstore_iostats.rd = pfl_opstat_init("backingstore-rd");
	sli_backingstore_iostats.wr = pfl_opstat_init("backingstore-wr");

	psc_poolmaster_init(&bmap_rls_poolmaster, struct bmap_iod_rls,
	    bir_lentry, PPMF_AUTO, 1024, 1024, 0, NULL, "bmaprls");
	bmap_rls_pool = psc_poolmaster_getmgr(&bmap_rls_poolmaster);

	sli_repl_init();
	pscthr_init(SLITHRT_STATFS, slistatfsthr_main, 0,
	    "slistatfsthr");

	pscthr_init(SLITHRT_HEALTH, slihealththr_main, 0,
	    "slihealththr");

	pscthr_init(SLITHRT_SEQNO, sliseqnothr_main, 0,
	    "sliseqnothr");

	pfl_workq_init(128, 1024, 1024);
	pfl_wkthr_spawn(SLITHRT_WORKER, SLI_NWORKER_THREADS, 0, "sliwkthr%d");

	slrpc_initcli();

	sl_drop_privs(1);

	sliconnthr = slconnthr_spawn(SLITHRT_CONN, "sli",
	    slirmiconnthr_upcall, NULL);

	prefmds = slcfg_local->cfg_prefmds;
	if (argc)
		prefmds = argv[0];

	sli_rmi_setmds(prefmds);

	pscthr_init(SLITHRT_UPDATE, sliupdthr_main, 0, "sliupdthr");

	psc_assert(globalConfig.gconf_fsuuid);
	psclog_info("gconf_fsuuid=%"PRIx64, globalConfig.gconf_fsuuid);

	pscrpc_nbreapthr_spawn(sl_nbrqset, SLITHRT_NBRQ, 8, "slinbrqthr");

	slibmaprlsthr_spawn();
	sli_rpc_initsvc();
	pfl_opstimerthr_spawn(SLITHRT_OPSTIMER, "sliopstimerthr");
	sl_freapthr_spawn(SLITHRT_FREAP, "slifreapthr");

	slrpc_batches_init(SLITHRT_BATCHRPC, SL_SLIOD, "sli");

	time(&now);
	psclogs_info(SLISS_INFO, "SLASH2 %s version %d started at %s",
	    __progname, sl_stk_version, ctime(&now));

#ifdef Linux

	/* fs.nr_open */
	if (psc_setrlimit(RLIMIT_NOFILE, 1048576, 1048576))
		psclog_warnx("Fail to raise open file limit to %d.",
		    1048576);
#else
	/* kern.maxfilesperproc */
	if (psc_setrlimit(RLIMIT_NOFILE, 131072, 131072))
		psclog_warnx("Fail to raise open file limit to %d.",
		    131072);
#endif

	pfl_fault_register(RIC_HANDLE_FAULT);

	slictlthr_spawn(sfn);
	me = pscthr_get();
	psc_ctlthr_mainloop(me);
	exit(0);
}
