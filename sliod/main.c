/* $Id$ */

#include <err.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <gcrypt.h>

#include "pfl/pfl.h"
#include "psc_util/alloc.h"
#include "psc_util/ctlsvr.h"
#include "psc_util/strlcpy.h"
#include "psc_util/thread.h"
#include "psc_util/usklndthr.h"

#include "bmap_iod.h"
#include "buffer.h"
#include "fdbuf.h"
#include "fidcache.h"
#include "pathnames.h"
#include "repl_iod.h"
#include "rpc_iod.h"
#include "slconfig.h"
#include "sliod.h"
#include "slvr.h"

GCRY_THREAD_OPTION_PTHREAD_IMPL;

const char *progname;

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

__dead void
usage(void)
{
	fprintf(stderr, "usage: %s [-f cfgfile] [-S socket]\n", progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	const char *cfn, *sfn, *mds;
	int c;

	gcry_control(GCRYCTL_SET_THREAD_CBS, &gcry_threads_pthread);
	if (!gcry_check_version(GCRYPT_VERSION))
		errx(1, "libgcrypt version mismatch");

	if (setenv("USOCK_PORTPID", "0", 1) == -1)
		err(1, "setenv");

	if (getenv("LNET_NETWORKS") == NULL)
		errx(1, "LNET_NETWORKS is not set");

	pfl_init();

	progname = argv[0];
	cfn = _PATH_SLASHCONF;
	sfn = _PATH_SLICTLSOCK;
	while ((c = getopt(argc, argv, "f:S:")) != -1)
		switch (c) {
		case 'f':
			cfn = optarg;
			break;
		case 'S':
			sfn = optarg;
			break;
		default:
			usage();
		}
	argc -= optind;
	if (argc)
		usage();

	pscthr_init(SLITHRT_CTL, 0, NULL, NULL,
	    sizeof(struct psc_ctlthr), "slictlthr");

	libsl_init(PSCNET_SERVER, 0);
	slcfg_parse(cfn);
	fdbuf_checkkeyfile();
	fdbuf_readkeyfile();

	_psc_poolmaster_init(&bmap_poolmaster, sizeof(struct bmapc_memb) +
	    sizeof(struct bmap_iod_info), offsetof(struct bmapc_memb, bcm_lentry),
	    PPMF_AUTO, 64, 64, 0, NULL, NULL, NULL, NULL, "bmap");
	bmap_pool = psc_poolmaster_getmgr(&bmap_poolmaster);

	fidcache_init(FIDC_USER_ION, NULL);
	//sl_buffer_cache_init();
	slvr_cache_init();
	sli_repl_init();
	sli_rpc_initsvc();
	slitimerthr_spawn();

	if ((mds = getenv("SLASH_MDS_NID")) == NULL)
		psc_fatalx("please export SLASH_MDS_NID");

	sli_rmi_setmds(mds);
	if (sli_rmi_getimp() == NULL)
		psc_fatalx("MDS server unavailable");

	slictlthr_main(sfn);
}
