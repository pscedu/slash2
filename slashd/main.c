/* $Id$ */

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

#include "control.h"
#include "fdbuf.h"
#include "fidcache.h"
#include "mdsrpc.h"
#include "pathnames.h"
#include "slashdthr.h"
#include "slconfig.h"

#include "zfs-fuse/zfs_slashlib.h"

GCRY_THREAD_OPTION_PTHREAD_IMPL;

const char *progname;

int
psc_usklndthr_get_type(const char *namefmt)
{
	if (strstr(namefmt, "lnetacthr"))
		return (SLTHRT_LNETAC);
	return (SLTHRT_USKLNDPL);
}

void
psc_usklndthr_get_namev(char buf[PSC_THRNAME_MAX], const char *namefmt,
    va_list ap)
{
	size_t n;

	n = strlcpy(buf, "sl", PSC_THRNAME_MAX);
	if (n < PSC_THRNAME_MAX)
		vsnprintf(buf + n, PSC_THRNAME_MAX - n, namefmt, ap);
}

__dead void
usage(void)
{
	fprintf(stderr, "usage: %s [-f cfgfile] [-P zpoolname] [-S socket]\n", progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	const char *cfn, *sfn, *zpoolname;
	int c;

	gcry_control(GCRYCTL_SET_THREAD_CBS, &gcry_threads_pthread);
	gcry_check_version(NULL);

	pfl_init();

	if (getenv("LNET_NETWORKS") == NULL)
		psc_fatalx("LNET_NETWORKS is not set");

	zpoolname = NULL;
	progname = argv[0];
	cfn = _PATH_SLASHCONF;
	sfn = _PATH_SLCTLSOCK;
	while ((c = getopt(argc, argv, "f:P:S:")) != -1)
		switch (c) {
		case 'f':
			cfn = optarg;
			break;
		case 'P':
			zpoolname = optarg;
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

	pscthr_init(SLTHRT_CTL, 0, NULL, NULL,
	    sizeof(struct psc_ctlthr), "slctlthr");

	fidcache_init(FIDC_USER_MDS, NULL);

	slashGetConfig(cfn);
	fdbuf_createkeyfile();
	fdbuf_readkeyfile();
	libsl_init(PSC_SERVER);
	mds_init();

	/* Initialize the zfs layer.
	 */
	do_init();

//	if (zpoolname)

	rpc_initsvc();
	sltimerthr_spawn();
	slctlthr_main(sfn);

	do_exit();
	exit(0);
}
