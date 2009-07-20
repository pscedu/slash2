/* $Id$ */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <gcrypt.h>

#include "pfl.h"
#include "psc_util/alloc.h"
#include "psc_util/ctlsvr.h"
#include "psc_util/strlcpy.h"
#include "psc_util/thread.h"
#include "psc_util/usklndthr.h"

#include "fdbuf.h"
#include "pathnames.h"
#include "rpc.h"
#include "slconfig.h"
#include "buffer.h"
#include "sliod.h"

GCRY_THREAD_OPTION_PTHREAD_IMPL;

const char *progname;

int
psc_usklndthr_get_type(const char *namefmt)
{
	if (namefmt[0] == 'a')
		return (SLIOTHRT_LNETAC);
	return (SLIOTHRT_USKLNDPL);
}

void
psc_usklndthr_get_namev(char buf[PSC_THRNAME_MAX], const char *namefmt,
    va_list ap)
{
	size_t n;

	n = strlcpy(buf, "slio", PSC_THRNAME_MAX);
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
	const char *cfn, *sfn;
	int c;

	gcry_control(GCRYCTL_SET_THREAD_CBS, &gcry_threads_pthread);
	gcry_check_version(NULL);

	pfl_init();

	if (getenv("LNET_NETWORKS") == NULL)
		psc_fatalx("LNET_NETWORKS is not set");

	progname = argv[0];
	cfn = _PATH_SLASHCONF;
	sfn = _PATH_SLIOCTLSOCK;
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

	pscthr_init(SLIOTHRT_CTL, 0, NULL, NULL,
	    sizeof(struct psc_ctlthr), "slioctlthr");

	slashGetConfig(cfn);
	fdbuf_checkkeyfile();
	fdbuf_readkeyfile();
	libsl_init(PSC_SERVER);
	fidcache_init(FIDC_USER_ION, NULL);
	sl_buffer_cache_init();
	rpc_initsvc();
	sliotimerthr_spawn();
	slioctlthr_main(sfn);
}
