/* $Id$ */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "pfl.h"
#include "psc_util/alloc.h"
#include "psc_util/ctlsvr.h"
#include "psc_util/strlcpy.h"
#include "psc_util/thread.h"
#include "psc_util/usklndthr.h"

#include "sliod.h"
#include "slconfig.h"
#include "rpc.h"
#include "pathnames.h"

const char *progname;

__dead void
usage(void)
{
	fprintf(stderr, "usage: %s [-f cfgfile] [-S socket]\n", progname);
	exit(1);
}

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

int
main(int argc, char *argv[])
{
	struct slio_ctlthr *sct;
	const char *cfn, *sfn;
	int c;

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

	pfl_init();

	if (getenv("LNET_NETWORKS") == NULL)
		psc_fatalx("please export LNET_NETWORKS");
	if (getenv("TCPLND_SERVER") == NULL)
		psc_fatalx("please export TCPLND_SERVER");

	sct = PSCALLOC(sizeof(*sct));
	pscthr_init(&pscControlThread, SLIOTHRT_CTL,
	    NULL, sct, sizeof(*sct), "slioctlthr");

	slashGetConfig(cfn);
	libsl_init(PSC_SERVER);
	rpc_initsvc();
	sliotimerthr_spawn();
	slioctlthr_main(sfn);
	exit(0);
}
