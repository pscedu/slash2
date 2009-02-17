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

#include "slconfig.h"
#include "pathnames.h"
#include "control.h"
#include "sb.h"
#include "slashdthr.h"
#include "fidc_common.h"
#include "mdsrpc.h"

void *zfsVfs;

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

int
main(int argc, char *argv[])
{
	const char *cfn, *sfn, *p;
	int c;

	progname = argv[0];
	cfn = _PATH_SLASHCONF;
	sfn = _PATH_SLCTLSOCK;
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

	pfl_init();

	pscthr_init(&pscControlThread, SLTHRT_CTL, NULL,
	    PSCALLOC(sizeof(struct psc_ctlthr)),
	    sizeof(struct psc_ctlthr), "slctlthr");

	if (getenv("LNET_NETWORKS") == NULL)
		psc_fatalx("please export LNET_NETWORKS");
	if ((p = getenv("TCPLND_SERVER")) == NULL || strcmp(p, "1"))
		if (setenv("TCPLND_SERVER", "1", 1) == -1)
			psc_fatal("setenv");

	fidcache_init(FIDC_MDS_HASH_SZ, NULL, NULL);

	//slFsops = PSCALLOC(sizeof(*slFsops));
        //slFsops->slfsop_getattr = slash2fuse_stat;

	slashGetConfig(cfn);
	libsl_init(PSC_SERVER);
	mds_init();
	//slash_superblock_init();
	//sl_journal_init();
	/* Initialize the zfs layer.
	 */
	do_init();


	rpc_initsvc();
	slctlthr_main(sfn);

	do_exit();
	exit(0);
}
