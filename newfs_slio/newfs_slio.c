/* $Id$ */

#include <sys/param.h>

#include <stdio.h>
#include <stdlib.h>

#include "slconfig.h"
#include "pathnames.h"

const char *progname;

void
spawn_lnet_thr(pthread_t *t, void *(*startf)(void *), void *arg)
{
	int rc;

	if ((rc = pthread_create(t, NULL, startf, arg)) != 0)
		psc_fatalx("pthread_create: %s", strerror(rc));
}

__dead void
usage(void)
{
	fprintf(stderr, "usage: %s [-f cfgfile]\n", progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	const char *cfgfn;
	char fn[PATH_MAX];
	int rc, c;

	progname = argv[0];
	cfgfn = _PATH_SLASHCONF;

	if (getenv("LNET_NETWORKS") == NULL)
		psc_fatalx("please export LNET_NETWORKS");
	lnet_thrspawnf = spawn_lnet_thr;

	while ((c = getopt(argc, argv, "f:")) != -1)
		switch (c) {
		case 'f':
			cfgfn = optarg;
			break;
		default:
			usage();
		}
	argc -= optind;
	if (argc)
		usage();

	slashGetConfig(cfgfn);
	libsl_init(PSC_SERVER);

	/* FID namespace */
	rc = snprintf(fn, sizeof(fn), "%s/%s", nodeInfo.node_res->res_fsroot,
	    _PATH_OBJROOT);
	if (rc == -1)
		psc_fatal("snprintf");
	if (rc >= (int)sizeof(fn))
		psc_fatalx("name too long");
	if (mkdir(fn, 0755) == -1)
		psc_fatal("mkdir %s", fn);
	exit(0);
}
