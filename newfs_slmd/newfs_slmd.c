/* $Id$ */

#include <stdio.h>
#include <stdlib.h>

#include "slconfig.h"
#include "pathnames.h"

const char *progname;

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
	sl_resm_t *r;
	int rc, c;

	progname = argv[0];
	cfgfn = _PATH_SLASHCONF;
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

	r = libsl_resm_lookup();
	if (!r)
		psc_fatalx("resource not found for this node");

	rc = snprintf(fn, sizeof(fn), "%s", r->resm_res->res_fsroot);
	if (rc == -1)
		psc_fatal("snprintf");
	if (rc >= (int)sizeof(fn))
		psc_fatalx("name too long");
	if (mkdir(fn, 0755) == -1)
		psc_fatal("mkdir %s", fn);

	rc = snprintf(fn, sizeof(fn), "%s/%s", r->resm_res->res_fsroot,
	    _PATH_OBJROOT);
	if (rc == -1)
		psc_fatal("snprintf");
	if (rc >= (int)sizeof(fn))
		psc_fatalx("name too long");
	if (mkdir(fn, 0755) == -1)
		psc_fatal("mkdir %s", fn);

	rc = snprintf(fn, sizeof(fn), "%s/%s", r->resm_res->res_fsroot,
	    _PATH_NS);
	if (rc == -1)
		psc_fatal("snprintf");
	if (rc >= (int)sizeof(fn))
		psc_fatalx("name too long");
	if (mkdir(fn, 0755) == -1)
		psc_fatal("mkdir %s", fn);

	exit(0);
}

int
lnet_localnids_get(__unusedx lnet_nid_t *nids, __unusedx size_t max)
{
	return (0);
}
