/* $Id$ */

#include <stdio.h>
#include <stdlib.h>

#include "slconfig.h"
#include "pathnames.h"
#include "../slashd/sb.h"

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
	struct slash_sb_store sbs;
	const char *cfgfn;
	char fn[PATH_MAX];
	int fd, rc, c;
	sl_resm_t *r;
	ssize_t sz;

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

	/* main slash directory */
	rc = snprintf(fn, sizeof(fn), "%s", r->resm_res->res_fsroot);
	if (rc == -1)
		psc_fatal("snprintf");
	if (rc >= (int)sizeof(fn))
		psc_fatalx("name too long");
	if (mkdir(fn, 0755) == -1)
		psc_fatal("mkdir %s", fn);

	/* FID namespace */
	rc = snprintf(fn, sizeof(fn), "%s/%s", r->resm_res->res_fsroot,
	    _PATH_OBJROOT);
	if (rc == -1)
		psc_fatal("snprintf");
	if (rc >= (int)sizeof(fn))
		psc_fatalx("name too long");
	if (mkdir(fn, 0755) == -1)
		psc_fatal("mkdir %s", fn);

	/* real namespace */
	rc = snprintf(fn, sizeof(fn), "%s/%s", r->resm_res->res_fsroot,
	    _PATH_NS);
	if (rc == -1)
		psc_fatal("snprintf");
	if (rc >= (int)sizeof(fn))
		psc_fatalx("name too long");
	if (mkdir(fn, 0755) == -1)
		psc_fatal("mkdir %s", fn);

	/* superblock */
	rc = snprintf(fn, sizeof(fn), "%s/%s", r->resm_res->res_fsroot,
	    _PATH_SB);
	if (rc == -1)
		psc_fatal("snprintf");
	if (rc >= (int)sizeof(fn))
		psc_fatalx("name too long");
	if ((fd = open(fn, O_CREAT | O_EXCL | O_WRONLY, 0644)) == -1)
		psc_fatal("open %s", fn);
	memset(&sbs, 0, sizeof(sbs));
	sz = write(fd, &sbs, sizeof(sbs));
	if (sz == -1)
		psc_fatal("write");
	else if (sz != sizeof(sbs))
		psc_fatalx("short write");
	close(fd);

	/* journal */
	rc = snprintf(fn, sizeof(fn), "%s/%s", r->resm_res->res_fsroot,
	    _PATH_SLJOURNAL);
	if (rc == -1)
		psc_fatal("snprintf");
	if (rc >= (int)sizeof(fn))
		psc_fatalx("name too long");
	if ((fd = open(fn, O_CREAT | O_EXCL | O_WRONLY, 0644)) == -1)
		psc_fatal("open %s", fn);
	close(fd);

	exit(0);
}

int
lnet_localnids_get(__unusedx lnet_nid_t *nids, __unusedx size_t max)
{
	return (0);
}
