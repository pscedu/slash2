/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
 *
 * Permission to use, copy, and modify this software and its documentation
 * without fee for personal use or non-commercial use within your organization
 * is hereby granted, provided that the above copyright notice is preserved in
 * all copies and that the copyright and this permission notice appear in
 * supporting documentation.  Permission to redistribute this software to other
 * organizations or individuals is not permitted without the written permission
 * of the Pittsburgh Supercomputing Center.  PSC makes no representations about
 * the suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

#include <sys/types.h>
#include <sys/stat.h>

#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "pfl/pfl.h"
#include "pfl/cdefs.h"
#include "pfl/str.h"
#include "psc_rpc/rpc.h"
#include "psc_util/log.h"

#include "slconfig.h"

char *progname;

int
psc_usklndthr_get_type(__unusedx const char *namefmt)
{
	return (0);
}

void
psc_usklndthr_get_namev(char buf[PSC_THRNAME_MAX],
    __unusedx const char *namefmt, __unusedx va_list ap)
{
	strlcpy(buf, "test", PSC_THRNAME_MAX);
}

void
slcfg_init_res(__unusedx struct sl_resource *res)
{
}

void
slcfg_init_resm(__unusedx struct sl_resm *resm)
{
}

void
slcfg_init_site(__unusedx struct sl_site *site)
{
}

__dead void
usage(void)
{
	fprintf(stderr, "usage: %s [-c file]\n", progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	char *cp, fn[PATH_MAX];
	int c;

	progname = argv[0];
	pfl_init();

	cp = psc_strdup(progname);
	snprintf(fn, sizeof(fn), "%s/example.slcfg", dirname(cp));
	PSCFREE(cp);

	setenv("LNET_NETWORKS", "tcp10(" CFS_LOOPBACK_IFNAME ")", 1);

	while (((c = getopt(argc, argv, "c:")) != -1))
		switch (c) {
		case 'c':
			if (strlcpy(fn, optarg, sizeof(fn)) >= sizeof(fn))
				psc_fatalx("name too long: %s", optarg);
			break;
		default:
			usage();
		}
	argc -= optind;
	if (argc)
		usage();

	slcfg_parse(fn);
	libsl_init(PSCNET_CLIENT, 0);
	exit(0);
}
