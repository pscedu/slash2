/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright (c) 2006-2013, Pittsburgh Supercomputing Center (PSC).
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
#include "pfl/rpc.h"
#include "pfl/log.h"

#include "slconfig.h"

extern const char *__progname;

int	 cfg_site_pri_sz;
int	 cfg_res_pri_sz;
int	 cfg_resm_pri_sz;

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
slcfg_destroy_res(__unusedx struct sl_resource *res)
{
}

void
slcfg_init_resm(__unusedx struct sl_resm *resm)
{
}

void
slcfg_destroy_resm(__unusedx struct sl_resm *resm)
{
}

void
slcfg_init_site(__unusedx struct sl_site *site)
{
}

void
slcfg_destroy_site(__unusedx struct sl_site *site)
{
}

__dead void
usage(void)
{
	fprintf(stderr, "usage: %s [-c file]\n", __progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	char *cp, fn[PATH_MAX];
	int c;

	pfl_init();

	cp = pfl_strdup(__progname);
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
	libsl_init(4096);
	exit(0);
}
