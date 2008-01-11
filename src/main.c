/* $Id$ */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "pfl.h"
#include "psc_util/alloc.h"
#include "psc_util/thread.h"

#include "slash.h"

#define SLASH_THRTBL_SIZE 19

const char *progname;

__dead void
usage(void)
{
	fprintf(stderr, "usage: %s\n", progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	int c;

	if (getenv("LNET_NETWORKS") == NULL)
		psc_fatalx("please export LNET_NETWORKS");
	if (getenv("TCPLND_SERVER") == NULL)
		psc_fatalx("please export TCPLND_SERVER");

	progname = argv[0];
	while ((c = getopt(argc, argv, "")) != -1)
		switch (c) {
		default:
			usage();
		}
	pfl_init(SLASH_THRTBL_SIZE);
	slmds_init();
	exit(0);
}

void
spawn_lnet_thr(pthread_t *t, void *(*startf)(void *), void *arg)
{
	extern int tcpnal_instances;
	struct psc_thread *pt;

	pt = PSCALLOC(sizeof(*pt));
	pscthr_init(pt, SLTHRT_LND, startf, "sllndthr%d",
	    tcpnal_instances - 1);
	*t = pt->pscthr_pthread;
	pt->pscthr_private = arg;
}

#if 0
void
spawn_lnet_thr(pthread_t *t, void *(*startf)(void *), void *arg)
{
	int rc;

	rc = pthread_create(t, NULL, startf, arg);
	if (rc)
		psc_fatalx("pthread_create: %s", strerror(rc));
}
#endif
