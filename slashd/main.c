/* $Id$ */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "pfl.h"
#include "psc_util/alloc.h"
#include "psc_util/ctlsvr.h"
#include "psc_util/thread.h"

#include "control.h"
#include "pathnames.h"
#include "sb.h"
#include "slashd.h"
#include "slconfig.h"

#define SLASH_THRTBL_SIZE 19

void *nal_thread(void *);

const char *progname;

__dead void
usage(void)
{
	fprintf(stderr, "usage: %s [-f cfgfile] [-S socket]\n", progname);
	exit(1);
}

void *
sllndthr_start(void *arg)
{
	struct psc_thread *thr;

	thr = arg;
	return (nal_thread(thr->pscthr_private));
}

void
spawn_lnet_thr(pthread_t *t, void *(*startf)(void *), void *arg)
{
	extern int tcpnal_instances;
	struct psc_thread *pt;

	if (startf != nal_thread)
		psc_fatalx("unexpected LND start routine");

	pt = PSCALLOC(sizeof(*pt));
	pscthr_init(pt, SLTHRT_LND, sllndthr_start, arg, "sllndthr%d",
	    tcpnal_instances - 1);
	*t = pt->pscthr_pthread;
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

	if (getenv("LNET_NETWORKS") == NULL)
		psc_fatalx("please export LNET_NETWORKS");
	if ((p = getenv("TCPLND_SERVER")) == NULL || strcmp(p, "1"))
		setenv("TCPLND_SERVER", "1", 1);

	pfl_init(SLASH_THRTBL_SIZE);
	pscthr_init(&pscControlThread, SLTHRT_CTL, NULL,
	    PSCALLOC(sizeof(struct psc_ctlthr)), "slctlthr");

	lnet_thrspawnf = spawn_lnet_thr;

	slashGetConfig(cfn);
	libsl_init(PSC_SERVER);

	slash_superblock_init();
	slash_journal_init();

	slbe_init();
	slmds_init();
	slctlthr_main(sfn);
	exit(0);
}
