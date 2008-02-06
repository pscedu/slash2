/* $Id$ */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "pfl.h"
#include "psc_util/alloc.h"
#include "psc_util/thread.h"
#include "psc_rpc/rpc.h"

#include "sliod.h"
#include "fid.h"
#include "../slashd/cfd.h"
#include "slashrpc.h"
#include "control.h"

#define SLIO_THRTBL_SIZE 19
//#define _PATH_SLIOCONF "/etc/sliod.conf"
#define _PATH_SLIOCONF "config/example.conf"
#define _PATH_SLIOCTLSOCK "../sliod.sock"

extern void *nal_thread(void *);

struct psc_thread slioControlThread;

const char *progname;

__dead void
usage(void)
{
	fprintf(stderr, "usage: %s [-f cfgfile] [-S socket]\n", progname);
	exit(1);
}

void *
sliolndthr_start(void *arg)
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
	pscthr_init(pt, SLIOTHRT_LND, sliolndthr_start, arg, "sllndthr%d",
	    tcpnal_instances - 1);
	*t = pt->pscthr_pthread;
}

int
main(int argc, char *argv[])
{
	struct slio_ctlthr *thr;
	const char *cfn, *sfn;
	int c;

	progname = argv[0];
	pfl_init(SLIO_THRTBL_SIZE);
	thr = PSCALLOC(sizeof(*thr));
	pscthr_init(&slioControlThread, SLIOTHRT_CTL, NULL, thr, "slioctlthr");

	if (getenv("LNET_NETWORKS") == NULL)
		psc_fatalx("please export LNET_NETWORKS");
	if (getenv("TCPLND_SERVER") == NULL)
		psc_fatalx("please export TCPLND_SERVER");
	lnet_thrspawnf = spawn_lnet_thr;

	cfn = _PATH_SLIOCONF;
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
        pscrpc_init_portals(PSC_SERVER);
	slio_init();
	slioctlthr_main(sfn);
	exit(0);
}

int
cfd2fid_cache(slash_fid_t *fidp, struct pscrpc_export *exp, u64 cfd)
{
	struct slashrpc_getfid_req *mq;
	struct slashrpc_getfid_rep *mp;
	struct pscrpc_request *rq;
	struct cfdent *c;
	int rc;

	/* Check in cfdtree. */
	if (cfd2fid(fidp, exp, cfd) == 0)
		return (0);

	/* Not there, contact slashd and populate it. */
	if ((rc = rpc_newreq(RPCSVC_MDS, SMDS_VERSION, SRMT_GETFID,
	    sizeof(*mq), sizeof(*mp), &rq, &mq)) != 0)
		return (rc);
	mq->pid = exp->exp_connection->c_peer.pid;
	mq->nid = exp->exp_connection->c_peer.nid;
	mq->cfd = cfd;
	if ((rc = rpc_getrep(rq, sizeof(*mp), &mp)) == 0)
		if ((c = cfdinsert(cfd, exp, fidp)) != NULL)
			*fidp = c->fid;
	pscrpc_req_finished(rq);
	return (rc);				/* XXX preserve errno */
}
