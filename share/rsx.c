/* $Id$ */

/*
 * Routines for handling simple RPC message exchanges.
 */

#include <errno.h>
#include <stdio.h>

#include "psc_rpc/rpc.h"
#include "rsx.h"

/*
 * rsx_newreq - Create a new request and associate it with the import.
 * @imp: import portal on which to create the request.
 * @version: version of communication protocol of channel.
 * @op: operation ID of command to send.
 * @reqlen: length of request buffer.
 * @replen: length of expected reply buffer.
 * @rqp: value-result of pointer to RPC request.
 * @mqp: value-result of pointer to start of request buffer.
 */
int
rsx_newreq(struct pscrpc_import *imp, int version, int op, int reqlen, int replen,
    struct pscrpc_request **rqp, void *mqp)
{
	int rc;

	rc = 0;
	*rqp = pscrpc_prep_req(imp, version, op, 1, &reqlen, NULL);
	if (*rqp == NULL)
		return (-ENOMEM);

	/* Setup request buffer. */
	*(void **)mqp = psc_msg_buf((*rqp)->rq_reqmsg, 0, reqlen);
	if (*(void **)mqp == NULL)
		psc_fatalx("psc_msg_buf");

	/* Setup reply buffer. */
	(*rqp)->rq_replen = psc_msg_size(1, &replen);
	return (0);
}

/*
 * rsx_getrep - Wait for a reply of a "simple" command, i.e. an error code.
 * @rq: the RPC request we sent.
 * @replen: anticipated size of response.
 * @mpp: value-result pointer where reply buffer start will be set.
 */
int
rsx_getrep(struct pscrpc_request *rq, int replen, void *mpp)
{
	int rc;

	/* Send the request and block on its completion. */
	rc = pscrpc_queue_wait(rq);
	if (rc)
		return (rc);
	*(void **)mpp = psc_msg_buf(rq->rq_repmsg, 0, replen);
	if (*(void **)mpp == NULL)
		return (-ENOMEM);
	return (0);
}
