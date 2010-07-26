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

/*
 * authbuf_sign - routines for signing and checking the signatures of
 * authbufs on RPC messages.  The secret key is shared among all hosts
 * in a SLASH network.
 */

#include <string.h>

#include "psc_rpc/rpc.h"
#include "psc_util/atomic.h"
#include "psc_util/base64.h"
#include "psc_util/log.h"

#include "authbuf.h"
#include "slashrpc.h"
#include "slerr.h"

/**
 * authbuf_sign - Sign a message with the secret key.
 * @rq: request structure to sign.
 * @msgtype: request or reply to sign.
 */
void
authbuf_sign(struct pscrpc_request *rq, int msgtype)
{
	struct srt_authbuf_footer *saf;
	struct pscrpc_msg *m;
	gcry_error_t gerr;
	gcry_md_hd_t hd;

	if (msgtype == PSCRPC_MSG_REQUEST)
		m = rq->rq_reqmsg;
	else
		m = rq->rq_repmsg;

	saf = pscrpc_msg_buf(m, 1, sizeof(*saf));
	saf->saf_secret.sas_magic = AUTHBUF_MAGIC;
	saf->saf_secret.sas_nonce = psc_atomic64_inc_getnew(&authbuf_nonce);
	saf->saf_secret.sas_src_nid = rq->rq_import->imp_connection->c_self;
	saf->saf_secret.sas_src_pid = PSCRPC_SVR_PID;
	saf->saf_secret.sas_dst_nid = rq->rq_import->imp_connection->c_peer.nid;
	saf->saf_secret.sas_dst_pid = rq->rq_import->imp_connection->c_peer.pid;

	gerr = gcry_md_copy(&hd, authbuf_hd);
	if (gerr)
		psc_fatalx("gcry_md_copy: %d", gerr);

	gcry_md_write(hd, pscrpc_msg_buf(m, 0, 0),
	    pscrpc_msg_buflen(m, 0));
	gcry_md_write(hd, &saf->saf_secret, sizeof(saf->saf_secret));

	psc_base64_encode(gcry_md_read(hd, 0),
	    saf->saf_hash, authbuf_alglen);

	gcry_md_close(hd);
}

/**
 * authbuf_check - Check signature validity of a authbuf.
 * @rq: request structure to check.
 * @msgtype: request or reply to check.
 */
int
authbuf_check(struct pscrpc_request *rq, int msgtype)
{
	struct srt_authbuf_footer *saf;
	char buf[AUTHBUF_REPRLEN];
	struct pscrpc_msg *m;
	gcry_error_t gerr;
	gcry_md_hd_t hd;

	if (msgtype == PSCRPC_MSG_REQUEST)
		m = rq->rq_reqmsg;
	else
		m = rq->rq_repmsg;

	saf = pscrpc_msg_buf(m, 1, sizeof(*saf));

	if (saf->saf_secret.sas_magic != AUTHBUF_MAGIC)
		return (SLERR_AUTHBUF_BADMAGIC);

	if (saf->saf_secret.sas_src_nid != rq->rq_peer.nid ||
	    saf->saf_secret.sas_src_pid != rq->rq_peer.pid ||
	    saf->saf_secret.sas_dst_nid != rq->rq_self ||
	    saf->saf_secret.sas_dst_pid != PSCRPC_SVR_PID)
		return (SLERR_AUTHBUF_BADPEER);

	gerr = gcry_md_copy(&hd, authbuf_hd);
	if (gerr)
		psc_fatalx("gcry_md_copy: %d", gerr);

	gcry_md_write(hd, pscrpc_msg_buf(m, 0, 0), pscrpc_msg_buflen(m, 0));
	gcry_md_write(hd, &saf->saf_secret, sizeof(saf->saf_secret));

	psc_base64_encode(gcry_md_read(hd, 0), buf, authbuf_alglen);
	gcry_md_close(hd);

	if (strcmp(buf, saf->saf_hash))
		return (SLERR_AUTHBUF_BADHASH);
	return (0);
}
