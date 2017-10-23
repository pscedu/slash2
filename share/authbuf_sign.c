/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015, Google, Inc.
 * Copyright (c) 2009-2015, Pittsburgh Supercomputing Center (PSC).
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

/*
 * authbuf_sign - routines for signing and checking the signatures of
 * authbufs on RPC messages.  The secret key is shared among all hosts
 * in a SLASH2 deployment.
 *
 * The authbuf is transmitted as the last buffer in any given RPC.
 */

#include <gcrypt.h>

#include "pfl/atomic.h"
#include "pfl/log.h"
#include "pfl/rpc.h"

#include "lnet/lib-lnet.h"

#include "authbuf.h"
#include "slashrpc.h"
#include "slconn.h"
#include "slerr.h"

int	sl_conn_debug = 1;

/*
 * Sign a message with the secret key.
 * @rq: request structure to sign.
 * @msgtype: request or reply to sign.
 */
void
authbuf_sign(struct pscrpc_request *rq, int msgtype)
{
	lnet_process_id_t self_prid, peer_prid;
	struct srt_authbuf_footer *saf;
	struct pscrpc_bulk_desc *bd;
	struct pscrpc_msg *m;
	char ebuf[BUFSIZ];
	gcry_error_t gerr;
	gcry_md_hd_t hd;
	uint32_t i;

	if (msgtype == PSCRPC_MSG_REQUEST)
		m = rq->rq_reqmsg;
	else {
		m = rq->rq_repmsg;

		/*
		 * If there was an invalid opcode or protocol violation,
		 * no reply message may have been set up.
		 */
		if (m == NULL)
			return;
	}

	psc_assert(m->bufcount > 1);

	saf = pscrpc_msg_buf(m, m->bufcount - 1, sizeof(*saf));

	/* If already computed, short circuit out of here. */
	if (saf->saf_secret.sas_magic == AUTHBUF_MAGIC)
		return;

	saf->saf_secret.sas_magic = AUTHBUF_MAGIC;
	saf->saf_secret.sas_nonce =
	    psc_atomic64_inc_getnew(&sl_authbuf_nonce);

	pscrpc_req_getprids(&sl_lnet_prids, rq, &self_prid, &peer_prid);
	saf->saf_secret.sas_src_nid = self_prid.nid;
	saf->saf_secret.sas_src_pid = self_prid.pid;
	saf->saf_secret.sas_dst_nid = peer_prid.nid;
	saf->saf_secret.sas_dst_pid = peer_prid.pid;

	/*
 	 * 07/23/2017: Called from msl_read_rpc_launch() and segfault
 	 * inside_int_malloc().
 	 *
 	 * mount_wokfs[9727]: segfault at 100000008 ip 00007fabe7d7e7fa
 	 * sp 00007faa197f1b80 error 4 in libc-2.17.so[7fabe7d03000+1b7000]
 	 */
	gerr = gcry_md_copy(&hd, sl_authbuf_hd);
	if (gerr) {
		gpg_strerror_r(gerr, ebuf, sizeof(ebuf));
		psc_fatalx("gcry_md_copy: %s [%d]", ebuf, gerr);
	}

	for (i = 0; i < m->bufcount - 1; i++)
		gcry_md_write(hd, pscrpc_msg_buf(m, i, 0),
		    pscrpc_msg_buflen(m, i));

	gcry_md_write(hd, &saf->saf_secret, sizeof(saf->saf_secret));

	memcpy(saf->saf_hash, gcry_md_read(hd, 0), AUTHBUF_ALGLEN);

	gcry_md_close(hd);

	bd = rq->rq_bulk;
	if (bd && bd->bd_type == BULK_GET_SOURCE)
		slrpc_bulk_sign(rq, saf->saf_bulkhash, bd->bd_iov,
		    bd->bd_iov_count);
}

/*
 * Check signature validity of a authbuf.
 * @rq: request structure to check.
 * @msgtype: request or reply to check.
 */
int
authbuf_check(struct pscrpc_request *rq, int msgtype, int flags)
{
	lnet_process_id_t self_prid, peer_prid;
	struct srt_authbuf_footer *saf;
	struct pscrpc_bulk_desc *bd;
	struct pscrpc_msg *m;
	char ebuf[BUFSIZ];
	gcry_error_t gerr;
	gcry_md_hd_t hd;
	uint32_t i;
	int rc = 0;

	if (msgtype == PSCRPC_MSG_REQUEST)
		m = rq->rq_reqmsg;
	else
		m = rq->rq_repmsg;

	if (m->bufcount < 2)
		return (PFLERR_BADMSG);

	saf = pscrpc_msg_buf(m, m->bufcount - 1, sizeof(*saf));

	if (saf == NULL)
		return (SLERR_AUTHBUF_ABSENT);

	if (saf->saf_secret.sas_magic != AUTHBUF_MAGIC) {
		psclog_debug("invalid authbuf magic: "
		    "%"PRIx64" vs %"PRIx64, saf->saf_secret.sas_magic,
		    AUTHBUF_MAGIC);
		return (SLERR_AUTHBUF_BADMAGIC);
	}

	pscrpc_req_getprids(&sl_lnet_prids, rq, &self_prid, &peer_prid);
	if (saf->saf_secret.sas_src_nid != peer_prid.nid ||
	    saf->saf_secret.sas_src_pid != peer_prid.pid ||
	    saf->saf_secret.sas_dst_nid != self_prid.nid ||
	    saf->saf_secret.sas_dst_pid != self_prid.pid)
		rc = SLERR_AUTHBUF_BADPEER;

	if ((sl_conn_debug == 2) ||
	    (rc == SLERR_AUTHBUF_BADPEER && sl_conn_debug == 1)) {

		lnet_process_id_t src_prid;
		lnet_process_id_t dst_prid;
		char src_buf[PSCRPC_NIDSTR_SIZE];
		char dst_buf[PSCRPC_NIDSTR_SIZE];
		char self_buf[PSCRPC_NIDSTR_SIZE];
		char peer_buf[PSCRPC_NIDSTR_SIZE];

		src_prid.nid = saf->saf_secret.sas_src_nid;
		src_prid.pid = saf->saf_secret.sas_src_pid;

		dst_prid.nid = saf->saf_secret.sas_dst_nid;
		dst_prid.pid = saf->saf_secret.sas_dst_pid;

		pscrpc_id2str(src_prid, src_buf);
		pscrpc_id2str(dst_prid, dst_buf);

		pscrpc_id2str(peer_prid, peer_buf);
		pscrpc_id2str(self_prid, self_buf);

		psclog_max("authbuf: (src=%s, dst=%s), "
		    "actual: (self=%s, peer=%s), rc = %d",
		    src_buf, dst_buf, self_buf, peer_buf, rc);
	}
	if (rc)
		return (rc);

	gerr = gcry_md_copy(&hd, sl_authbuf_hd);
	if (gerr) {
		gpg_strerror_r(gerr, ebuf, sizeof(ebuf));
		psc_fatalx("gcry_md_copy: %s [%d]", ebuf, gerr);
	}

	for (i = 0; i < m->bufcount - 1; i++)
		gcry_md_write(hd, pscrpc_msg_buf(m, i, 0),
		    pscrpc_msg_buflen(m, i));

	gcry_md_write(hd, &saf->saf_secret, sizeof(saf->saf_secret));

	if (memcmp(gcry_md_read(hd, 0), saf->saf_hash, AUTHBUF_ALGLEN)) {
		psc_fatalx("authbuf did not hash correctly -- "
		    "ensure key files are synced");
		rc = SLERR_AUTHBUF_BADHASH;
	}
	gcry_md_close(hd);
	if (rc)
		return (rc);

	bd = rq->rq_bulk;
	if (bd && bd->bd_type == BULK_PUT_SINK &&
	    msgtype == PSCRPC_MSG_REPLY &&
	    (flags & SRPCWAITF_DEFER_BULK_AUTHBUF_CHECK) == 0 &&
	    /*
	     * XXX can this flag be arbitrarily spoofed to ignore this check?
	     */
	    (m->flags & MSG_ABORT_BULK) == 0)
		rc = slrpc_bulk_check(rq, saf->saf_bulkhash, bd->bd_iov,
		    bd->bd_iov_count);

	return (rc);
}
