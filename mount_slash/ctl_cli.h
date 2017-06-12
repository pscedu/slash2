/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright 2006-2016, Pittsburgh Supercomputing Center
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
 * Interface for controlling live operation of a mount_slash instance.
 */

#ifndef _SL_CTL_CLI_H_
#define _SL_CTL_CLI_H_

#include "pfl/list.h"
#include "pfl/lock.h"
#include "pfl/lockedlist.h"
#include "pfl/waitq.h"

#include "fid.h"
#include "slconfig.h"

struct psc_ctlmsghdr;

/* for retrieving info about replication status */
struct msctlmsg_replst {
	slfid_t			mrs_fid;
	char			mrs_iosv[SL_MAX_REPLICAS][RES_NAME_MAX];
	uint32_t		mrs_nios;
	uint32_t		mrs_newreplpol;	/* default replication policy */
};

struct msctlmsg_replst_slave {
	slfid_t			mrsl_fid;
	uint32_t		mrsl_boff;	/* bmap starting offset */
	uint32_t		mrsl_nbmaps;	/* # of bmaps in this chunk */
	uint32_t		mrsl_flags;	/* see MRSLF_* flags below */
	char			mrsl_data[0];	/* bcs_repls data */
};

#define MRSLF_EOF		1

/* for issuing/controlling replication requests */
struct msctlmsg_replrq {
	slfid_t			mrq_fid;
	char			mrq_iosv[SL_MAX_REPLICAS][RES_NAME_MAX];
	uint32_t		mrq_nios;	/* # elements in iosv */
	sl_bmapno_t		mrq_bmapno;	/* start position */
	sl_bmapno_t		mrq_nbmaps;	/* length */
	 int32_t		mrq_sys_prio;	/* priority of request */
	 int32_t		mrq_usr_prio;
};

struct msctlmsg_fattr {
	slfid_t			mfa_fid;
	int32_t			mfa_attrid;
	int32_t			mfa_val;
};

struct msctlmsg_bmapreplpol {
	slfid_t			mfbrp_fid;
	sl_bmapno_t		mfbrp_bmapno;
	sl_bmapno_t		mfbrp_nbmaps;
	int32_t			mfbrp_pol;
};

struct msctlmsg_biorq {
	slfid_t			msr_fid;
	sl_bmapno_t		msr_bno;
	 int32_t		msr_ref;
	uint32_t		msr_off;
	uint32_t		msr_len;
	uint32_t		msr_flags;
	uint32_t		msr_retries;
	char			msr_last_sliod[RES_NAME_MAX];
	struct pfl_timespec	msr_expire;
	 int32_t		msr_npages;
	long			msr_addr;
};

struct msctlmsg_bmpce {
	slfid_t			mpce_fid;
	sl_bmapno_t		mpce_bno;
	 int32_t		mpce_ref;
	 int32_t		mpce_rc;
	uint32_t		mpce_flags;
	uint32_t		mpce_off;
	uint32_t		mpce_start;
	struct pfl_timespec	mpce_laccess;
	 int32_t		mpce_nwaiters;
	 int32_t		mpce_npndgaios;
};

/* mount_slash message types */
#define MSCMT_ADDREPLRQ		(NPCMT +  0)
#define MSCMT_DELREPLRQ		(NPCMT +  1)
#define MSCMT_GETCONNS		(NPCMT +  2)
#define MSCMT_GETFCMH		(NPCMT +  3)
#define MSCMT_GETREPLST		(NPCMT +  4)
#define MSCMT_GETREPLST_SLAVE	(NPCMT +  5)
#define MSCMT_GET_BMAPREPLPOL	(NPCMT +  6)
#define MSCMT_GET_FATTR		(NPCMT +  7)
#define MSCMT_SET_BMAPREPLPOL	(NPCMT +  8)
#define MSCMT_SET_FATTR		(NPCMT +  9)
#define MSCMT_GETBMAP		(NPCMT + 10)
#define MSCMT_GETBIORQ		(NPCMT + 11)
#define MSCMT_GETBMPCE		(NPCMT + 12)

#define SLASH_FSID		0x51a54

struct msctl_replstq {
	struct psc_listentry		 mrsq_lentry;
	struct psc_waitq		 mrsq_waitq;
	int				 mrsq_id;
	int				 mrsq_fd;
	struct pfl_mutex		*mrsq_fdlock;
	int				 mrsq_rc;
	const struct psc_ctlmsghdr	*mrsq_mh;
	psc_spinlock_t			 mrsq_lock;
	slfid_t				 mrsq_fid;
};

void mrsq_release(struct msctl_replstq *, int);

extern struct psc_lockedlist	msctl_replsts;

extern struct psc_thread	*msl_ctlthr0;
extern void			*msl_ctlthr0_private;

#endif /* _SL_CTL_CLI_H_ */
