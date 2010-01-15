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

#ifndef _SLASH_CLI_BMAP_H_
#define _SLASH_CLI_BMAP_H_

#include "psc_rpc/rpc.h"
#include "psc_util/lock.h"

#include "bmap.h"
#include "bmpc.h"
#include "inode.h"

/*
 * msbmap_crcrepl_states - must be the same as bh_crcstates and bh_repls
 *  in slash_bmap_od.
 */
struct msbmap_crcrepl_states {
	uint8_t	msbcr_crcstates[SL_CRCS_PER_BMAP];	/* crc descriptor bits  */
	uint8_t	msbcr_repls[SL_REPLICA_NBYTES];		/* replica bit map        */
};

/*
 * bmap_cli_data - assigned to bmap->bcm_pri for mount slash client.
 */
struct bmap_cli_info {
	struct bmap_pagecache		 msbd_bmpc;
	struct bmapc_memb		*msbd_bmap;
	lnet_nid_t			 msbd_ion;
	struct msbmap_crcrepl_states	 msbd_msbcr;
	struct srt_bmapdesc_buf		 msbd_bdb;	/* open bmap descriptor */
	struct psclist_head		 msbd_lentry;
};

#define bmap_2_msbd(b)			((struct bmap_cli_info *)(b)->bcm_pri)
#define bmap_2_msbmpc(b)		&(bmap_2_msbd(b)->msbd_bmpc)
#define bmap_2_msion(b)			bmap_2_msbd(b)->msbd_ion

/* bmap client modes */
#define BMAP_CLI_MCIP			(_BMAP_FLSHFT << 0)  /* mode change in progress */
#define	BMAP_CLI_MCC			(_BMAP_FLSHFT << 1)  /* mode change compete */
#define BMAP_CLI_FLUSHPROC              (_BMAP_FLSHFT << 2)  /* proc'd by flush thr */

#endif /* _SLASH_CLI_BMAP_H_ */
