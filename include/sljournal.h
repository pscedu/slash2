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

#ifndef _SL_JOURNAL_
#define _SL_JOURNAL_

#include "bmap.h"
#include "inode.h"
#include "pathnames.h"
#include "slconfig.h"

#define SLJ_MDS_JNENTS		(128 * 1024)	/* 131072 */
#define SLJ_MDS_RA		1024		/* SLJ_MDS_JNENTS % SLJ_MDS_RA == 0 */
#define SLJ_MDS_NCRCS		28

#define SLJ_MDS_PJET_VOID	0
#define SLJ_MDS_PJET_INUM	1
#define SLJ_MDS_PJET_BMAP	2
#define SLJ_MDS_PJET_INODE	3

/*
 * slmds_jent_crc - is used to log crc updates which come from the ION's.
 * @sjc_ion: the ion who sent the request.
 * @sjc_fid: what file.
 * @sjc_bmapno: which bmap region.
 * @sjc_crc: array of slots and crcs.
 * Notes: I presume that this will be the most common operation into the
 *    journal.
 */
struct slmds_jent_crc {
	slfid_t			sjc_fid;
	sl_blkno_t		sjc_bmapno;
	sl_ios_id_t		sjc_ion; /* Track the ion which did the I/O */
	uint32_t		sjc_ncrcs;
	uint64_t		sjc_fsize;
	struct srm_bmap_crcwire	sjc_crc[SLJ_MDS_NCRCS];
} __packed;

#define slion_jent_crc slmds_jent_crc

/*
 * slmds_jent_repgen - log changes to the replication state of a bmap which
 *    occur upon processing a new write for a replicated bmap.
 * @sjp_fid: what file.
 * @sjp_bmapno: which bmap region.
 * @sjp_bgen: the new bmap generation.
 * @sjp_reptbl: the replica table.
 */
struct slmds_jent_repgen {
	slfid_t			sjp_fid;
	sl_blkno_t		sjp_bmapno;
	sl_blkgen_t		sjp_bgen;
	uint8_t			sjp_reptbl[SL_REPLICA_NBYTES];
} __packed;


/*
 * slmds_jent_ino_addrepl - add a new replica IOS to the inode or the inode
 *    extras.
 * @sjir_fid: what file.
 * @sjir_ios: the IOS being added.
 * @sjir_pos: the slot or position the replica IOS is to be added to.
 */
struct slmds_jent_ino_addrepl {
	slfid_t			sjir_fid;
	sl_ios_id_t		sjir_ios;
	uint32_t		sjir_pos;
} __packed;


struct slmds_jent_bmapseq {
	uint64_t sjbsq_high_wm;
	uint64_t sjbsq_low_wm;
} __packed;

/* List all of the journaling structures here so that the maximum
 *  size can be obtained.
 */
struct slmds_jents {
	union {
		struct slmds_jent_repgen	sjr;
		struct slmds_jent_crc		sjc;
		struct slmds_jent_ino_addrepl	sjia;
		struct slmds_jent_bmapseq       sjsq;
	} slmds_jent_types;
};

/*
 * The combined size of the standard header of each log entry (i.e., struct psc_journal_enthdr)
 * and its data, if any, should occupy less than this size.
 */
#define	SLJ_MDS_ENTSIZE		512

#endif /* _SL_JOURNAL_ */
