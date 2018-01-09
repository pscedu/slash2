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
 * SLASH2 remote procedure call (RPC) message (SRM) definitions, for
 * issuing operations on and communicating with other hosts in a SLASH2
 * deployment.
 */

#ifndef _SLASHRPC_H_
#define _SLASHRPC_H_

#include "pfl/cdefs.h"
#include "pfl/time.h"

#include "authbuf.h"
#include "bmap.h"
#include "cache_params.h"
#include "creds.h"
#include "fid.h"
#include "sltypes.h"

struct stat;
struct statvfs;

/*
 * Technically, RPCs between different kinds of pairs of peers 
 * can have different versions. However, to avoid hassle in terms 
 * of maintainence and administration. Let us use one version.
 */
#define	SL_RPC_VERSION		2

/* RPC channel to MDS from CLI. */
#define SRMC_REQ_PORTAL		10
#define SRMC_REP_PORTAL		11
#define SRMC_BULK_PORTAL	12
#define SRMC_CTL_PORTAL		13

#define SRMC_VERSION		SL_RPC_VERSION
#define SRMC_MAGIC		UINT64_C(0xaabbccddeeff0022)

/* RPC channel to MDS from MDS. */
#define SRMM_REQ_PORTAL		15
#define SRMM_REP_PORTAL		16
#define SRMM_BULK_PORTAL	17
#define SRMM_CTL_PORTAL		18

#define SRMM_VERSION		SL_RPC_VERSION
#define SRMM_MAGIC		UINT64_C(0xaabbccddeeff0033)

/* RPC channel to MDS from ION. */
#define SRMI_REQ_PORTAL		20
#define SRMI_REP_PORTAL		21
#define SRMI_BULK_PORTAL	22
#define SRMI_CTL_PORTAL		23

#define SRMI_VERSION		SL_RPC_VERSION
#define SRMI_MAGIC		UINT64_C(0xaabbccddeeff0044)

/* RPC channel to CLI from MDS. */
#define SRCM_REQ_PORTAL		25
#define SRCM_REP_PORTAL		26
#define SRCM_BULK_PORTAL	27
#define SRCM_CTL_PORTAL		28

#define SRCM_VERSION		SL_RPC_VERSION
#define SRCM_MAGIC		UINT64_C(0xaabbccddeeff0055)

/* RPC channel to ION from CLI. */
#define SRIC_REQ_PORTAL		30
#define SRIC_REP_PORTAL		31
#define SRIC_BULK_PORTAL	32
#define SRIC_CTL_PORTAL		33

#define SRIC_VERSION		SL_RPC_VERSION
#define SRIC_MAGIC		UINT64_C(0xaabbccddeeff0066)

/* RPC channel to ION from ION. */
#define SRII_REQ_PORTAL		35
#define SRII_REP_PORTAL		36
#define SRII_BULK_PORTAL	37
#define SRII_CTL_PORTAL		38

#define SRII_VERSION		SL_RPC_VERSION
#define SRII_MAGIC		UINT64_C(0xaabbccddeeff0077)

/* RPC channel to ION from MDS. */
#define SRIM_REQ_PORTAL		40
#define SRIM_REP_PORTAL		41
#define SRIM_BULK_PORTAL	42
#define SRIM_CTL_PORTAL		43

#define SRIM_VERSION		SL_RPC_VERSION
#define SRIM_MAGIC		UINT64_C(0xaabbccddeeff0088)

/* RPC channel to CLI from ION. */
#define SRCI_REQ_PORTAL		45
#define SRCI_REP_PORTAL		46
#define SRCI_BULK_PORTAL	47
#define SRCI_CTL_PORTAL		48

#define SRCI_VERSION		SL_RPC_VERSION
#define SRCI_MAGIC		UINT64_C(0xaabbccddeeff0099)

/* sizeof(pscrpc_msg) + hdr + sizeof(authbuf_footer) */
#define SLRPC_MSGADJ		(176)

#define SLRPC_MSGF_STATFS	_PFLRPC_MSGF_LAST

/* SLASH2 RPC message types and submessage types (for BATCH). */
enum {
	/* control operations */
	SRMT_CONNECT = 1,			/*  1: connect */
	SRMT_PING,				/*  2: ping */

	/* namespace operations */
	SRMT_NAMESPACE_UPDATE,			/*  3: send a batch of namespace update logs */
	SRMT_NAMESPACE_FORWARD,			/*  4: a namespace operation request from a peer */

	/* bmap operations */
	SRMT_BMAPCHWRMODE,			/*  5: change read/write access mode */
	SRMT_UPDATEFILE,			/*  6: update file status */
	SRMT_BMAPDIO,				/*  7: request client direct I/O on a bmap */
	SRMT_BMAP_PTRUNC,			/*  8: partial truncate and redo CRC for bmap */
	SRMT_BMAP_WAKE,				/*  9: client work may now progress after EAGAIN */
	SRMT_GETBMAP,				/* 10: get client lease for bmap access */
	SRMT_GETBMAPCRCS,			/* 11: get bmap data checksums */
	SRMT_GETBMAPMINSEQ,			/* 12: get minimum sequence # */
	SRMT_RELEASEBMAP,			/* 13: relinquish a client's bmap access lease */
	SRMT_EXTENDBMAPLS,			/* 14: bmap lease extension request */
	SRMT_REASSIGNBMAPLS,			/* 15: reassign lease */

	/* garbage operations */
	SRMT_RECLAIM,				/* 16: trash storage space for a given FID+GEN */

	/* replication operations */
	SRMT_REPL_ADDRQ,			/* 17: add/refresh replication */
	SRMT_REPL_DELRQ,			/* 18: eject replication */
	SRMT_REPL_GETST,			/* 19: get replication request status */
	SRMT_REPL_GETST_SLAVE,			/* 20: all bmap repl info for a file */
	SRMT_REPL_READ,				/* 21: ION to ION replicate */
	SRMT_REPL_READAIO,			/* 22: ION aio response */
	SRMT_REPL_SCHEDWK,			/* 23: MDS to ION replication staging */
	SRMT_SET_BMAPREPLPOL,			/* 24: bmap replication policy */

	/* other SLASH2-specific operations */
	SRMT_SET_FATTR,				/* 25: set file attribute */
	SRMT_GET_INODE,				/* 26: get file attributes  */

	/* standard file system operations */
	SRMT_CREATE,				/* 27: creat(2) */
	SRMT_GETATTR,				/* 28: stat(2) */
	SRMT_LINK,				/* 29: link(2) */
	SRMT_LOOKUP,				/* 30: lookup (namei) */
	SRMT_MKDIR,				/* 31: mkdir(2) */
	SRMT_MKNOD,				/* 32: mknod(2) */
	SRMT_READ,				/* 33: read(2) */
	SRMT_READDIR,				/* 34: readdir(2) */
	SRMT_READLINK,				/* 35: readlink(2) */
	SRMT_RENAME,				/* 36: rename(2) */
	SRMT_RMDIR,				/* 37: rmdir(2) */
	SRMT_SETATTR,				/* 38: chmod(2), chown(2), utimes(2) */
	SRMT_STATFS,				/* 39: statvfs(2) */
	SRMT_SYMLINK,				/* 40: symlink(2) */
	SRMT_UNLINK,				/* 41: unlink(2) */
	SRMT_WRITE,				/* 42: write(2) */
	SRMT_LISTXATTR,				/* 43: listxattr(2) */
	SRMT_SETXATTR,				/* 44: setxattr(2) */
	SRMT_GETXATTR,				/* 45: getxattr(2) */
	SRMT_REMOVEXATTR,			/* 46: removexattr(2) */

	/* import/export */
	SRMT_IMPORT,				/* 47: import */

	SRMT_PRECLAIM,				/* 48: partial file reclaim */
	SRMT_BATCH_RQ,				/* 49: async batch request */
	SRMT_BATCH_RP,				/* 50: async batch reply */
	SRMT_CTL,				/* 51: generic control */

	SRMT_TOTAL
};

extern const char *slrpc_names[];

/* ----------------------------- BEGIN MESSAGES ----------------------------- */

/*
 * Note: member ordering within structures must always follow 64-bit boundaries
 * to preserve compatibility between 32-bit and 64-bit machines.
 */

struct srm_generic_rep {
	 int32_t		rc;		/* return code, 0 for success or slerrno */
	 int32_t		_pad;		/* overloadable field */
} __packed;

struct srm_batch_req {
	uint64_t		bid;
	 int32_t		opc;
	 int32_t		rc;		/* for BATCH_RP */
	 int32_t		len;
	 int32_t		cnt;
/* bulk data is array of user-defined entries */
};

struct srm_batch_rep {
	 int32_t		rc;		/* return code, 0 for success or slerrno */
	 int32_t		opc;		/* opcode - to be used for santity check */
} __packed;

struct srm_ctl_req {
	uint32_t		opc;		/* operation */
	char			buf[128];
};

#define SRM_CTLOP_SETOPT	0

#define srm_ctl_rep		srm_generic_rep

struct srt_ctlsetopt {
	uint32_t		opt;		/* option */
	 int32_t		_pad;
	uint64_t		opv;		/* value */
};

#define SRMCTL_OPT_HEALTH	0

/* ---------------------- BEGIN ENCAPSULATED MESSAGES ----------------------- */

/*
 * Note: these messages are contained within other messages and thus must
 * end on 64-bit boundaries.
 */

#define AUTHBUF_MAGIC		UINT64_C(0x4321432143214321)

struct srt_authbuf_secret {
	uint64_t		sas_magic;
	uint64_t		sas_nonce;
	uint64_t		sas_dst_nid;
	uint64_t		sas_src_nid;
	uint32_t		sas_dst_pid;
	uint32_t		sas_src_pid;
} __packed;

/* this is appended after every RPC message */
struct srt_authbuf_footer {
	struct srt_authbuf_secret saf_secret;
	char			saf_hash[AUTHBUF_ALGLEN];
	char			saf_bulkhash[AUTHBUF_ALGLEN];
} __packed;

struct srt_bmapdesc {
	struct sl_fidgen	sbd_fg;
	uint64_t		sbd_seq;
	uint64_t		sbd_key;

	uint64_t		sbd_nid;	/* XXX go away */
	uint32_t		sbd_pid;	/* XXX go away */

	sl_ios_id_t		sbd_ios;
	sl_bmapno_t		sbd_bmapno;
	uint32_t		sbd_flags;	/* SRM_LEASEBMAPF_DIO, etc. */
} __packed;

/* RPC transportably safe structures. */
struct srt_stat {
	struct sl_fidgen	sst_fg;		/* file ID + truncate generation */
	uint64_t		sst_dev;	/* ID of device containing file */
	uint32_t		sst_ptruncgen;	/* partial truncate generation */
	uint32_t		sst_utimgen;	/* utimes generation number */
	uint32_t		sst__pad;
	uint32_t		sst_mode;	/* file type & permissions (e.g., S_IFREG, S_IRWXU) */
	uint64_t		sst_nlink;	/* number of hard links */
	uint32_t		sst_uid;	/* user ID of owner */
	uint32_t		sst_gid;	/* group ID of owner */
	uint64_t		sst_rdev;	/* device ID (if special file) */
	uint64_t		sst_size;	/* total size, in bytes */
	uint64_t		sst_blksize;	/* blocksize for file system I/O */
	uint64_t		sst_blocks;	/* number of 512B blocks allocated */
	struct pfl_timespec	sst_atim;	/* time of last access */
	struct pfl_timespec	sst_mtim;	/* time of last modification */
	struct pfl_timespec	sst_ctim;	/* time of creation */
#define sst_fid		sst_fg.fg_fid
#define sst_gen		sst_fg.fg_gen
#define sst_atime	sst_atim.tv_sec
#define sst_atime_ns	sst_atim.tv_nsec
#define sst_mtime	sst_mtim.tv_sec
#define sst_mtime_ns	sst_mtim.tv_nsec
#define sst_ctime	sst_ctim.tv_sec
#define sst_ctime_ns	sst_ctim.tv_nsec
} __packed;

#define DEBUG_SSTB(level, sstb, fmt, ...)				\
	psclog((level), "sstb@%p fg:"SLPRI_FG" "			\
	    "dev:%"PRIu64" mode:%#o "					\
	    "nlink:%"PRIu64" uid:%u gid:%u "				\
	    "rdev:%"PRIu64" sz:%"PRIu64" "				\
	    "blksz:%"PRIu64" blkcnt:%"PRIu64" "				\
	    "atime:%"PRIu64":%"PRIu64" "				\
	    "mtime:%"PRIu64":%"PRIu64" "				\
	    "ctime:%"PRIu64":%"PRIu64" " fmt,				\
	    (sstb), SLPRI_FG_ARGS(&(sstb)->sst_fg),			\
	    (sstb)->sst_dev, (sstb)->sst_mode,				\
	    (sstb)->sst_nlink, (sstb)->sst_uid, (sstb)->sst_gid,	\
	    (sstb)->sst_rdev, (sstb)->sst_size,				\
	    (sstb)->sst_blksize, (sstb)->sst_blocks,			\
	    (sstb)->sst_atime, (sstb)->sst_atime_ns,			\
	    (sstb)->sst_mtime, (sstb)->sst_mtime_ns,			\
	    (sstb)->sst_ctime, (sstb)->sst_ctime_ns, ## __VA_ARGS__)

/* copy an sstb without overwriting the FID field */
#define COPY_SSTB(src, dst)						\
	do {								\
		size_t _n;						\
									\
		if ((dst)->sst_fid != (src)->sst_fid)			\
			(dst)->sst_fid = (src)->sst_fid;		\
		_n = sizeof((dst)->sst_fid);				\
		memcpy((char *)(dst) + _n, (char *)(src) + _n,		\
		    sizeof(*(dst)) - _n);				\
	} while (0)

#define BW_UNITSZ		1024*1024		/* denomination of bw */

struct srt_bwqueued {
	 int32_t		 sbq_ingress;
	 int32_t		 sbq_egress;
	 int32_t		 sbq_aggr;
	 int32_t		_sbq_pad;
} __packed;

struct srt_statfs {
	char			sf_type[16];
	uint32_t		sf_bsize;	/* file system block size */
	uint32_t		sf_frsize;	/* fragment size */
	uint64_t		sf_blocks;	/* size of fs in f_frsize units */
	uint64_t		sf_bfree;	/* # free blocks */
	uint64_t		sf_bavail;	/* # free blocks for non-root */
	uint64_t		sf_files;	/* # inodes */
	uint64_t		sf_ffree;	/* # free inodes */
	uint64_t		sf_favail;	/* # free inodes for non-root */
} __packed;

#define DEBUG_SSTATFS(level, s, fmt, ...)				\
	psclog((level), "sstatfs@%p type=%s "				\
	    "bsz=%u iosz=%u nblks=%"PRIu64" bfree=%"PRIu64" "		\
	    "bavail=%"PRIu64" " fmt,					\
	    (s), (s)->sf_type, (s)->sf_bsize, (s)->sf_iosize,		\
	    (s)->sf_blocks, (s)->sf_bfree, (s)->sf_bavail,		\
	    ## __VA_ARGS__)

struct srt_bmapminseq {
	uint64_t		bminseq;
} __packed;

struct srt_creds {
	uint32_t		scr_uid;
	uint32_t		scr_gid;
} __packed;

struct srt_inode {
	uint32_t		flags;
	uint16_t		newreplpol;
	uint16_t		nrepls;
	sl_replica_t		reptbl[SL_MAX_REPLICAS];
};

/* ------------------------ BEGIN NAMESPACE MESSAGES ------------------------ */

/* namespace update */
struct srm_update_req {
	uint64_t		seqno;
	 int32_t		size;		/* size of the bulk data to follow */
	 int16_t		count;		/* # of entries to follow */
	 int16_t		siteid;		/* site ID for tracking purpose */
/* followed by bulk data of srt_update_entry structures */
} __packed;

struct srm_update_rep {
	 int32_t		rc;
	 int32_t		_pad;
	uint64_t		seqno;		/* the last seqno I have received from you */
} __packed;

struct srt_update_entry {
	uint64_t		xid;
	uint8_t			op;		/* operation type (i.e. enum namespace_operation) */
	uint8_t			namelen;	/* NUL not transmitted */
	uint8_t			namelen2;	/* NUL not transmitted */
	 int8_t			_pad;

	uint64_t		parent_fid;	/* parent dir FID */
	uint64_t		target_fid;

	uint64_t		target_gen;	/* reclaim only */
	uint64_t		new_parent_fid;	/* rename only */

	uint32_t		mask;		/* attribute mask */

	uint32_t		mode;		/* file permission */
	 int32_t		uid;		/* user ID of owner */
	 int32_t		gid;		/* group ID of owner */
	uint64_t		atime;		/* time of last access */
	uint64_t		atime_ns;
	uint64_t		mtime;		/* time of last modification */
	uint64_t		mtime_ns;
	uint64_t		ctime;		/* time of last status change */
	uint64_t		ctime_ns;

	uint64_t		size;		/* file size */
	char			name[396];	/* one or two names */
/* if len1 + len2 > sizeof(name), names are in bulk */
} __packed;

#define UPDATE_ENTRY_LEN(e)						\
	(offsetof(typeof(*(e)), name) + (e)->namelen + (e)->namelen2)

/* forward a namespace operation */
struct srm_forward_req {
	 int32_t		op;		/* create, mkdir, unlink, rmdir, setattr */
	uint32_t		mode;		/* mkdir/creat mode */
	 int32_t		to_set;		/* SETATTR field flags */
	 int32_t		_pad;
	struct srt_creds	creds;		/* st_uid owner for new dir/file */
	struct sl_fidgen	fg;		/* parent dir or target */
	struct sl_fidgen	nfg;		/* new parent dir or target */
	union {
		struct srt_stat	sstb;
		char		name[PSC_ALIGN(SL_TWO_NAME_MAX + 2, 8)];
	} req;
} __packed;

struct srm_forward_rep {
	struct srt_stat		attr;		/* target/child attributes, include new FID */
	 int32_t		rc;		/* return code, 0 for success or slerrno */
	 int32_t		_pad;
} __packed;

/* -------------------------- BEGIN BMAP MESSAGES --------------------------- */

#define NPREFIOS		1

struct srm_leasebmap_req {
	struct sl_fidgen	fg;
	sl_ios_id_t		prefios[NPREFIOS];/* client's preferred IOS ID */
	sl_bmapno_t		bmapno;		/* Starting bmap index number */
	 int32_t		rw;		/* 'enum rw' value for access */
	uint32_t		flags;		/* see SRM_LEASEBMAPF_* below */
} __packed;

#define SRM_LEASEBMAPF_DIO	(1 << 0)	/* reply: client can't cache */
#define SRM_LEASEBMAPF_GETINODE	(1 << 1)	/* request: fetch inode (replica table, etc.) */
#define SRM_LEASEBMAPF_DATA	(1 << 2)	/* reply: true if any crcstates has SLVR_DATA */
#define SRM_LEASEBMAPF_NODIO	(1 << 3)	/* request: do not force modechange */

struct srm_leasebmap_rep {
	struct srt_bmapdesc	sbd;		/* descriptor for bmap */
	uint8_t			repls[SL_REPLICA_NBYTES];
	 int32_t		rc;		/* 0 for success or slerrno */
	uint32_t		flags;		/* return SRM_LEASEBMAPF_* success */
	struct srt_inode	ino;		/* if SRM_LEASEBMAPF_GETINODE */
} __packed;

struct srm_leasebmapext_req {
	struct srt_bmapdesc	sbd;
} __packed;

struct srm_leasebmapext_rep {
	struct srt_bmapdesc	sbd;
	int32_t			rc;
	int32_t			_pad;
} __packed;

#define srm_leasebmapext_rep srm_leasebmap_rep

struct srm_reassignbmap_req {
	struct srt_bmapdesc	sbd;
	sl_ios_id_t		prev_sliods[SL_MAX_IOSREASSIGN];
	sl_ios_id_t		pios;
	int32_t			nreassigns;
} __packed;

#define srm_reassignbmap_rep srm_leasebmap_rep

/*
 * ION requesting CRC table from the MDS.
 */
struct srm_getbmap_full_req {
	struct sl_fidgen	fg;
	sl_bmapno_t		bmapno;
	 int32_t		rw;
} __packed;

struct srm_getbmap_full_rep {
	uint64_t		crcs[SLASH_SLVRS_PER_BMAP];
	uint8_t			crcstates[SLASH_SLVRS_PER_BMAP];
	uint64_t		minseq;
	 int32_t		rc;
	 int32_t		_pad;
} __packed;

struct srm_bmap_chwrmode_req {
	struct srt_bmapdesc	sbd;
	sl_ios_id_t		prefios[NPREFIOS];	/* preferred I/O system ID (if WRITE) */
	 int32_t		_pad;
} __packed;

struct srm_bmap_chwrmode_rep {
	struct srt_bmapdesc	sbd;
	 int32_t		rc;
	 int32_t		_pad;
} __packed;

struct srm_bmap_dio_req {
	uint64_t		fid;
	uint64_t		seq;		/* bmap sequence # */
	uint32_t		bno;		/* bmap number */
	uint32_t		dio;		/* XXX should be a flag */
	uint32_t		mode;		/* read or write XXX unused */
	 int32_t		_pad;
} __packed;

#define srm_bmap_dio_rep	srm_generic_rep

struct srm_delete_req {
	struct sl_fidgen	fg;
	uint32_t		flag;		/* unused for now */
	 int32_t		_pad;
} __packed;

#define srm_delete_rep		srm_generic_rep

struct srt_update_rec {
	struct sl_fidgen	fg;
	uint64_t		nblks;
} __packed;

/*
 * I tried 32 and the RPC does not go through.  See SLM_RMI_BUFSZ.
 */
#define	MAX_FILE_UPDATES	16

struct srm_updatefile_req {
	uint16_t		count;
	uint16_t		flags;
	struct srt_update_rec	updates[MAX_FILE_UPDATES];
} __packed;

#define srm_updatefile_rep	srm_generic_rep

struct srm_bmap_iod_get {
	uint64_t		fid;
	uint32_t		bno;
	 int32_t		_pad;
} __packed;

#define MAX_BMAP_RELEASE	5
struct srm_bmap_release_req {
	struct srt_bmapdesc	sbd[MAX_BMAP_RELEASE];
	uint32_t		nbmaps;
	 int32_t		_pad;
} __packed;

struct srm_bmap_release_rep {
	 int32_t		rc;
	 int32_t		_pad;
} __packed;

struct srm_getbmapminseq_req {			/* XXX use ping */
} __packed;

struct srm_getbmapminseq_rep {
	int32_t			 rc;
	int32_t			_pad;
	uint64_t		 seqno;
} __packed;

struct srt_ptrunc_req {
	struct sl_fidgen	fg;
	sl_bmapno_t		bmapno;
	sl_bmapgen_t		bgen;
	 int32_t		offset;		/* relative to in this bmap */
	 int32_t		rc;
} __packed;

struct srt_ptrunc_rep {
	int32_t			rc;
	int32_t			_pad;
} __packed;

struct srm_bmap_wake_req {
	struct sl_fidgen	fg;
	sl_bmapno_t		bmapno;
	 int32_t		_pad;
} __packed;

#define srm_bmap_wake_rep	srm_generic_rep

/* ------------------------- BEGIN GARBAGE MESSAGES ------------------------- */

struct srt_preclaim_req {
	uint64_t		xid;
	struct sl_fidgen	fg;
	sl_bmapno_t		bno;
	sl_bmapgen_t		bgen;
} __packed;

struct srt_preclaim_rep {
	int32_t			rc;
	int32_t			_pad;
} __packed;

struct srm_reclaim_req {
	uint64_t		batchno;
	uint64_t		xid;
	 int32_t		size;		/* size of the bulk data to follow */
	 int16_t		count;		/* # of entries to follow */
	 int16_t		ios;		/* ID of the IOS */
/* bulk data is array of struct srt_reclaim_entry */
} __packed;

struct srm_reclaim_rep {
	 int32_t		rc;		/* return code, 0 for success or slerrno */
	 int32_t		_pad;
	uint64_t		seqno;		/* last seqno received from peer */
} __packed;

#define RECLAIM_MAGIC_VER	UINT64_C(0x0000000000000001)
#define RECLAIM_MAGIC_FID	UINT64_C(0xffffffffffffffff)
#define RECLAIM_MAGIC_GEN	UINT64_C(0xabcdefabcdef5678)

struct srt_reclaim_entry {
	uint64_t		xid;
	struct sl_fidgen	fg;
} __packed;

/* ------------------------- BEGIN CONTROL MESSAGES ------------------------- */

struct srm_connect_req {			/* 40 bytes in all */
	uint64_t		magic;		/* e.g., SRCM_MAGIC */
	uint32_t		version;
	 int32_t		nnids;
	uint64_t		fsuuid;		/* file system unique ID */
	uint64_t		uptime;
	uint32_t		stkvers;	/* software stack version */
	uint32_t		upnonce;	/* uptime instance */
} __packed;

struct srm_connect_rep {
	uint64_t		fsuuid;
	uint64_t		uptime;
	 int32_t		stkvers;
	 int32_t		rc;
} __packed;

struct srm_ping_req {
	 int32_t		rc;
	uint32_t		upnonce;	/* system uptime nonce to detect reboots */
} __packed;

#define srm_ping_rep		srm_ping_req

/* ----------------------- BEGIN REPLICATION MESSAGES ----------------------- */

/* for a REPL_GETST about a replication request */
struct srm_replst_master_req {
	struct sl_fidgen	fg;
	sl_replica_t		repls[SL_MAX_REPLICAS];
	 int32_t		id;		/* user-provided passback value */
	 int32_t		rc;		/* or EOF */
	uint32_t		newreplpol;	/* default replication policy for new data */
	uint32_t		nrepls;		/* # of I/O systems in 'repls' */
	unsigned char		data[48];	/* slave data here if it fits, really used? */
} __packed;

#define srm_replst_master_rep	srm_replst_master_req

/*
 * bmap data carrier for a REPL_GETST when data is larger than can
 * fit in srm_replst_master_req.data
 */
struct srm_replst_slave_req {
	struct sl_fidgen	fg;
	 int32_t		id;		/* user-provided passback value */
	 int32_t		len;		/* of bulk data */
	 int32_t		rc;
	uint32_t		nbmaps;		/* # of bmaps in this chunk */
	sl_bmapno_t		boff;		/* offset into inode of first bmap in bulk */
	 int32_t		_pad;
	char			buf[296];
/* bulk data is sections of (srt_replst_bhdr,bcs_repls) data */
} __packed;

/* per-bmap header, prepended before each bcs_repls content */
struct srt_replst_bhdr {
	uint32_t		srsb_replpol:1;
	uint32_t		srsb_usr_pri:31;
	uint32_t		srsb_sys_pri;
} __packed;

#define SL_NBITS_REPLST_BHDR	8

#define SRM_REPLST_PAGESIZ	LNET_MTU

#define srm_replst_slave_rep	srm_replst_slave_req

struct srt_replwk_req {				/* batch arrangement request MDS -> IOS */
	struct sl_fidgen	fg;
	sl_bmapno_t		bno;
	sl_bmapgen_t		bgen;
	sl_ios_id_t		src_resid;
	 int32_t		_pad;
	uint32_t		len;
} __packed;

struct srt_replwk_rep {				/* batch arrangement success/failure IOS -> MDS */
	int32_t			rc;
	int32_t			_pad;
} __packed;

struct srm_repl_read_req {			/* data pull IOS -> IOS */
	struct sl_fidgen	fg;
	uint32_t		len;		/* #bytes in this message, to find #slivers */
	sl_bmapno_t		bmapno;
	 int32_t		slvrno;
	 int32_t		rc;
} __packed;

#define srm_repl_read_rep	srm_io_rep

struct srm_set_fattr_req {			/* set non-POSIX file attribute CLI -> MDS */
	struct sl_fidgen	fg;
	 int32_t		attrid;
	 int32_t		val;
} __packed;

#define SL_FATTR_IOS_AFFINITY	0
#define SL_FATTR_REPLPOL	1

#define srm_set_fattr_rep	srm_generic_rep

struct srm_set_bmapreplpol_req {
	struct sl_fidgen	fg;
	sl_bmapno_t		bmapno;
	sl_bmapno_t		nbmaps;
	 int32_t		pol;
	 int32_t		_pad;
} __packed;

#define srm_set_bmapreplpol_rep	srm_generic_rep

struct srm_get_inode_req {
	struct sl_fidgen	fg;
};

struct srm_get_inode_rep {
	struct srt_inode	ino;
	int32_t			rc;
	int32_t			_pad;
};

/* ----------------------- BEGIN FILE SYSTEM MESSAGES ----------------------- */

struct srm_create_req {
	struct sl_fidgen	pfg;		/* parent dir's file ID + generation */
	struct pfl_timespec	time;		/* time of request */
	struct srt_creds	owner;		/* st_uid owner for new file */
	char			name[SL_NAME_MAX + 1];
	uint32_t		mode;		/* mode_t permission for new file */

	/* parameters for fetching first bmap */
	sl_ios_id_t		prefios[NPREFIOS];/* preferred I/O system ID */
	uint32_t		flags;		/* see SRM_BMAPF_* flags */
	 int32_t		_pad;
} __packed;

struct srm_create_rep {
	struct srt_stat		cattr;		/* attrs of new file */
	struct srt_stat		pattr;		/* parent dir attributes */
	 int32_t		rc;		/* 0 for success or slerrno */
	 int32_t		_pad;

	/* parameters for fetching first bmap */
	uint32_t		rc2;		/* (for GETBMAP) 0 or slerrno */
	uint32_t		flags;		/* see SRM_BMAPF_* flags */
	struct srt_bmapdesc	sbd;
} __packed;

struct srm_getattr_req {
	struct sl_fidgen	fg;
	sl_ios_id_t		iosid;
	 int32_t		_pad;
} __packed;

struct srm_getattr_rep {
	struct srt_stat		attr;
	 uint32_t		xattrsize;
	 int32_t		rc;
} __packed;

struct srm_getattr2_rep {
	struct srt_stat		cattr;		/* child node */
	struct srt_stat		pattr;		/* parent dir */
	 int32_t		rc;
	 int32_t		_pad;
} __packed;

struct srm_io_req {
	struct srt_bmapdesc	sbd;		/* bmap descriptor */
	uint32_t		ptruncgen;	/* partial trunc gen # */
	uint32_t		utimgen;	/* utimes(2) generation # */
	uint32_t		flags:31;	/* see SRM_IOF_* */
	uint32_t		op:1;		/* read/write */
	uint32_t		size;		/* I/O length; always < LNET_MTU */
	uint32_t		offset;		/* relative within bmap */
	 int32_t		rc;		/* async I/O return code */
	uint64_t		id;		/* async I/O identifier */
/* WRITE data is bulk request. */
} __packed;

/* I/O operations */
#define SRMIOP_RD		0
#define SRMIOP_WR		1

/* I/O flags */
#define SRM_IOF_APPEND		(1 << 0)	/* ignore offset; position WRITE at EOF */
#define SRM_IOF_DIO		(1 << 1)	/* direct I/O; no caching */
#define SRM_IOF_BENCH		(1 << 2)	/* for benchmarking only; junk data */

struct srm_io_rep {
	uint64_t		id;		/* async I/O identifier */
	 int32_t		rc;
	uint32_t		size;
/* READ data is in bulk reply. */
} __packed;

struct srm_link_req {
	struct sl_fidgen	pfg;		/* parent dir */
	struct sl_fidgen	fg;
	char			name[SL_NAME_MAX + 1];
} __packed;

#define srm_link_rep		srm_getattr2_rep

struct srm_lookup_req {
	struct sl_fidgen	pfg;		/* parent dir */
	char			name[SL_NAME_MAX + 1];
} __packed;

#define srm_lookup_rep		srm_getattr_rep

struct srm_mkdir_req {
	struct sl_fidgen	pfg;		/* parent dir */
	char			name[SL_NAME_MAX + 1];
	struct srt_stat		sstb;		/* owner/etc. */
	uint32_t		to_set;
	 int32_t		_pad;
} __packed;

#define srm_mkdir_rep		srm_getattr2_rep

struct srm_mknod_req {
	struct srt_creds	creds;		/* st_uid owner for new file */
	char			name[SL_NAME_MAX + 1];
	struct sl_fidgen	pfg;		/* parent dir */
	uint32_t		mode;
	uint32_t		rdev;
} __packed;

#define srm_mknod_rep		srm_getattr2_rep

struct srt_readdir_ent {  // XXX rename to srt_readdir_stpref
	struct srt_stat		sstb;
	uint32_t		xattrsize;
	 int32_t		_pad;
} __packed;

#define SRM_READDIR_BUFSZ(siz, nents)					\
	(sizeof(struct srt_readdir_ent) * (nents) + (siz))

struct srm_readdir_req {
	struct sl_fidgen	fg;
	 int64_t		offset;
	uint32_t		size;
	uint32_t		flags;
} __packed;

struct srm_readdir_rep {
	uint64_t		size;		/* sum of dirents, XXX make 32-bit */
	uint32_t		eof:1;		/* flag: directory read EOF */
	uint32_t		nents:31;	/* #dirents returned */
	 int32_t		rc;
	unsigned char		ents[824];
/* XXX ents should be in a portable format, not fuse_dirent */
/* XXX ents is (fuse_dirent * N, 64-bit align, srt_readdir_ent * N) */
} __packed;

struct srm_readlink_req {
	struct sl_fidgen	fg;
} __packed;

struct srm_readlink_rep {
	 int32_t		rc;
	uint16_t		len;
	uint16_t		flag;
	unsigned char		buf[832];
/* buf is in bulk */
} __packed;

struct srm_rename_req {
	struct sl_fidgen	npfg;		/* new parent dir */
	struct sl_fidgen	opfg;		/* old parent dir */
	uint32_t		fromlen;	/* NUL not transmitted */
	uint32_t		tolen;		/* NUL not transmitted */
#define SRM_RENAME_NAMEMAX	448
	char			buf[SRM_RENAME_NAMEMAX];
/*
 * 'from' and 'to' component names are in bulk data without terminating
 * NULs, unless they are small and can fit directly in the RPC.
 */
} __packed;

struct srm_rename_rep {
	struct srt_stat		srr_npattr;	/* new parent */
	struct srt_stat		srr_opattr;	/* old parent */
	struct srt_stat		srr_cattr;	/* child node */
	struct srt_stat		srr_clattr;	/* clobbered node */
	 int32_t		rc;
	 int32_t		_pad;
} __packed;

struct srm_replrq_req {
	struct sl_fidgen	fg;
	sl_replica_t		repls[SL_MAX_REPLICAS];		/* target IOS(es) */
	uint32_t		nrepls;
	uint32_t		usr_prio;	/* priority */
	uint32_t		sys_prio;
	sl_bmapno_t		bmapno;		/* the first bmap */
	sl_bmapno_t		nbmaps;		/* length or -1 for all */
	 int32_t		_pad;
} __packed;

struct srm_replrq_rep {
	 int32_t		rc;
	 sl_bmapno_t		nbmaps_processed;
} __packed;

struct srm_setattr_req {
	struct srt_stat		attr;
	 int32_t		to_set;		/* see SETATTR_MASKF_* */
	 int32_t		_pad;
} __packed;

#define srm_setattr_rep		srm_getattr_rep

struct srm_statfs_req {
	slfid_t			fid;
	sl_ios_id_t		iosid;
	 int32_t		_pad;
} __packed;

struct srm_statfs_rep {
	struct srt_statfs	ssfb;
	 int32_t		rc;
	 int32_t		_pad;
} __packed;

struct srm_symlink_req {
	struct srt_stat		sstb;		/* owner/etc. */
	struct sl_fidgen	pfg;		/* parent dir */
	char			name[SL_NAME_MAX + 1];
	uint32_t		linklen;	/* NUL not transmitted */
	 int32_t		_pad;
/* link path name is in bulk */
} __packed;

#define srm_symlink_rep		srm_getattr2_rep

struct srm_unlink_req {
	slfid_t			pfid;		/* parent dir */
	char			name[SL_NAME_MAX + 1];
} __packed;

struct srm_unlink_rep {
	struct srt_stat		cattr;		/* child node - FID always valid */
	struct srt_stat		pattr;		/* parent dir */
	 int32_t		valid;		/* child attr valid */
	 int32_t		rc;
} __packed;

struct srm_listxattr_req {
	struct sl_fidgen	fg;
	uint32_t		size;
	 int32_t		_pad;
} __packed;

struct srm_listxattr_rep {
	uint32_t		size;
	 int32_t		rc;
	unsigned char		buf[832];
/* in buf if it can fit; otherwise in BULK */
} __packed;

struct srm_getxattr_req {
	struct sl_fidgen	fg;
	char			name[SL_NAME_MAX + 1];
	uint32_t		size;
	 int32_t		_pad;
} __packed;

struct srm_getxattr_rep {
	 int32_t		rc;
	uint32_t		valuelen;
	unsigned char		buf[832];
/* in buf if it can fit; otherwise in BULK */
} __packed;

struct srm_setxattr_req {
	struct sl_fidgen	fg;
	char			name[SL_NAME_MAX + 1];
	 int32_t		_pad;
	uint32_t		valuelen;
/* XXX BULK CLI -> MDS is value */
} __packed;

struct srm_setxattr_rep {
	 int32_t		rc;
	 int32_t		_pad;
} __packed;

struct srm_removexattr_req {
	struct sl_fidgen	fg;
	char			name[SL_NAME_MAX + 1];
} __packed;

struct srm_removexattr_rep {
	 int32_t		rc;
	 int32_t		_pad;
} __packed;

/* ---------------------- BEGIN IMPORT/EXPORT MESSAGES ---------------------- */

struct srm_import_req {
	struct sl_fidgen	pfg;		/* destination parent dir */
	char			cpn[SL_NAME_MAX + 1];
	struct srt_stat		sstb;
	 int32_t		flags;
	 int32_t		_pad;
} __packed;

#define SRM_IMPORTF_XREPL	(1 << 0)	/* register additional replica */

struct srm_import_rep {
	struct sl_fidgen	fg;
	 int32_t		rc;
	 int32_t		_pad;
} __packed;

#endif /* _SLASHRPC_H_ */
