/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2011, Pittsburgh Supercomputing Center (PSC).
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
 * SLASH remote procedure call message (SRM) definitions, for issuing
 * operations on and communicating with other hosts in a SLASH network.
 */

#ifndef _SLASHRPC_H_
#define _SLASHRPC_H_

#include "pfl/cdefs.h"

#include "authbuf.h"
#include "bmap.h"
#include "cache_params.h"
#include "creds.h"
#include "fid.h"
#include "sltypes.h"

struct stat;
struct statvfs;

/* SLASH RPC channel to MDS from CLI. */
#define SRMC_REQ_PORTAL		10
#define SRMC_REP_PORTAL		11
#define SRMC_BULK_PORTAL	12
#define SRMC_CTL_PORTAL		13

#define SRMC_VERSION		1
#define SRMC_MAGIC		UINT64_C(0xaabbccddeeff0022)

/* SLASH RPC channel to MDS from MDS. */
#define SRMM_REQ_PORTAL		15
#define SRMM_REP_PORTAL		16
#define SRMM_BULK_PORTAL	17
#define SRMM_CTL_PORTAL		18

#define SRMM_VERSION		1
#define SRMM_MAGIC		UINT64_C(0xaabbccddeeff0033)

/* SLASH RPC channel to MDS from ION. */
#define SRMI_REQ_PORTAL		20
#define SRMI_REP_PORTAL		21
#define SRMI_BULK_PORTAL	22
#define SRMI_CTL_PORTAL		23

#define SRMI_VERSION		1
#define SRMI_MAGIC		UINT64_C(0xaabbccddeeff0044)

/* SLASH RPC channel to CLI from MDS. */
#define SRCM_REQ_PORTAL		25
#define SRCM_REP_PORTAL		26
#define SRCM_BULK_PORTAL	27
#define SRCM_CTL_PORTAL		28

#define SRCM_VERSION		1
#define SRCM_MAGIC		UINT64_C(0xaabbccddeeff0055)

/* SLASH RPC channel to ION from CLI. */
#define SRIC_REQ_PORTAL		30
#define SRIC_REP_PORTAL		31
#define SRIC_BULK_PORTAL	32
#define SRIC_CTL_PORTAL		33

#define SRIC_VERSION		1
#define SRIC_MAGIC		UINT64_C(0xaabbccddeeff0066)

/* SLASH RPC channel to ION from ION. */
#define SRII_REQ_PORTAL		35
#define SRII_REP_PORTAL		36
#define SRII_BULK_PORTAL	37
#define SRII_CTL_PORTAL		38

#define SRII_VERSION		1
#define SRII_MAGIC		UINT64_C(0xaabbccddeeff0077)

/* SLASH RPC channel to ION from MDS. */
#define SRIM_REQ_PORTAL		40
#define SRIM_REP_PORTAL		41
#define SRIM_BULK_PORTAL	42
#define SRIM_CTL_PORTAL		43

#define SRIM_VERSION		1
#define SRIM_MAGIC		UINT64_C(0xaabbccddeeff0088)

/* SLASH RPC channel to CLI from ION. */
#define SRCI_REQ_PORTAL		45
#define SRCI_REP_PORTAL		46
#define SRCI_BULK_PORTAL	47
#define SRCI_CTL_PORTAL		48

#define SRCI_VERSION		1
#define SRCI_MAGIC		UINT64_C(0xaabbccddeeff0099)

enum {
	SLXCTLOP_SET_BMAP_REPLPOL,		/* set replication policy on bmap */
	SLXCTLOP_SET_FILE_NEWBMAP_REPLPOL	/* set file's default new bmap repl policy */
};

/* SLASH RPC message types. */
enum {
	/* control operations */
	SRMT_CONNECT = 1,
	SRMT_DESTROY,
	SRMT_PING,

	/* namespace operations */
	SRMT_NAMESPACE_UPDATE,			/* send a batch of namespace update logs */

	/* bmap operations */
	SRMT_BMAPCHWRMODE,			/* change read/write access mode */
	SRMT_BMAPCRCWRT,			/* update bmap data checksums */
	SRMT_BMAPDIO,				/* request client direct I/O on a bmap */
	SRMT_BMAP_PTRUNC,			/* partial truncate and redo CRC for bmap */
	SRMT_BMAP_WAKE,				/* client work may now progress after EAGAIN */
	SRMT_GETBMAP,				/* get client lease for bmap access */
	SRMT_GETBMAPCRCS,			/* get bmap data checksums */
	SRMT_GETBMAPMINSEQ,
	SRMT_RELEASEBMAP,			/* relinquish a client's bmap access lease */

	/* garbage operations */
	SRMT_RECLAIM,				/* trash storage space for a given FID+GEN */

	/* replication operations */
	SRMT_REPL_ADDRQ,
	SRMT_REPL_DELRQ,
	SRMT_REPL_GETST,			/* get replication request status */
	SRMT_REPL_GETST_SLAVE,			/* all bmap repl info for a file */
	SRMT_REPL_READ,				/* ION to ION replicate */
	SRMT_REPL_SCHEDWK,			/* MDS to ION replication staging */
	SRMT_SET_BMAPREPLPOL,			/* bmap replication policy */
	SRMT_SET_NEWREPLPOL,			/* file new bmap repl policy */

	/* file system operations */
	SRMT_CREATE,				/* creat(2) */
	SRMT_GETATTR,				/* stat(2) */
	SRMT_LINK,				/* link(2) */
	SRMT_LOOKUP,
	SRMT_MKDIR,				/* mkdir(2) */
	SRMT_MKNOD,				/* mknod(2) */
	SRMT_READ,				/* read(2) */
	SRMT_READDIR,				/* readdir(2) */
	SRMT_READLINK,				/* readlink(2) */
	SRMT_RENAME,				/* rename(2) */
	SRMT_RMDIR,				/* rmdir(2) */
	SRMT_SETATTR,				/* chmod(2), chown(2), utimes(2) */
	SRMT_STATFS,				/* statvfs(2) */
	SRMT_SYMLINK,				/* symlink(2) */
	SRMT_UNLINK,				/* unlink(2) */
	SRMT_WRITE,				/* write(2) */
	SRMT_XCTL				/* ancillary operation */

	,SRMT_NAMESPACE_FORWARD			/* a namespace operation request from a peer  */
};

/* ----------------------------- BEGIN MESSAGES ----------------------------- */

/*
 * Note: Member ordering within structures must always follow 64-bit boundaries
 * to preserve compatibility between 32-bit and 64-bit machines.
 */

struct srm_generic_rep {
	 int32_t		rc;		/* return code, 0 for success or slerrno */
	 int32_t		_pad;
} __packed;

/* ---------------------- BEGIN ENCAPSULATED MESSAGES ----------------------- */

/*
 * Note: these messages are contained within other messages and thus must
 * end on 64-bit boundaries.
 */

#define AUTHBUF_REPRLEN		45		/* strlen(base64(SHA256(secret)) + NUL */
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
	char			saf_hash[AUTHBUF_REPRLEN];
	char			saf__pad[3];
} __packed;

#define SRM_NIDS_MAX		8

struct srt_bmapdesc {
	struct slash_fidgen	sbd_fg;
	uint64_t		sbd_seq;
	uint64_t		sbd_key;
	uint64_t		sbd_ion_nid;	/* owning I/O node if write */
	sl_ios_id_t		sbd_ios_id;
	sl_bmapno_t		sbd_bmapno;
	uint32_t		sbd_flags;
	 int32_t		sbd__pad;
} __packed;

/* SLASH RPC transportably safe structures. */
struct srt_stat {
	struct slash_fidgen	sst_fg;		/* file ID + truncate generation */
	uint64_t		sst_dev;	/* ID of device containing file */
	uint32_t		sst_ptruncgen;	/* partial truncate generation */
	uint32_t		sst_utimgen;    /* utimes generation number */
	uint32_t		sst__pad;
	uint32_t		sst_mode;	/* file type & permissions (e.g., S_IFREG, S_IRWXU) */
	uint64_t		sst_nlink;	/* number of hard links */
	uint32_t		sst_uid;	/* user ID of owner */
	uint32_t		sst_gid;	/* group ID of owner */
	uint64_t		sst_rdev;	/* device ID (if special file) */
	uint64_t		sst_size;	/* total size, in bytes */
	uint64_t		sst_blksize;	/* blocksize for file system I/O */
	uint64_t		sst_blocks;	/* number of 512B blocks allocated */
	struct sl_timespec	sst_atim;	/* time of last access */
	struct sl_timespec	sst_mtim;	/* time of last modification */
	struct sl_timespec	sst_ctim;	/* time of creation */
#define sst_fid		sst_fg.fg_fid
#define sst_gen		sst_fg.fg_gen
#define sst_atime	sst_atim.tv_sec
#define sst_atime_ns	sst_atim.tv_nsec
#define sst_mtime	sst_mtim.tv_sec
#define sst_mtime_ns	sst_mtim.tv_nsec
#define sst_ctime	sst_ctim.tv_sec
#define sst_ctime_ns	sst_ctim.tv_nsec

/* reappropriated fields specific to directories */
#define sstd_freplpol	sst_ptruncgen		/* new file repl pol for dirs */
} __packed;

#define DEBUG_SSTB(level, sstb, fmt, ...)					\
	psc_log((level), "sstb (%p) dev:%"PRIu64" mode:%#o nlink:%"PRIu64" "	\
	    "uid:%u gid:%u rdev:%"PRIu64" "					\
	    "sz:%"PRIu64" blksz:%"PRIu64" blkcnt:%"PRIu64" "			\
	    "atime:%"PRIu64" mtime:%"PRIu64" ctime:%"PRIu64" " fmt,		\
	    (sstb), (sstb)->sst_dev, (sstb)->sst_mode, (sstb)->sst_nlink,	\
	    (sstb)->sst_uid, (sstb)->sst_gid, (sstb)->sst_rdev,			\
	    (sstb)->sst_size, (sstb)->sst_blksize, (sstb)->sst_blocks,		\
	    (sstb)->sst_atime, (sstb)->sst_mtime, (sstb)->sst_ctime, ## __VA_ARGS__)

struct srt_statfs {
	uint64_t		sf_bsize;	/* file system block size */
	uint64_t		sf_frsize;	/* fragment size */
	uint64_t		sf_blocks;	/* size of fs in f_frsize units */
	uint64_t		sf_bfree;	/* # free blocks */
	uint64_t		sf_bavail;	/* # free blocks for non-root */
	uint64_t		sf_files;	/* # inodes */
	uint64_t		sf_ffree;	/* # free inodes */
	uint64_t		sf_favail;	/* # free inodes for non-root */
	uint64_t		sf_fsid;	/* file system ID */
	uint64_t		sf_flag;	/* mount flags */
	uint64_t		sf_namemax;	/* maximum filename length */
} __packed;

struct srt_bmapminseq {
	uint64_t		bminseq;
};

/* ------------------------ BEGIN NAMESPACE MESSAGES ------------------------ */

/* namespace update */
struct srm_update_req {
	uint64_t		seqno;
	uint64_t		crc;		/* CRC of the bulk data */
	 int32_t		size;		/* size of the bulk data to follow */
	 int16_t		count;		/* # of entries to follow */
	 int16_t		siteid;		/* Site ID for tracking purpose */
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
	uint8_t			namelen;	/* NUL not counted */
	uint8_t			namelen2;	/* NUL not counted */
	 int8_t			_pad;

	uint64_t		parent_fid;	/* parent dir FID */
	uint64_t		target_fid;

	uint64_t		target_gen;	/* reclaim only */
	uint64_t		new_parent_fid;	/* rename only  */

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
} __packed;

#define UPDATE_ENTRY_LEN(e)						\
	(offsetof(typeof(*(e)), name) + (e)->namelen + (e)->namelen2)

/* namespace forward */
struct srm_forward_req {
	 int16_t		op;		/* create, mkdir, unlink, rmdir, etc. */
	 int16_t		namelen;
	uint32_t		mode;
	struct slash_creds	creds;		/* st_uid owner for new dir/file */
	struct slash_fidgen	pfg;		/* parent dir */
	slfid_t			fid;		/* provided by the peer MDS */
} __packed;

struct srm_forward_rep {
	struct srt_stat		cattr;		/* child node */
	struct srt_stat		pattr;		/* parent dir */
	 int32_t		rc;		/* return code, 0 for success or slerrno */
	 int32_t		_pad;
} __packed;

/* -------------------------- BEGIN BMAP MESSAGES --------------------------- */

struct srm_leasebmap_req {
	struct slash_fidgen	fg;
	sl_ios_id_t		prefios;	/* client's preferred IOS ID */
	sl_bmapno_t		bmapno;		/* Starting bmap index number */
	 int32_t		rw;		/* 'enum rw' value for access */
	uint32_t		flags;		/* see SRM_LEASEBMAPF_* below */
} __packed;

#define SRM_LEASEBMAPF_DIRECTIO	  (1 << 0)	/* client wants direct I/O */
#define SRM_LEASEBMAPF_GETREPLTBL (1 << 1)	/* fetch inode replica table */

struct srm_leasebmap_rep {
	struct srt_bmapdesc	sbd;		/* descriptor for bmap */
	struct bmap_core_state	bcs;
	 int32_t		rc;		/* 0 for success or slerrno */
	 int32_t		_pad;
	uint32_t		flags;		/* return SRM_LEASEBMAPF_* success */

	/* fetch fcmh repl table if SRM_LEASEBMAPF_GETREPLTBL */
	uint32_t		nrepls;
	sl_replica_t		reptbl[SL_MAX_REPLICAS];
} __packed;

/*
 * ION requesting CRC table from the MDS.
 */
struct srm_getbmap_full_req {
	struct slash_fidgen	fg;
	sl_bmapno_t		bmapno;
	 int32_t		rw;
} __packed;

struct srm_getbmap_full_rep {
	struct bmap_ondisk	bod;
	uint64_t		minseq;
	 int32_t		rc;
	 int32_t		_pad;
} __packed;

struct srm_bmap_chwrmode_req {
	struct srt_bmapdesc	sbd;
	sl_ios_id_t		prefios;	/* preferred I/O system ID (if WRITE) */
	 int32_t		_pad;
} __packed;

struct srm_bmap_chwrmode_rep {
	struct srt_bmapdesc	sbd;
	 int32_t		rc;
	 int32_t		_pad;
} __packed;

struct srm_bmap_dio_req {
	uint64_t		fid;
	uint64_t		seq;
	uint32_t		blkno;
	uint32_t		dio;
	uint32_t		mode;
	 int32_t		_pad;
} __packed;

#define srm_bmap_dio_rep	srm_generic_rep

struct srm_bmap_crcwire {
	uint64_t		crc;		/* CRC of the corresponding sliver */
	uint32_t		slot;		/* sliver number in the owning bmap */
	 int32_t		_pad;
} __packed;

#define MAX_BMAP_INODE_PAIRS	24		/* ~520 bytes (max) per srm_bmap_crcup */

struct srm_bmap_crcup {
	struct slash_fidgen	fg;
	uint64_t		fsize;		/* largest known size applied in mds_bmap_crc_update() */
	uint32_t		blkno;		/* bmap block number */
	uint32_t		nups;		/* number of CRC updates */
	uint32_t		utimgen;
	 int32_t		extend;
	struct srm_bmap_crcwire	crcs[0];	/* see above, MAX_BMAP_INODE_PAIRS max */
} __packed;

#define MAX_BMAP_NCRC_UPDATES	64		/* max number of CRC updates in a batch */

struct srm_bmap_crcwrt_req {
	uint64_t		crc;		/* yes, a CRC of the CRC's */
	uint8_t			ncrcs_per_update[MAX_BMAP_NCRC_UPDATES];
	uint32_t		ncrc_updates;
	uint32_t		flags;
} __packed;

#define SRM_BMAPCRCWRT_PTRUNC	(1 << 0)	/* in response to partial trunc CRC recalc */

struct srm_bmap_crcwrt_rep {
	 int32_t		crcup_rc[MAX_BMAP_NCRC_UPDATES];
	 int32_t		rc;
	 int32_t		_pad;
} __packed;

struct srm_bmap_iod_get {
	uint64_t		fid;
	uint32_t		blkno;
	 int32_t		_pad;
} __packed;

struct srm_bmap_id {
	slfid_t			fid;
	uint64_t		key;
	uint64_t		seq;
	uint64_t		cli_nid;
	uint32_t		cli_pid;
	sl_bmapno_t		bmapno;
} __packed;

#define MAX_BMAP_RELEASE	8
struct srm_bmap_release_req {
	struct srm_bmap_id	bmaps[MAX_BMAP_RELEASE];
	uint32_t		nbmaps;
	 int32_t		_pad;
} __packed;

struct srm_bmap_release_rep {
	 int32_t		bidrc[MAX_BMAP_RELEASE];
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

struct srm_bmap_ptrunc_req {
	struct slash_fidgen	fg;
	sl_bmapno_t		bmapno;
	sl_bmapgen_t		bgen;
	 int32_t		offset;
	 int32_t		rc;
	 int32_t		crc;		/* whether to recompute CRC */
	 int32_t		_pad;
} __packed;

#define srm_bmap_ptrunc_rep	srm_generic_rep

struct srm_bmap_wake_req {
	struct slash_fidgen	fg;
	sl_bmapno_t		bmapno;
	 int32_t		_pad;
};

#define srm_bmap_wake_rep	srm_generic_rep

/* ------------------------- BEGIN GARBAGE MESSAGES ------------------------- */

struct srm_reclaim_req {
	uint64_t		xid;
	uint64_t		crc;		/* CRC of the bulk data */
	 int32_t		size;		/* size of the bulk data to follow */
	 int16_t		count;		/* # of entries to follow */
	 int16_t		ios;		/* ID of the IOS */
/* bulk data is array of struct srt_reclaim_entry */
} __packed;

struct srm_reclaim_rep {
	 int32_t		rc;		/* return code, 0 for success or slerrno */
	 int32_t		_pad;
	uint64_t		seqno;		/* the last seqno I have received from you */
} __packed;

struct srt_reclaim_entry {
	uint64_t		xid;
	struct slash_fidgen	fg;
	char			_pad[488];
} __packed;

/* ------------------------- BEGIN CONTROL MESSAGES ------------------------- */

struct srm_connect_req {
	uint64_t		magic;
	uint32_t		version;
	 int32_t		nnids;
	uint64_t		nids[SRM_NIDS_MAX];
} __packed;

#define srm_connect_rep		srm_generic_rep

struct srm_ping_req {
} __packed;

#define srm_ping_rep		srm_generic_rep

/* ----------------------- BEGIN REPLICATION MESSAGES ----------------------- */

/* for a REPL_GETST about a replication request */
struct srm_replst_master_req {
	struct slash_fidgen	fg;
	sl_replica_t		repls[SL_MAX_REPLICAS];
	 int32_t		id;		/* user-provided passback value */
	 int32_t		rc;		/* or EOF */
	uint32_t		newreplpol;	/* default replication policy */
	uint32_t		nrepls;		/* # of I/O systems in 'repls' */
	unsigned char		data[56];	/* slave data here if it fits */
} __packed;

#define srm_replst_master_rep	srm_replst_master_req

/*
 * bmap data carrier for a REPL_GETST when data is larger than can
 * fit in srm_replst_master_req.data
 */
struct srm_replst_slave_req {
	struct slash_fidgen	fg;
	 int32_t		id;		/* user-provided passback value */
	 int32_t		len;		/* of bulk data */
	 int32_t		rc;
	uint32_t		nbmaps;		/* # of bmaps in this chunk */
	sl_bmapno_t		boff;		/* offset into inode of first bmap in bulk */
	 int32_t		_pad;
/* bulk data is sections of bcs_repls data */
} __packed;

/* per-bmap header submessage, prepended before each bcs_repls content */
struct srsm_replst_bhdr {
	uint8_t			srsb_replpol;
} __packed;

#define SL_NBITS_REPLST_BHDR	8

#define SRM_REPLST_PAGESIZ	(1024 * 1024)	/* should be network MSS */

#define srm_replst_slave_rep	srm_replst_slave_req

struct srm_repl_schedwk_req {
	uint64_t		nid;		/* XXX gross, use iosid */
	struct slash_fidgen	fg;
	sl_bmapno_t		bmapno;
	sl_bmapgen_t		bgen;
	uint32_t		len;
	 int32_t		rc;
} __packed;

#define srm_repl_schedwk_rep	srm_generic_rep

struct srm_repl_read_req {
	struct slash_fidgen	fg;
	uint64_t		len;		/* #bytes in this message, to find #slivers */
	sl_bmapno_t		bmapno;
	 int32_t		slvrno;
} __packed;

#define srm_repl_read_rep	srm_io_rep

struct srm_set_newreplpol_req {
	struct slash_fidgen	fg;
	 int32_t		pol;
	 int32_t		_pad;
} __packed;

#define srm_set_newreplpol_rep	srm_generic_rep

struct srm_set_bmapreplpol_req {
	struct slash_fidgen	fg;
	sl_bmapno_t		bmapno;
	sl_bmapno_t		nbmaps;
	 int32_t		pol;
} __packed;

#define srm_set_bmapreplpol_rep	srm_generic_rep

/* ----------------------- BEGIN FILE SYSTEM MESSAGES ----------------------- */

struct srm_create_req {
	struct slash_fidgen	pfg;		/* parent dir's file ID + generation */
	struct slash_creds	creds;		/* st_uid owner for new file */
	char			name[SL_NAME_MAX + 1];
	uint32_t		mode;		/* mode_t permission for new file */

	/* parameters for fetching first bmap */
	sl_ios_id_t		prefios;	/* preferred I/O system ID */
	uint32_t		flags;		/* see SRM_BMAPF_* flags */
	 int32_t		_pad;
} __packed;

struct srm_create_rep {
	struct srt_stat		cattr;		/* attrs of new file */
	struct srt_stat		pattr;		/* parent dir attributes */
	 int32_t		rc;		/* 0 for success or slerrno */
	 int32_t		_pad;

	/* parameters for fetching first bmap */
	uint32_t		rc2;		/* (for LEASEBMAP) 0 or slerrno */
	uint32_t		flags;		/* see SRM_BMAPF_* flags */
	struct srt_bmapdesc	sbd;
} __packed;

struct srm_destroy_req {
} __packed;

struct srm_getattr_req {
	struct slash_fidgen	fg;
} __packed;

struct srm_getattr_rep {
	struct srt_stat		attr;
	 int32_t		rc;
	 int32_t		_pad;
} __packed;

struct srm_getattr2_rep {
	struct srt_stat		cattr;		/* child node */
	struct srt_stat		pattr;		/* parent dir */
	 int32_t		rc;
	 int32_t		_pad;
} __packed;

struct srm_io_req {
	struct srt_bmapdesc	sbd;
	uint32_t		ptruncgen;
	uint32_t		utimgen;
	uint32_t		flags:31;
	uint32_t		op:1;		/* read/write */
	uint32_t		size;
	uint32_t		offset;		/* relative within bmap */
	 int32_t		_pad;
/* WRITE data is bulk request. */
} __packed;

/* I/O operations */
#define SRMIOP_RD		0
#define SRMIOP_WR		1

/* I/O flags */
#define SRM_IOF_APPEND		(1 << 0)
#define SRM_IOF_DIO		(1 << 1)

struct srm_io_rep {
	 int32_t		rc;
	uint32_t		size;
/* READ data is in bulk reply. */
} __packed;

struct srm_link_req {
	struct slash_fidgen	pfg;		/* parent dir */
	struct slash_fidgen	fg;
	char			name[SL_NAME_MAX + 1];
} __packed;

#define srm_link_rep		srm_getattr2_rep

struct srm_lookup_req {
	struct slash_fidgen	pfg;		/* parent dir */
	char			name[SL_NAME_MAX + 1];
} __packed;

#define srm_lookup_rep		srm_getattr_rep

struct srm_mkdir_req {
	struct slash_creds	creds;		/* st_uid owner for new file */
	struct slash_fidgen	pfg;		/* parent dir */
	char			name[SL_NAME_MAX + 1];
	uint32_t		mode;
	 int32_t		_pad;
} __packed;

#define srm_mkdir_rep		srm_getattr2_rep

struct srm_mknod_req {
	struct slash_creds	creds;		/* st_uid owner for new file */
	char			name[SL_NAME_MAX + 1];
	struct slash_fidgen	pfg;		/* parent dir */
	uint32_t		mode;
	uint32_t		rdev;
} __packed;

#define srm_mknod_rep		srm_getattr2_rep

#define DEF_READDIR_NENTS	100
#define MAX_READDIR_NENTS	1000
#define MAX_READDIR_BUFSIZ	(sizeof(struct srt_stat) * MAX_READDIR_NENTS)

struct srm_readdir_req {
	struct slash_fidgen	fg;
	uint64_t		offset;
	uint64_t		size;		/* XXX make 32-bit */
	uint32_t		nstbpref;	/* max prefetched attributes */
	 int32_t		_pad;
} __packed;

struct srm_readdir_rep {
	uint64_t		size;		/* XXX make 32-bit */
	uint32_t		num;		/* #dirents returned */
	 int32_t		rc;
/*
 * XXX accompanied by bulk data is (but should not be) in fuse dirent format
 *	and must be 64-bit aligned.
 */
} __packed;

struct srm_readlink_req {
	struct slash_fidgen	fg;
} __packed;

struct srm_readlink_rep {
	 int32_t		rc;
	 int32_t		_pad;
/* buf is in bulk of size SL_PATH_MAX */
} __packed;

struct srm_rename_req {
	struct slash_fidgen	npfg;		/* new parent dir */
	struct slash_fidgen	opfg;		/* old parent dir */
	uint32_t		fromlen;	/* NUL not counted */
	uint32_t		tolen;		/* NUL not counted */
/* 'from' and 'to' component names are in bulk data without terminating NULs */
} __packed;

#define srm_rename_rep		srm_getattr2_rep
#define srr_npattr		cattr
#define srr_opattr		pattr

struct srm_replrq_req {
	struct slash_fidgen	fg;
	sl_replica_t		repls[SL_MAX_REPLICAS];
	uint32_t		nrepls;
	sl_bmapno_t		bmapno;		/* bmap to access or -1 for all */
} __packed;

#define srm_replrq_rep		srm_generic_rep

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
	struct slash_creds	creds;		/* st_uid owner for new file */
	struct slash_fidgen	pfg;		/* parent dir */
	char			name[SL_NAME_MAX + 1];
	uint32_t		linklen;	/* NUL not counted */
	 int32_t		_pad;
/* link path name is in bulk */
} __packed;

#define srm_symlink_rep		srm_getattr2_rep

struct srm_unlink_req {
	slfid_t			pfid;		/* parent dir */
	char			name[SL_NAME_MAX + 1];
} __packed;

#define srm_unlink_rep		srm_getattr_rep

#endif /* _SLASHRPC_H_ */
