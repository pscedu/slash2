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
 * SLASH Remote prodecure call Message (SRM) definitions.
 */

#ifndef _SLASHRPC_H_
#define _SLASHRPC_H_

#include "pfl/cdefs.h"

#include "creds.h"
#include "fid.h"
#include "sltypes.h"

struct stat;
struct statvfs;

/* Slash RPC channel to MDS from client. */
#define SRMC_REQ_PORTAL		10
#define SRMC_REP_PORTAL		11
#define SRMC_BULK_PORTAL	12

#define SRMC_VERSION		1
#define SRMC_MAGIC		UINT64_C(0xaabbccddeeff0022)

/* Slash RPC channel to MDS from MDS. */
#define SRMM_REQ_PORTAL		15
#define SRMM_REP_PORTAL		16

#define SRMM_VERSION		1
#define SRMM_MAGIC		UINT64_C(0xaabbccddeeff0033)

/* Slash RPC channel to MDS from ION. */
#define SRMI_REQ_PORTAL		20
#define SRMI_REP_PORTAL		21
#define SRMI_BULK_PORTAL	22

#define SRMI_VERSION		1
#define SRMI_MAGIC		UINT64_C(0xaabbccddeeff0044)

/* Slash RPC channel to client from MDS. */
#define SRCM_REQ_PORTAL		25
#define SRCM_REP_PORTAL		26
#define SRCM_BULK_PORTAL	27

#define SRCM_VERSION		1
#define SRCM_MAGIC		UINT64_C(0xaabbccddeeff0055)

/* Slash RPC channel to ION from client. */
#define SRIC_REQ_PORTAL		30
#define SRIC_REP_PORTAL		31
#define SRIC_BULK_PORTAL	32

#define SRIC_VERSION		1
#define SRIC_MAGIC		UINT64_C(0xaabbccddeeff0066)

/* Slash RPC channel to ION from ION. */
#define SRII_REQ_PORTAL		35
#define SRII_REP_PORTAL		36
#define SRII_BULK_PORTAL	37

#define SRII_VERSION		1
#define SRII_MAGIC		UINT64_C(0xaabbccddeeff0077)

/* Slash RPC channel to ION from MDS. */
#define SRIM_REQ_PORTAL		40
#define SRIM_REP_PORTAL		41
#define SRIM_BULK_PORTAL	42

#define SRIM_VERSION		1
#define SRIM_MAGIC		UINT64_C(0xaabbccddeeff0088)

/* Slash RPC message types. */
enum {
	/* control operations */
	SRMT_CONNECT,
	SRMT_DESTROY,
	SRMT_PING,

	/* namespace operations */
	SRMT_SEND_NAMESPACE,		/* send a batch of namespace operation logs */
	SRMT_RECV_NAMESPACE,		/* acknowledge the receipt/application of namespace operation logs */

	/* bmap operations */
	SRMT_BMAPCHMODE,
	SRMT_BMAPCRCWRT,
	SRMT_BMAPDIO,
	SRMT_GETBMAP,
	SRMT_GETBMAPMINSEQ,
	SRMT_GETBMAPCRCS,
	SRMT_RELEASEBMAP,

	/* replication operations */
	SRMT_REPL_ADDRQ,
	SRMT_REPL_DELRQ,
	SRMT_REPL_GETST,
	SRMT_REPL_GETST_SLAVE,
	SRMT_REPL_READ,
	SRMT_REPL_SCHEDWK,
	SRMT_SET_BMAPREPLPOL,
	SRMT_SET_NEWREPLPOL,

	/* file system operations */
	SRMT_CHMOD,
	SRMT_CHOWN,
	SRMT_CREATE,
	SRMT_FGETATTR,
	SRMT_FTRUNCATE,
	SRMT_GETATTR,
	SRMT_LINK,
	SRMT_LOCK,
	SRMT_LOOKUP,
	SRMT_MKDIR,
	SRMT_MKNOD,
	SRMT_READ,
	SRMT_READDIR,
	SRMT_READLINK,
	SRMT_RENAME,
	SRMT_RMDIR,
	SRMT_SETATTR,
	SRMT_STATFS,
	SRMT_SYMLINK,
	SRMT_TRUNCATE,
	SRMT_UNLINK,
	SRMT_UTIMES,
	SRMT_WRITE
};

/* ----------------------------- BEGIN MESSAGES ----------------------------- */

/*
 * Note: Member ordering within structures must always follow 64-bit boundaries
 * to preserve compatibility between 32-bit and 64-bit machines.
 */

struct srm_generic_rep {
	uint64_t		data;		/* context overloadable data */
	int32_t			rc;		/* return code, 0 for success or slerrno */
	int32_t			_pad;
} __packed;

/* ---------------------- BEGIN ENCAPSULATED MESSAGES ----------------------- */

/*
 * Note: these messages are contained within other messages and thus must end on
 * 64-bit boundaries.
 */

#define DESCBUF_REPRLEN		45		/* strlen(base64(SHA256(secret)) + NUL */

#define SBDB_MAGIC		UINT64_C(0x4321432143214321)

struct srt_bdb_secret {
	uint64_t		sbs_magic;
	struct slash_fidgen	sbs_fg;
	uint64_t		sbs_nonce;
	uint64_t		sbs_seq;
	uint64_t		sbs_key;
	uint64_t		sbs_ion_nid;
	sl_bmapno_t		sbs_bmapno;
	sl_ios_id_t		sbs_ios_id;
	uint64_t		sbs_cli_nid;
	uint32_t		sbs_cli_pid;
	uint32_t		sbs__pad;
} __packed;

/* hash = base64(SHA256(secret + key)) */
struct srt_bmapdesc_buf {
	struct srt_bdb_secret	sbdb_secret;	/* encrypted */
	char			sbdb_hash[DESCBUF_REPRLEN];
	char			sbdb__pad[3];
} __packed;

/* ------------------------ BEGIN NAMESPACE MESSAGES ------------------------ */

struct srt_namespace_entry {
	uint64_t		parent;		/* SLASH ID of the parent directory */
	uint64_t		target;		/* SLASH ID of the target */
	uint64_t		seqno;
	char			name[NAME_MAX + 1];
	uint8_t			op;
	uint8_t			type;
	uint8_t			perm;
	int8_t			_pad;
} __packed;

struct srm_send_namespace_req {
	int32_t			count;
	int32_t			_pad;
	struct srt_namespace_entry entries[0];
} __packed;

struct srm_send_namespace_rep {
	uint32_t		rc;
	int32_t			_pad;
} __packed;

/* -------------------------- BEGIN BMAP MESSAGES --------------------------- */

struct srm_getbmap_req {
	struct slash_fidgen	fg;
	sl_ios_id_t		pios;		/* client's preferred IOS ID */
	sl_bmapno_t		bmapno;		/* Starting bmap index number */
	uint32_t		nbmaps;		/* read-ahead support */
	int32_t			rw;		/* 'enum rw' value for access */
	uint32_t		flags;		/* see SRM_BMAPF_* flags below */
	int32_t			_pad;
} __packed;

#define SRM_GETBMAPF_DIRECTIO	(1 << 0)	/* client wants direct I/O */
#define SRM_GETBMAPF_GETREPLTBL	(1 << 1)	/* fetch inode replica table */

struct srm_getbmap_rep {
	uint64_t		ios_nid;	/* responsible I/O server ID if write */
	uint64_t		seq;		/* bmap global sequence number */
	uint64_t		key;		/* MDS odtable key */
	uint32_t		nbmaps;		/* number of bmaps actually returned */
	uint32_t		nrepls;		/* # sl_replica_t's set in bulk */
	uint32_t		flags;		/* see SRM_BMAPF_* flags */
	uint32_t		rc;		/* 0 for success or slerrno */
/*
 * Bulk data contents:
 *
 *	+-------------------------------+-------------------------------+
 *	| data type			| description			|
 *	+-------------------------------+-------------------------------+
 *	| struct slash_bmap_od		| bmap contents			|
 *	| struct srt_bmapdesc_buf	| descriptor			|
 *	| sl_replica_t (if GETREPLTBL)	| inode replica index list	|
 *	+-------------------------------+-------------------------------+
 */
} __packed;

/*
 * ION requesting CRC table from the MDS.  Passes back the srt_bdb_secret
 *  which was handed to him by the client.
 */
struct srm_bmap_wire_req {
	struct slash_fidgen	fg;
	sl_bmapno_t		bmapno;
	int32_t			rw;
	int32_t			_pad;
	//struct srt_bmapdesc_buf sbdb;
} __packed;

struct srm_bmap_wire_rep {
	uint64_t		minseq;
	int32_t			rc;
	int32_t			_pad;
/*
 * Bulk data contains a number of the following structures:
 *
 *      +-------------------------+---------------+
 *      | data type               | description   |
 *      +-------------------------+---------------+
 *      | struct slash_bmap_od    | bmap contents |
 *      +-------------------------+---------------+
 */
} __packed;

struct srm_bmap_chmode_req {
	struct slash_fidgen	fg;
	uint32_t		blkno;
	int32_t			rw;
} __packed;

struct srm_bmap_chmode_rep {
	struct srt_bmapdesc_buf	sbdb;
	int32_t			rc;
	int32_t			_pad;
} __packed;

struct srm_bmap_dio_req {
	uint64_t		fid;
	uint64_t		seq;
	uint32_t		blkno;
	uint32_t		dio;
	uint32_t		mode;
	int32_t			_pad;
} __packed;

struct srm_bmap_crcwire {
	uint64_t		crc;		/* CRC of the corresponding sliver */
	uint32_t		slot;		/* sliver number in the owning bmap */
	int32_t			_pad;
} __packed;

#define MAX_BMAP_INODE_PAIRS	28		/* ~520 bytes (max) per srm_bmap_crcup */

struct srm_bmap_crcup {
	struct slash_fidgen	fg;
	uint64_t		fsize;		/* largest known size */
	uint64_t                seq;            /* for bmap odtable release */
	uint64_t                key;            /* for bmap odtable release */
	uint32_t		blkno;		/* bmap block number */
	uint32_t		nups:31;	/* number of CRC updates */
	uint32_t		rls:1;		/* try to release the bmap odtable entry */
	struct srm_bmap_crcwire	crcs[0];	/* see above, MAX_BMAP_INODE_PAIRS max */
} __packed;

#define MAX_BMAP_NCRC_UPDATES	64		/* max number of CRC updates in a batch */

struct srm_bmap_crcwrt_req {
	uint64_t		crc;		/* yes, a CRC of the CRC's */
	uint8_t			ncrcs_per_update[MAX_BMAP_NCRC_UPDATES];
	uint32_t		ncrc_updates;
	int32_t			_pad;
} __packed;

struct srm_bmap_iod_get {
	uint64_t		fid;
	uint32_t		blkno;
	int32_t			_pad;
} __packed;

struct srm_bmap_id {
	struct slash_fidgen     fg;
	uint64_t		key;
	uint64_t		seq;
	sl_bmapno_t		bmapno;
	int32_t			_pad;
} __packed;

#define MAX_BMAP_RELEASE 8
struct srm_bmap_release_req {
	struct srm_bmap_id	bmaps[MAX_BMAP_RELEASE];
	uint32_t		nbmaps;
	int32_t			_pad;
} __packed;

struct srm_bmap_release_rep {
	uint32_t		bidrc[MAX_BMAP_RELEASE];
	int32_t			rc;
	int32_t			_pad;
} __packed;

struct srm_bmap_minseq_get {
	int32_t			data;
	int32_t			_pad;
} __packed;

/* ------------------------- BEGIN CONTROL MESSAGES ------------------------- */

struct srm_connect_req {
	uint64_t		magic;
	uint32_t		version;
	int32_t			_pad;
} __packed;

struct srm_ping_req {
	int64_t			data;		/* context overloadable data */
} __packed;

/* ----------------------- BEGIN REPLICATION MESSAGES ----------------------- */

/* for a GETSTATUS on a replication request */
struct srm_replst_master_req {
	struct slash_fidgen	fg;
	sl_replica_t		repls[SL_MAX_REPLICAS];
	int32_t			id;		/* user-provided passback value */
	int32_t			rc;		/* or EOF */
	uint32_t		nbmaps;		/* # of bmaps in file */
	uint32_t		newreplpol;	/* default replication policy */
	uint32_t		nrepls;		/* # of I/O systems in 'repls' */
	int32_t			_pad;
	unsigned char		data[16];	/* slave data here if fits */
} __packed;

#define srm_replst_master_rep srm_replst_master_req

/*
 * bmap data carrier for a replrq GETSTATUS when data is larger than can
 * fit in srm_replst_master_req.data
 */
struct srm_replst_slave_req {
	struct slash_fidgen	fg;
	int32_t			id;		/* user-provided passback value */
	int32_t			len;		/* of bulk data */
	uint32_t		rc;
	int32_t			nbmaps;		/* # of bmaps in this chunk */
	sl_blkno_t		boff;		/* offset into inode of first bmap in bulk */
	int32_t			_pad;
/* bulk data is sections of bh_repls data */
} __packed;

/* per-bmap header submessage, prepended before each bh_repls content */
struct srsm_replst_bhdr {
	uint8_t			srsb_repl_policy;
} __packed;

#define SL_NBITS_REPLST_BHDR	(8)

#define SRM_REPLST_PAGESIZ	(1024 * 1024)	/* should be network MSS */

#define srm_replst_slave_rep srm_replst_slave_req

struct srm_repl_schedwk_req {
	uint64_t		nid;
	struct slash_fidgen	fg;
	sl_bmapno_t		bmapno;
	sl_bmapgen_t		bgen;
	uint32_t		len;
	uint32_t		rc;
} __packed;

struct srm_repl_read_req {
	struct slash_fidgen	fg;
	uint64_t		len;		/* #bytes in this message, to find #slivers */
	sl_bmapno_t		bmapno;
	int32_t			slvrno;
} __packed;

#define srm_repl_read_rep srm_io_rep

struct srm_set_newreplpol_req {
	struct slash_fidgen	fg;
	int32_t			pol;
	int32_t			_pad;
} __packed;

struct srm_set_bmapreplpol_req {
	struct slash_fidgen	fg;
	sl_bmapno_t		bmapno;
	int32_t			pol;
} __packed;

/* ----------------------- BEGIN FILE SYSTEM MESSAGES ----------------------- */

struct srm_create_req {
	struct slash_fidgen	pfg;		/* parent dir's file ID + generation */
	struct slash_creds	creds;		/* credentials of user */
	char			name[NAME_MAX + 1];
	uint32_t		mode;		/* mode_t permission for new file */

	/* parameters for fetching first bmap */
	sl_ios_id_t		pios;		/* preferred I/O system ID */
	uint32_t		flags;		/* see SRM_BMAPF_* flags */
	int32_t			_pad;
} __packed;

struct srm_create_rep {
	struct slash_fidgen	fg;		/* new file's file ID */
	struct srt_stat		attr;		/* stat(2) buffer of new file attrs */
	int32_t			rc;		/* 0 for success or slerrno */

	/* parameters for fetching first bmap */
	uint32_t		rc2;		/* (for GETBMAP) 0 or slerrno */
	uint64_t		ios_nid;	/* responsible I/O server ID if write */
	uint64_t		seq;		/* bmap global sequence number */
	uint64_t		key;		/* MDS odtable key */
	uint32_t		flags;		/* see SRM_BMAPF_* flags */
	int32_t			_pad;
/*
 * Bulk data contents:
 *
 *	+-------------------------------+-------------------------------+
 *	| data type			| description			|
 *	+-------------------------------+-------------------------------+
 *	| struct slash_bmap_od		| bmap contents			|
 *	| struct srt_bmapdesc_buf	| descriptor			|
 *	+-------------------------------+-------------------------------+
 */
} __packed;

struct srm_destroy_req {
} __packed;

struct srm_getattr_req {
	struct slash_fidgen	fg;
} __packed;

/* XXX factor out since this is encapsulated within READDIR */
struct srm_getattr_rep {
	struct srt_stat		attr;
	int32_t			rc;
	uint32_t		_pad;
} __packed;

struct srm_io_req {
	struct srt_bmapdesc_buf	sbdb;
	uint32_t		ptruncgen;
	uint32_t		flags:31;
	uint32_t		op:1;		/* read/write */
	uint32_t		size;
	uint32_t		offset;
/* WRITE data is bulk request. */
} __packed;

/* I/O operations */
#define SRMIOP_RD		0
#define SRMIOP_WR		1

/* I/O flags */
#define SRM_IOF_APPEND		(1 << 0)

struct srm_io_rep {
	int32_t			rc;
	uint32_t		size;
/* READ data is in bulk reply. */
} __packed;

struct srm_link_req {
	struct slash_creds	creds;
	struct slash_fidgen	pfg;		/* parent dir */
	struct slash_fidgen	fg;
	char			name[NAME_MAX + 1];
} __packed;

struct srm_link_rep {
	struct slash_fidgen	fg;
	struct srt_stat		attr;
	int32_t			rc;
	int32_t			_pad;
} __packed;

struct srm_lookup_req {
	struct slash_fidgen	pfg;		/* parent dir */
	char			name[NAME_MAX + 1];
} __packed;

struct srm_lookup_rep {
	struct slash_fidgen	fg;
	struct srt_stat		attr;
	int32_t			rc;
	int32_t			_pad;
} __packed;

struct srm_mkdir_req {
	struct slash_creds	creds;
	struct slash_fidgen	pfg;		/* parent dir */
	char			name[NAME_MAX + 1];
	uint32_t		mode;
	int32_t			_pad;
} __packed;

struct srm_mkdir_rep {
	struct slash_fidgen	fg;
	struct srt_stat		attr;
	int32_t			rc;
	int32_t			_pad;
} __packed;

struct srm_mknod_req {
	struct slash_creds	creds;
	char			name[NAME_MAX + 1];
	struct slash_fidgen	pfg;		/* parent dir */
	uint32_t		mode;
	uint32_t		rdev;
} __packed;

#define MAX_READDIR_NENTS	1000
#define MAX_READDIR_BUFSIZ	(sizeof(struct srt_stat) * MAX_READDIR_NENTS)

struct srm_readdir_req {
	struct slash_fidgen	fg;
	uint64_t		offset;
	uint64_t		size;
	uint32_t		nstbpref;	/* number of prefetched attributes */
	int32_t			_pad;
} __packed;

struct srm_readdir_rep {
	uint64_t		size;
	uint32_t		num;		/* how many dirents were returned */
	int32_t			rc;
/*
 * XXX accompanied by bulk data is but should not be in fuse dirent format
 *	and must be 64-bit aligned.
 */
} __packed;

struct srm_readlink_req {
	struct slash_fidgen	fg;
} __packed;

struct srm_readlink_rep {
	int32_t			rc;
	int32_t			_pad;
/* buf is in bulk of size PATH_MAX */
} __packed;

struct srm_rename_req {
	struct slash_creds	creds;
	struct slash_fidgen	npfg;		/* new parent dir */
	struct slash_fidgen	opfg;		/* old parent dir */
	uint32_t		fromlen;
	uint32_t		tolen;
/* 'from' and 'to' component names are in bulk data without terminating NULs */
} __packed;

struct srm_replrq_req {
	struct slash_fidgen	fg;
	sl_replica_t		repls[SL_MAX_REPLICAS];
	uint32_t		nrepls;
	sl_bmapno_t		bmapno;		/* bmap to access or -1 for all */
} __packed;

struct srm_setattr_req {
	struct slash_fidgen	fg;
	struct srt_stat		attr;
	int32_t			to_set;
	int32_t			_pad;
} __packed;

/* to_set flags */
#define SRM_SETATTRF_MODE	(1 << 0)	/* chmod */
#define SRM_SETATTRF_UID	(1 << 1)	/* chown */
#define SRM_SETATTRF_GID	(1 << 2)	/* chgrp */
#define SRM_SETATTRF_SIZE	(1 << 3)	/* metadata truncate */
#define SRM_SETATTRF_ATIME	(1 << 4)	/* utimes */
#define SRM_SETATTRF_MTIME	(1 << 5)	/* utimes */
#define SRM_SETATTRF_FSIZE	(1 << 6)	/* file content size update */
#define SRM_SETATTRF_PTRUNCGEN	(1 << 7)	/* file content non-zero trunc */

#define srm_setattr_rep srm_getattr_rep

struct srm_statfs_req {
} __packed;

struct srm_statfs_rep {
	struct srt_statfs	ssfb;
	int32_t			rc;
	int32_t			_pad;
} __packed;

struct srm_symlink_req {
	struct slash_creds	creds;
	struct slash_fidgen	pfg;		/* parent dir */
	char			name[NAME_MAX + 1];
	uint32_t		linklen;
	int32_t			_pad;
/* link path name is in bulk */
} __packed;

struct srm_symlink_rep {
	struct slash_fidgen	fg;
	struct srt_stat		attr;
	int32_t			rc;
	int32_t			_pad;
} __packed;

struct srm_unlink_req {
	struct slash_fidgen	pfg;		/* parent dir */
	char			name[NAME_MAX + 1];
} __packed;

#define srm_unlink_rep srm_generic_rep

#endif /* _SLASHRPC_H_ */
