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
 * Slash RPC subsystem definitions including messages definitions.
 */

#ifndef _SLASHRPC_H_
#define _SLASHRPC_H_

#include <sys/vfs.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <fcntl.h>
#include <unistd.h>

#include "psc_rpc/rpc.h"
#include "psc_util/crc.h"

#include "creds.h"
#include "fid.h"
#include "slconfig.h"

#define SLASH_SVR_PID		54321

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

/* OPEN message flags */
#define SLF_READ	(1 << 0)
#define SLF_WRITE	(1 << 1)
#define SLF_APPEND	(1 << 2)
#define SLF_CREAT	(1 << 3)
#define SLF_TRUNC	(1 << 4)
#define SLF_SYNC	(1 << 5)
#define SLF_EXCL	(1 << 6)

/* Slash RPC message types. */
enum {
	SRMT_BMAPCHMODE,
	SRMT_BMAPCRCWRT,
	SRMT_BMAPDIO,
	SRMT_CHMOD,
	SRMT_CHOWN,
	SRMT_CONNECT,
	SRMT_CREATE,
	SRMT_DESTROY,
	SRMT_DISCONNECT,
	SRMT_FGETATTR,
	SRMT_FTRUNCATE,
	SRMT_GETATTR,
	SRMT_GETBMAP,
	SRMT_GETBMAPCRCS,
	SRMT_GETREPTBL,
	SRMT_LINK,
	SRMT_LOCK,
	SRMT_LOOKUP,
	SRMT_MKDIR,
	SRMT_MKNOD,
	SRMT_OPEN,
	SRMT_OPENDIR,
	SRMT_PING,
	SRMT_READ,
	SRMT_READDIR,
	SRMT_READLINK,
	SRMT_RELEASE,
	SRMT_RELEASEBMAP,
	SRMT_RELEASEDIR,
	SRMT_RENAME,
	SRMT_REPL_ADDRQ,
	SRMT_REPL_DELRQ,
	SRMT_REPL_GETST,
	SRMT_REPL_GETST_SLAVE,
	SRMT_REPL_READ,
	SRMT_REPL_SCHEDWK,
	SRMT_RMDIR,
	SRMT_SETATTR,
	SRMT_SET_BMAPREPLPOL,
	SRMT_SET_NEWREPLPOL,
	SRMT_STATFS,
	SRMT_SYMLINK,
	SRMT_TRUNCATE,
	SRMT_UNLINK,
	SRMT_UTIMES,
	SRMT_WRITE
};

enum slconn_type {
	SLCONNT_CLI,
	SLCONNT_IOD,
	SLCONNT_MDS,
	SLNCONNT
};

#define DESCBUF_REPRLEN	45		/* strlen(base64(SHA256(secret)) */

struct srt_fdb_secret {
	uint64_t		sfs_magic;
	struct slash_fidgen	sfs_fg;
	uint64_t		sfs_cfd;	/* stream handle/ID */
	lnet_process_id_t	sfs_cli_prid;	/* client NID/PID */
	uint64_t		sfs_nonce;
};

/* hash = base64(SHA256(secret + key)) */
struct srt_fd_buf {
	struct srt_fdb_secret	sfdb_secret;	/* encrypted */
	char			sfdb_hash[DESCBUF_REPRLEN];
};

#define SFDB_MAGIC		UINT64_C(0x1234123412341234)
#define SBDB_MAGIC		UINT64_C(0x4321432143214321)

struct srt_bdb_secret {
	uint64_t		sbs_magic;
	struct slash_fidgen	sbs_fg;
	uint64_t		sbs_cfd;	/* stream handle/ID */
	lnet_process_id_t	sbs_cli_prid;	/* client NID/PID */
	lnet_nid_t		sbs_ion_nid;
	sl_ios_id_t		sbs_ios_id;
	sl_blkno_t		sbs_bmapno;
	uint64_t		sbs_nonce;
};

/* hash = base64(SHA256(secret + key)) */
struct srt_bmapdesc_buf {
	struct srt_bdb_secret	sbdb_secret;	/* encrypted */
	char			sbdb_hash[DESCBUF_REPRLEN];
};

struct srm_bmap_req {
	struct srt_fd_buf	sfdb;
	uint32_t		pios;		/* client's preferred IOS ID	*/
	uint32_t		blkno;		/* Starting block number	*/
	uint32_t		nblks;		/* Read-ahead support		*/
	uint32_t		dio;		/* Client wants directio	*/
	int32_t			rw;
	uint32_t                getreptbl;	/* whether to include inode replicas */
};

struct srm_bmap_rep {
	uint32_t		nblks;		/* The number of bmaps actually returned */
	uint64_t		ios_nid;	/* responsible I/O server ID if write */
	uint32_t		nrepls;		/* # sl_replica_t's set in bulk */
	uint32_t		rc;
/*
 * Bulk data contents:
 *
 *	+-------------------------------+-------------------------------+
 *	| data type			| description			|
 *	+-------------------------------+-------------------------------+
 *	| struct slash_bmap_od		| bmap contents			|
 *	| struct srt_bmapdesc_buf	| descriptor			|
 *	| sl_replica_t (if getreptbl)	| inode replica index list	|
 *	+-------------------------------+-------------------------------+
 */
};

/*
 * ION requesting CRC table from the MDS.  Passes back the srt_bdb_secret
 *  which was handed to him by the client.
 */
struct srm_bmap_wire_req {
	struct slash_fidgen	fg;
	sl_blkno_t		bmapno;
	int32_t			rw;
	//struct srt_bmapdesc_buf sbdb;
};

struct srm_bmap_wire_rep {
	int32_t                 rc;
/*
 * Bulk data contains a number of the following structures:
 *
 *      +-------------------------+---------------+
 *      | data type               | description   |
 *      +-------------------------+---------------+
 *      | struct slash_bmap_od    | bmap contents |
 *      +-------------------------+---------------+
 */
};

struct srm_bmap_chmode_req {
	struct srt_fd_buf	sfdb;
	uint32_t		blkno;
	int32_t			rw;
};

struct srm_bmap_chmode_rep {
	struct srt_bmapdesc_buf	sbdb;
	int32_t			rc;
};

struct srm_bmap_dio_req {
	uint64_t		fid;
	uint32_t		blkno;
	uint32_t		dio;
	uint32_t		mode;
};

struct srm_bmap_crcwire {
	uint64_t		crc;		/* CRC of the corresponding sliver */
	uint32_t		slot;		/* sliver number in the owning bmap */
} __attribute__ ((__packed__));

#define MAX_BMAP_INODE_PAIRS	28		/* ~520 bytes (max) per srm_bmap_crcup */

struct srm_bmap_crcup {
	struct slash_fidgen	fg;
	uint64_t		fsize;		/* largest known size */
	uint32_t		blkno;		/* bmap block number */
	uint32_t		nups;		/* number of crc updates */
	struct srm_bmap_crcwire	crcs[0];	/* see above, MAX_BMAP_INODE_PAIRS max */
};

#define MAX_BMAP_NCRC_UPDATES	64		/* max number of CRC updates in a batch */

struct srm_bmap_crcwrt_req {
	uint32_t		ncrc_updates;
	uint8_t			ncrcs_per_update[MAX_BMAP_NCRC_UPDATES];
	uint64_t		crc;		/* yes, a crc of the crc's */
};

struct srm_bmap_iod_get {
	uint64_t                fid;
	uint32_t                blkno;
};

struct srm_connect_req {
	uint64_t		magic;
	uint32_t		version;
};

struct srm_create_req {
	struct slash_creds	creds;
	uint64_t		pino;		/* parent inode */
	char			name[NAME_MAX + 1];
	uint32_t		mode;
	uint32_t		flags;
};

#define srm_create_rep srm_open_rep

struct srm_open_req {
	struct slash_creds	creds;
	uint64_t		ino;
	uint32_t		flags;
};

struct srm_open_rep {
	struct srt_fd_buf	sfdb;
	struct stat		attr;		/* XXX struct srt_stat */
	int32_t			rc;
};

#define srm_opencreate_rep srm_open_rep

struct srm_destroy_req {
};

#define srm_disconnect_req srm_destroy_req

struct srm_getattr_req {
	struct slash_creds	creds;
	uint64_t		ino;
};

struct srm_getattr_rep {
	struct stat		attr;		/* XXX struct srt_stat */
	uint64_t		gen;
	int32_t			rc;
};

struct srm_io_req {
	struct srt_bmapdesc_buf	sbdb;
	uint32_t		size;
	uint32_t		offset;
	uint32_t		op;		/* read/write */
/* WRITE data is bulk request. */
};

#define SRMIO_RD 0
#define SRMIO_WR 1

struct srm_io_rep {
	int32_t			rc;
	uint32_t		size;
/* READ data is in bulk reply. */
};

struct srm_link_req {
	struct slash_creds	creds;
	uint64_t		pino;		/* parent inode */
	uint64_t		ino;
	char			name[NAME_MAX + 1];
};

struct srm_link_rep {
	struct slash_fidgen	fg;
	struct stat		attr;		/* XXX struct srt_stat */
	int32_t			rc;
};

struct srm_lookup_req {
	struct slash_creds	creds;
	uint64_t		pino;		/* parent inode */
	char			name[NAME_MAX + 1];
};

struct srm_lookup_rep {
	struct slash_fidgen	fg;
	struct stat		attr;		/* XXX struct srt_stat */
	int32_t			rc;
};

struct srm_mkdir_req {
	struct slash_creds	creds;
	uint64_t		pino;		/* parent inode */
	char			name[NAME_MAX + 1];
	uint32_t		mode;
};

struct srm_mkdir_rep {
	struct slash_fidgen	fg;
	struct stat		attr;		/* XXX struct srt_stat */
	int32_t			rc;
};

struct srm_mknod_req {
	struct slash_creds	creds;
	char			name[NAME_MAX + 1];
	uint64_t		pino;		/* parent inode */
	uint32_t		mode;
	uint32_t		rdev;
};

struct srm_opendir_req {
	struct slash_creds	creds;
	uint64_t		ino;
};

#define srm_opendir_rep srm_open_rep

struct srm_ping_req {
};

#define SRM_READDIR_STBUF_PREFETCH (1 << 0)

struct srm_readdir_req {
	struct srt_fd_buf	sfdb;
	struct slash_creds	creds;
	uint64_t		offset;
	uint64_t		size;
	uint32_t		nstbpref;
};

struct srm_readdir_rep {
	uint64_t		size;
	uint32_t		num;    /* how many dirents were returned */
	int32_t			rc;
	/* accompanied by bulk data in pure getdents(2) format */
};

struct srm_readlink_req {
	struct slash_creds	creds;
	uint64_t		ino;
};

struct srm_readlink_rep {
	int32_t			rc;
/* buf is in bulk of size PATH_MAX */
};

struct srm_release_req {
	struct srt_fd_buf	sfdb;
	struct slash_creds	creds;
	uint32_t		flags;
};

struct srm_releasebmap_req {
};

struct srm_rename_req {
	struct slash_creds	creds;
	uint64_t		npino;		/* new parent inode */
	uint64_t		opino;		/* old parent inode */
	uint32_t		fromlen;
	uint32_t		tolen;
/* 'from' and 'to' component names are in bulk data */
};

struct srm_replrq_req {
	struct slash_fidgen	fg;
	sl_replica_t		repls[SL_MAX_REPLICAS];
	uint32_t		nrepls;
	sl_blkno_t		bmapno;		/* bmap to access or -1 for all */
};

/* request/response for a GETSTATUS on a replication request */
struct srm_replst_master_req {
	struct slash_fidgen	fg;
	int32_t			id;		/* user-provided passback value */
	int32_t			rc;		/* or EOF */
	uint32_t		nbmaps;		/* # of bmaps in this payload */
	uint32_t		newreplpol;	/* default replication policy */
	uint32_t		nrepls;		/* # of I/O systems in 'repls' */
	sl_replica_t		repls[SL_MAX_REPLICAS];
};

#define srm_replst_master_rep srm_replst_master_req

/* bmap data carrier for a replrq GETSTATUS */
struct srm_replst_slave_req {
	struct slash_fidgen	fg;
	int32_t			id;		/* user-provided passback value */
	int32_t			len;		/* of bulk data */
	uint32_t		rc;
	int32_t			nbmaps;		/* # of bmaps in this chunk */
	sl_blkno_t		boff;		/* offset into inode of first bmap in bulk */
/* bulk data is sections of bh_repls data */
};

/* per-bmap header submessage, prepended before each bh_repls content */
struct srsm_replst_bhdr {
	uint8_t			srsb_repl_policy;
};

#define SL_NBITS_REPLST_BHDR	(8)

#define SRM_REPLST_PAGESIZ	(1024 * 1024)	/* should be network MSS */

#define srm_replst_slave_rep srm_replst_slave_req

struct srm_repl_schedwk_req {
	uint64_t		nid;
	struct slash_fidgen	fg;
	sl_bmapno_t		bmapno;
	sl_blkgen_t		bgen;
	uint32_t		len;
	uint32_t		rc;
};

struct srm_repl_read_req {
	struct slash_fidgen	fg;
	uint64_t		len;	/* #bytes in this message, to find #slivers */
	sl_bmapno_t		bmapno;
	int32_t			slvrno;
};

#define srm_repl_read_rep srm_io_rep

#if 0
/* Slash RPC transportably safe structures. */
struct srt_stat {
	int32_t		st_dev;		/* ID of device containing file */
	uint64_t	st_ino;		/* inode number */
	int32_t		st_mode;	/* protection */
	int32_t		st_nlink;	/* number of hard links */
	int32_t		st_uid;		/* user ID of owner */
	int32_t		st_gid;		/* group ID of owner */
	int32_t		st_rdev;	/* device ID (if special file) */
	uint64_t	st_size;	/* total size, in bytes */
	int32_t		st_blksize;	/* blocksize for file system I/O */
	uint64_t	st_blocks;	/* number of 512B blocks allocated */
	uint64_t	st_atime;	/* time of last access */
	uint64_t	st_mtime;	/* time of last modification */
	uint64_t	st_ctime;	/* time of last status change */
};

struct srt_statvfs {
};
#endif

struct srm_setattr_req {
	struct srt_fd_buf	sfdb;
	struct slash_creds	creds;
	struct stat		attr;		/* XXX struct srt_stat */
	uint64_t		ino;
	int32_t			to_set;
};

struct srm_set_newreplpol_req {
	struct slash_fidgen	fg;
	int32_t			pol;
};

struct srm_set_bmapreplpol_req {
	struct slash_fidgen	fg;
	sl_bmapno_t		bmapno;
	int			pol;
};

#define srm_setattr_rep srm_getattr_rep

struct srm_statfs_req {
	struct slash_creds	creds;
};

struct srm_statfs_rep {
	struct statvfs		stbv;	/* XXX use a portable structure */
	int32_t			rc;
};

struct srm_symlink_req {
	struct slash_creds	creds;
	uint64_t		pino;		/* parent inode */
	char			name[NAME_MAX + 1];
	uint32_t		linklen;
/* link path name is in bulk */
};

struct srm_symlink_rep {
	struct slash_fidgen	fg;
	struct slash_creds	creds;
	struct stat		attr;
	int32_t			rc;
};

struct srm_unlink_req {
	struct slash_creds	creds;
	uint64_t		pino;		/* parent inode */
	char			name[NAME_MAX + 1];
};

#define srm_unlink_rep srm_generic_rep

struct srm_generic_rep {
	int32_t			rc;
};

struct slashrpc_cservice {
	struct pscrpc_import	*csvc_import;
	psc_spinlock_t		*csvc_lockp;
	void			*csvc_waitinfo;
	int			 csvc_flags;
	int			 csvc_lasterrno;
	psc_atomic_t		 csvc_refcnt;
	time_t			 csvc_mtime;		/* last connection try */
};

#define CSVCF_CONNECTING	(1 << 0)
#define CSVCF_USE_MULTIWAIT	(1 << 1)

#define CSVC_RECONNECT_INTV	30			/* seconds */

#define CSVC_LOCK_ENSURE(c)	LOCK_ENSURE((c)->csvc_lockp)
#define CSVC_LOCK(c)		spinlock((c)->csvc_lockp)
#define CSVC_ULOCK(c)		freelock((c)->csvc_lockp)
#define CSVC_RLOCK(c)		reqlock((c)->csvc_lockp)
#define CSVC_URLOCK(c, lk)	ureqlock((c)->csvc_lockp, (lk))

struct slashrpc_export {
	uint64_t		 slexp_nextcfd;
	enum slconn_type	 slexp_peertype;
	void			*slexp_data;
	int			 slexp_flags;
	struct pscrpc_export	*slexp_export;
};

/* slashrpc_export flags */
#define SLEXPF_CLOSING		(1 << 0)

struct slashrpc_cservice *
	sl_csvc_get(struct slashrpc_cservice **, int, struct pscrpc_export *,
	    lnet_nid_t, uint32_t, uint32_t, uint64_t, uint32_t,
	    psc_spinlock_t *, void *, enum slconn_type);
void	sl_csvc_decref(struct slashrpc_cservice *);
void	sl_csvc_incref(struct slashrpc_cservice *);
void	sl_csvc_free(struct slashrpc_cservice *);

struct slashrpc_export *
	slexp_get(struct pscrpc_export *, enum slconn_type);
void	slexp_destroy(void *);

extern struct psc_dynarray lnet_nids;

extern void (*slexp_freef[SLNCONNT])(struct pscrpc_export *);

#endif /* _SLASHRPC_H_ */
