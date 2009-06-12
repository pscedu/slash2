/* $Id$ */

/*
 * Slash RPC subsystem definitions including messages definitions.
 */

#ifndef _SLASHRPC_H_
#define _SLASHRPC_H_

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <sys/vfs.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <fcntl.h>
#include <unistd.h>

#include "psc_types.h"
#include "psc_rpc/rpc.h"
#include "psc_util/crc.h"

#include "slconfig.h"
#include "fid.h"
#include "creds.h"

#define SLASH_SVR_PID		54321

/* Slash RPC channel to MDS from client. */
#define SRMC_REQ_PORTAL		20
#define SRMC_REP_PORTAL		21
#define SRMC_BULK_PORTAL	22

#define SRMC_VERSION		1
#define SRMC_MAGIC		0xaabbccddeeff0022ULL

/* Slash RPC channel to MDS from MDS. */
#define SRMM_REQ_PORTAL		30
#define SRMM_REP_PORTAL		31

#define SRMM_VERSION		1
#define SRMM_MAGIC		0xaabbccddeeff0033ULL

/* Slash RPC channel to MDS from ION. */
#define SRMI_REQ_PORTAL		40
#define SRMI_REP_PORTAL		41

#define SRMI_VERSION		1
#define SRMI_MAGIC		0xaabbccddeeff0044ULL

/* Slash RPC channel to client from MDS. */
#define SRCM_REQ_PORTAL		50
#define SRCM_REP_PORTAL		51

#define SRCM_VERSION		1
#define SRCM_MAGIC		0xaabbccddeeff0055ULL

/* Slash RPC channel to ION from client. */
#define SRIC_REQ_PORTAL		60
#define SRIC_REP_PORTAL		61
#define SRIC_BULK_PORTAL	62

#define SRIC_VERSION		1
#define SRIC_MAGIC		0xaabbccddeeff0066ULL

/* Slash RPC channel to ION from ION. */
#define SRII_REQ_PORTAL		70
#define SRII_REP_PORTAL		71
#define SRII_BULK_PORTAL	72

#define SRII_VERSION		1
#define SRII_MAGIC		0xaabbccddeeff0077ULL

/* Slash RPC channel to ION from MDS. */
#define SRIM_REQ_PORTAL		80
#define SRIM_REP_PORTAL		81
#define SRIM_BULK_PORTAL	82

#define SRIM_VERSION		1
#define SRIM_MAGIC		0xaabbccddeeff0088ULL

/* Slash OPEN message flags */
#define SL_FREAD	1
#define SL_FWRITE	2
#define SL_FAPPEND	O_APPEND
//#define FAPPEND	8
#define SL_FCREAT	O_CREAT
#define SL_FTRUNC	O_TRUNC
#define SL_FOFFMAX	O_LARGEFILE
#define SL_FSYNC	O_SYNC
#define SL_FDSYNC	O_DSYNC
#define SL_FRSYNC	O_RSYNC
#define SL_FEXCL	O_EXCL
#define SL_DIRECTORY	O_DIRECTORY
#define SL_FNODSYNC	0x10000		/* fsync pseudo flag */
#define SL_FNOFOLLOW	0x20000		/* don't follow symlinks */
#define SL_FIGNORECASE	0x80000		/* request case-insensitive lookups */

#define SL_READ		00400
#define SL_WRITE	00200
#define SL_EXEC		00100

/* Slash RPC message types. */
enum {
	SRMT_ACCESS,
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
	SRMT_LINK,
	SRMT_LOCK,
	SRMT_LOOKUP,
	SRMT_MKDIR,
	SRMT_MKNOD,
	SRMT_OPEN,
	SRMT_OPENDIR,
	SRMT_READ,
	SRMT_READDIR,
	SRMT_READLINK,
	SRMT_RELEASE,
	SRMT_RELEASEBMAP,
	SRMT_RELEASEDIR,
	SRMT_RENAME,
	SRMT_RMDIR,
	SRMT_SETATTR,
	SRMT_STATFS,
	SRMT_SYMLINK,
	SRMT_TRUNCATE,
	SRMT_UNLINK,
	SRMT_UTIMES,
	SRMT_WRITE,
	SNRT
};

struct srt_fdb_secret {
	uint64_t		sfs_magic;
	struct slash_fidgen	sfs_fg;
	uint64_t		sfs_cfd;	/* stream handle/ID */
	lnet_process_id_t	sfs_cprid;	/* client NID/PID */
	uint64_t		sfs_nonce;
};

struct srt_fd_buf {
	struct srt_fdb_secret	sfdb_secret;	/* encrypted */
	char			sfdb_hash[45];	/* base64(SHA256(srt_fdb_secret + key)) */
};

#define SFDB_MAGIC		UINT64_C(0x1234123412341234)

#define SRIC_BMAP_READ  0
#define SRIC_BMAP_WRITE 1

struct srm_access_req {
	struct slash_creds	creds;
	uint64_t		ino;
	uint32_t		mask;
};

struct srm_bmap_req {
	struct srt_fd_buf	sfdb;
	uint64_t		fid;		/* Optional, may be filled in server-side */
	uint32_t		pios;		/* preferred ios (provided by client)     */
	uint32_t		blkno;		/* Starting block number                  */
	uint32_t		nblks;		/* Read-ahead support                     */
	uint32_t		dio;		/* Client wants directio                  */
	uint32_t		rw;
};

struct srm_bmap_rep {
	uint32_t		nblks;		/* The number of bmaps actually returned */
	uint32_t		rc;
};

struct srm_bmap_mode_req {
	struct srt_fd_buf	sfdb;
	uint32_t		blkno;
	uint32_t		rw;
};

struct srm_bmap_dio_req {
	uint64_t		fid;
	uint32_t		blkno;
	uint32_t		dio;
	uint32_t		mode;
};

struct srm_bmap_crcwire {
	uint64_t		crc;
	uint32_t		slot;
};

struct srm_bmap_crcup {
	uint64_t		fid;
	uint32_t		blkno;		/* bmap block number */
	uint32_t		nups;		/* number of crc updates */
	struct srm_bmap_crcwire	crcs[0];
};

#define MAX_BMAP_INODE_PAIRS 64

struct srm_bmap_crcwrt_req {
	uint32_t		ncrc_updates;
	uint8_t			ncrcs_per_update[MAX_BMAP_INODE_PAIRS];
	uint64_t		crc;		/* yes, a crc of the crc's */
};

/* Slash RPC message to ION from client */
struct srm_ic_connect_secret {
	uint64_t		magic;
	uint64_t		wndmap_min;
};

#define SRM_CI_CONNECT_SIGMAGIC	0x1234123412341234

struct srm_ic_connect_req {
	struct srm_ic_connect_secret crypt_i;
	uint64_t		magic;
	uint32_t		version;
};

struct srm_connect_req {
	uint64_t		magic;
	uint32_t		version;
};

struct srm_create_req {
	struct slash_creds	creds;
	char			name[NAME_MAX + 1];
	uint64_t		pino;		/* parent inode */
	int32_t			len;
	uint32_t		mode;
	uint32_t		flags;
};

struct srm_open_req {
	struct slash_creds	creds;
	uint64_t		ino;
	uint32_t		flags;
	uint32_t		mode;
};

struct srm_opencreate_rep {
	struct srt_fd_buf	sfdb;
	struct stat		attr;		/* XXX struct srt_stat */
	int32_t			rc;
};

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

struct srm_link_req {
	struct slash_creds	creds;
	char			name[NAME_MAX + 1];
	uint32_t		len;
	uint64_t		pino;		/* parent inode */
	uint64_t		ino;
};

struct srm_link_rep {
	struct slash_fidgen	fg;
	struct stat		attr;		/* XXX struct srt_stat */
	int32_t			rc;
};

struct srm_lookup_req {
	struct slash_creds	creds;
	char			name[NAME_MAX + 1];
	int			len;
	uint64_t		pino;		/* parent inode */
};

struct srm_lookup_rep {
	struct slash_fidgen	fg;
	struct stat		attr;		/* XXX struct srt_stat */
	int32_t			rc;
};

struct srm_mkdir_req {
	struct slash_creds	creds;
	char			name[NAME_MAX + 1];
	uint64_t		pino;		/* parent inode */
	uint32_t		len;
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

#define srm_opendir_rep srm_opencreate_rep

struct srm_io_req {
	struct srt_fd_buf	sfdb;
	uint32_t		size;
	uint32_t		offset;
	uint32_t		flags;
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
	uint32_t		num;		/* how many dirents were returned */
	int32_t			rc;
	/* accompanied by bulk data in pure getdents(2) format */
};

struct srm_readlink_req {
	struct slash_creds	creds;
	uint64_t		ino;
};

struct srm_readlink_rep {
	char			buf[PATH_MAX];
	uint32_t		len;
	int32_t			rc;
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
/* 'from' and 'to' names are in bulk data */
};

#define srm_unlink_rep srm_generic_rep

#if 0
/* Slash RPC transportably safe structures. */
struct srt_stat {
	int32_t		st_dev;			/* ID of device containing file */
	uint64_t	st_ino;			/* inode number */
	int32_t		st_mode;		/* protection */
	int32_t		st_nlink;		/* number of hard links */
	int32_t		st_uid;			/* user ID of owner */
	int32_t		st_gid;			/* group ID of owner */
	int32_t		st_rdev;		/* device ID (if special file) */
	uint64_t	st_size;		/* total size, in bytes */
	int32_t		st_blksize;		/* blocksize for file system I/O */
	uint64_t	st_blocks;		/* number of 512B blocks allocated */
	uint64_t	st_atime;		/* time of last access */
	uint64_t	st_mtime;		/* time of last modification */
	uint64_t	st_ctime;		/* time of last status change */
};
#endif

struct srm_setattr_req {
	struct srt_fd_buf	sfdb;
	struct slash_creds	creds;
	struct stat		attr;		/* XXX struct srt_stat */
	uint64_t		ino;
	int32_t			to_set;
};

#define srm_setattr_rep srm_getattr_rep

struct srm_statfs_req {
	struct slash_creds	creds;
};

struct srm_statfs_rep {
	struct statvfs		stbv;
	int32_t			rc;
};

struct srm_symlink_req {
	struct slash_creds	creds;
	char			name[NAME_MAX + 1];
	uint64_t		pino;		/* parent inode */
	uint32_t		namelen;
	uint32_t		linklen;
};

struct srm_symlink_rep {
	struct slash_fidgen	fg;
	struct slash_creds	creds;
	struct stat		attr;
	int32_t			rc;
};

struct srm_unlink_req {
	struct slash_creds	creds;
	char			name[NAME_MAX + 1];
	uint64_t		pino;		/* parent inode */
	int32_t			len;
};

struct srm_generic_rep {
	uint64_t		data;
	int32_t			rc;
};

struct slashrpc_cservice {
	struct pscrpc_import	*csvc_import;
	psc_spinlock_t		 csvc_lock;
	int			 csvc_failed;
	int			 csvc_initialized;
};

void slashrpc_export_destroy(void *);

struct slashrpc_cservice *rpc_csvc_create(u32, u32);
int rpc_issue_connect(lnet_nid_t, struct pscrpc_import *, u64, u32);

extern lnet_process_id_t lpid;

#endif /* _SLASHRPC_H_ */
