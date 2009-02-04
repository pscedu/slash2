#ifndef __SLASHRPC_H__
#define __SLASHRPC_H__ 1

/* $Id$ */

/*
 * Slash RPC subsystem definitions including messages definitions.
 * Note: many of the messages will take on different sizes between
 * 32-bit and 64-bit machine architectures.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE 1
#endif

#include <fcntl.h>
#include <sys/vfs.h> 
#include <sys/statvfs.h> 
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "psc_rpc/rpc.h"
#include "psc_types.h"
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
#define SRIM_BULK_PORTAL        82

#define SRIM_VERSION		1
#define SRIM_MAGIC		0xaabbccddeeff0088ULL


#define SL_FREAD   1
#define SL_FWRITE  2
#define SL_FAPPEND O_APPEND
//#define FAPPEND  8                                                            
#define SL_FCREAT  O_CREAT
#define SL_FTRUNC  O_TRUNC
#define SL_FOFFMAX O_LARGEFILE
#define SL_FSYNC   O_SYNC
#define SL_FDSYNC  O_DSYNC
#define SL_FRSYNC  O_RSYNC
#define SL_FEXCL   O_EXCL
#define SL_DIRECTORY O_DIRECTORY
#define SL_FNODSYNC        0x10000 /* fsync pseudo flag */
#define SL_FNOFOLLOW       0x20000 /* don't follow symlinks */
#define SL_FIGNORECASE     0x80000 /* request case-insensitive lookups */

#define SL_READ    00400
#define SL_WRITE   00200
#define SL_EXEC    00100


/* Slash RPC message types. */
enum {
	SRMT_ACCESS,
	SRMT_BMAPCRCWRT,
	SRMT_BMAPDIO,
	SRMT_BMAPCHMODE,
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
	SRMT_READ,
	SRMT_WRITE,
	SNRT
};

#if 0

struct srm_open_secret {
	u64		magic;
	u64		fid;
	u64		fidgen;
	u64		cnid; /* client NID */
	u64		cpid; /* client PID */
};

#define SR_OPEN_SIGMAGIC	0x1234123412341234

#endif

#define SRIC_BMAP_READ  0
#define SRIC_BMAP_WRITE 1

struct srm_access_req {
	struct slash_creds creds;
	u64 ino;
	u32 mask;	
};

struct srm_bmap_req {
	u32 pios;       /* preferred ios (provided by client)     */
	u64 cfd;
	u32 blkno;	/* Starting block number                  */
	u32 nblks;	/* Read-ahead support                     */
	u64 fid;	/* Optional, may be filled in server-side */
	u32 dio;        /* Client wants directio                  */
	u32 rw;
};

struct srm_bmap_rep {
	u32 nblks;	/* The number of bmaps actually returned  */
	u32 rc;
};

struct srm_bmap_mode_req {
	u64 cfd;
	u32 blkno;
	u32 rw;
};

struct srm_bmap_dio_req {
        u64 fid;
        u32 blkno;
        u32 dio;
	u32 mode;
};


struct srm_bmap_crcwire {
	u64 crc;
	u32 slot;
};

struct srm_bmap_crcup {
	u64 fid;      
        u32 blkno;    /* bmap block number */
	u32 nups;     /* number of crc updates */
	struct srm_bmap_crcwire crcs[0];
};

#define MAX_BMAP_INODE_PAIRS 64
struct srm_bmap_crcwrt_req {
	u32 ncrc_updates;
	u8  ncrcs_per_update[MAX_BMAP_INODE_PAIRS];
	u64 crc; /* yes, a crc of the crc's */
};

/* Slash RPC message for ION from client */
struct srm_ic_connect_secret {
	u64 magic;
	u64 wndmap_min;
};

#define SRM_CI_CONNECT_SIGMAGIC	0x1234123412341234

struct srm_ic_connect_req {
	struct srm_ic_connect_secret crypt_i;
	u64 magic;
	u32 version;
};

struct srm_connect_req {
	u64 magic;
	u32 version;
};

struct srm_create_req {
	struct slash_creds creds;
	char name[NAME_MAX];
	int len;
	u64 pino;
	u32 mode;
	u32 flags;
};

struct srm_open_req {
	struct slash_creds creds;
	u64 ino;
	u32 flags;
	u32 mode;
};

struct srm_opencreate_rep {
	//	struct srm_open_secret sig_m;
	struct slash_fidgen fg; /* XXX for now, put in dsig later */
	struct stat attr;
	u64 cfd;
	s32 rc;
};

struct srm_destroy_req {
};

#define srm_disconnect_req srm_destroy_req

struct srm_getattr_req {
	struct slash_creds creds;
	u64 ino;
};

struct srm_getattr_rep {
	struct stat attr;
	u64 gen;
	s32 rc;
};

struct srm_link_req {
	struct slash_creds creds;
	char name[NAME_MAX];
	u32 len;
	u64 pino, ino;
};

struct srm_link_rep {
	struct stat attr;
	struct slash_fidgen fg;
	s32 rc;
};

struct srm_lookup_req {
	struct slash_creds creds;
	char name[NAME_MAX];
	int  len;
	u64 pino;
};

struct srm_lookup_rep {
	struct slash_fidgen fg; /* XXX for now, put in dsig later */
	struct stat attr;
	s32 rc;
};

struct srm_mkdir_req {
	struct slash_creds creds;
	char name[NAME_MAX];
	u32 len;
	u64 pino;
	u32 mode;
};
#define srm_mkdir_rep srm_opencreate_rep


struct srm_mknod_req {
	struct slash_creds creds;
	char name[NAME_MAX];
	u64 pino;
	u32 mode;
	u32 rdev;
};


struct srm_opendir_req {
	struct slash_creds creds;
	u64 ino;
};

struct srm_opendir_rep {
//	struct srm_open_secret sig_m;
	struct slash_fidgen fg; /* XXX for now, put in dsig later */
	u64 cfd;
	s32 rc;
};

#if 0
struct srm_io_secret {
	struct srm_open_secret sig_m;
	size_t wndmap_pos;
	u64 magic;
	u32 size;	/* write(2) len */
	crc_t crc;	/* crc of write data */
	granting mds server id
};
#endif

struct srm_io_req {
//	struct srm_io_secret crypt_i;
	struct slash_fidgen fg; /* XXX go away */
	u64 cfd;
	u32 size;
	u32 offset;
	u32 flags;
	u32 op; /* rw */
};

#define SRMIO_RD 0
#define SRMIO_WR 1

/* WRITE data is bulk request. */

struct srm_io_rep {
	s32 rc;
	u32 size;
};

/* READ data is in bulk reply. */

struct srm_readdir_req {
	struct slash_creds creds;
	u64 cfd;
	u64 offset;
	u64 size;
};

struct srm_readdir_rep {
	s32 rc;
	u64 size;
	u32 num; /* how many dirents were returned */
	/* accompanied by bulk data in pure getdents(2) format */
};

struct srm_readlink_req {
	struct slash_creds creds;
	u64 ino;
};

struct srm_readlink_rep {
	char buf[PATH_MAX+1];
	u32 len;
	s32 rc;
};

struct srm_release_req {
	struct slash_creds creds;
	u64 cfd;
	u32 flags;
};

struct srm_releasebmap_req {
};

struct srm_rename_req {
	struct slash_creds creds;
	u64 npino, opino;
	u32 fromlen;
	u32 tolen;
};


#define srm_unlink_rep srm_generic_rep

struct srm_setattr_req {
	struct slash_creds creds;
	u64 cfd;
	u64 ino;
	struct stat attr;
	int to_set;
};

#define srm_setattr_rep srm_getattr_rep

struct srm_statfs_req {
	struct slash_creds creds;
};

struct srm_statfs_rep {
	struct statvfs stbv;
	s32 rc;
};

struct srm_symlink_req {
	struct slash_creds creds;
	char name[NAME_MAX];
	u32 namelen;
	u32 linklen;
	u64 pino;
};

struct srm_symlink_rep {
	struct slash_creds creds;
	struct slash_fidgen fg;
	struct stat attr;
	s32 rc;
};

struct srm_unlink_req {
	struct slash_creds creds;
	char name[NAME_MAX];
	int len;
	u64 pino;
};

struct srm_generic_rep {
	u64 data;
	s32 rc;
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

#define p_ntoh_u16(v) ntoh16(v)
#define p_ntoh_u32(v) ntoh32(v)

#define p_hton_u16(v) hton16(v)
#define p_hton_u32(v) hton32(v)

#define p_ntoh_s16(v) ntoh16(v)
#define p_ntoh_s32(v) ntoh32(v)

#define p_hton_s16(v) hton16(v)
#define p_hton_s32(v) hton32(v)

#if __BYTE_ORDER == __LITTLE_ENDIAN
# define p_ntoh_u64(v)		((((v) & UINT64_C(0x00000000000000ff)) << 56) |	\
				 (((v) & UINT64_C(0x000000000000ff00)) << 40) |	\
				 (((v) & UINT64_C(0x0000000000ff0000)) << 24) |	\
				 (((v) & UINT64_C(0x00000000ff000000)) <<  8) |	\
				 (((v) & UINT64_C(0x000000ff00000000)) >>  8) |	\
				 (((v) & UINT64_C(0x0000ff0000000000)) >> 24) |	\
				 (((v) & UINT64_C(0x00ff000000000000)) >> 40) |	\
				 (((v) & UINT64_C(0xff00000000000000)) >> 56))
# define p_hton_u64(v) p_ntoh_u64(v)

# define p_ntoh_s64(v) ERROR
# define p_hton_s64(v) ERROR
#else
# define p_ntoh_u64(v) (v)
# define p_hton_u64(v) (v)

# define p_ntoh_s64(v) (v)
# define p_hton_s64(v) (v)
#endif

extern lnet_process_id_t lpid;

#endif
