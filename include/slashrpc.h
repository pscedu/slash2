/* $Id$ */

/*
 * Slash RPC subsystem definitions including messages definitions.
 * Note: many of the messages will take on different sizes between
 * 32-bit and 64-bit machine architectures.
 */

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

#define SRIM_VERSION		1
#define SRIM_MAGIC		0xaabbccddeeff0088ULL

/* Slash RPC message types. */
enum {
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

struct srm_bmap_req {
	u32 pios;       /* preferred ios (provided by client)     */
	u64 cfd;
	u32 blkno;	/* Starting block number                  */
	u32 nblks;	/* Read-ahead support                     */
	u64 fid;	/* Optional, may be filled in server-side */
	u32 rw;
};

struct srm_bmap_rep {
	u32 nblks;	/* The number of bmaps actually returned  */
};

struct srm_bmap_mode_req {
	u64 cfd;
	u32 blkno;
	u32 rw;
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
	u32 fnlen;
	u32 mode;
	u32 flags;
};

struct srm_create_rep {
//	struct srm_open_secret sig_m;
struct slash_fidgen fg; /* XXX for now, put in dsig later */
	u64 cfd;
	s32 rc;
};

struct srm_chmod_req {
	struct slash_creds creds;
	u32 fnlen;
	u32 mode;
};

struct srm_chown_req {
	struct slash_creds creds;
	u32 fnlen;
	u32 uid;
	u32 gid;
};

struct srm_destroy_req {
};

#define srm_disconnect_req srm_destroy_req

struct srm_getattr_req {
	struct slash_creds creds;
	u32 fnlen;
};

struct srm_getattr_rep {
	u64 size;
	u64 atime;
	u64 mtime;
	u64 ctime;
	s32 rc;
	u32 mode;
	u32 nlink;
	u32 uid;
	u32 gid;
};

struct srm_fgetattr_req {
	u64 cfd;
};

#define srm_fgetattr_rep srm_getattr_rep

struct srm_ftruncate_req {
	u64 cfd;
	u64 size;
};

struct srm_link_req {
	struct slash_creds creds;
	u32 fromlen;
	u32 tolen;
};

struct srm_mkdir_req {
	struct slash_creds creds;
	u32 fnlen;
	u32 mode;
};

struct srm_mknod_req {
	struct slash_creds creds;
	u32 fnlen;
	u32 mode;
	u32 dev;
};

struct srm_open_req {
	struct slash_creds creds;
	u32 fnlen;
	u32 flags;
};

struct srm_open_rep {
//	struct srm_open_secret sig_m;
struct slash_fidgen fg; /* XXX for now, put in dsig later */
	u64 cfd;
	s32 rc;
};

struct srm_opendir_req {
	struct slash_creds creds;
	u32 fnlen;
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
	u64 cfd;
	u64 offset;
};

struct srm_readdir_rep {
	s32 rc;
	u32 size;
	/* accompanied by bulk data in pure getdents(2) format */
};

struct srm_readlink_req {
	struct slash_creds creds;
	u32 fnlen;
	u32 size;
};

struct srm_readlink_rep {
	s32 rc;
};

struct srm_release_req {
	u64 cfd;
};

struct srm_releasedir_req {
	u64 cfd;
};

struct srm_releasebmap_req {
};

struct srm_rename_req {
	struct slash_creds creds;
	u32 fromlen;
	u32 tolen;
};

struct srm_rmdir_req {
	struct slash_creds creds;
	u32 fnlen;
};

struct srm_statfs_req {
	struct slash_creds creds;
	u32 fnlen;
};

struct srm_statfs_rep {
	u64 f_files;
	u64 f_ffree;
	u32 f_bsize;
	u32 f_blocks;
	u32 f_bfree;
	u32 f_bavail;
	s32 rc;
};

struct srm_symlink_req {
	struct slash_creds creds;
	u32 fromlen;
	u32 tolen;
};

struct srm_truncate_req {
	struct slash_creds creds;
	u64 size;
	u32 fnlen;
};

struct srm_unlink_req {
	struct slash_creds creds;
	u32 fnlen;
};

struct srm_utimes_req {
	struct slash_creds creds;
	struct timeval times[2];
	u32 fnlen;
};

struct srm_generic_rep {
	s32 rc;
};

struct slashrpc_cservice {
	struct pscrpc_import	*csvc_import;
	psc_spinlock_t		 csvc_lock;
	struct psclist_head	 csvc_old_imports;
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
# define p_ntoh_u64(v)		((((v) & U64CONST(0x00000000000000ff)) << 56) |	\
				 (((v) & U64CONST(0x000000000000ff00)) << 40) |	\
				 (((v) & U64CONST(0x0000000000ff0000)) << 24) |	\
				 (((v) & U64CONST(0x00000000ff000000)) <<  8) |	\
				 (((v) & U64CONST(0x000000ff00000000)) >>  8) |	\
				 (((v) & U64CONST(0x0000ff0000000000)) >> 24) |	\
				 (((v) & U64CONST(0x00ff000000000000)) >> 40) |	\
				 (((v) & U64CONST(0xff00000000000000)) >> 56))
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
