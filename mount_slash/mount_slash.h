/* $Id$ */

#include <sys/types.h>

#include <stdarg.h>

#include <fuse.h>

#include "psc_types.h"
#include "psc_ds/tree.h"
#include "psc_rpc/rpc.h"

struct pscrpc_request;
struct pscrpc_export;

/* RPC services. */
#define RPCSVC_MDS		0
#define RPCSVC_IO		1
#define NRPCSVC			2

/* RPC portals. */
#define RPCMDS_REQ_PORTAL	20
#define RPCMDS_REP_PORTAL	21
#define RPCIO_REQ_PORTAL	22
#define RPCIO_REP_PORTAL	23
#define RPCIO_BULK_PORTAL	24

#define SMDS_VERSION		1
#define SMDS_MAGIC		0xaabbccddeeff0011ULL

#define SIO_VERSION		1
#define SIO_MAGIC		0xaabbccddeeff0011ULL

SPLAY_HEAD(rctree, readdir_cache_ent);

struct slashrpc_service {
	struct pscrpc_import	 *svc_import;
	psc_spinlock_t		  svc_lock;
	struct psclist_head	  svc_old_imports;
	int			  svc_failed;
	int			  svc_initialized;
	int			(*svc_connect)(lnet_nid_t);
};

struct slashrpc_export {
	uid_t				 uid;
	gid_t				 gid;
	psc_spinlock_t			 rclock;	/* readdir cache lock */
	struct rctree			 rctree;
};

struct readdir_cache_ent {
	void				*buf;
	fuse_fill_dir_t			 filler;
	off_t				 offset;
	u64				 cfd;
	SPLAY_ENTRY(readdir_cache_ent)	 entry;
};

int  rce_cmp(const void *, const void *);
void rc_add(struct readdir_cache_ent *, struct pscrpc_export *);
void rc_remove(struct readdir_cache_ent *, struct pscrpc_export *);
struct readdir_cache_ent *
     rc_lookup(struct pscrpc_export *, u64, u64);

int rpc_svc_init(void);
int rpc_newreq(int, int, int, int, int, struct pscrpc_request **, void *);
int rpc_getrep(struct pscrpc_request *, int, void *);
int rpc_sendmsg(int, ...);
int rpc_connect(lnet_nid_t, int, u64, u32);

int slash_read(const char *, char *, size_t, off_t, struct fuse_file_info *);
int slash_readdir(const char *, void *, fuse_fill_dir_t, off_t, struct fuse_file_info *);
int slash_write(const char *, const char *, size_t, off_t, struct fuse_file_info *);

#define slashrpc_mds_connect(svr) rpc_connect(svr, RPCSVC_MDS, SMDS_MAGIC, SMDS_VERSION)
#define slashrpc_io_connect(svr)  rpc_connect(svr, RPCSVC_IO, SIO_MAGIC, SIO_VERSION)

SPLAY_PROTOTYPE(rctree, readdir_cache_ent, entry, rce_cmp);
