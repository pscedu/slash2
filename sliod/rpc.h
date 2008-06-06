/* $Id$ */

#include <sys/types.h>

#include "psc_types.h"
#include "../slashd/cfd.h"

#define SRIM_NTHREADS	8
#define SRIM_NBUFS	1024
#define SRIM_BUFSZ	256
#define SRIM_REPSZ	256
#define SRIM_SVCNAME	"slrimthr"

#define SRIC_NTHREADS	8
#define SRIC_NBUFS	1024
#define SRIC_BUFSZ	(4096 + 256)
#define SRIC_REPSZ	128
#define SRIC_SVCNAME	"slricthr"

#define SRII_NTHREADS	8
#define SRII_NBUFS	1024
#define SRII_BUFSZ	(4096 + 256)
#define SRII_REPSZ	128
#define SRII_SVCNAME	"slriithr"

struct slashrpc_cservice {
	struct pscrpc_import	 *csvc_import;
	psc_spinlock_t		  csvc_lock;
	struct psclist_head	  csvc_old_imports;
	int			  csvc_failed;
	int			  csvc_initialized;
};

struct slashrpc_export {
};

struct slashrpc_cservice *
	rpc_csvc_create(u32, u32);
void	rpc_svc_init(void);
int	rpc_issue_connect(lnet_nid_t, struct pscrpc_import *, u64, u32);

int slrim_handler(struct pscrpc_request *);
int slric_handler(struct pscrpc_request *);
int slrii_handler(struct pscrpc_request *);

extern struct slashrpc_cservice *rim_csvc;
