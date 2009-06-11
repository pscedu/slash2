/* $Id$ */

#include <sys/types.h>

#include "psc_types.h"

#include "../slashd/cfd.h"

#define SRIM_NTHREADS	8
#define SRIM_NBUFS	1024
#define SRIM_BUFSZ	256
#define SRIM_REPSZ	256
#define SRIM_SVCNAME	"slrim"

#define SRIC_NTHREADS	8
#define SRIC_NBUFS	1024
#define SRIC_BUFSZ	(4096 + 256)
#define SRIC_REPSZ	128
#define SRIC_SVCNAME	"slric"

#define SRII_NTHREADS	8
#define SRII_NBUFS	1024
#define SRII_BUFSZ	(4096 + 256)
#define SRII_REPSZ	128
#define SRII_SVCNAME	"slrii"

extern struct cfd_svrops *cfdOps;

/* SERVER_CLIENT */
enum slash_exp_types {
        MDS_ION_EXP = (1<<0),
        MDS_CLI_EXP = (1<<1),
	MDS_MDS_EXP = (1<<2),
	ION_CLI_EXP = (1<<3),
	ION_MDS_EXP = (1<<4),
	ION_ION_EXP = (1<<5),
	CLI_MDS_EXP = (1<<6)
};

struct slashrpc_export {
	u64                  sexp_conn_gen;
	u64                  sexp_nextcfd;
	struct cfdtree       sexp_cfdtree;
	enum slash_exp_types sexp_type;
	void                *sexp_data;
};

void	rpc_initsvc(void);

int slrim_handler(struct pscrpc_request *);
int slric_handler(struct pscrpc_request *);
int slrii_handler(struct pscrpc_request *);

extern struct slashrpc_cservice *rmi_csvc;
