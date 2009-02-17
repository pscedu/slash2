/* $Id$ */

#include <sys/types.h>

#include "psc_types.h"

#include "../slashd/cfd.h"
#include "slashexport.h"

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

extern struct cfd_svrops *cfdOps;

int slrim_handler(struct pscrpc_request *);
int slric_handler(struct pscrpc_request *);
int slrii_handler(struct pscrpc_request *);

extern struct slashrpc_cservice *rmi_csvc;
