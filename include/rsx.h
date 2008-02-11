/* $Id$ */

struct pscrpc_request;
struct pscrpc_import;

int rsx_newreq(struct pscrpc_import *, int, int, int, int, struct pscrpc_request **, void *);
int rsx_getrep(struct pscrpc_request *, int, void *);
