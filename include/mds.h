#ifndef MDS_H
#define MDS_H 1

#include "mdsexpc.h"
#include "slashrpc.h"
#include "fidcache.h"

int mds_bmap_load(struct mexpfcm *, struct srm_bmap_req *, struct bmapc_memb **);

int mds_bmap_crc_write(struct srm_bmap_crcup *, lnet_nid_t);


#endif
