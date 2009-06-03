/* $Id$ */

#ifndef _SLASH_MDS_H_
#define _SLASH_MDS_H_

#include "fidcache.h"
#include "mdsexpc.h"
#include "slashrpc.h"

int  mds_bmap_crc_write(struct srm_bmap_crcup *, lnet_nid_t);
int  mds_bmap_load(struct mexpfcm *, struct srm_bmap_req *, struct bmapc_memb **);
void mds_bmap_repl_log(struct bmapc_memb *);

#endif /* _SLASH_MDS_H_ */
