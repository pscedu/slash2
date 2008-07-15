#ifndef __CACPARAMS_H__
#define __CACPARAMS_H__

/* Hand computed */
#define MDS_FID_CACHE_DEFSZ 1024   /* Number of fcmh's to allocate by default */
#define MDS_FID_CACHE_MAXSZ 131072 /* Max fcmh's */

#define SLASH_BMAP_SIZE  134217728
#define SLASH_BMAP_WIDTH 8
#define SLASH_BMAP_DEPTH 5
#define SLASH_BMAP_SHIFT 11
/* End hand computed */

#define SLASH_BMAP_BLKSZ (SLASH_BMAP_SIZE / power((size_t)SLASH_BMAP_WIDTH, \
						  (size_t)(SLASH_BMAP_DEPTH-1)))
#define SLASH_BMAP_BLKMASK ~(SLASH_BMAP_BLKSZ-1)

#define SLASH_MAXBLKS_PER_REQ (LNET_MTU / SLASH_BMAP_BLKSZ)

#define BMAP_MAX_GET 63

#endif
