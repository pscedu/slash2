#ifndef __CACPARAMS_H__
#define __CACPARAMS_H__

#define FIDC_CLI_HASH_SZ 1023
#define FIDC_ION_HASH_SZ 4095
#define FIDC_MDS_HASH_SZ 32767

enum fid_cache_users {
	FIDC_USER_CLI = 0,
	FIDC_USER_ION = 1,
	FIDC_USER_MDS = 2
};

/* Hand computed */
#define FIDC_MDS_DEFSZ 32768   /* Number of fcmh's to allocate by default */
#define FIDC_MDS_MAXSZ 1048576 /* Max fcmh's */

#define FIDC_CLI_DEFSZ 1024   /* Number of fcmh's to allocate by default */
#define FIDC_CLI_MAXSZ 131072 /* Max fcmh's */

#define FIDC_ION_DEFSZ 4096   /* Number of fcmh's to allocate by default */
#define FIDC_ION_MAXSZ 524288 /* Max fcmh's */

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
