/* $Id$ */

#ifndef _SL_TYPES_H_
#define _SL_TYPES_H_

typedef uint32_t sl_mds_id_t;
typedef uint32_t sl_blkno_t;
typedef uint32_t sl_bmapno_t;		/* bmap index number */
typedef uint32_t sl_ios_id_t;

#define IOS_ID_ANY	(~(sl_ios_id_t)0)
#define BLKNO_ANY	(~(sl_blkno_t)0)

#endif /* _SL_TYPES_H_ */
