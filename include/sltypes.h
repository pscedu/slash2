/* $Id$ */

#ifndef _SL_TYPES_H_
#define _SL_TYPES_H_

typedef uint32_t sl_blkno_t;		/* deprecated */
typedef uint32_t sl_bmapno_t;		/* bmap index number */

typedef uint16_t sl_siteid_t;
typedef uint32_t sl_ios_id_t;

typedef uint64_t sl_ino_t;

#define BLKNO_ANY	(~(sl_blkno_t)0)	/* deprecated */
#define BMAPNO_ANY	((sl_bmapno_t)~0U)

#define IOS_ID_ANY	((sl_ios_id_t)~0U)
#define SITE_ID_ANY	((sl_siteid_t)~0U)

/* breakdown of I/O system ID: # of bits for each part */
#define SL_SITE_BITS		16
#define SL_RES_BITS		16

#define SL_SITE_MASK		0xffff0000
#define SL_RES_MASK		0x0000ffff	/* resource mask */

#endif /* _SL_TYPES_H_ */
