/* $Id$ */

#ifndef _SLERR_H_
#define _SLERR_H_

char *slstrerror(int);

#define _SLERR_START			1000		/* must be >max errno */
#define SLERR_REPL_ALREADY_ACT		(_SLERR_START)
#define SLERR_REPL_ALREADY_INACT	(_SLERR_START + 1)
#define SLERR_REPLS_ALL_INACT		(_SLERR_START + 2)
#define SLERR_INVALID_BMAP		(_SLERR_START + 3)
#define SLERR_UNKNOWN_IOS		(_SLERR_START + 4)

#endif /* _SLERR_H_ */
