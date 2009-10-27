/* $Id$ */

#ifndef _SLERR_H_
#define _SLERR_H_

char *slstrerror(int);

#define _SLERR_START		1000
#define SLERR_REPL_ACT		(_SLERR_START)
#define SLERR_INVALID_BMAP	(_SLERR_START + 1)
#define SLERR_UNKNOWN_IOS	(_SLERR_START + 2)

#endif /* _SLERR_H_ */
