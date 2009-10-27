/* $Id$ */

#ifndef _SL_MKFN_H_
#define _SL_MKFN_H_

int	mkfnv(char[PATH_MAX], const char *, va_list);
int	mkfn(char[PATH_MAX], const char *, ...);
void	xmkfn(char[PATH_MAX], const char *, ...);

#endif /* _SL_MKFN_H_ */
