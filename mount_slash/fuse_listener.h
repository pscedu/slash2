/* $Id$ */

#ifndef _FUSE_LISTENER_H_
#define _FUSE_LISTENER_H_

#include "msl_fuse.h"

#define FUSE_OPTIONS "fsname=%s,allow_other,suid,dev"

void	slash2fuse_listener_exit(void);
int	slash2fuse_listener_init(void);
int	slash2fuse_listener_start(void);
int	slash2fuse_newfs(const char *, struct fuse_chan *);

extern int exit_fuse_listener;

#endif /* _FUSE_LISTENER_H_ */
