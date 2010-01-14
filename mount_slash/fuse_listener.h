/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2009, Pittsburgh Supercomputing Center (PSC).
 *
 * Permission to use, copy, and modify this software and its documentation
 * without fee for personal use or non-commercial use within your organization
 * is hereby granted, provided that the above copyright notice is preserved in
 * all copies and that the copyright and this permission notice appear in
 * supporting documentation.  Permission to redistribute this software to other
 * organizations or individuals is not permitted without the written permission
 * of the Pittsburgh Supercomputing Center.  PSC makes no representations about
 * the suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

#ifndef _FUSE_LISTENER_H_
#define _FUSE_LISTENER_H_

#include <fuse/fuse_lowlevel.h>

#define FUSE_OPTIONS "fsname=%s,allow_other,suid,dev,max_write=131072"

void	slash2fuse_listener_exit(void);
int	slash2fuse_listener_init(void);
int	slash2fuse_listener_start(void);
int	slash2fuse_newfs(const char *, struct fuse_chan *);

extern int exit_fuse_listener;

#endif /* _FUSE_LISTENER_H_ */
