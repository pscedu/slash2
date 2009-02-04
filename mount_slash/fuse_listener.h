/*
 * CDDL HEADER START
 *
 * The contents of this file are subject to the terms of the
 * Common Development and Distribution License (the "License").
 * You may not use this file except in compliance with the License.
 *
 * You can obtain a copy of the license at usr/src/OPENSOLARIS.LICENSE
 * or http://www.opensolaris.org/os/licensing.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * When distributing Covered Code, include this CDDL HEADER in each
 * file and include the License file at usr/src/OPENSOLARIS.LICENSE.
 * If applicable, add the following below this CDDL HEADER, with the
 * fields enclosed by brackets "[]" replaced with your own identifying
 * information: Portions Copyright [yyyy] [name of copyright owner]
 *
 * CDDL HEADER END
 */
/*
 * Copyright 2006 Ricardo Correia.
 * Use is subject to license terms.
 */

#ifndef SLASH2FUSE_LISTENER_H
#define SLASH2FUSE_LISTENER_H 1

#include "msl_fuse.h"

#define FUSE_OPTIONS "fsname=%s,allow_other,suid,dev,debug"
//#define FUSE_OPTIONS "fsname=%s,allow_other,suid,dev"

extern int exit_fuse_listener;

extern int slash2fuse_listener_init();
extern int slash2fuse_listener_start();
extern void slash2fuse_listener_exit();
extern int slash2fuse_newfs(const char *, struct fuse_chan *);

#endif
