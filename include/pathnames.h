/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015, Google, Inc.
 * Copyright (c) 2006-2015, Pittsburgh Supercomputing Center (PSC).
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or (at
 * your option) any later version.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License contained in the file
 * `COPYING-GPL' at the top of this distribution or at
 * https://www.gnu.org/licenses/gpl-2.0.html for more details.
 * ---------------------------------------------------------------------
 * %END_LICENSE%
 */

#ifndef _SL_PATHNAMES_H_
#define _SL_PATHNAMES_H_

#include <paths.h>

/* configuration/control socket paths */
#if DEVELPATHS
# define SL_PATH_RUNTIME_DIR	".."
# define SL_PATH_ETC_DIR	"../config/"
#else
# define SL_PATH_RUNTIME_DIR	"/var/run"
# define SL_PATH_ETC_DIR	"/etc/slash"
#endif

#define SL_PATH_DEV_SHM		"/dev/shm"

#define SL_PATH_SLMCTLSOCK	SL_PATH_RUNTIME_DIR"/slashd.%h.sock"
#define SL_PATH_SLICTLSOCK	SL_PATH_RUNTIME_DIR"/sliod.%h.sock"
#define SL_PATH_MSCTLSOCK	SL_PATH_RUNTIME_DIR"/mount_slash.%h.sock"

/* runtime/data paths */
#define SL_PATH_DATA_DIR	"/var/lib/slash"

/* runtime files */
#define SL_FN_AUTHBUFKEY	"authbuf.key"
#define SL_FN_MAPFILE		"mapfile"
#define SL_FN_OPJOURNAL		"op-journal"
#define SL_FN_UPSCHDB		"upsch.db"

/* configuration files */
#define SL_PATH_CONF		SL_PATH_ETC_DIR"/slcfg"

/*
 * SLASH2 internal files and directories that enable us to:
 *	(1) find out the last ZFS commit transaction group number;
 *	(2) access SLASH2 files by SLASH2 FIDs.
 */
#define SL_RPATH_META_DIR	".slmd"

#define SL_RPATH_FIDNS_DIR	"fidns"
#define SL_RPATH_TMP_DIR	"tmp"

/*
 * The following was introduced by commit:
 *
 * commit 8d6ee7efa9aa4e4f8fea97ba9b5fe1c7a11102bc
 * Author: Jared Yanovich <yanovich@psc.edu>
 *
 */

#define SL_FN_BMAP_ODTAB	"bmap.odtab"

#define SL_FN_UPDATELOG		"op-update"
#define SL_FN_UPDATEPROG	"op-update-prog"

#define SL_FN_RECLAIMLOG	"op-reclaim"
#define SL_FN_RECLAIMPROG	"op-reclaim-prog"

#define SL_FN_CURSOR		"cursor"
#define SL_FN_FSUUID		"fsuuid"
#define SL_FN_RESID		"resid"

extern const char *sl_datadir;

#endif /* _SL_PATHNAMES_H_ */
