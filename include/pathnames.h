/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2012, Pittsburgh Supercomputing Center (PSC).
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

#ifndef _SLASH_PATHNAMES_H_
#define _SLASH_PATHNAMES_H_

/* configuration/control socket paths */
#if DEVELPATHS
# define SL_PATH_RUNTIME_DIR	".."
# define SL_PATH_ETC_DIR	"../config/"
#else
# define SL_PATH_RUNTIME_DIR	"/var/run"
# define SL_PATH_ETC_DIR	"/etc/slash"
#endif

#define SL_PATH_SLMCTLSOCK	SL_PATH_RUNTIME_DIR"/slashd.%h.sock"
#define SL_PATH_SLICTLSOCK	SL_PATH_RUNTIME_DIR"/sliod.%h.sock"
#define SL_PATH_MSCTLSOCK	SL_PATH_RUNTIME_DIR"/mount_slash.%h.sock"

/* runtime/data paths */
#define SL_PATH_DATA_DIR	"/var/lib/slash"

/* runtime files */
#define SL_FN_AUTHBUFKEY	"authbuf.key"
#define SL_FN_OPJOURNAL		"op-journal"
#define SL_FN_UPSCHDB		"upsch.db"

/* configuration files */
#define SL_PATH_CONF		SL_PATH_ETC_DIR"/slcfg"

/*
 * SLASH2 internal files and directories that enable us to
 *	(1) find out the last ZFS commit transaction group number;
 *	(2) register the replication work that needs to be done;
 *	(3) access SLASH2 files by SLASH2 FIDs.
 */
#define SL_RPATH_META_DIR	".slmd"

#define SL_RPATH_FIDNS_DIR	"fidns"
#define SL_RPATH_TMP_DIR	"tmp"

#define SL_FN_BMAP_ODTAB	"bmap.odtab"
#define SL_FN_REPL_ODTAB	"repl.odt"
#define SL_FN_PTRUNC_ODTAB	"ptrunc.odt"

#define SL_FN_UPDATELOG		"op-update"
#define SL_FN_UPDATEPROG	"op-update-prog"

#define SL_FN_RECLAIMLOG	"op-reclaim"
#define SL_FN_RECLAIMPROG	"op-reclaim-prog"

#define SL_FN_CURSOR		"cursor"
#define SL_FN_FSUUID		"fsuuid"

extern const char *sl_datadir;

#endif /* _SLASH_PATHNAMES_H_ */
