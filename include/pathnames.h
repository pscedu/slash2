/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2011, Pittsburgh Supercomputing Center (PSC).
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

/*
 * SLASH2 internal files and directories that enable us to (1) find out the
 * last ZFS commit transaction group number; (2) register the replication
 * work that needs to be done; (3) access SLASH2 files by SLASH2 FIDs.
 */
#define SL_PATH_PREFIX		".sl"
#define SL_PATH_UPSCH		".slupsch"
#define SL_PATH_FIDNS		".slfidns"
#define SL_PATH_CURSOR		".slcursor"

/* configuration/control socket paths */
#if 0
#define SL_PATH_CONF		"/etc/slash.conf"
#define SL_PATH_SLMCTLSOCK	"/var/run/slash/slashd.%h.sock"
#define SL_PATH_SLICTLSOCK	"/var/run/slash/sliod.%h.sock"
#define SL_PATH_MSCTLSOCK	"/var/run/slash/mount_slash.%h.sock"
#endif

#define SL_PATH_CONF		"../config/example.conf"
#define SL_PATH_SLMCTLSOCK	"../slashd.%h.sock"
#define SL_PATH_SLICTLSOCK	"../sliod.%h.sock"
#define SL_PATH_MSCTLSOCK	"../mount_slash.%h.sock"

/* runtime/data paths */
#define SL_PATH_DATADIR		"/var/lib/slash"

#define SL_FN_AUTHBUFKEY	"authbuf.key"
#define SL_FN_IONBMAPS_ODT	"ion_bmaps.odt"
#define SL_FN_OPJOURNAL		"op-journal"

#define SL_FN_UPDATELOG		"op-update"
#define SL_FN_UPDATEPROG	"op-update-prog"

#define SL_FN_RECLAIMLOG	"op-reclaim"
#define SL_FN_RECLAIMPROG	"op-reclaim-prog"

extern const char *sl_datadir;

#endif /* _SLASH_PATHNAMES_H_ */
