/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
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

/* immutable namespace paths */
#define SL_PATH_PREFIX		".sl"
#define SL_PATH_REPLS		".slrepls"
#define SL_PATH_FIDNS		".slfidns"

/* configuration/control socket paths */
#if 0
#define SL_PATH_CONF		"/etc/slash.conf"
#define SL_PATH_SLMCTLSOCK	"/var/run/slash/slashd.%h.sock"
#define SL_PATH_SLICTLSOCK	"/var/run/slash/sliod.%h.sock"
#define SL_PATH_MSCTLSOCK	"/var/run/slash/mount_slash.%h.sock"
#endif

#define SL_PATH_CONF		"../slashd/config/example.conf"
#define SL_PATH_SLMCTLSOCK	"../slashd.%h.sock"
#define SL_PATH_SLICTLSOCK	"../sliod.%h.sock"
#define SL_PATH_MSCTLSOCK	"../mount_slash.%h.sock"

/* runtime/data paths */
#define SL_PATH_DATADIR		"/var/lib/slashd"

#define SL_FN_DESCBUFKEY	"descbuf.key"
#define SL_FN_IONBMAPS_ODT	"ion_bmaps.odt"
#define SL_FN_OPJOURNAL		"op-journal"

extern const char *sl_datadir;

#endif /* _SLASH_PATHNAMES_H_ */
