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

#define SL_PATH_PREFIX		".sl"
#define SL_PATH_REPLS		".slrepls"
#define SL_PATH_FIDNS		".slfidns"

#if 0
#define _PATH_SLASHCONF		"/etc/slash.conf"
#endif

#define _PATH_SLASHCONF		"../slashd/config/example.conf"
#define _PATH_SLMCTLSOCK	"../slashd.%h.sock"
#define _PATH_SLICTLSOCK	"../sliod.%h.sock"
#define _PATH_MSCTLSOCK		"../mount_slash.%h.sock"

#define _RELPATH_SLODTABLE	"ion_bmaps.odt"
#define _RELPATH_SLJOURNAL	"slopjrnl"

#define _PATH_SLASHD_DIR	"/var/lib/slashd"

#endif /* _SLASH_PATHNAMES_H_ */
