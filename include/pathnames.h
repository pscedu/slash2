/* $Id$ */

#ifndef _SLASH_PATHNAMES_H_
#define _SLASH_PATHNAMES_H_

#define SL_PATH_PREFIX		".sl"
#define SL_PATH_REPLS		".slrepls"
#define SL_PATH_FIDNS		".slfidns"

#if 0
#define _PATH_SLASHCONF		"/etc/slash.conf"
#define _PATH_SLCTLSOCK		"/var/run/slashd.%h.sock"
#define _PATH_SLIOCTLSOCK	"/var/run/sliod.%h.sock"
#define _PATH_MSCTLSOCK		"/var/run/mount_slash.%h.sock"
#endif

#define _PATH_SLASHCONF		"../slashd/config/example.conf"
#define _PATH_SLCTLSOCK		"../slashd.%h.sock"
#define _PATH_SLIOCTLSOCK	"../sliod.%h.sock"
#define _PATH_MSCTLSOCK		"../mount_slash.%h.sock"

#define _PATH_SLODTABLE		"/var/lib/slashd/ion_bmaps.odt"
#define _PATH_SLJOURNAL		"/var/lib/slashd/slopjrnl"
#define _PATH_SLASHD_DIR	"/var/lib/slashd"

#endif /* _SLASH_PATHNAMES_H_ */
