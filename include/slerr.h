/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2012, Pittsburgh Supercomputing Center (PSC).
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

#ifndef _SLERR_H_
#define _SLERR_H_

char *slstrerror(int);

#define _SLERR_START			1000		/* must be >max errno */
#define SLERR_REPL_ALREADY_ACT		(_SLERR_START)
#define SLERR_REPL_NOT_ACT		(_SLERR_START + 1)
#define SLERR_REPL_ALREADY_INACT	(_SLERR_START + 2)
#define SLERR_REPLS_ALL_INACT		(_SLERR_START + 3)
#define SLERR_BMAP_INVALID		(_SLERR_START + 4)
#define SLERR_BMAP_DIOWAIT		(_SLERR_START + 5)
#define SLERR_BMAP_ZERO			(_SLERR_START + 6)
#define SLERR_RES_UNKNOWN		(_SLERR_START + 7)
#define SLERR_IOS_UNKNOWN		(_SLERR_START + 8)
#define SLERR_ION_UNKNOWN		(_SLERR_START + 9)
#define SLERR_ION_OFFLINE		(_SLERR_START + 10)
#define SLERR_ION_NOTREPL		(_SLERR_START + 11)
#define SLERR_XACT_FAIL			(_SLERR_START + 12)
#define SLERR_SHORTIO			(_SLERR_START + 13)
#define SLERR_AUTHBUF_BADMAGIC		(_SLERR_START + 14)
#define SLERR_AUTHBUF_BADPEER		(_SLERR_START + 15)
#define SLERR_AUTHBUF_BADHASH		(_SLERR_START + 16)
#define SLERR_AUTHBUF_ABSENT		(_SLERR_START + 17)
#define SLERR_USER_NOTFOUND		(_SLERR_START + 18)
#define SLERR_BADCRC			(_SLERR_START + 19)
#define SLERR_GEN_OLD			(_SLERR_START + 20)
#define SLERR_GEN_INVALID		(_SLERR_START + 21)
#define SLERR_NOTCONN			(_SLERR_START + 22)
#define SLERR_BMAP_IN_PTRUNC		(_SLERR_START + 23)
#define SLERR_BMAP_PTRUNC_STARTED	(_SLERR_START + 24)
#define SLERR_AIOWAIT			(_SLERR_START + 25)
#define SLERR_REIMPORT_OLD		(_SLERR_START + 26)
#define SLERR_IMPORT_XREPL_DIFF		(_SLERR_START + 27)
#define SLERR_BMAP_LEASEEXT_FAILED	(_SLERR_START + 28)
#define SLERR_BMAP_LEASEREASSIGN_FAILED	(_SLERR_START + 29)
#define SLERR_RES_BADTYPE		(_SLERR_START + 30)
#define SLERR_ALREADY			(_SLERR_START + 31)
#define SLERR_INVAL			(_SLERR_START + 32)
#define SLERR_2BIG			(_SLERR_START + 33)
#define SLERR_CRCABSENT			(_SLERR_START + 34)

#endif /* _SLERR_H_ */
