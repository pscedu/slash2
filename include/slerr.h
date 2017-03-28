/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright (c) 2009-2014, Pittsburgh Supercomputing Center (PSC).
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

#ifndef _SLERR_H_
#define _SLERR_H_

#include <sys/errno.h>

#include "pfl/err.h"

#define _SLERR_START			1000		/* must be >max errno */

#if defined(ELAST) && ELAST >= _SLERR_START
#  error system error codes into application space, need to adjust and recompile
#endif

#define SLERR_REPL_ALREADY_ACT		(_SLERR_START +  0)
#define SLERR_REPL_NOT_ACT		(_SLERR_START +  1)
#define SLERR_RPCIO			(_SLERR_START +  2)
#define SLERR_REPLICA_STATE_INVALID	(_SLERR_START +  3)
#define SLERR_BMAP_INVALID		(_SLERR_START +  4)
#define SLERR_BMAP_DIOWAIT		(_SLERR_START +  5)
#define SLERR_BMAP_ZERO			(_SLERR_START +  6)		/* unused */
#define SLERR_RES_UNKNOWN		(_SLERR_START +  7)
#define SLERR_IOS_UNKNOWN		(_SLERR_START +  8)
#define SLERR_ION_UNKNOWN		(_SLERR_START +  9)
#define SLERR_ION_OFFLINE		(_SLERR_START + 10)
#define SLERR_LASTREPL			(_SLERR_START + 11)
#define SLERR_XACT_FAIL			(_SLERR_START + 12)
#define SLERR_SHORTIO			(_SLERR_START + 13)
#define SLERR_AUTHBUF_BADMAGIC		(_SLERR_START + 14)
#define SLERR_AUTHBUF_BADPEER		(_SLERR_START + 15)
#define SLERR_AUTHBUF_BADHASH		(_SLERR_START + 16)
#define SLERR_AUTHBUF_ABSENT		(_SLERR_START + 17)
#define SLERR_USER_NOTFOUND		(_SLERR_START + 18)
/* 19 - reuse */
#define SLERR_GEN_OLD			(_SLERR_START + 20)
#define SLERR_GEN_INVALID		(_SLERR_START + 21)
/* 22 - reuse */
#define SLERR_BMAP_IN_PTRUNC		(_SLERR_START + 23)
#define SLERR_BMAP_PTRUNC_STARTED	(_SLERR_START + 24)
#define SLERR_AIOWAIT			(_SLERR_START + 25)
#define SLERR_REIMPORT_OLD		(_SLERR_START + 26)
#define SLERR_IMPORT_XREPL_DIFF		(_SLERR_START + 27)
#define SLERR_ION_READONLY		(_SLERR_START + 28)
/* 29 - reuse */
#define SLERR_RES_BADTYPE		(_SLERR_START + 30)
/* 31 - reuse me */
/* 32 - reuse me */
/* 33 - reuse me */
#define SLERR_CRCABSENT			(_SLERR_START + 34)

void sl_errno_init(void);

#define sl_strerror(rc)	strerror(rc)

#endif /* _SLERR_H_ */
