/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2013, Pittsburgh Supercomputing Center (PSC).
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
 *
 * Pittsburgh Supercomputing Center	phone: 412.268.4960  fax: 412.268.5832
 * 300 S. Craig Street			e-mail: remarks@psc.edu
 * Pittsburgh, PA 15213			web: http://www.psc.edu/
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

/*
 * This file contains definitions for the implementation of the protocol
 * to exchange namespace updates among metadata servers.
 */

#ifndef _SLM_NAMESPACE_H_
#define _SLM_NAMESPACE_H_

enum namespace_direction {
	NS_DIR_RECV,
	NS_DIR_SEND,
	NS_NDIRS
};

enum namespace_operation {
/* 0 */	NS_OP_CREATE,
/* 1 */	NS_OP_LINK,
/* 2 */	NS_OP_MKDIR,
/* 3 */	NS_OP_RENAME,
/* 4 */	NS_OP_RMDIR,
/* 5 */	NS_OP_SETSIZE,		/* special case of NS_OP_SETATTR, until a better way is found */
/* 6 */	NS_OP_SETATTR,
/* 7 */	NS_OP_SYMLINK,
/* 8 */	NS_OP_UNLINK,
/* 9 */	NS_OP_RECLAIM,
	NS_NOPS
};

enum namespace_summary {
	NS_SUM_FAIL,		/* # failures */
	NS_SUM_PEND,		/* # pending */
	NS_SUM_SUCC,		/* # success */
	NS_NSUMS
};

#endif /* _SLM_NAMESPACE_H_ */
