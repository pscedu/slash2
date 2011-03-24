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
