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

#ifndef _SLM_NAMESPACE_H_
#define _SLM_NAMESPACE_H_

enum namespace_direction {
	NS_DIR_RECV,
	NS_DIR_SEND,
	NS_NDIRS
};

enum namespace_operation {
	NS_OP_CREATE,
	NS_OP_LINK,
	NS_OP_MKDIR,
	NS_OP_RENAME,
	NS_OP_RMDIR,
	NS_OP_SETATTR,
	NS_OP_SYMLINK,
	NS_OP_UNLINK,
	NS_NOPS
};

enum namespace_summary {
	NS_SUM_FAIL,		/* # failures */
	NS_SUM_PEND,		/* # pending */
	NS_SUM_SUCC,		/* # success */
	NS_NSUMS
};

#endif /* _SLM_NAMESPACE_H_ */
