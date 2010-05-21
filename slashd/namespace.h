/* $Id$ */
/* %PSC_COPYRIGHT% */

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
