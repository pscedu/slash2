/* $Id$ */
#ifndef __SLASH_CREDS_H__
#define __SLASH_CREDS_H__ 1

struct slash_creds {
	u32	uid;
	u32	gid;
};

typedef struct slash_creds cred_t;

#endif
