/* $Id$ */

#ifndef _SLASH_CREDS_H_
#define _SLASH_CREDS_H_

#include <stdint.h>

struct slash_creds {
	uint32_t	uid;
	uint32_t	gid;
};

typedef struct slash_creds cred_t;

#endif /* _SLASH_CREDS_H_ */
