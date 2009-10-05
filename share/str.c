/* $Id$ */

#include <string.h>

const char *
slstrerror(int error)
{
	error = abs(error);

	/* add custom error codes here */
#if 0
	if (error == 10000)
		return ("error 10000");
#endif

	return (strerror(error));
}
