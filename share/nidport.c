/* $Id$ */

#include <stdio.h>

void
nidstr_extractport(char *nidstr, in_port_t *port)
{
	char *p, *s, *endp;
	long l;

	*port = 0;
	if ((p = strchr(nidstr, ':')) != NULL) {
		if ((s = strchr(p, '@')) == NULL)
			return;
		*s++ = '\0';

		/* Extract port */
		l = strtol(p + 1, &endp, 10);
		if (l == LONG_MIN || l == LONG_MAX ||
		    endp + 1 != s || endp == p + 1)
			return;
		*port = l;

		/* Copy network overtop of port */
		*p++ = '@';
		while (*s)
			*p++ = *s++;
		*p = '\0';
	}
}
