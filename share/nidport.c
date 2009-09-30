/* $Id$ */

#include <netinet/in.h>

#include <stdio.h>
#include <string.h>

/*
 * nidstr_extractport - Remove TCP port from a nidstr and
 *	return its numerical value.
 * @nidstr: NID string.
 * @port: value-result port value.
 */
int
nidstr_extractport(char *nidstr, in_port_t *port)
{
	char *p, *s, *endp;
	long l;

	*port = 0;
	if ((p = strrchr(nidstr, ':')) != NULL) {
		if ((s = strchr(p, '@')) == NULL)
			return (-1);
		*s++ = '\0';

		/* Extract port */
		l = strtol(p + 1, &endp, 10);
		if (l == LONG_MIN || l == LONG_MAX ||
		    endp + 1 != s || endp == p + 1)
			return (-1);
		*port = l;

		/* Copy network overtop of port */
		*p++ = '@';
		while (*s)
			*p++ = *s++;
		*p = '\0';
	}
	return (0);
}
