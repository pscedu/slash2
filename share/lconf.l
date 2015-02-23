/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2007-2013, Pittsburgh Supercomputing Center (PSC).
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

%{
#define YYSTYPE char *

#include <err.h>
#include <stdio.h>
#include <string.h>

#include "pfl/alloc.h"
#include "pfl/log.h"
#include "pfl/str.h"

#include "slconfig.h"
#include "yconf.h"

extern char		  cfg_filename[];
extern int		  cfg_lineno;

extern char		 *yylval;

extern struct sl_site	  *currentSite;
extern struct sl_resource *currentRes;
extern struct sl_resm	  *currentResm;
%}

%option nounput

floatv		[0-9]*\.?[0-9]+
globpath	[/][a-zA-Z0-9|/.,_\[\]+?-]+
hexnum		0[Xx][0-9A-Fa-f]+
ipquad		[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*
lnetname	tcp[0-9]+|sdp[0-9]+
name		[A-Za-z][0-9A-Za-z_.-]+
num		[0-9]+
pathname	[/][a-zA-Z0-9/._-]+
quotedstr	\"[^"]*\"
sizeval		[0-9]+[KMGTBkmgtb]?

%%

global		{ yywarn("`global' deprecated by `set'"); return (SET); }
endpoints	{ return (ENDPOINTS); }
include		{ return (INCLUDE); }
nids		{ return (NIDS); }
peers|ios	{ return (PEERS); }
resource	{ return (RESOURCE); }
route		{ return (ROUTE); }
set		{ return (SET); }
site		{ return (SITE); }

yes|no {
	psclog_trace("BOOL %s", yytext);
	yylval = pfl_strdup(yytext);
	return (BOOL);
}

parallel_lfs|cluster_noshare_lfs|parallel_lfs_compnt|standalone_fs|archival_fs|mds {
	psclog_trace("Resource type string %s", yytext);
	yylval = pfl_strdup(yytext);
	return (RESOURCE_TYPE);
}

{ipquad} {
	psclog_trace("IPADRRESS %s", yytext);
	yylval = pfl_strdup(yytext);
	return (IPADDR);
}

{num} {
	psclog_trace("NUM %s", yytext);
	yylval = pfl_strdup(yytext);
	return (NUM);
}

{hexnum} {
	psclog_trace("HEXNUM %s", yytext);
	yylval = pfl_strdup(yytext);
	return (HEXNUM);
}

{lnetname} {
	psclog_trace("LNETNAME string %s", yytext);
	yylval = pfl_strdup(yytext);
	return (LNETNAME);
}

{name} {
	psclog_trace("NAME %s", yytext);
	yylval = pfl_strdup(yytext);
	return (NAME);
}

{pathname} {
	psclog_trace("PATHNAME %s", yytext);
	yylval = pfl_strdup(yytext);
	return (PATHNAME);
}

{globpath} {
	psclog_trace("GLOBPATH %s", yytext);
	yylval = pfl_strdup(yytext);
	return (GLOBPATH);
}

{floatv} {
	psclog_trace("FLOATVAL %s", yytext);
	yylval = pfl_strdup(yytext);
	return (FLOATVAL);
}

{sizeval} {
	psclog_trace("SIZEVAL %s", yytext);
	yylval = pfl_strdup(yytext);
	return (SIZEVAL);
}

{quotedstr} {
	size_t len;

	psclog_trace("Quoted string %s", yytext);
	len = strlen(yytext);
	psc_assert(len > 1);

	/* len = q["] + str + q["] */
	/* remove 2 for the quotes but add 1 for NUL; total: subtract 1 */
	len--;

	yylval = PSCALLOC(len);
	strlcpy(yylval, yytext + 1, len);
	return (QUOTEDS);
}

[ \t]*#.*	;
[ \t]+		;
[\n]		cfg_lineno++;

.		{ return yytext[0]; }

%%

int
yywrap(void)
{
	return (1);
}
