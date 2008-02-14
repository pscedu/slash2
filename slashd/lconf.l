/* $Id$ */

%{
#define YYSTYPE char *

#include <stdio.h>
#include <string.h>

#include "psc_util/log.h"
#include "yconf.h"

extern int cfg_lineno;
extern char *yylval;
%}

eq   [=]
sep  [:]
nsep [,]
endl [;]

num      [0-9]+
hexnum   0x[0-9a-f]+
sitename [\@][A-Z]+
char     [A-Za-z]
ipquad   [0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}
name     [A-Za-z][0-9A-Za-z_.-]+
floatv   [0-9]*\.?[0-9]+
resname  [A-Za-z][0-9A-Za-z]+[\@][A-Z]+
lnettcp  tcp[0-9]+
sizeval  [0-9]+[KMGTBkmgtb]
pathname [/][a-zA-Z0-9/._-]+
globpath [/][a-zA-Z0-9|/.\,_\[\]+?-]+
quotedstr [\"].*[\"]

%%

none {
        psc_trace("NONE");
	return NONE;
}

NONE {
        psc_trace("NONE");
	return NONE;
}

";" {
	return END;
}

":" {
	return SEP;
}

"," {
        psc_trace("Sep ','");
	return NSEP;
}

"=" {
	return EQ;
}

global {
        psc_trace("GLOBAL %s", yytext);
        return GLOBAL;
}

site {
	psc_trace("SITETAG %s", yytext);
	return SITE_PROFILE;
}

resource {
        psc_trace("RESOURCE_PROFILE %s", yytext);
        return RESOURCE_PROFILE;           
}

peers {
        psc_trace("PEERTAG %s", yytext);
        return PEERTAG;
}

fsroot {
        psc_trace("FSROOT %s", yytext);
        return FSROOT;
}

ifs {
        psc_trace("INTERFACETAG %s", yytext);
        return INTERFACETAG;      
}


"{" {
	psc_trace("SUBSECT_START TAG %s", yytext);
	return SUBSECT_START;
}

"}" {
	psc_trace("SUBSECT_END TAG %s", yytext);
	return SUBSECT_END;
}

yes {
	psc_trace("BOOL %s", yytext);
	yylval = strdup(yytext);
	return BOOL;
}

no {
	psc_trace("BOOL %s", yytext);
	yylval = strdup(yytext);
	return BOOL;
}

{num} {
	psc_trace("NUM %s", yytext);
	yylval = strdup(yytext);
	return NUM;
}

parallel_fs {
        psc_trace("Resource type string %s", yytext);
        yylval = strdup(yytext); 
        return RESOURCE_TYPE;
}

archival_fs {
        psc_trace("Resource type string %s", yytext);
        yylval = strdup(yytext); 
        return RESOURCE_TYPE;
}

compute {
        psc_trace("Resource type string %s", yytext);
        yylval = strdup(yytext); 
        return RESOURCE_TYPE;
}

{hexnum} {
	psc_trace("HEXNUM %s", yytext);
	yylval = strdup(yytext);
	return HEXNUM;
}

{lnettcp} {
        psc_trace("Lnet tcp network string %s", yytext);  
        yylval = strdup(yytext);        
        return LNETTCP;      
}

{name} {
	psc_trace("NAME %s", yytext);
	yylval = strdup(yytext);
	return NAME;
}

{pathname} {
	psc_trace("PATHNAME %s", yytext);
	yylval = strdup(yytext);
	return PATHNAME;
}

{globpath} {
        psc_trace("GLOBPATH %s", yytext);
        yylval = strdup(yytext);
        return GLOBPATH;
}

{floatv} {
	psc_trace("FLOATVAL %s", yytext);
        yylval = strdup(yytext);
        return FLOATVAL;
}

{sizeval} {
	psc_trace("SIZEVAL %s", yytext);
        yylval = strdup(yytext);
        return SIZEVAL;
}

{sitename} {
        psc_trace("SITE_NAME %s", yytext);
        yylval = strdup(yytext); 
        return SITE_NAME;
}

{resname} {
        psc_trace("RESOURCE_NAME %s", yytext);
        yylval = strdup(yytext); 
        return RESOURCE_NAME;
}

{ipquad} {
        psc_trace("IPADRRESS %s", yytext);
        yylval = strdup(yytext); 
        return IPADDR;
}

{quotedstr} {
        psc_trace("Quoted string %s", yytext);
        yylval = strdup(yytext); 
        return QUOTEDS;
}



[ \t]*#.*	;
[ \t]+		;
[\n]		cfg_lineno++;

.		psc_warnx("Unrecognized character: ;%s;", yytext);

%%

int yywrap(void)
{
	return 1;
}
