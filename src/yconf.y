/* $Id$ */

%{
#define YYSTYPE char *

#include <ctype.h>
#include <fcntl.h>
#include <limits.h>
#include <stdarg.h>
#include <stddef.h> /* offsetof() */
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "libcfs/kp30.h"

#include "psc_types.h"
#include "psc_util/alloc.h"
#include "psc_util/assert.h"
#include "psc_util/log.h"
#include "slconfig.h"

enum sym_types {
	SL_FUNCTION = 1,
	SL_VARIABLE = 2,
	SL_METATAG  = 4,
	SL_FLAG     = 8
};

enum sym_parameter_types {
	SL_TYPE_STR        = 0,
	SL_TYPE_STRP       = 1,
	SL_TYPE_INT        = 2,
	SL_TYPE_BOOL       = 3,
	SL_TYPE_FLOAT      = 4,
	SL_TYPE_SIZET      = 5,
	SL_TYPE_HEXU64     = 6,
	SL_TYPE_NONE       = 7
};

enum sym_structure_types {
	SL_STRUCT_SITE   = 1024,
	SL_STRUCT_RES    = 1025,
	SL_STRUCT_GLOBAL = 1026
};

#define BOOL_MAX 3

typedef u32 (*sym_handler)(char *);

struct symtable {
	char *name;
	enum  sym_types sym_type;
	enum  sym_structure_types sym_struct_type;
	enum  sym_parameter_types sym_param_type;
	int   param;
	int   offset;
	sym_handler handler;
};

u32
global_net_handler(char *net);
/*
 * Define a table macro for each structure type filled in by the config
 */
#define TABENT_GLOB(name, type, max, field, handler)				\
        { name, SL_VARIABLE, SL_STRUCT_GLOBAL, type, max, offsetof(sl_gconf_t, field), handler }

#define TABENT_SITE(name, type, max, field, handler)				\
        { name, SL_VARIABLE, SL_STRUCT_SITE, type, max, offsetof(sl_site_t, field), handler }

#define TABENT_RES(name, type, max, field, handler)				\
        { name, SL_VARIABLE, SL_STRUCT_RES, type, max, offsetof(sl_resource_t, field), handler }

/* declare and initialize the global table */
 struct symtable sym_table[] = {
	 TABENT_GLOB("port",      SL_TYPE_INT,  INTSTR_MAX, gconf_port,  NULL),
	 TABENT_GLOB("net",       SL_TYPE_INT,  MAXNET,     gconf_netid, global_net_handler),
	 TABENT_SITE("site_id",   SL_TYPE_INT,  INTSTR_MAX, site_id,     NULL),
	 TABENT_SITE("site_desc", SL_TYPE_STRP, DESC_MAX,   site_desc,   NULL),
	 TABENT_RES ("desc",      SL_TYPE_STRP, DESC_MAX,   res_desc,    NULL),
	 TABENT_RES ("type",      SL_TYPE_INT,  INTSTR_MAX, res_type,    libsl_str2restype),
	 TABENT_RES ("id",        SL_TYPE_INT,  INTSTR_MAX, res_id,      NULL),
	 TABENT_RES ("mds",       SL_TYPE_BOOL, BOOL_MAX,   res_mds,     NULL),
	 { NULL, 0, 0, 0, 0, 0, NULL }
};

extern int  yylex(void);
extern void yyerror(const char *, ...);
extern int  yyparse(void);
void        store_tok_val(const char *tok, char *val);
extern int  run_yacc(const char *);

sl_gconf_t globalConfig;
sl_nodeh_t nodeInfo;

int errors;
int cfg_lineno;

const char *cfg_filename;

sl_site_t     *currentSite = NULL;
sl_resource_t *currentRes  = NULL;
sl_gconf_t    *currentConf = &globalConfig;

int cfgMode = SL_STRUCT_GLOBAL;
%}

%start config

%token END
%token EQ

%token SEP
%token NSEP

%token SUB
%token SUBSECT_START
%token SUBSECT_END

%token NUM
%token HEXNUM
%token NAME
%token PATHNAME
%token GLOBPATH
%token BOOL
%token SIZEVAL
%token FLOATVAL

%token GLOBAL
%token RESOURCE_PROFILE
%token RESOURCE_NAME
%token RESOURCE_TYPE
%token SITE_PROFILE
%token SITE_NAME

%token IPADDR
%token PEERTAG
%token INTERFACETAG
%token QUOTEDS
%token LNETTCP

%token NONE

%%
config         : globals site_profiles
{
	sl_site_t     *s=NULL;
	sl_resource_t *r=NULL;
	/*
	 * Config has been loaded, iterate through the sites'
	 *  peer lists and resolve the names to numerical id's.
	 */
	psclist_for_each_entry(s, &globalConfig.gconf_sites, site_list) {
		psclist_for_each_entry(r, &s->site_resources, res_list) {
			u32 i;

			r->res_peers = PSCALLOC(sizeof(sl_ios_id_t) *
						r->res_npeers);

			for (i=0; i < r->res_npeers; i++) {
				r->res_peers[i] = libsl_str2id(r->res_peertmp[i]);
				psc_assert(r->res_peers[i] != IOS_ID_ANY);
				free(r->res_peertmp[i]);
			}
			free(r->res_peertmp);
			/*
			 * Associate nids with their respective resources,
			 *   and add the nids to the global hash table.
			 */
			for (i=0; i < r->res_nnids; i++)
				libsl_nid_associate(r->res_nids[i], r);
		}
	}
};

globals        : /* NULL */              |
                 global globals;

global         : GLOBAL statement;

site_profiles  : site_profile            |
                 site_profile site_profiles;

site_profile   : site_profile_start site_defs SUBSECT_END
{
	psclist_add(&currentSite->site_list,
		    &currentConf->gconf_sites);
	currentSite = PSCALLOC(sizeof(sl_site_t));
	INIT_SITE(currentSite);
};

site_profile_start : SITE_PROFILE SITE_NAME SUBSECT_START
{
	cfgMode = SL_STRUCT_SITE;
	strncpy(currentSite->site_name, $2, SITE_NAME_MAX);
};

site_defs      : statements site_resources
{};

site_resources : site_resource              |
                 site_resources site_resource
{
	cfgMode = SL_STRUCT_SITE;
};

site_resource  : site_resource_start resource_def SUBSECT_END
{
	currentRes->res_id = sl_global_id_build(currentSite->site_id,
						currentRes->res_id,
						currentRes->res_mds);
	psclist_add(&currentRes->res_list,
		    &currentSite->site_resources);
	currentRes = PSCALLOC(sizeof(sl_resource_t));
	INIT_RES(currentRes);
};

site_resource_start : RESOURCE_PROFILE NAME SUBSECT_START
{
	cfgMode = SL_STRUCT_RES;
	if (snprintf(currentRes->res_name, RES_NAME_MAX, "%s%s",
		     $2, currentSite->site_name) > RES_NAME_MAX)
		psc_fatal("Resource name too long");
	psc_trace("ResName %s", currentRes->res_name);
};

resource_def   : statements interfacelist peerlist |
                 statements interfacelist          |
                 statements peerlist
{};

peerlist       : PEERTAG EQ peers END
{};

peers          : peer                              |
                 peer NSEP peers

{};

peer           : RESOURCE_NAME
{
	char **tmp;
	tmp = realloc(currentRes->res_peertmp,
		      (sizeof(char **) * (currentRes->res_npeers++)));
	psc_assert(tmp);
	tmp[(currentRes->res_npeers)-1] = strdup($1);
	currentRes->res_peertmp = tmp;
};

interfacelist  : INTERFACETAG EQ interfaces END
{};

interfaces     : interface                 |
                 interface NSEP interfaces
{};

interface      : IPADDR
{
	lnet_nid_t *i;
	char        nidstr[MAXNET];

	i = realloc(currentRes->res_nids,
		    (sizeof(lnet_nid_t) * (currentRes->res_nnids + 1)));
        psc_assert(i);

	if ((snprintf(nidstr, 32, "%s@%s", $1, currentConf->gconf_net))
	    > MAXNET)
		psc_fatalx("Interface to NID failed, ifname too long %s", $1);

        i[currentRes->res_nnids] = libcfs_str2nid(nidstr);

	psc_info("Got nidstr ;%s; nid2str ;%s;",
		 nidstr, libcfs_nid2str(i[currentRes->res_nnids]));

	currentRes->res_nnids++;
        currentRes->res_nids = i;
};

statements        : /* NULL */               |
                    statement statements;

statement         : restype_stmt |
                    path_stmt    |
                    num_stmt     |
                    bool_stmt    |
                    size_stmt    |
                    glob_stmt    |
                    hexnum_stmt  |
                    float_stmt   |
                    lnettcp_stmt |
                    quoteds_stmt;

restype_stmt : NAME EQ RESOURCE_TYPE END
{
	psc_notify("Found Fstype Statement: Tok '%s' Val '%s'",
		   $1, $3);
	store_tok_val($1, $3);
	free($1);
	free($3);
}

path_stmt : NAME EQ PATHNAME END
{
	psc_notify("Found Path Statement: Tok '%s' Val '%s'",
	       $1, $3);
	store_tok_val($1, $3);
	free($1);
	free($3);
};

glob_stmt : NAME EQ GLOBPATH END
{
        psc_notify("Found Glob Statement: Tok '%s' Val '%s'",
               $1, $3);
	store_tok_val($1, $3);
	free($1);
	free($3);
};

bool_stmt : NAME EQ BOOL END
{
	psc_notify("Found Bool Statement: Tok '%s' Val '%s'",
	       $1, $3);
	store_tok_val($1, $3);
	free($1);
	free($3);
};

size_stmt : NAME EQ SIZEVAL END
{
	psc_notify("Found Sizeval Statement: Tok '%s' Val '%s'",
	       $1, $3);
	store_tok_val($1, $3);
	free($1);
	free($3);
};

num_stmt : NAME EQ NUM END
{
        psc_notify("Found Num Statement: Tok '%s' Val '%s'",
		$1, $3);
	store_tok_val($1, $3);
	free($1);
	free($3);
};

float_stmt : NAME EQ FLOATVAL END
{
	psc_notify("Found Float Statement: Tok '%s' Val '%s'",
	       $1, $3);
	store_tok_val($1, $3);
	free($1);
	free($3);
};

hexnum_stmt : NAME EQ HEXNUM END
{
	psc_notify("Found Hexnum Statement: Tok '%s' Val '%s'",
	       $1, $3);
	store_tok_val($1, $3);
	free($1);
	free($3);
};

quoteds_stmt : NAME EQ QUOTEDS END
{
	psc_notify("Found Quoted String Statement: Tok '%s' Val '%s'",
		   $1, $3);

	store_tok_val($1, $3);
	free($1);
	free($3);
	/* Don't free the string itself, it's pointer has been copied */
};


lnettcp_stmt : NAME EQ LNETTCP END
{
	psc_notify("Found Lnettcp String Statement: Tok '%s' Val '%s'",
		   $1, $3);

	store_tok_val($1, $3);
	free($1);
	free($3);
};

%%

u32
global_net_handler(char *net)
{
	strncpy(globalConfig.gconf_net, net, MAXNET);
	return (libcfs_str2net(net));
}

static struct symtable *
get_symbol(const char *name)
{
        struct symtable *e;

        psc_notify("symbol lookup '%s'", name);

        for (e = sym_table; e != NULL && e->name != NULL ; e++)
                if (e->name && !strcmp(e->name, name))
                        break;

        if (e == NULL || e->name == NULL) {
                psc_warnx("Symbol '%s' was not found", name);
		return NULL;
	}
        return e;
}

void
store_tok_val(const char *tok, char *val)
{
	struct symtable *e;
	void            *ptr;

	psc_notify("val %s tok %s", val, tok);

	e = get_symbol(tok);
	if (!e)
		psc_fatalx("'%s' is an unknown symbol in the current context",
			   tok);
	psc_trace("%p %d", e, e->sym_type );
	psc_assert(e->sym_type == SL_VARIABLE ||
		   e->sym_type == SL_FLAG);

	psc_notify("sym entry %p, name %s, param_type %d",
		   e, e->name, e->sym_param_type);
	/*
	 * Access the correct structure based on the
	 *  type stored in the symtab entry.
	 */
	switch (e->sym_struct_type) {
	case SL_STRUCT_GLOBAL:
		ptr = e->offset + (char *)currentConf;
		break;

	case SL_STRUCT_SITE:
		ptr = e->offset + (char *)currentSite;
		break;

	case SL_STRUCT_RES:
		ptr = e->offset + (char *)currentRes;
		break;
	default:
		psc_fatalx("Invalid structure type %d", e->sym_struct_type);
	}

	psc_trace("Type %d ptr %p", e->sym_struct_type, ptr);

	switch (e->sym_param_type) {
	case SL_TYPE_STR:
		strncpy(ptr, val, e->param);
		((char *)ptr)[e->param - 1] = '\0';
		psc_trace("SL_TYPE_STR Tok '%s' set to '%s'",
		       e->name, (char *)ptr);
		break;

	case SL_TYPE_STRP:
		*(char **)ptr = strdup(val);
		psc_trace("SL_TYPE_STRP Tok '%s' set to '%s' %p",
			  e->name, (char *)ptr, ptr);
		break;

	case SL_TYPE_HEXU64:
		*(u64 *)ptr = strtoull(val, NULL, 16);
		psc_trace("SL_TYPE_HEXU64 Tok '%s' set to '%"ZLPX64"'",
		       e->name, (u64)*(u64 *)(ptr));
		break;

	case SL_TYPE_INT:
		if (e->handler)
			*(int *)ptr = (e->handler)(val);
		else
			//*(long *)ptr = strtol(val, NULL, 10);
			*(int *)ptr = atoi(val);
		psc_trace("SL_TYPE_INT Tok '%s' set to '%ld'",
		       e->name, (long)*(long *)(ptr));
		break;

	case SL_TYPE_BOOL:
		if ( !strncmp("yes", val, 3) ||
		     !strncmp("1",   val, 1) ) {
			//*(int *)ptr |= e->param;
			*(int *)ptr = 1;
			psc_trace("SL_TYPE_BOOL Option '%s' enabled", e->name);

		} else
			psc_trace("SL_TYPE_BOOL Option '%s' disabled", e->name);
		break;

	case SL_TYPE_FLOAT:
		{
			char   *c, floatbuf[17];
			float   f;

			f = atof(val);
			c = floatbuf;

			bzero(floatbuf, 16);
			snprintf(floatbuf, 16, "%f", f);

			psc_trace("float_val %f '%s'",
				f, floatbuf);

			while (*(c++) != '\0')
				if (*c == '.') *c = '\0';

			*(long *)ptr = strtol(floatbuf, NULL, 10);

			if ( *c != '\0' ) {
				*(long *)(ptr+1) = strtol(c, NULL, 10);
			}
			psc_trace("SL_TYPE_FLOAT Tok '%s' Secs %ld Usecs %lu",
				e->name, *(long *)ptr, *(long *)(ptr+1));
		}
		break;

	case SL_TYPE_SIZET:
		{
			u64   i;
			int   j;
			char *c;

			j = strlen(val);
			c = &val[j-1];

			switch (tolower(*c)) {
			case 'b':
				i = (u64)1;
				break;
			case 'k':
				i = (u64)1024;
				break;
			case 'm':
				i = (u64)1024*1024;
				break;
			case 'g':
				i = (u64)1024*1024*1024;
				break;
			case 't':
				i = (u64)1024*1024*1024*1024;
				break;
			default:
				psc_fatalx("Sizeval '%c' is not valid", *c);
			}
			psc_trace("ival   = %"ZLPU64, i);

			*c = '\0';
			*(u64 *)ptr = (u64)(i * strtoull(val, NULL, 10));

			psc_trace("SL_TYPE_SIZET Tok '%s' set to '%"ZLPU64"'",
				e->name, *(u64 *)ptr);
		}
		break;

	default:
		psc_fatalx("Invalid Token '%s'", e->name);
	}
}

int run_yacc(const char *config_file)
{
	extern FILE *yyin;

	errors = 0;

	yyin = fopen(config_file, "r");
	if (yyin == NULL)
		psc_fatal("open() failed ;%s;", config_file);

	cfg_filename = config_file;

	/* Pre-allocate the first resource and site */
	currentSite = PSCALLOC(sizeof(sl_site_t));
	currentRes  = PSCALLOC(sizeof(sl_resource_t));

	INIT_SITE(currentSite);
	INIT_RES(currentRes);
	INIT_GCONF(&globalConfig);

	yyparse();

	fclose(yyin);

	if (errors)
		psc_fatalx("%d error(s) encountered", errors);

	/* Sanity checks */

	return 0;
}

void
yyerror(const char *fmt, ...)
{
	char buf[LINE_MAX];
	va_list ap;

	errors++;

	va_start(ap, fmt);
	vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);

	psc_errorx("%s:%d: %s", cfg_filename, cfg_lineno, buf);
}
