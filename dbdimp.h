/*
	$Id$
	
	Copyright (c) 2000-2004 PostgreSQL Global Development Group
	Copyright (c) 1997,1998,1999,2000 Edmund Mergl
	Portions Copyright (c) 1994,1995,1996,1997 Tim Bunce
	
	You may distribute under the terms of either the GNU General Public
	License or the Artistic License, as specified in the Perl README file.
*/

#include "types.h"

#ifdef WIN32
#define snprintf _snprintf
#endif
/* XXX - is snprintf used anywhere? -gsm */

#define sword  signed int
#define sb2    signed short
#define ub2    unsigned short

/* Define drh implementor data structure */
struct imp_drh_st {
	dbih_drc_t com; /* MUST be first element in structure */
};

/* Define dbh implementor data structure */
struct imp_dbh_st {
	dbih_dbc_t com;          /* MUST be first element in structure */

	PGconn  *conn;           /* connection structure */
	bool    done_begin;      /* have we done a begin? (e.g. are we in a transaction?) */
	bool    pg_auto_escape;  /* initialize AutoEscape  XXX used? -gsm */
	bool    pg_bool_tf;      /* do bools return 't'/'f'? */
	bool    pg_enable_utf8;  /* should we attempt to make utf8 strings? */
	int     pg_protocol;     /* value of PQprotocolVersion usually 0, 2, or 3 */
	char    server_prepare;  /* do we want to use PQexecPrepared? 0=no 1=yes 2=smart */
	int     prepare_number;  /* internal prepared statement name modifier */
	bool    prepare_now;     /* force immediate prepares, even with placeholders */
  char    *sqlstate;       /* from the last result */
	char    errorlevel;      /* PQsetErrorVerbosity, defaults to 0 */
};


/* Each statement is broken up into segments */
struct seg_st {
	char *segment;       /* non-placeholder string segment */
	char *placeholder;   /* final name of matching placeholder e.g. "$1" */
	char *value;         /* literal value passed in */
	char *quoted;        /* for old-style execute, the quoted value */
	int  quoted_len;     /* length of the quoted value */
	bool boundbyclient;  /* bound by the client, not us */

	sql_type_info_t* bind_type; /* type information for this placeholder */

	struct seg_st *nextseg; /* linked lists are fun */
};
typedef struct seg_st seg_t;

/* Define sth implementor data structure */
struct imp_sth_st {
	dbih_stc_t com;         /* MUST be first element in structure */

	PGresult*  result;      /* result structure from the executed query */
	PGresult*  result2;      /* result structure from the executed query */
	int        cur_tuple;   /* current tuple being fetched */
	int        rows;        /* number of affected rows */

	char  server_prepare;   /* inherited from dbh. 3 states: 0=no 1=yes 2=smart */
	char  *prepare_name;    /* name of the prepared query; NULL if not prepared */
	bool  prepare_now;      /* prepare this statement right away, even if it has placeholders */
	bool  prepared_by_us;   /* false if {prepare_name} set directly */
	bool  direct;           /* allow bypassing of the statement parsing */
	char  *firstword;       /* first word of the statement */
	bool  is_dml;           /* is this SELECT/INSERT/UPDATE/DELETE? */
	int   numsegs;          /* how many segments this statement has */
	int   numphs;           /* how many placeholders this statement has */
	int   numbound;         /* how many placeholders were explicitly bound by the client, not us */
	int   totalsize;        /* total string length of the statement */
	char  placeholder_type; /* which style is being used 1=? 2=$1 3=:foo */
	seg_t *seg;             /* linked list of segments */
};

/* Other functions we have added to dbdimp.c (large object ones are in large_object.h) */

SV * dbd_db_pg_notifies (SV *dbh, imp_dbh_t *imp_dbh);
int dbd_db_ping ();
int pg_db_putline ();
int pg_db_getline ();
int pg_db_endcopy ();

/* end of dbdimp.h */

