/*
	$Id$
	
	Copyright (c) 2001-2004 PostgreSQL Global Development Group
	Copyright (c) 1997,1998,1999,2000 Edmund Mergl
	Portions Copyright (c) 1994,1995,1996,1997 Tim Bunce
	
	You may distribute under the terms of either the GNU General Public
	License or the Artistic License, as specified in the Perl README file.
*/

#ifdef WIN32
#define snprintf _snprintf
#endif

/* Define drh implementor data structure */
struct imp_drh_st {
	dbih_drc_t com; /* MUST be first element in structure */
};

/* Define dbh implementor data structure */
struct imp_dbh_st {
	dbih_dbc_t com;         /* MUST be first element in structure */

	PGconn  *conn;         /* connection structure */
	int     init_commit;    /* initialize AutoCommit */
	int     pg_auto_escape; /* initialize AutoEscape */
	int     pg_bool_tf;     /* do bools return 't'/'f'? */
	int     done_begin;     /* Have we done a begin?
														 Only used if AutoCommit is off. */
	int     pg_protocol;
	int     pg_enable_utf8; /* should we attempt to make utf8 strings? */

};


#define sword  signed int
#define sb2    signed short
#define ub2    unsigned short
typedef struct phs_st phs_t;    /* scalar placeholder */

struct phs_st {    /* scalar placeholder EXPERIMENTAL */
	int          ftype;      /* field type */
	char         *quoted;   /* Quoted value bound to placeholder*/
	size_t       quoted_len;
	unsigned int count;
	bool         is_bound;
  
	char         name [1]; /* struct is malloc'd bigger as needed  */
};

/* Define sth implementor data structure */
struct imp_sth_st {
	dbih_stc_t   com;             /* MUST be first element in structure */

	PGresult*    result;          /* result structure */
	int          cur_tuple;       /* current tuple */
	int          rows;            /* number of affected rows */

	/* Input Details */
	char         *statement;      /* sql (see sth_scan)    */
	HV           *all_params_hv;  /* all params, keyed by name  */

	bool         server_prepared; /* Did we prepare this server side?*/
	phs_t        **place_holders;
	unsigned int phc;

	/*char *orig_statement; */  /*? Origional SQL statement for debug?? ?*/
};

SV * dbd_db_pg_notifies (SV *dbh, imp_dbh_t *imp_dbh);

/* end of dbdimp.h */

