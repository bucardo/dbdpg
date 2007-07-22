/*
	$Id$
	
    Copyright (c) 2000-2007 Greg Sabino Mullane and others: see the Changes file
	Portions Copyright (c) 1997-2000 Edmund Mergl
	Portions Copyright (c) 1994-1997 Tim Bunce
	
	You may distribute under the terms of either the GNU General Public
	License or the Artistic License, as specified in the Perl README file.
*/

#include "types.h"

/* Define drh implementor data structure */
struct imp_drh_st {
	dbih_drc_t com; /* MUST be first element in structure */
};

/* Define dbh implementor data structure */
struct imp_dbh_st {
	dbih_dbc_t com;            /* MUST be first element in structure */

	bool    pg_bool_tf;        /* do bools return 't'/'f'? Set by user, default is 0 */
	bool    pg_enable_utf8;    /* should we attempt to make utf8 strings? Set by user, default is 0 */
	bool    prepare_now;       /* force immediate prepares, even with placeholders. Set by user, default is 0 */
	bool    done_begin;        /* have we done a begin? (e.g. are we in a transaction?) */
	bool    dollaronly;        /* Only consider $1, $2 ... as valid placeholders */
	bool    expand_array;      /* Transform arrays from the db into Perl arrays? Default is 1 */

	int     pg_protocol;       /* value of PQprotocolVersion, usually 0, 2, or 3 */
	int     pg_server_version; /* Server version e.g. 80100 */
	int     pid_number;        /* prefixed before prepare_number */
	int     prepare_number;    /* internal prepared statement name modifier */
	int     copystate;         /* 0=none PGRES_COPY_IN PGRES_COPY_OUT */
	int     pg_errorlevel;     /* PQsetErrorVerbosity. Set by user, defaults to 1 */
	int     server_prepare;    /* do we want to use PQexecPrepared? 0=no 1=yes 2=smart. Can be changed by user */
	int     async_status;      /* 0=no async 1=async started -1=async has been cancelled */

    imp_sth_t *async_sth;      /* current async statement handle */
	AV      *savepoints;       /* list of savepoints */
	PGconn  *conn;             /* connection structure */
	char    *sqlstate;         /* from the last result */
};


/* Each statement is broken up into segments */
struct seg_st {
	char *segment;          /* non-placeholder string segment */
	int placeholder;        /* which placeholder this points to, 0=none */
	struct ph_st *ph;       /* points to the relevant ph structure */
	struct seg_st *nextseg; /* linked lists are fun */
};
typedef struct seg_st seg_t;

/* The placeholders are also a linked list */
struct ph_st {
	char  *fooname;             /* Name if using :foo style */
	char  *value;               /* the literal passed-in value, may be binary */
	STRLEN valuelen;            /* length of the value */
	char  *quoted;              /* quoted version of the value, for PQexec only */
	STRLEN quotedlen;           /* length of the quoted value */
	bool   referenced;          /* used for PREPARE AS construction */
	bool   defaultval;          /* is it using a generic 'default' value? */
	bool   iscurrent;           /* is it using a generic 'default' value? */
	bool   isdefault;           /* Are we passing a literal 'DEFAULT'? */
	sql_type_info_t* bind_type; /* type information for this placeholder */
	struct ph_st *nextph;       /* more linked list goodness */
};
typedef struct ph_st ph_t;

/* Define sth implementor data structure */
struct imp_sth_st {
	dbih_stc_t com;         /* MUST be first element in structure */

	int    server_prepare;   /* inherited from dbh. 3 states: 0=no 1=yes 2=smart */
	int    placeholder_type; /* which style is being used 1=? 2=$1 3=:foo */
	int    numsegs;          /* how many segments this statement has */
	int    numphs;           /* how many placeholders this statement has */
	int    numbound;         /* how many placeholders were explicitly bound by the client, not us */
	int    cur_tuple;        /* current tuple being fetched */
	int    rows;             /* number of affected rows */
	int    async_flag;       /* async? 0=no 1=async 2=cancel 4=wait */
	int    async_status;     /* 0=no async 1=async started -1=async has been cancelled */

	STRLEN totalsize;        /* total string length of the statement (with no placeholders)*/

	char   *prepare_name;    /* name of the prepared query; NULL if not prepared */
	char   *firstword;       /* first word of the statement */

	PGresult  *result;       /* result structure from the executed query */
	sql_type_info_t **type_info; /* type of each column in result */

	seg_t  *seg;             /* linked list of segments */
	ph_t   *ph;              /* linked list of placeholders */

	bool   prepare_now;      /* prepare this statement right away, even if it has placeholders */
	bool   prepared_by_us;   /* false if {prepare_name} set directly */
	bool   onetime;          /* this statement is guaranteed not to be run again - so don't use SSP */
	bool   direct;           /* allow bypassing of the statement parsing */
	bool   is_dml;           /* is this SELECT/INSERT/UPDATE/DELETE? */
	bool   has_binary;       /* does it have one or more binary placeholders? */
	bool   has_default;      /* does it have one or more 'DEFAULT' values? */
	bool   has_current;      /* does it have one or more 'DEFAULT' values? */
	bool   dollaronly;          /* Only use $1 as placeholders, allow all else */
};

/* Other (non-static) functions we have added to dbdimp.c */

int dbd_db_ping(SV *dbh);
int dbd_db_getfd (SV *dbh, imp_dbh_t *imp_dbh);
SV * dbd_db_pg_notifies (SV *dbh, imp_dbh_t *imp_dbh);
int pg_db_putline (SV *dbh, const char *buffer);
int pg_db_getline (SV *dbh, char *buffer, int length);
int pg_db_endcopy (SV * dbh);
void pg_db_pg_server_trace (SV *dbh, FILE *fh);
void pg_db_pg_server_untrace (SV *dbh);
int pg_db_savepoint (SV *dbh, imp_dbh_t *imp_dbh, char * savepoint);
int pg_db_rollback_to (SV *dbh, imp_dbh_t *imp_dbh, char * savepoint);
int pg_db_release (SV *dbh, imp_dbh_t *imp_dbh, char * savepoint);
unsigned int pg_db_lo_creat (SV *dbh, int mode);
int pg_db_lo_open (SV *dbh, unsigned int lobjId, int mode);
int pg_db_lo_close (SV *dbh, int fd);
int pg_db_lo_read (SV *dbh, int fd, char *buf, size_t len);
int pg_db_lo_write (SV *dbh, int fd, char *buf, size_t len);
int pg_db_lo_lseek (SV *dbh, int fd, int offset, int whence);
int pg_db_lo_tell (SV *dbh, int fd);
int pg_db_lo_unlink (SV *dbh, unsigned int lobjId);
unsigned int pg_db_lo_import (SV *dbh, char *filename);
int pg_db_lo_export (SV *dbh, unsigned int lobjId, char *filename);
int pg_quickexec (SV *dbh, const char *sql, int asyncflag);
int dbdpg_ready (SV *dbh, imp_dbh_t *imp_dbh);
int dbdpg_result (SV *dbh, imp_dbh_t *imp_dbh);
int dbdpg_cancel (SV *h, imp_dbh_t *imp_dbh);
int dbdpg_cancel_sth (SV *sth, imp_sth_t *imp_sth);
SV * pg_stringify_array(SV * input, const char * array_delim, int server_version);

/* end of dbdimp.h */

