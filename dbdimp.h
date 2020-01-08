/*
    Copyright (c) 2000-2020 Greg Sabino Mullane and others: see the Changes file
	Portions Copyright (c) 1997-2000 Edmund Mergl
	Portions Copyright (c) 1994-1997 Tim Bunce
	
	You may distribute under the terms of either the GNU General Public
	License or the Artistic License, as specified in the Perl README file.
*/

/* Define drh implementor data structure */
struct imp_drh_st {
	dbih_drc_t com; /* MUST be first element in structure */
};

/* Define dbh implementor data structure */
struct imp_dbh_st {
	dbih_dbc_t com;            /* MUST be first element in structure */

	int     pg_protocol;       /* value of PQprotocolVersion, usually 3 (could also be 0) */
	int     pg_server_version; /* server version e.g. 80100 */
	int     pid_number;        /* prefixed before prepare_number */
	int     prepare_number;    /* internal prepared statement name modifier */
	int     copystate;         /* 0=none PGRES_COPY_IN PGRES_COPY_OUT */
	bool    copybinary;        /* whether the copy is in binary format */
	int     pg_errorlevel;     /* PQsetErrorVerbosity. Set by user, defaults to 1 */
	bool    server_prepare;    /* do we want to use PQexecPrepared? Can be changed by user */
	int     switch_prepared;   /* how many executes until we switch to PQexecPrepared */
	int     async_status;      /* 0=no async 1=async started -1=async has been cancelled */

    imp_sth_t *async_sth;      /* current async statement handle */
	AV      *savepoints;       /* list of savepoints */
	PGconn  *conn;             /* connection structure */
	char    *sqlstate;         /* from the last result */


	bool    pg_bool_tf;        /* do bools return 't'/'f'? Set by user, default is 0 */
	bool    prepare_now;       /* force immediate prepares, even with placeholders. Set by user, default is 0 */
	bool    done_begin;        /* have we done a begin? (e.g. are we in a transaction?) */
	bool    dollaronly;        /* only consider $1, $2 ... as valid placeholders */
	bool    nocolons;          /* do not consider :1, :2 ... as valid placeholders */
	bool    ph_escaped;        /* allow backslash to escape placeholders */
	bool    expand_array;      /* transform arrays from the db into Perl arrays? Default is 1 */
	bool    txn_read_only;     /* are we in read-only mode? Set with $dbh->{ReadOnly} */

	int     pg_enable_utf8;    /* legacy utf8 flag: force utf8 flag on or off, regardless of client_encoding */
	bool    pg_utf8_flag;      /* are we currently flipping the utf8 flag on? */
    bool    client_encoding_utf8; /* is the client_encoding utf8 last we checked? */

    PGresult  *last_result;     /* PGresult structure from the last executed query (can be from imp_dbh or imp_sth) */
    long sth_result_owner;      /* Unique address of the sth that created it the above */
	imp_sth_t *do_tmp_sth;      /* temporary sth to refer inside a do() call */
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
	char  *fooname;             /* name if using :foo style */
	char  *value;               /* the literal passed-in value, may be binary */
	STRLEN valuelen;            /* length of the value */
	char  *quoted;              /* quoted version of the value, for PQexec only */
	STRLEN quotedlen;           /* length of the quoted value */
	bool   referenced;          /* used for PREPARE AS construction */
	bool   defaultval;          /* is it using a generic 'default' value? */
	bool   iscurrent;           /* do we want to use a literal CURRENT_TIMESTAMP? */
	bool   isdefault;           /* are we passing a literal 'DEFAULT'? */
	bool   isinout;             /* is this a bind_param_inout value? */
	SV     *inout;              /* what variable we are updating via inout magic */
	sql_type_info_t* bind_type; /* type information for this placeholder */
	struct ph_st *nextph;       /* more linked list goodness */
};
typedef struct ph_st ph_t;

typedef enum
	{
		PLACEHOLDER_NONE,
		PLACEHOLDER_QUESTIONMARK,
		PLACEHOLDER_DOLLAR,
		PLACEHOLDER_COLON
	} PGPlaceholderType;
#define PLACEHOLDER_TYPE_COUNT (PLACEHOLDER_COLON + 1)

/* Define sth implementor data structure */
struct imp_sth_st {
	dbih_stc_t com;          /* MUST be first element in structure */

	bool   server_prepare;    /* inherited from dbh */
	int    switch_prepared;   /* inherited from dbh */
    int    number_iterations; /* how many times has the statement been executed? Used by switch_prepared */
	PGPlaceholderType placeholder_type;  /* which style is being used 1=? 2=$1 3=:foo */
	int    numsegs;           /* how many segments this statement has */
	int    numphs;            /* how many placeholders this statement has */
	int    numbound;          /* how many placeholders were explicitly bound by the client, not us */
	int    cur_tuple;         /* current tuple being fetched */
	long   rows;              /* number of affected rows */
	int    async_flag;        /* async? 0=no 1=async 2=cancel 4=wait */
	int    async_status;      /* 0=no async 1=async started -1=async has been cancelled */

	STRLEN totalsize;        /* total string length of the statement (with no placeholders)*/

	const char ** PQvals;    /* List of values to pass to PQ* */
	int         * PQlens;    /* List of lengths to pass to PQ* */
	int         * PQfmts;    /* List of formats to pass to PQ* */
	Oid         * PQoids;    /* List of types to pass to PQ* */
	char   *prepare_name;    /* name of the prepared query; NULL if not prepared */
	char   *firstword;       /* first word of the statement */

	PGresult  *result;       /* result structure from the executed query */
	sql_type_info_t **type_info; /* type of each column in result */

	seg_t  *seg;             /* linked list of segments */
	ph_t   *ph;              /* linked list of placeholders */

	bool   prepare_now;      /* prepare this statement right away, even if it has placeholders */
	bool   prepared_by_us;   /* false if {prepare_name} set directly */
	bool   direct;           /* allow bypassing of the statement parsing */
	bool   is_dml;           /* is this SELECT/INSERT/UPDATE/DELETE? */
	bool   has_binary;       /* does it have one or more binary placeholders? */
	bool   has_default;      /* does it have one or more 'DEFAULT' values? */
	bool   has_current;      /* does it have one or more 'DEFAULT' values? */
	bool   dollaronly;       /* Only use $1 as placeholders, allow all else */
	bool   nocolons;         /* do not consider :1, :2 ... as valid placeholders */
	bool   use_inout;        /* Any placeholders using inout? */
	bool   all_bound;        /* Have all placeholders been bound? */
};


/* Avoid name clashes by assigning DBI funcs to a pg_ name. */
/* In order of appearance in dbdimp.c */

#define dbd_init  pg_init
extern void dbd_init (dbistate_t *dbistate);

#define dbd_db_login6 pg_db_login6
int dbd_db_login6 (SV * dbh, imp_dbh_t * imp_dbh, char * dbname, char * uid, char * pwd, SV *attr);

#define dbd_db_ping  pg_db_ping
int dbd_db_ping(SV *dbh);

#define dbd_db_commit  pg_db_commit
int dbd_db_commit (SV * dbh, imp_dbh_t * imp_dbh);

#define dbd_db_rollback  pg_db_rollback
int dbd_db_rollback (SV * dbh, imp_dbh_t * imp_dbh);

#define dbd_db_disconnect  pg_db_disconnect
int dbd_db_disconnect (SV * dbh, imp_dbh_t * imp_dbh);

#define dbd_db_destroy  pg_db_destroy
void dbd_db_destroy (SV * dbh, imp_dbh_t * imp_dbh);

#define dbd_db_FETCH_attrib  pg_db_FETCH_attrib
SV * dbd_db_FETCH_attrib (SV * dbh, imp_dbh_t * imp_dbh, SV * keysv);

#define dbd_db_STORE_attrib  pg_db_STORE_attrib
int dbd_db_STORE_attrib (SV * dbh, imp_dbh_t * imp_dbh, SV * keysv, SV * valuesv);

#define dbd_st_FETCH_attrib  pg_st_FETCH_attrib
SV * dbd_st_FETCH_attrib (SV * sth, imp_sth_t * imp_sth, SV * keysv);

#define dbd_st_STORE_attrib  pg_st_STORE_attrib
int dbd_st_STORE_attrib (SV * sth, imp_sth_t * imp_sth, SV * keysv, SV * valuesv);

#define dbd_discon_all  pg_discon_all
int dbd_discon_all (SV * drh, imp_drh_t * imp_drh);

#define dbd_st_prepare_sv  pg_st_prepare_sv
int dbd_st_prepare_sv (SV * sth, imp_sth_t * imp_sth, SV * statement_sv, SV * attribs);

#define dbd_bind_ph pg_bind_ph
int dbd_bind_ph (SV * sth, imp_sth_t * imp_sth, SV * ph_name, SV * newvalue, IV sql_type, SV * attribs, int is_inout, IV maxlen);

#define dbd_st_execute pg_st_execute
long dbd_st_execute (SV * sth, imp_sth_t * imp_sth);

#define dbd_st_fetch  pg_st_fetch
AV * dbd_st_fetch (SV * sth, imp_sth_t * imp_sth);

#define dbd_st_rows pg_st_rows
long dbd_st_rows (SV * sth, imp_sth_t * imp_sth);

#define dbd_st_finish  pg_st_finish
int dbd_st_finish (SV * sth, imp_sth_t * imp_sth);

#define dbd_st_cancel pg_st_cancel
int dbd_st_cancel (SV * sth, imp_sth_t * imp_sth);

#define dbd_st_destroy  pg_st_destroy
void dbd_st_destroy (SV * sth, imp_sth_t * imp_sth);

#define dbd_st_blob_read pg_st_blob_read
int dbd_st_blob_read (SV * sth, imp_sth_t * imp_sth, int lobjId, long offset, long len, SV * destrv, long destoffset);

#define dbd_st_canonical_ids pg_st_canonical_ids
SV* dbd_st_canonical_ids(SV *sth, imp_sth_t *imp_sth);

#define dbd_st_canonical_names pg_st_canonical_names
SV* dbd_st_canonical_names(SV *sth, imp_sth_t *imp_sth);


/* 
   Everything else should map back to the DBI version, or be handled by Pg.pm
   TODO: Explicitly map out each one.
*/


/* Custom PG functions, in order they appear in dbdimp.c */

int pg_db_getfd (imp_dbh_t * imp_dbh);

SV * pg_db_pg_notifies (SV *dbh, imp_dbh_t *imp_dbh);

SV * pg_rightgraded_sv(pTHX_ SV *input, bool utf8);

SV * pg_stringify_array(SV * input, const char * array_delim, int server_version, bool utf8);

long pg_quickexec (SV *dbh, const char *sql, const int asyncflag);

int pg_db_putline (SV *dbh, SV *svbuf);

int pg_db_getline (SV *dbh, SV * svbuf, int length);

int pg_db_getcopydata (SV *dbh, SV * dataline, int async);

int pg_db_putcopydata (SV *dbh, SV * dataline);

int pg_db_putcopyend (SV * dbh);

int pg_db_endcopy (SV * dbh);

SV * pg_db_error_field (SV *dbh, char * fieldname);

void pg_db_pg_server_trace (SV *dbh, FILE *fh);

void pg_db_pg_server_untrace (SV *dbh);

int pg_db_savepoint (SV *dbh, imp_dbh_t *imp_dbh, char * savepoint);

int pg_db_rollback_to (SV *dbh, imp_dbh_t *imp_dbh, const char * savepoint);

int pg_db_release (SV *dbh, imp_dbh_t *imp_dbh, char * savepoint);

unsigned int pg_db_lo_creat (SV *dbh, int mode);

int pg_db_lo_open (SV *dbh, unsigned int lobjId, int mode);

int pg_db_lo_close (SV *dbh, int fd);

int pg_db_lo_read (SV *dbh, int fd, char *buf, size_t len);

int pg_db_lo_write (SV *dbh, int fd, char *buf, size_t len);

int pg_db_lo_lseek (SV *dbh, int fd, int offset, int whence);

int pg_db_lo_tell (SV *dbh, int fd);

int pg_db_lo_truncate (SV *dbh, int fd, size_t len);

int pg_db_lo_unlink (SV *dbh, unsigned int lobjId);

unsigned int pg_db_lo_import (SV *dbh, char *filename);

unsigned int pg_db_lo_import_with_oid (SV *dbh, char *filename, unsigned int lobjId);

int pg_db_lo_export (SV *dbh, unsigned int lobjId, char *filename);

long pg_db_result (SV *h, imp_dbh_t *imp_dbh);

int pg_db_ready(SV *h, imp_dbh_t *imp_dbh);

int pg_db_cancel (SV *h, imp_dbh_t *imp_dbh);

int pg_db_cancel_sth (SV *sth, imp_sth_t *imp_sth);

/* end of dbdimp.h */
