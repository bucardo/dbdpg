/*

   $Id$

   Copyright (c) 2002-2004 PostgreSQL Global Development Group
   Copyright (c) 2002 Jeffrey W. Baker
   Copyright (c) 1997,1998,1999,2000 Edmund Mergl
   Portions Copyright (c) 1994,1995,1996,1997 Tim Bunce
   
   You may distribute under the terms of either the GNU General Public
   License or the Artistic License, as specified in the Perl README file.

*/


#include "Pg.h"
#include <assert.h>
#include "types.h"

/* XXX DBI should provide a better version of this */
#define IS_DBI_HANDLE(h) (SvROK(h) && SvTYPE(SvRV(h)) == SVt_PVHV && SvRMAGICAL(SvRV(h)) && (SvMAGIC(SvRV(h)))->mg_type == 'P')

DBISTATE_DECLARE;

/* Someday, we can abandon pre-7.4 and life will be much easier... */

#ifdef HAVE_PQprotocol
#define PG74 1
#else
/* Limited emulation - use with care! XXX - hokey */
typedef enum
{
	PQTRANS_IDLE,				/* connection idle */
	PQTRANS_ACTIVE,				/* command in progress */
	PQTRANS_INTRANS,			/* idle, within transaction block */
	PQTRANS_INERROR,			/* idle, within failed transaction */
	PQTRANS_UNKNOWN				/* cannot determine status */
} PGTransactionStatusType;
PGresult *PQexecPrepared() { };
Oid PQftable() { return InvalidOid; };
int PQftablecol() { return 0; };
#define PG_DIAG_SQLSTATE 'C'
#endif

#ifndef PGErrorVerbosity
typedef enum
{
	PGERROR_TERSE,				/* single-line error messages */
	PGERROR_DEFAULT,			/* recommended style */
	PGERROR_VERBOSE				/* all the facts, ma'am */
} PGErrorVerbosity;
#endif

void pg_error();
ExecStatusType _result();
void dbd_st_split_statement();
int dbd_st_prepare_statement();
int dbd_db_transaction_status();
int dbd_st_deallocate_statement();
PGTransactionStatusType dbd_db_txn_status();
#include "large_object.c"


/* ================================================================== */
ExecStatusType _result(imp_dbh, com)
		 imp_dbh_t *imp_dbh;
		 const char *com;
{
	PGresult* result;
	ExecStatusType status;

	result = PQexec(imp_dbh->conn, com);
	status = result ? PQresultStatus(result) : -1;
#ifdef PG74
	strncpy(imp_dbh->sqlstate,
					NULL == PQresultErrorField(result,PG_DIAG_SQLSTATE) ? "00000" : 
					PQresultErrorField(result,PG_DIAG_SQLSTATE),
					5);
#else
	strncpy(imp_dbh->sqlstate, "S1000",5); /* DBI standard says this is the default */
#endif
	PQclear(result);
	return status;

} /* end of _result */


/* ================================================================== */

/* Turn database notices into perl warnings for proper handling. */
static void pg_warn (arg, message)
		 void *arg;
		 const char *message;
{
	D_imp_dbh( sv_2mortal(newRV((SV*)arg)) );
	
	if (DBIc_WARN(imp_dbh))
		warn(message);

}


/* ================================================================== */

/* Database specific error handling. */
void pg_error (h, error_num, error_msg)
		 SV *h;
		 int error_num;
		 char *error_msg;
{
	D_imp_xxh(h);
	char *err, *src, *dst; 
	int len = strlen(error_msg);
	
	New(0, err, len+1, char);
	if (!err)
		return;
	
	src = error_msg;
	dst = err;
	
	/* copy error message without trailing newlines */
	while (*src != '\0') {
		*dst++ = *src++;
	}
	*dst = '\0';
	
	sv_setiv(DBIc_ERR(imp_xxh), (IV)error_num);		 /* set err early */
	sv_setpv(DBIc_ERRSTR(imp_xxh), (char*)err);
	DBIh_EVENT2(h, ERROR_event, DBIc_ERR(imp_xxh), DBIc_ERRSTR(imp_xxh));
	if (dbis->debug >= 2) {
		PerlIO_printf(DBILOGFP, "%s error %d recorded: %s\n",
									err, error_num, SvPV(DBIc_ERRSTR(imp_xxh),na));
	}
	Safefree(err);

} /* end of pg_error */


/* ================================================================== */

void dbd_init (dbistate)
		 dbistate_t *dbistate;
{
	DBIS = dbistate;
}


/* ================================================================== */

int dbd_db_login (dbh, imp_dbh, dbname, uid, pwd)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
		 char *dbname;
		 char *uid;
		 char *pwd;
{
	dTHR;
	
	char *conn_str;
	char *src;
	char *dest;
	int connect_string_size;
	char inquote = 0;
	
	if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "dbd_db_login\n"); }
	
	/* DBD::Pg syntax: 'dbname=dbname;host=host;port=port' */
	/* libpq syntax: 'dbname=dbname host=host port=port user=uid password=pwd' */

	/* Figure out how large our connection string is going to be */
	connect_string_size = strlen(dbname);
	if (strlen(uid)) {
		connect_string_size += strlen(" user=") + strlen(uid);
	}
	if (strlen(pwd)) {
		connect_string_size += strlen(" password=") + strlen(pwd);
	}

	New(0, conn_str, connect_string_size+1, char);
	if (!conn_str)
		croak("No memory");
	
	/* Change all semi-colons in dbname to a space, unless quoted */
	src = dbname;
	dest = conn_str;
	while (*src) {
		if (';' == *src && !inquote)
			*dest++ = ' ';
		else {
			if ('\'' == *src)
				inquote = !inquote;
			*dest++ = *src;
		}
		src++;
	}
	*dest = '\0';
	
	/* Add in the user and/or password if they exist */
	if (strlen(uid)) {
		strcat(conn_str, " user=");
		strcat(conn_str, uid);
	}
	if (strlen(pwd)) {
		strcat(conn_str, " password=");
		strcat(conn_str, pwd);
	}
	
	if (dbis->debug >= 2) { PerlIO_printf(DBILOGFP, "dbd_db_login: conn_str = >%s<\n", conn_str); }
	
	/* Make a connection to the database */
	
	imp_dbh->conn = PQconnectdb(conn_str);
	Safefree(conn_str);
	
	/* Check to see that the backend connection was successfully made */
	if (CONNECTION_OK != PQstatus(imp_dbh->conn)) {
		pg_error(dbh, PQstatus(imp_dbh->conn), PQerrorMessage(imp_dbh->conn));
		PQfinish(imp_dbh->conn);
		return 0;
	}
	
	/* Enable warnings to go through perl */
	PQsetNoticeProcessor(imp_dbh->conn, pg_warn, (void *)SvRV(dbh));
	
	/* Figure out what protocol this server is using */
#ifdef PG74
	imp_dbh->pg_protocol = PQprotocolVersion(imp_dbh->conn);
#else
	imp_dbh->pg_protocol = 0;
#endif	

	New(0, imp_dbh->sqlstate, 6, char);
	strcpy(imp_dbh->sqlstate, "S1000");
	imp_dbh->done_begin = 0; /* We are not inside a transaction */
	imp_dbh->pg_bool_tf = 0;
	imp_dbh->pg_enable_utf8 = 0;
	imp_dbh->prepare_number = 1;
	imp_dbh->prepare_now = 0;
	imp_dbh->errorlevel = 1; /* Matches PG default */

	/* Change the below someday to default to "on" */
	imp_dbh->server_prepare = imp_dbh->pg_protocol >=3 ? 0 : 0;

	DBIc_IMPSET_on(imp_dbh); /* imp_dbh set up now */
	DBIc_ACTIVE_on(imp_dbh); /* call disconnect before freeing */

	return 1;

} /* end of dbd_db_login */


/* ================================================================== */

int dbd_db_ping (dbh)
		 SV *dbh;
{
	D_imp_dbh(dbh);
	PGresult* result;
	ExecStatusType status;

	/* XXX Todo: can we just look at status directly? Whole test better? */

	if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "dbd_db_ping\n"); }
	
	if (NULL == imp_dbh->conn)
		return 0;

	status = _result(imp_dbh," "); /*?: SELECT 'dbdpg ping' */

	if (PGRES_EMPTY_QUERY != status)
		return 0;
		
	return 1;

} /* end of dbd_db_ping */


/* ================================================================== */
PGTransactionStatusType dbd_db_txn_status (imp_dbh)
		 imp_dbh_t *imp_dbh;
{

	/* Non - 7.3 *compiled* servers always return unknown */

	/* XXX Todo: note warning about 7.3 servers using "autocommit" */

#ifdef PG74
	return PQtransactionStatus(imp_dbh->conn);
#else
	return PQTRANS_UNKNOWN; /* See PG74 def */
#endif

} /* end of dbd_db_txn_status */


/* rollback and commit share so much code they get one function: */

/* ================================================================== */
int dbd_db_rollback_commit (dbh, imp_dbh, action)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
		 char * action;
{

	PGTransactionStatusType tstatus;
	ExecStatusType status;

	if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "%s\n", action); }
	
	/* no action if AutoCommit = on or the connection is invalid */
	if ((NULL == imp_dbh->conn) || (FALSE != DBIc_has(imp_dbh, DBIcf_AutoCommit)))
		return 0;

	/* We only perform these actions if we need to. For newer servers, we 
		 ask it for the status directly and double-check things */

#ifdef PG74
	tstatus = dbd_db_txn_status(imp_dbh);
	if (PQTRANS_IDLE == tstatus) { /* Not in a transaction */
		if (imp_dbh->done_begin) {
			/* We think we ARE in a tranaction but we really are not */
			if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "Warning: invalid done_begin turned off\n"); }
			imp_dbh->done_begin = 0;
		}
	}
	else if (PQTRANS_UNKNOWN != tstatus) { /* In a transaction */
		if (!imp_dbh->done_begin) {
			/* We think we are NOT in a transaction but we really are */
			if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "Warning: invalid done_begin turned on\n"); }
			imp_dbh->done_begin = 1;
		}
	}
	else { /* Something is wrong: transation status unknown */
		if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "Warning: cannot determine transaction status\n"); }
	}
#endif

	if (!imp_dbh->done_begin)
		return 0;

	status = _result(imp_dbh, action);
		
	if (PGRES_COMMAND_OK != status) {
		pg_error(dbh, status, PQerrorMessage(imp_dbh->conn));
		return 0;
	}

	imp_dbh->done_begin = 0;
	return 1;

} /* end of dbd_db_rollback_commit */
/* ================================================================== */

int dbd_db_commit (dbh, imp_dbh)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
{
	return dbd_db_rollback_commit(dbh, imp_dbh, "commit");
}
/* ================================================================== */

int dbd_db_rollback (dbh, imp_dbh)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
{
	return dbd_db_rollback_commit(dbh, imp_dbh, "rollback");
}


/* ================================================================== */

int dbd_db_disconnect (dbh, imp_dbh)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
{
	dTHR;
	ExecStatusType status;
	
	if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "dbd_db_disconnect\n"); }

	/* We assume that disconnect will always work	
		 since most errors imply already disconnected. */

	DBIc_ACTIVE_off(imp_dbh);
	
	if (NULL != imp_dbh->conn) {
		/* Rollback if needed */
		if (dbd_db_rollback(dbh, imp_dbh) && dbis->debug >= 2)
			PerlIO_printf(DBILOGFP, "dbd_db_disconnect: AutoCommit=off -> rollback\n");
		
		PQfinish(imp_dbh->conn);
		
		imp_dbh->conn = NULL;
	}

	/* We don't free imp_dbh since a reference still exists	*/
	/* The DESTROY method is the only one to 'free' memory.	*/
	/* Note that statement objects may still exists for this dbh!	*/

	return 1;

} /* end of dbd_db_disconnect */


/* ================================================================== */

void dbd_db_destroy (dbh, imp_dbh)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
{
	if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "dbd_db_destroy\n"); }

	Safefree(imp_dbh->sqlstate);

	if (DBIc_ACTIVE(imp_dbh)) {
		dbd_db_disconnect(dbh, imp_dbh);
	}

	DBIc_IMPSET_off(imp_dbh);

} /* end of dbd_db_destroy */


/* ================================================================== */

int dbd_db_STORE_attrib (dbh, imp_dbh, keysv, valuesv)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
		 SV *keysv;
		 SV *valuesv;
{
	ExecStatusType status;
	STRLEN kl;
	char *key = SvPV(keysv,kl);
	int newval = SvTRUE(valuesv);
	int oldval;

	if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "dbd_db_STORE\n"); }
	
	if (kl==10 && strEQ(key, "AutoCommit")) {
		oldval = DBIc_has(imp_dbh, DBIcf_AutoCommit);
		if (oldval == newval)
			return 1;
		if (oldval) {
			/* Commit if necessary */
			if (dbd_db_commit(dbh, imp_dbh) && dbis->debug >= 2)
				PerlIO_printf(DBILOGFP, "dbd_db_STORE: AutoCommit on forced a commit\n");
		}
		DBIc_set(imp_dbh, DBIcf_AutoCommit, newval);
		return 1;
	}
	else if (kl==14 && strEQ(key, "pg_auto_escape")) {
		imp_dbh->pg_auto_escape = newval ? 1 : 0;
	} 
	else if (kl==10 && strEQ(key, "pg_bool_tf")) {
		imp_dbh->pg_bool_tf = newval ? 1 : 0;
#ifdef is_utf8_string
	} 
	else if (kl==14 && strEQ(key, "pg_enable_utf8")) {
		imp_dbh->pg_enable_utf8 = newval ? 1 : 0;
#endif
	}
	else if (kl==14 && strEQ(key, "server_prepare")) {
		if (imp_dbh->pg_protocol >=3) {
			imp_dbh->server_prepare = newval ? 1 : 0;
		}
	}
	else if (kl==11 && strEQ(key, "prepare_now")) {
		if (imp_dbh->pg_protocol >=3) {
			imp_dbh->prepare_now = newval ? 1 : 0;
		}
	}
	else {
		return 0;
	}

} /* end of dbd_db_STORE_attrib */


/* ================================================================== */

SV * dbd_db_FETCH_attrib (dbh, imp_dbh, keysv)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
		 SV *keysv;
{
	STRLEN kl;
	char *key = SvPV(keysv,kl);
	SV *retsv = Nullsv;
	char *host = NULL;
	
	if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "dbd_db_FETCH\n"); }
	
	if (kl==10 && strEQ(key, "AutoCommit")) {
		retsv = boolSV(DBIc_has(imp_dbh, DBIcf_AutoCommit));
	} else if (kl==14 && strEQ(key, "pg_auto_escape")) {
		retsv = newSViv((IV)imp_dbh->pg_auto_escape);
	} else if (kl==10 && strEQ(key, "pg_bool_tf")) {
		retsv = newSViv((IV)imp_dbh->pg_bool_tf);
#ifdef is_utf8_string
	} else if (kl==14 && strEQ(key, "pg_enable_utf8")) {
		retsv = newSViv((IV)imp_dbh->pg_enable_utf8);
#endif
	} else if (kl==11 && strEQ(key, "pg_INV_READ")) {
		retsv = newSViv((IV)INV_READ);
	} else if (kl==12 && strEQ(key, "pg_INV_WRITE")) {
		retsv = newSViv((IV)INV_WRITE);
	} else if (kl==11 && strEQ(key, "pg_protocol")) {
		retsv = newSViv((IV)imp_dbh->pg_protocol);
	} else if (kl==14 && strEQ(key, "server_prepare")) {
		retsv = newSViv((IV)imp_dbh->server_prepare);
	} else if (kl==11 && strEQ(key, "prepare_now")) {
		retsv = newSViv((IV)imp_dbh->prepare_now);
	} 
	/* All the following are called too infrequently to bother caching */

	else if (kl==5 && strEQ(key, "pg_db")) {
		retsv = newSVpv(PQdb(imp_dbh->conn),0);
	} else if (kl==7 && strEQ(key, "pg_user")) {
		retsv = newSVpv(PQuser(imp_dbh->conn),0);
	} else if (kl==7 && strEQ(key, "pg_pass")) {
		retsv = newSVpv(PQpass(imp_dbh->conn),0);
	} else if (kl==7 && strEQ(key, "pg_host")) {
		host = PQhost(imp_dbh->conn); /* May return null */
		if (NULL==host)
			return Nullsv;
		retsv = newSVpv(host,0);
	} else if (kl==7 && strEQ(key, "pg_port")) {
		retsv = newSVpv(PQport(imp_dbh->conn),0);
	} else if (kl==10 && strEQ(key, "pg_options")) {
		retsv = newSVpv(PQoptions(imp_dbh->conn),0);
	} else if (kl==9 && strEQ(key, "pg_socket")) {
		retsv = newSViv((IV)PQsocket(imp_dbh->conn));
	} else if (kl==6 && strEQ(key, "pg_pid")) {
		retsv = newSViv((IV)PQbackendPID(imp_dbh->conn));
	}
	
	if (!retsv)
		return Nullsv;
	
	if (retsv == &sv_yes || retsv == &sv_no) {
		return retsv; /* no need to mortalize yes or no */
	}
	return sv_2mortal(retsv);

} /* end of dbd_db_FETCH_attrib */


/* ================================================================== */

int dbd_discon_all (drh, imp_drh)
		 SV *drh;
		 imp_drh_t *imp_drh;
{
	dTHR;
	
	if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "dbd_discon_all\n"); }
	
	/* The disconnect_all concept is flawed and needs more work */
	if (!dirty && !SvTRUE(perl_get_sv("DBI::PERL_ENDING",0))) {
		sv_setiv(DBIc_ERR(imp_drh), (IV)1);
		sv_setpv(DBIc_ERRSTR(imp_drh),
						 (char*)"disconnect_all not implemented");
		DBIh_EVENT2(drh, ERROR_event,
								DBIc_ERR(imp_drh), DBIc_ERRSTR(imp_drh));
	}
	return FALSE;

} /* end of dbd_discon_all */


/* ================================================================== */

int dbd_db_getfd (dbh, imp_dbh)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
{
	if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "dbd_db_getfd\n"); }
	
	return PQsocket(imp_dbh->conn);

} /* end of dbd_db_getfd */


/* ================================================================== */

SV * dbd_db_pg_notifies (dbh, imp_dbh)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
{
	char id;
	PGnotify* notify;
	AV* ret;
	SV* retsv;
	int status;
	
	if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "dbd_db_pg_notifies\n"); }
	
	status = PQconsumeInput(imp_dbh->conn);
	if (0 == status) { 
		pg_error(dbh, PQstatus(imp_dbh->conn), PQerrorMessage(imp_dbh->conn));
		return 0;
	}
	
	notify = PQnotifies(imp_dbh->conn);
	
	if (!notify) return &sv_undef; 
	
	ret=newAV();
	
	av_push(ret, newSVpv(notify->relname,0) );
	av_push(ret, newSViv(notify->be_pid) );
	
	/* Should free notify memory with PQfreemem() */
#ifdef PG74
 	PQfreemem(notify);
#else
	Safefree(notify);
#endif

	retsv = newRV(sv_2mortal((SV*)ret));
	
	return retsv;

} /* end of dbd_db_pg_notifies */


/* ================================================================== */

int dbd_st_prepare (sth, imp_sth, statement, attribs)
		 SV *sth;
		 imp_sth_t *imp_sth;
		 char *statement;
		 SV *attribs; /* hashref of arguments passed to prepare */
{

	D_imp_dbh_from_sth;
	unsigned int mypos=0, wordstart, newsize; /* Used to find and set firstword */
	SV **svp; /* To help parse the arguments */
	char server_prepare = 2; /* Three states: 0=no 1=yes 2=not set yet */
	bool direct = 0; /* Allow bypass of splitting and binding */
	int prepare_now = imp_dbh->prepare_now; /* Force an immediate prepare? */

	if (dbis->debug >= 1)
		PerlIO_printf(DBILOGFP, "dbd_st_prepare: >%s<\n", statement);
	
	/* Must have at least one character */
	if (NULL == statement) {
		fprintf(stderr, "NULL statement!\n");
	}

	/* Not preparable DML until proved otherwise */
	imp_sth->is_dml = 0;

	/* Figure out the first word in the statement */
	while (*statement && isSPACE(*statement)) {
		mypos++;
		statement++;
	}
	if (!*statement || !isALPHA(*statement)) {
		imp_sth->firstword = NULL;
	}
	else {
		wordstart = mypos;
		while(*statement && isALPHA(*statement)) {
			mypos++;
			statement++;
		}
		newsize = mypos-wordstart;
		New(0, imp_sth->firstword, newsize+1, char);
		if (!imp_sth->firstword)
			croak ("No memory");
		Copy(statement-newsize,imp_sth->firstword,newsize,char);
		imp_sth->firstword[newsize] = '\0';
		/* Try to prevent transaction commands */
		if (!strcasecmp(imp_sth->firstword, "END") ||
				!strcasecmp(imp_sth->firstword, "BEGIN") ||
				!strcasecmp(imp_sth->firstword, "ABORT") ||
				!strcasecmp(imp_sth->firstword, "COMMIT") ||
				!strcasecmp(imp_sth->firstword, "ROLLBACK")
				) {
			croak ("Please use DBI functions for transaction handling");
		}
		/* Note whether this is preparable DML */
		if (!strcasecmp(imp_sth->firstword, "SELECT") ||
				!strcasecmp(imp_sth->firstword, "INSERT") ||
				!strcasecmp(imp_sth->firstword, "UPDATE") ||
				!strcasecmp(imp_sth->firstword, "DELETE")
				) {
			imp_sth->is_dml = 1;
		}
	}
	statement -= mypos; /* Rewind statement */

	/* Parse and set any attributes passed in */
	if (attribs) {
		if ((svp = hv_fetch((HV*)SvRV(attribs),"server_prepare", 14, 0)) != NULL)
			server_prepare = 0==SvIV(*svp) ? 0 : 1;
		if ((svp = hv_fetch((HV*)SvRV(attribs),"direct", 6, 0)) != NULL)
			direct = 0==SvIV(*svp) ? 0 : 1;
		if ((svp = hv_fetch((HV*)SvRV(attribs),"prepare_now", 11, 0)) != NULL)
			prepare_now = 0==SvIV(*svp) ? 0 : 1;
		/* bind_type is done in Pg.pm for now */
	}

	imp_sth->server_prepare = server_prepare;
	imp_sth->direct = direct;
	imp_sth->prepared_by_us = 0;
	imp_sth->result	= 0;
	imp_sth->cur_tuple = 0;
	imp_sth->rows = -1;
	imp_sth->totalsize = imp_sth->numsegs = imp_sth->numphs = 0;
	imp_sth->prepare_name = NULL;

	/* Break the statement into segments by placeholder */
	dbd_st_split_statement(sth, imp_sth, statement);

	/*
		We prepare it right away if:
		1. The statement is DML
		2. It's not "direct"
		3. We can handle server-side prepares
		4. dbh->{server_prepare} is true
		5. They have not explicitly turned it off via sth->{server_prepare}
		6. "prepare_now" (via arguments) is not false (0)
		7. There are no placeholders OR "prepare_now" (via args/dbh) is on
	*/
	if (imp_sth->is_dml && 
			!imp_sth->direct &&
			imp_dbh->pg_protocol >= 3 &&
			imp_dbh->server_prepare == 1 &&
			imp_sth->server_prepare != 0 &&
			prepare_now != 0 &&
			(0==imp_sth->numphs || 1==prepare_now)
			) {
		if (dbis->debug >= 2)
			PerlIO_printf(DBILOGFP, "  dbdpg: immediate prepare\n");

		if (dbd_st_prepare_statement(sth, imp_sth)) {
			return -2; /* XXX unlikely, but may want to handle better.. */
		}
	}

	DBIc_IMPSET_on(imp_sth);

	return imp_sth->numphs;

} /* end of dbd_st_prepare */


/* ================================================================== */
int dbd_st_prepare_statement (sth, imp_sth)
		 SV *sth;
		 imp_sth_t *imp_sth;
{

	D_imp_dbh_from_sth;
	char *statement;
	unsigned int execsize, foonumber;
	PGresult* result;
	ExecStatusType status;
	seg_t *currseg;

	New(0, imp_sth->prepare_name, 25, char);
	if (!imp_sth->prepare_name)
		croak("No memory");

	/* Name is simply "dbdpg_#" */
	sprintf(imp_sth->prepare_name,"dbdpg_%d", imp_dbh->prepare_number);
	imp_sth->prepare_name[strlen(imp_sth->prepare_name)]='\0';

	if (dbis->debug >= 2)
		PerlIO_printf(DBILOGFP, "  dbdpg: new statement name \"%s\"\n",
									imp_sth->prepare_name);

	assert(strlen(imp_sth->prepare_name)<25);

	/* Compute the size of everything, allocate, and populate */
	execsize = strlen("PREPARE  AS ") + strlen(imp_sth->prepare_name);
	execsize += imp_sth->totalsize;

	if (imp_sth->numphs) {
		execsize += strlen("()");
		for (currseg=imp_sth->seg; NULL != currseg; currseg=currseg->nextseg) {
			if (NULL != currseg->placeholder) {
				/* We may have gotten here without binding a type. If so, give a default */
				if (NULL == currseg->bind_type) {
					currseg->bind_type = pg_type_data(VARCHAROID);
				}
				execsize += strlen(currseg->bind_type->type_name)+1;
				/* If we are using the :foo style, we just give it a default room for 9999 placeholders */
				execsize += 3==imp_sth->placeholder_type ? 5 : strlen(currseg->placeholder);
			}
		}
	}

	New(0, statement, execsize+1, char);
	sprintf(statement, "PREPARE %s", imp_sth->prepare_name);
	
	if (imp_sth->numphs) {
		strcat(statement, "(");
		for (currseg=imp_sth->seg; NULL != currseg; currseg=currseg->nextseg) {
			if (NULL != currseg->placeholder) {
				strcat(statement, currseg->bind_type->type_name);
				strcat(statement, (NULL != currseg->nextseg && 
													 NULL != currseg->nextseg->placeholder) ? "," : ")");
			}
		}
	}

	strcat(statement, " AS ");

	/* For the ":foo" style, map each to a number, up to $9999 */
	foonumber = 1;
	for (currseg=imp_sth->seg; NULL != currseg; currseg=currseg->nextseg) {
		strcat(statement, currseg->segment);
		if (NULL != currseg->bind_type) {
			if (10000 == foonumber)
				croak("When using the :foo style of placeholder, only (!) 9999 placeholders can be used");
			if (3==imp_sth->placeholder_type) {
				sprintf(statement, "%s$%d", statement, foonumber++);
			}
			else {
				strcat(statement, currseg->placeholder);
			}
		}
	}
	statement[execsize] = '\0';
	if (dbis->debug >= 2)
		PerlIO_printf(DBILOGFP, "  dbdpg: \"%s\"\n", statement);

	status = _result(imp_dbh, statement);
	Safefree(statement);

	if (PGRES_COMMAND_OK != status) {
		pg_error(sth,status,PQerrorMessage(imp_dbh->conn));
		return -2;
	}
	imp_sth->prepared_by_us = 1; /* Done here so deallocate is not called spuriously */
	imp_dbh->prepare_number++; /* We do this at the end so we don't increment if we fail above */

	return 0;
	
} /* end of dbd_st_prepare_statement */


/* ================================================================== */

void dbd_st_split_statement (sth, imp_sth, statement)
		 SV *sth;
		 imp_sth_t *imp_sth;
		 const char *statement;
{

	/* Builds the "segment" structures for a statement handle */

	unsigned int mypos=0, sectionstart=1, sectionstop, newsize;
	char ch, block=0, quote=0;
	unsigned int backslashes=0;
	char placeholder=0, oldplaceholder=0;
	char qsize=1;
	unsigned int qtop=10, topdollar = 0;
	seg_t *newseg, *currseg;
	imp_sth->seg = currseg = newseg = NULL;

	if (dbis->debug >= 1)
		PerlIO_printf(DBILOGFP, "dbd_st_split_statement\n");

	/* It's okay to split non-DML as we will never PREPARE it, but we may want to prepare it */
	if (imp_sth->direct) { 	/* User has specifically asked that we not split it */
		while(*statement++) {
			mypos++;
		}
		mypos++;
		imp_sth->direct=1;
	}

	while ((ch = *statement++)) {

		mypos++;

		/* Check for the end of a block */
		if (block) {
			if (
					/* dashdash and slashslash only terminate at newline */
					(('-' == block || '/' == block) && '\n' == ch) ||
					/* slashstar ends with a matching starslash */
					('*' == block && '*' == ch && '/' == *statement) ||
					/* end of array */
					(']' == ch)
					) {
				block = 0;
			}
			continue;
		}

		/* Check for the end of a quote */
		if (quote) {
			if (ch == quote) {
				if (!(backslashes & 1)) 
					quote = 0;
			}
			continue;
		}

		/* Check for the start of a quote */
		if ('\'' == ch || '"' == ch) {
			if (!(backslashes & 1))
				quote = ch;
			continue;
		}

		/* If a backslash, just count them to handle escaped quotes */
		if ('\\' == ch) {
			backslashes++;
			continue;
		}
		else {
			backslashes=0;
		}

		/* Check for the start of a 2 character block (e.g. comments) */
		if (('-' == ch && '-' == *statement) ||
				('/' == ch && '/' == *statement) ||
				('/' == ch && '*' == *statement)) {
			block = *statement;
			continue;
		}

		/* Check for the start of an array */
		if ('[' == ch) {
			block = ']'; 
			continue;
		}

		/* All we care about at this point is placeholder characters */
		if ('?' != ch && '$' != ch && ':' != ch)
			continue;

		sectionstop=mypos;

		placeholder = 0;

		/* Normal question mark style */
		if ('?' == ch) {
			placeholder = 1;
		}
		/* Dollar sign placeholder style */
		else if ('$' == ch && isDIGIT(*statement)) {
			if ('0' == *statement)
				croak("Invalid placeholder value");
			while(isDIGIT(*statement)) {
				++statement;
				++mypos;
			}
			placeholder = 2;
		}
		/* Colon style */
		else if (':' == ch) {
			/* Skip multiple colons (casting, e.g. "myval::float") */
			if (':' == *statement) {
				while(':' == *statement) {
					++statement;
					++mypos;
				}
				continue;
			}
			if (isALNUM(*statement)) {
				while(isALNUM(*statement)) {
					++statement;
					++mypos;
				}
				placeholder = 3;
			}
		}

		/* Bail unless it was a true placeholder */
		if (!placeholder)
			continue;

		if (oldplaceholder && placeholder != oldplaceholder) {
			croak("Cannot mix placeholder styles \"%s\" and \"%s\"",
						1==oldplaceholder ? "?" : 2==oldplaceholder ? "$1" : ":foo",
						1==placeholder ? "?" : 2==placeholder ? "$1" : ":foo");
		}
		oldplaceholder = placeholder;

		/* Create a new structure for this segment */
		imp_sth->numphs++;
		newsize = sectionstop-sectionstart;
		New(0, newseg, 1, seg_t);
		if (!newseg)
			croak ("No memory");
		newseg->nextseg = NULL;
		newseg->bind_type = NULL;
		newseg->quoted = newseg->value = NULL;
		New(0, newseg->segment, newsize+1, char);
		if (!newseg->segment)
			croak ("No memory");
		Copy(statement-(mypos-sectionstart+1),newseg->segment,newsize,char);
		newseg->segment[newsize]='\0';
		imp_sth->totalsize += newsize;

		/* Store information about this placeholder */
		if (1==placeholder) {
			if (imp_sth->numphs >= qtop) {
				qtop = 10^qsize++;
			}
			newsize = qsize+1;
		}
		else {
			newsize = mypos-sectionstop+1;
		}
		New(0, newseg->placeholder, newsize+1, char);
		if (!newseg->placeholder)
			croak ("No memory");
		if (1==placeholder) { /* The '?' type */
			sprintf(newseg->placeholder, "$%d", imp_sth->numphs);
		}
		else {
			Copy(statement-(mypos-sectionstop+1),newseg->placeholder,newsize,char);
		}
		newseg->placeholder[newsize]='\0';

		if (dbis->debug >= 2)
			PerlIO_printf(DBILOGFP, "  dbdpg segment: \"%s\"  placeholder: \"%s\"\n",
										newseg->segment, newseg->placeholder);

		/* Tie it in to the previous one */
		if (NULL == currseg) {
			imp_sth->seg = currseg = newseg;
		}
		else {
			currseg->nextseg = newseg;
			currseg = newseg;
		}
		sectionstart = mypos+1;

	} /* end statement parsing */

	imp_sth->numsegs = imp_sth->numphs;

	/* Store the final segment if needed */
	if (sectionstart <= mypos) {
		imp_sth->numsegs++;
		newsize = mypos-sectionstart+1;
		/* Create a new structure to hold this segment */
		New(0, newseg, 1, seg_t);
		if (!newseg)
			croak ("No memory");
		newseg->nextseg = NULL;
		newseg->bind_type = NULL;
		newseg->quoted = newseg->value = newseg->placeholder = NULL;
		New(0, newseg->segment, newsize+1, char);
		if (!newseg->segment)
			croak ("No memory");
		Move(statement-(mypos-sectionstart+2),newseg->segment,newsize,char);
		newseg->segment[newsize]='\0';

		if (dbis->debug >= 2)
			PerlIO_printf(DBILOGFP, "  dbdpg segment: \"%s\"  placeholder: \"%s\"\n",
										newseg->segment, newseg->placeholder);

		/* Tie it in to the previous one */
		if (NULL == currseg) {
			imp_sth->seg = currseg = newseg;
		}
		else {
			currseg->nextseg = newseg;
			currseg = newseg;
		}
		imp_sth->totalsize += newsize;
	}

	imp_sth->placeholder_type = placeholder;

	/* XXX Todo?: check for identical :foo names and treat as a single placeholder */

	/* Special checks for dollar sign placeholders */
	if (2==placeholder && imp_sth->numphs > 1) {
		/* 
			 We follow the Pg rules: must start with $1, repeats are allowed, 
			 numbers must be sequential. We change numphs if repeats found
		*/

		int totalfound=0;
		for (qtop=1; qtop<=imp_sth->numphs; qtop++) {
			for (currseg=imp_sth->seg; NULL != currseg; currseg=currseg->nextseg) {
				if (NULL != currseg->placeholder) {
					if (qtop == atoi(currseg->placeholder+1)) {
						totalfound++;
						if (topdollar<qtop)
							topdollar=qtop;
					}
				}
			}
			if (topdollar != qtop) {
				if (totalfound==imp_sth->numphs) {
					break;
				}
				croak("Invalid placeholders");
			}
		}
		imp_sth->numphs = topdollar;
		if (dbis->debug >= 1)
			PerlIO_printf(DBILOGFP, "Reset number of placeholders to %d\n", topdollar);
	}

	DBIc_NUM_PARAMS(imp_sth) = imp_sth->numphs;

} /* end dbd_st_split_statement */

/* XXX Todo?: Allow a simple bind_type() which sets only the type (not value) to a placeholder */

/* ================================================================== */

int dbd_bind_ph (sth, imp_sth, ph_name, newvalue, sql_type, attribs, is_inout, maxlen)
		 SV *sth;
		 imp_sth_t *imp_sth;
		 SV *ph_name;
		 SV *newvalue;
		 IV sql_type;
		 SV *attribs;
		 int is_inout;
		 IV maxlen;
{
	D_imp_dbh_from_sth;
	SV **phs_svp;
	SV **svp;
	STRLEN name_len;
	char *name = Nullch;
	char namebuf[20];
	sql_type_info_t *bind_type;
	int pg_type = 0;
	char *value_string;
	STRLEN value_len;

	unsigned int x, y;
	int matches=0;
	seg_t *currseg;
	bool reprepare = 0;
	
	if (dbis->debug >= 1)
		PerlIO_printf(DBILOGFP, "dbd_bind_ph\n");
	
	if (is_inout)
		croak("bind_inout not supported by this driver");

	if (!imp_sth->numphs) {
		croak("Statement has no placeholders to bind");
	}

	/* Check the placeholder name and transform to a standard form */
	if (SvGMAGICAL(ph_name)) {
		mg_get(ph_name);
	}
	name = SvPV(ph_name, name_len);
	if (3==imp_sth->placeholder_type) {
		if (':' != *name) {
			croak("Placeholders must begin with ':' when using the \":foo\" style");
		}
	}
	else {
		if (name_len >= 20) {
			croak("Placeholder should be in the format \"$1\"\n");
		}
		for (x=0; *(name+x); x++) {
			if (!isDIGIT(*(name+x)) && (x!=0 || '$'!=*(name+x))) {
				croak("Invalid placeholder name");
			}
		}
		if ('$' != *name) {
			namebuf[0] = '$';
			Copy(name,namebuf+1,name_len,char);
			namebuf[name_len+1]='\0';
			name = namebuf;
			name_len++;
		}
	}
	assert(name != Nullch);

	/* Check the value */
	if (SvTYPE(newvalue) > SVt_PVLV) { /* hook for later array logic	*/
		croak("Cannot bind a non-scalar value (%s)", neatsvpv(newvalue,0));
	}
	if ((SvROK(newvalue) &&!IS_DBI_HANDLE(newvalue) &&!SvAMAGIC(newvalue))) {
		/* dbi handle allowed for cursor variables */
		croak("Cannot bind a reference (%s)", neatsvpv(newvalue,0));
	}
	if (SvTYPE(newvalue) == SVt_PVLV && is_inout) {
		croak("Cannot bind ``lvalue'' mode scalar as inout parameter (currently)");
	}
	if (dbis->debug >= 1) {
		PerlIO_printf(DBILOGFP, "		 bind %s <== %s (type %ld", name, neatsvpv(newvalue,0), (long)sql_type);
		if (is_inout) {
			PerlIO_printf(DBILOGFP, ", inout 0x%lx, maxlen %ld", (long)newvalue, (long)maxlen);
		}
		if (attribs) {
			PerlIO_printf(DBILOGFP, ", attribs: %s", neatsvpv(attribs,0));
		}
		PerlIO_printf(DBILOGFP, ")\n");
	}
	
	/* Check for pg_type */
	if (attribs) {
		if((svp = hv_fetch((HV*)SvRV(attribs),"pg_type", 7, 0)) != NULL)
			pg_type = SvIV(*svp);
	}
	
	if (sql_type && pg_type)
		croak ("Cannot specify both sql_type and pg_type");
	
	if (pg_type) {
		if ((bind_type = pg_type_data(pg_type))) {
			if (!bind_type->bind_ok) { /* Re-evaluate with new prepare */
				croak("Cannot bind %s, sql_type %s not supported by DBD::Pg",
							name, bind_type->type_name);
			}
		}
		else {
			croak("Cannot bind %s unknown pg_type %" IVdf, name, pg_type);
		}
	}
	else if (sql_type) {
		/* always bind as pg_type, because we know we are 
			 inserting into a pg database... It would make no 
			 sense to quote something to sql semantics and break
			 the insert.
		*/
		if (!(bind_type = sql_type_data(sql_type))) {
			croak("Cannot bind %s unknown sql_type %" IVdf, name, sql_type);
		}
		if (!(bind_type = pg_type_data(bind_type->type.pg))) {
			croak("Cannot find a pg_type for %" IVdf, sql_type);
		}
 	}
	else {
		bind_type = pg_type_data(VARCHAROID);
		if (!bind_type)
			croak("Default type is bad!!!!???");
	}

	if (pg_type || sql_type) {
		/* Possible re-prepare, depending on whether the type name also changes */
		if (imp_sth->prepared_by_us && NULL != imp_sth->prepare_name)
			reprepare=1;
	}

	/* convert to a string ASAP */
	if (!SvPOK(newvalue) && SvOK(newvalue)) {
		sv_2pv(newvalue, &na);
	}

	/* upgrade to at least string */
	(void)SvUPGRADE(newvalue, SVt_PV);
	value_string = SvPV(newvalue, value_len);

	/* If placeholder type is '?', jump directly to that number */
	if (1==imp_sth->placeholder_type) {
		x = atoi(name+1);
		if (x > imp_sth->numphs) {
			croak("Placeholders only go up to %d", imp_sth->numphs); /* throw nicer error? */
		}
		for (y=1, currseg=imp_sth->seg; NULL != currseg; currseg=currseg->nextseg) {
			if (x==y++ && NULL != currseg->placeholder) {
				/* Do we really need a reprepare? */
				if (reprepare) {
					if (NULL != currseg->bind_type &&
							strcmp(currseg->bind_type->type_name, bind_type->type_name)) {
						reprepare=0;
					}
				}
				/* We only change the type if explicitly given one or none exists */
				if (NULL == currseg->bind_type || sql_type || pg_type)
					currseg->bind_type = bind_type;
				if (!SvOK(newvalue)) {
					currseg->value = NULL;
				}
				else {
					New(0, currseg->value, strlen(value_string)+1, char);
					strcpy(currseg->value, value_string);
					currseg->value[strlen(value_string)] = '\0';
				}
				matches++;
				break;
			}
		}
		if (!matches)
			croak("Invalid placeholder (%d)", x); /* We should not get here! */
	}
	else {
		/* Loop and replace all */
		for (currseg=imp_sth->seg; NULL != currseg && NULL != currseg->placeholder; currseg=currseg->nextseg) {
			if (strEQ(currseg->placeholder,name)) {
				matches++;
				/* Do we really need a reprepare? */
				if (reprepare) {
					if (NULL != currseg->bind_type &&
							strcmp(currseg->bind_type->type_name, bind_type->type_name)) {
						reprepare=0;
					}
				}
				/* We only change the type if explicitly given one or none exists */
				if (NULL == currseg->bind_type || sql_type || pg_type)
					currseg->bind_type=bind_type;
				if (!SvOK(newvalue)) {
					currseg->value = NULL;
				}
				else {
					New(0, currseg->value, strlen(value_string)+1, char);
					strcpy(currseg->value, value_string);
					currseg->value[strlen(value_string)] = '\0';
				}
			}
		}
	}
		
	if (!matches)
		croak("Cannot bind unknown placeholder '%s' (%s)", name, neatsvpv(ph_name,0));

	if (reprepare) {
		if (dbis->debug >= 2)
			PerlIO_printf(DBILOGFP, "  dbdpg: binding has forced a re-prepare\n");
		/* Deallocate sets the prepare_name to NULL */
		if (dbd_st_deallocate_statement(sth, imp_sth)) {
			/* Deallocation failed. Let's mark it and move on */
			imp_sth->prepare_name = NULL;
			if (dbis->debug >= 1)
				PerlIO_printf(DBILOGFP, "  dbdpg: failed to deallocate!\n");
		}
		assert(NULL == imp_sth->prepare_name);
	}

	if (dbis->debug >= 3)
		PerlIO_printf(DBILOGFP, "  dbdpg: placeholder \"%s\" bound as type \"%s\", value of \"%s\"\n",
									name, bind_type->type_name, value_string);

	return matches;

} /* end of dbd_bind_ph */


/* ================================================================== */
int dbd_st_execute (sth, imp_sth) /* <= -2:error, >=0:ok row count, (-1=unknown count) */
		 SV *sth;
		 imp_sth_t *imp_sth;
{
	D_imp_dbh_from_sth;
	ExecStatusType status = -1;
	PGTransactionStatusType tstatus;
	char *cmdStatus, *cmdTuples, *statement;
	int ret = -2;
	int num_fields;
	seg_t *currseg;
	unsigned int execsize;
	int x,y,z;

	if (dbis->debug >= 1)
		PerlIO_printf(DBILOGFP, "dbd_st_execute\n");
	
	if (NULL == imp_dbh->conn) {
		pg_error(sth, -1, "execute on disconnected handle");
		return -2;
	}

	/* XXX ? 
	//int outparams = (imp_sth->out_params_av) ? AvFILL(imp_sth->out_params_av)+1 : 0;
	//	print"Outparams is %d\n", outparams;
  */

	/* Ensure that all the placeholders have been bound */
	if (imp_sth->numphs) {
		for (currseg=imp_sth->seg; NULL != currseg; currseg=currseg->nextseg) {
			if (NULL != currseg->placeholder && NULL == currseg->bind_type) {
				pg_error(sth, -1, "Execute called with an unbound placeholder");
				return -2;
			}
		}
	}

	/* Start a new transaction if necessary */
	/* We could check PQtransactionStatus here, but is it worth the cost? -gsm */
	if (!imp_dbh->done_begin && FALSE == DBIc_has(imp_dbh, DBIcf_AutoCommit)) {
		status = _result(imp_dbh, "begin");
		if (PGRES_COMMAND_OK != status) {
			pg_error(sth, status, PQerrorMessage(imp_dbh->conn));
			return -2;
		}
		imp_dbh->done_begin = 1;
	}


	/* clear old result (if any) */
	if (imp_sth->result) {
		PQclear(imp_sth->result);
	}
	
	/* Are we using new or old style prepare? */

	if (!imp_sth->direct &&
			imp_sth->is_dml && 
			imp_dbh->server_prepare >= 1 && 
			(1==imp_sth->server_prepare || 
			 (2==imp_sth->server_prepare && imp_dbh->pg_protocol >= 3))) {
		const char **paramValues;
	
		if (dbis->debug >= 4)
			PerlIO_printf(DBILOGFP, "  dbdpg: new-style prepare\n");

		/* Prepare if it has not already been prepared (or it needs repreparing) */
		if (NULL == imp_sth->prepare_name) {
			if (imp_sth->prepared_by_us) {
				if (dbis->debug >= 3)
					PerlIO_printf(DBILOGFP, "  dbdpg: re-preparing statement\n");
			}
			if (dbd_st_prepare_statement(sth, imp_sth)) {
				return -2;
			}
		} /* end prepare this named statement */
		else {
			if (dbis->debug >= 3)
				PerlIO_printf(DBILOGFP, "  dbdpg: using previously prepared statement \"%s\"\n", imp_sth->prepare_name);
		}

		/* If none are binary, life is simpler */
		x=0; y=0; z=0;
		paramValues = calloc(imp_sth->numphs, sizeof(*paramValues));
		for (currseg=imp_sth->seg; NULL != currseg; currseg=currseg->nextseg) {
			if (NULL != currseg->placeholder) {
				paramValues[x++] = currseg->value;
			}
		}

		if (dbis->debug >= 3)
			PerlIO_printf(DBILOGFP, "  dbdpg: calling PQexecPrepared\n");

		/* XXX This is still text-only */
		imp_sth->result = PQexecPrepared(imp_dbh->conn, imp_sth->prepare_name, imp_sth->numphs,
																		 paramValues, NULL, NULL, 0);

	} /* end new-style prepare */
	else {
		/* prepare the old fashioned way (quote-n-paste) */

		if (dbis->debug >= 4)
			PerlIO_printf(DBILOGFP, "  dbdpg: old-style prepare\n");

		/* Use PQexecParams someday? */

		/* Go through and quote each value, then turn into a giant statement */
		execsize = imp_sth->totalsize;
		for (currseg=imp_sth->seg; NULL != currseg; currseg=currseg->nextseg) {
			if (NULL != currseg->placeholder) {
				if (NULL == currseg->value) {
					New(0, currseg->quoted, sizeof("NULL")+1, char);
					if (!currseg->quoted)
						croak("No memory");
					strcpy(currseg->quoted, "NULL");
					currseg->quoted_len = strlen(currseg->quoted);
				}
				else {
					currseg->quoted = currseg->bind_type->quote(currseg->value, strlen(currseg->value), &currseg->quoted_len);
				}
				execsize += currseg->quoted_len;
			}
		}

		/* Build it up! */
		New(0, statement, execsize+1, char);
		statement[0] = '\0';
		for (currseg=imp_sth->seg; NULL != currseg; currseg=currseg->nextseg) {
			strcat(statement, currseg->segment);
			if (NULL != currseg->quoted) {
				strcat(statement, currseg->quoted);
			}
		}			
		statement[execsize] = '\0';

		imp_sth->result = PQexec(imp_dbh->conn, statement);
		Safefree(statement);

	} /* end old-style prepare */

	/* Some form of PQexec has been run at this point */

	status = imp_sth->result ? PQresultStatus(imp_sth->result) : -1;

	/* We don't want the result cleared yet, so we don't use _result */

#ifdef PG74
	strncpy(imp_dbh->sqlstate,
					NULL == PQresultErrorField(imp_sth->result,PG_DIAG_SQLSTATE) ? "00000" : 
					PQresultErrorField(imp_sth->result,PG_DIAG_SQLSTATE),
					5);
#else
	strncpy(imp_dbh->sqlstate, "S1000",5); /* DBI standard says this is the default */
#endif

	cmdStatus = imp_sth->result ? (char *)PQcmdStatus(imp_sth->result) : "";
	cmdTuples = imp_sth->result ? (char *)PQcmdTuples(imp_sth->result) : "";
	
	if (dbis->debug >= 3)
		PerlIO_printf(DBILOGFP, "  dbdpg: received a status of %d\n", status);

	if (PGRES_TUPLES_OK == status) {
		num_fields = PQnfields(imp_sth->result);
		imp_sth->cur_tuple = 0;
		DBIc_NUM_FIELDS(imp_sth) = num_fields;
		DBIc_ACTIVE_on(imp_sth);
		ret = PQntuples(imp_sth->result);
		if (dbis->debug >= 3)
			PerlIO_printf(DBILOGFP, "  dbdpg: status was PGRES_TUPLES_OK, fields=%d, tuples=%d\n",
										num_fields, ret);
	}
	else if (PGRES_COMMAND_OK == status) {
		/* non-select statement */
		if (dbis->debug >= 3)
			PerlIO_printf(DBILOGFP, "  dbdpg: status was PGRES_COMMAND_OK\n");
		if (! strncmp(cmdStatus, "DELETE", 6) || ! strncmp(cmdStatus, "INSERT", 6) || ! strncmp(cmdStatus, "UPDATE", 6)) {
			ret = atoi(cmdTuples);
		} else {
			ret = -1;
		}
	}
	else if (PGRES_COPY_OUT == status || PGRES_COPY_IN == status) {
		/* Copy Out/In data transfer in progress */
		ret = -1;
	}
	else {
		pg_error(sth, status, PQerrorMessage(imp_dbh->conn));
		ret = -2;
	}
	
	/* store the number of affected rows */
	imp_sth->rows = ret;
	
	return ret;

} /* end of dbd_st_execute */


/* ================================================================== */

is_high_bit_set(val)
		 char *val;
{
	while (*val++)
		if (*val & 0x80) return 1;
	return 0;
}


/* ================================================================== */

AV * dbd_st_fetch (sth, imp_sth)
		 SV *sth;
		 imp_sth_t *imp_sth;
{
	sql_type_info_t *type_info;
	int num_fields;
	char *value;
	char *p;
	int i, pg_type, value_len, chopblanks, len;
	AV *av;
	D_imp_dbh_from_sth;
	
	if (dbis->debug >= 1)
		PerlIO_printf(DBILOGFP, "dbd_st_fetch\n");

	/* Check that execute() was executed sucessfully */
	if ( !DBIc_ACTIVE(imp_sth) ) {
		pg_error(sth, 1, "no statement executing\n");	
		return Nullav;
	}
	
	if (imp_sth->cur_tuple == PQntuples(imp_sth->result) ) {
		if (dbis->debug >= 4)
			PerlIO_printf(DBILOGFP, "  dbdpg: fetched the last tuple (%d)\n", imp_sth->cur_tuple);
		imp_sth->cur_tuple = 0;
		DBIc_ACTIVE_off(imp_sth);
		return Nullav; /* we reached the last tuple */
	}
	
	av = DBIS->get_fbav(imp_sth);
	num_fields = AvFILL(av)+1;
	
	chopblanks = DBIc_has(imp_sth, DBIcf_ChopBlanks);
	
	for(i = 0; i < num_fields; ++i) {
		SV *sv;

		if (dbis->debug >= 4)
			PerlIO_printf(DBILOGFP, "  dbdpg: fetching a field\n");

		sv = AvARRAY(av)[i];
		if (PQgetisnull(imp_sth->result, imp_sth->cur_tuple, i)) {
			sv_setsv(sv, &sv_undef);
		}
		else {
			value = (char*)PQgetvalue(imp_sth->result, imp_sth->cur_tuple, i); 
			
			pg_type = PQftype(imp_sth->result, i);
			type_info = pg_type_data(pg_type);
			
			if (type_info)
				type_info->dequote(value, &value_len); /* dequote in place */
			else
				value_len = strlen(value);
			
			if (type_info && (type_info->type_id == BOOLOID) &&
					imp_dbh->pg_bool_tf)
				{
					*value = (*value == '1') ? 't' : 'f';
				}
			
			sv_setpvn(sv, value, value_len);
			
			if (type_info && (type_info->type_id == BPCHAROID) && 
					chopblanks)
				{
					p = SvEND(sv);
					len = SvCUR(sv);
					while(len && *--p == ' ')
						--len;
					if (len != SvCUR(sv)) {
						SvCUR_set(sv, len);
						*SvEND(sv) = '\0';
					}
				}
			
#ifdef is_utf8_string
			/* XXX Under what circumstances is type_info NULL? */
			if (imp_dbh->pg_enable_utf8 && type_info) {
				SvUTF8_off(sv);
				switch(type_info->type_id) {
				case CHAROID:
				case TEXTOID:
				case BPCHAROID:
				case VARCHAROID:
					if (is_high_bit_set(value) && is_utf8_string(value, value_len)) {
						SvUTF8_on(sv);
					}
				}
			}
#endif
		}
	}
	
	imp_sth->cur_tuple += 1;
	
	return av;

} /* end of dbd_st_fetch */


/* ================================================================== */

int dbd_st_rows (sth, imp_sth)
		 SV *sth;
		 imp_sth_t *imp_sth;
{
	if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "dbd_st_rows\n"); }
	
	return imp_sth->rows;

} /* end of dbd_st_rows */


/* ================================================================== */

int dbd_st_finish (sth, imp_sth)
		 SV *sth;
		 imp_sth_t *imp_sth;
{
	dTHR;
	
	if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "dbd_st_finish\n"); }
	
	if (DBIc_ACTIVE(imp_sth) && imp_sth->result) {
		PQclear(imp_sth->result);
		imp_sth->result = 0;
		imp_sth->rows = 0;
	}
	
	DBIc_ACTIVE_off(imp_sth);
	return 1;

} /* end of sbs_st_finish */


/* ================================================================== */

int dbd_st_deallocate_statement (sth, imp_sth)
		 SV *sth;
		 imp_sth_t *imp_sth;
{
	char *stmt;
	PGresult *result;
	ExecStatusType status;
	PGTransactionStatusType tstatus;
	D_imp_dbh_from_sth;
	
	if (dbis->debug >= 1)
		PerlIO_printf(DBILOGFP, "dbd_st_deallocate_statement\n");

	if (NULL == imp_dbh->conn || NULL == imp_sth->prepare_name)
		return 0;
	
	/* What is our status? */
	tstatus = dbd_db_txn_status(imp_dbh);
	if (dbis->debug >= 3)
		PerlIO_printf(DBILOGFP, "  dbdpg: transaction status is %d\n", tstatus);

	/* If we are in a failed transaction, rollback before deallocating */
	if (PQTRANS_INERROR == tstatus) {
		if (dbis->debug >= 2)
			PerlIO_printf(DBILOGFP, "  dbdpg: Issuing rollback before deallocate\n", tstatus);

		status = _result(imp_dbh, "rollback");
		if (PGRES_COMMAND_OK != status) {
			/* This is not fatal, it just means we cannot deallocate */
			if (dbis->debug >= 1)
				PerlIO_printf(DBILOGFP, "  dbdpg: Rollback failed, so no deallocate\n");
			return 1;
		}
		imp_dbh->done_begin = 0;
	}

	New(0, stmt, strlen("DEALLOCATE ") + strlen(imp_sth->prepare_name) + 1, char);
	if (!stmt)
		croak("No memory");

	sprintf(stmt, "DEALLOCATE %s", imp_sth->prepare_name);

	if (dbis->debug >= 2)
		PerlIO_printf(DBILOGFP, "  dbdpg: deallocating \"%s\"\n", imp_sth->prepare_name);

	status = _result(imp_dbh, stmt);
	Safefree(stmt);
	if (PGRES_COMMAND_OK != status) {
		pg_error(sth,status, PQerrorMessage(imp_dbh->conn));
		return 2;
	}

	imp_sth->prepare_name = NULL;

	return 0;

} /* end of dbd_st_deallocate_statement */


/* ================================================================== */

void dbd_st_destroy (sth, imp_sth)
		 SV *sth;
		 imp_sth_t *imp_sth;
{

	seg_t *currseg, *nextseg;

	if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "dbd_st_destroy\n"); }

	if (NULL == imp_sth->seg) { /* Already been destroyed! */
		croak("dbd_st_destroy called twice!");
		return;
	}

	/* Deallocate only if we named this statement ourselves */
	if (imp_sth->prepared_by_us) {
		if (dbd_st_deallocate_statement(sth, imp_sth)) {
			if (dbis->debug >= 1)
				PerlIO_printf(DBILOGFP, "  dbdpg: could not deallocate\n");
		}
	}	

	/* Free all the segments */
	currseg = imp_sth->seg;
	while (NULL != currseg) {
			Safefree(currseg->segment);
			Safefree(currseg->placeholder);
			Safefree(currseg->value);
			Safefree(currseg->quoted);
			nextseg = currseg->nextseg;
			Safefree(currseg);
			currseg = nextseg;
	}

	/* Free the rest of the structure */
	Safefree(imp_sth->prepare_name);
	Safefree(imp_sth->firstword);

	if (NULL != imp_sth->result) {
		PQclear(imp_sth->result);
		imp_sth->result = NULL;
	}
	
	DBIc_IMPSET_off(imp_sth); /* let DBI know we've done it */

} /* end of dbd_st_destroy */


/* ================================================================== */

int dbd_st_STORE_attrib (sth, imp_sth, keysv, valuesv)
		 SV *sth;
		 imp_sth_t *imp_sth;
		 SV *keysv;
		 SV *valuesv;
{
	STRLEN kl;
	char *key = SvPV(keysv,kl);
	STRLEN vl;
	char *value = SvPV(valuesv,vl);

	if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "dbd_st_STORE\n"); }
	
	if (kl==14 && strEQ(key, "server_prepare")) {
		imp_sth->server_prepare = strEQ(value,"0") ? 0 : 1;
		/* Need to fool DBI into thinking we already have a valid statement */
	}
	else if (12==kl && strEQ(key, "prepare_name")) {
		New(0, imp_sth->prepare_name, vl+1, char);
		Copy(value, imp_sth->prepare_name, vl, char);
		imp_sth->prepare_name[vl] = '\0';
	}
	else if (12==kl && strEQ(key, "prepare_args")) {
		DBIc_NUM_PARAMS(imp_sth) = Atol(value); /* XXX not complete yet */
	}
	else {
		return 0;
	}

} /* end of sbs_st_STORE_attrib */


/* ================================================================== */

SV * dbd_st_FETCH_attrib (sth, imp_sth, keysv)
		 SV *sth;
		 imp_sth_t *imp_sth;
		 SV *keysv;
{
	STRLEN kl;
	char *key = SvPV(keysv,kl);
	int i, x, y, sz;
	SV *retsv = Nullsv;
	char *type_name;
	sql_type_info_t *type_info;

	if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "dbd_st_FETCH\n"); }
	
	/* Some can be done before the execute */
	if (kl==12 && strEQ(key, "prepare_name")) {
		retsv = newSVpv((char *)imp_sth->prepare_name, 0);
		return retsv;
	}
	else if (kl==14 && strEQ(key, "server_prepare")) {
		retsv = newSViv((IV)imp_sth->server_prepare);
		return retsv;
 	}
	else if (kl==11 && strEQ(key, "ParamValues")) {
		HV *pvhv = newHV();
		seg_t *currseg;
		for (i=0,currseg=imp_sth->seg; NULL != currseg; currseg=currseg->nextseg,i++) {
			if (NULL == currseg->value) {
				hv_store_ent(pvhv, newSVpv(currseg->placeholder,0), Nullsv, i);
			}
			else if (NULL == currseg->quoted) {
				hv_store_ent(pvhv, newSVpv(currseg->placeholder,0), newSVpv(currseg->value,0),i);
			}
			else {
				hv_store_ent(pvhv, newSVpv(currseg->placeholder,0), newSVpv(currseg->quoted,0),i);
			}
		}
		retsv = newRV_noinc((SV*)pvhv);
		return retsv;
	}

	if (! imp_sth->result) {
		return Nullsv;
	}
	i = DBIc_NUM_FIELDS(imp_sth);
	
	if (kl == 4 && strEQ(key, "NAME")) {
		AV *av = newAV();
		retsv = newRV(sv_2mortal((SV*)av));
		while(--i >= 0) {
			av_store(av, i, newSVpv(PQfname(imp_sth->result, i),0));
		}
	}
	else if ( kl== 4 && strEQ(key, "TYPE")) {
		/* Need to convert the Pg type to ANSI/SQL type. */
		AV *av = newAV();
		retsv = newRV(sv_2mortal((SV*)av));
		while(--i >= 0) {
			type_info = pg_type_data(PQftype(imp_sth->result, i));
			av_store(av, i, newSViv( type_info ? type_info->type.sql : 0 ) );
		}
	}
	else if (kl==9 && strEQ(key, "PRECISION")) {
		AV *av = newAV();
		retsv = newRV(sv_2mortal((SV*)av));
		while(--i >= 0) {
			x = PQftype(imp_sth->result, i);
			switch (x) {
			case BPCHAROID:
			case VARCHAROID:
				sz = PQfmod(imp_sth->result, i);
				break;
			case NUMERICOID:
				sz = (PQfmod(imp_sth->result, i)-4) >> 16;
				break;
			default:
				sz = PQfsize(imp_sth->result, i);
				break;
			}
			av_store(av, i, sz > 0 ? newSViv(sz) : &sv_undef);
		}
	}
	else if (kl==5 && strEQ(key, "SCALE")) {
		AV *av = newAV();
		retsv = newRV(sv_2mortal((SV*)av));
		while(--i >= 0) {
			x = PQftype(imp_sth->result, i);
			if (NUMERICOID==x) {
				x = PQfmod(imp_sth->result, i)-4;
				av_store(av, i, newSViv(x % (x>>16)));
			}
			else {
				av_store(av, i, &sv_undef);
			}
		}
	}
	else if (kl==8 && strEQ(key, "NULLABLE")) {
		AV *av = newAV();
		retsv = newRV(sv_2mortal((SV*)av));
		PGresult* result;
		PGTransactionStatusType status;
		D_imp_dbh_from_sth;
		char *statement;
		int nullable; /* 0 = not nullable, 1 = nullable 2 = unknown */

		New(0, statement, 100, char);
		while(--i >= 0) {
			nullable=2;
			x = PQftable(imp_sth->result, i);
			y = PQftablecol(imp_sth->result, i);
			if (InvalidOid != x && y > 0) { /* We know what table and column this came from */
				sprintf(statement, "SELECT attnotnull FROM pg_catalog.pg_attribute WHERE attrelid=%d AND attnum=%d", x, y);
				statement[strlen(statement)]='\0';
				result = PQexec(imp_dbh->conn, statement);
				status = imp_sth->result ? PQresultStatus(imp_sth->result) : -1;
				if (PGRES_TUPLES_OK == status && PQntuples(result)) {
					switch(PQgetvalue(result,0,0)[0]) {
					case 't':
						nullable = 0;
						break;
					case 'f':
						nullable = 1;
					}
				}
				PQclear(result);
			}
			av_store(av, i, newSViv(nullable));
		}
		Safefree(statement);
	}
	else if (kl==10 && strEQ(key, "CursorName")) {
		retsv = &sv_undef;
	}
	else if (kl==11 && strEQ(key, "RowsInCache")) {
		retsv = &sv_undef;
	}
	else if (kl==7 && strEQ(key, "pg_size")) {
		AV *av = newAV();
		retsv = newRV(sv_2mortal((SV*)av));
		while(--i >= 0) {
			av_store(av, i, newSViv(PQfsize(imp_sth->result, i)));
		}
	}
	else if (kl==7 && strEQ(key, "pg_type")) {
		AV *av = newAV();
		retsv = newRV(sv_2mortal((SV*)av));
		while(--i >= 0) {			
			type_info = pg_type_data(PQftype(imp_sth->result,i));
			type_name = (type_info) ? type_info->type_name : "unknown";
			av_store(av, i, newSVpv(type_name, 0));			
		}
	}
	else if (kl==13 && strEQ(key, "pg_oid_status")) {
		retsv = newSViv(PQoidValue(imp_sth->result));
	}
	else if (kl==13 && strEQ(key, "pg_cmd_status")) {
		retsv = newSVpv((char *)PQcmdStatus(imp_sth->result), 0);
	}
	else {
		return Nullsv;
	}
	
	return sv_2mortal(retsv);

} /* end of dbd_st_FETCH_attrib */


/* end of dbdimp.c */






