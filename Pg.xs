/*
  $Id$

  Copyright (c) 2000-2005 PostgreSQL Global Development Group
  Portions Copyright (c) 1997-2000 Edmund Mergl
  Portions Copyright (c) 1994-1997 Tim Bunce

  You may distribute under the terms of either the GNU General Public
  License or the Artistic License, as specified in the Perl README file.

*/


#include "Pg.h"

#ifdef _MSC_VER
#define strncasecmp(a,b,c) _strnicmp((a),(b),(c))
#endif

DBISTATE_DECLARE;

MODULE = DBD::Pg	PACKAGE = DBD::Pg


I32
constant(name=Nullch)
	char *name
	PROTOTYPE:
	ALIAS:
	PG_BOOL      = 16
	PG_BYTEA     = 17
	PG_CHAR      = 18
	PG_INT8      = 20
	PG_INT2      = 21
	PG_INT4      = 23
	PG_TEXT      = 25
	PG_OID       = 26
	PG_FLOAT4    = 700
	PG_FLOAT8    = 701
	PG_ABSTIME   = 702
	PG_RELTIME   = 703
	PG_TINTERVAL = 704
	PG_BPCHAR    = 1042
	PG_VARCHAR   = 1043
	PG_DATE      = 1082
	PG_TIME      = 1083
	PG_DATETIME  = 1184
	PG_TIMESPAN  = 1186
	PG_TIMESTAMP = 1296
	CODE:
		if (0==ix) {
			if (!name) {
				name = GvNAME(CvGV(cv));
			}
			croak("Unknown DBD::Pg constant '%s'", name);
		}
		else {
			RETVAL = ix;
		}
	OUTPUT:
		RETVAL

INCLUDE: Pg.xsi


# ------------------------------------------------------------
# db functions
# ------------------------------------------------------------
MODULE=DBD::Pg	PACKAGE = DBD::Pg::db


#TODO: make quote(foo, {type=>SQL_INTEGER}) work  #rl
#TODO: make quote(foo, {pg_type=>DBD::Pg::PG_INTEGER}) work  #rl
SV*
quote(dbh, to_quote_sv, type_sv=Nullsv)
	SV* to_quote_sv
	SV* type_sv

	CODE:
	{
		sql_type_info_t *type_info;
		char *to_quote;
		char *quoted;
		STRLEN len;
		STRLEN retlen=0;
		SV **svp;
			
		SvGETMAGIC(to_quote_sv);

		/* Null is always returned as "NULL", so we can ignore any type given */
		if (!SvOK(to_quote_sv)) {
			RETVAL = newSVpvn("NULL", 4);
		}
		else {

			/* If no valid type is given, we default to varchar */
			if (!type_sv || !SvOK(type_sv)) {
				type_info = pg_type_data(VARCHAROID);
			}
			else {
				if SvMAGICAL(type_sv)
					(void)mg_get(type_sv);
				if (SvNIOK(type_sv)) {
					type_info = sql_type_data(SvIV(type_sv));
				}
				else {
					if ((svp = hv_fetch((HV*)SvRV(type_sv),"pg_type", 7, 0)) != NULL) {
						type_info = pg_type_data(SvIV(*svp));
					}
					else if ((svp = hv_fetch((HV*)SvRV(type_sv),"type", 4, 0)) != NULL) {
						type_info = sql_type_data(SvIV(*svp));
					}
					else {
						type_info = NULL;
					}
				}
				if (!type_info) {
					warn("Unknown type %" IVdf ", defaulting to VARCHAR",SvIV(type_sv));
					type_info = pg_type_data(VARCHAROID);
				}
			}

			/* At this point, type_info points to a valid struct, one way or another */

			if (SvMAGICAL(to_quote_sv))
				(void)mg_get(to_quote_sv);
				
			to_quote = SvPV(to_quote_sv, len);
			/* Need good debugging here */
			quoted = type_info->quote(to_quote, len, &retlen);
			RETVAL = newSVpvn(quoted, retlen);
			if (SvUTF8(to_quote_sv))
				SvUTF8_on(RETVAL);
			Safefree (quoted);
		}
	}
	OUTPUT:
		RETVAL
	

# ------------------------------------------------------------
# database level interface PG specific
# ------------------------------------------------------------
MODULE = DBD::Pg	PACKAGE = DBD::Pg::db


void state(dbh)
	SV *dbh
	CODE:
	D_imp_dbh(dbh);
	ST(0) = strEQ(imp_dbh->sqlstate,"00000") ? &sv_no : newSVpv(imp_dbh->sqlstate, 5);


void do(dbh, statement, attr=Nullsv, ...)
	SV * dbh
	char * statement
	SV * attr
	PROTOTYPE: $$;$@
	CODE:
	{
		D_imp_dbh(dbh);
		struct imp_sth_ph_st* params = NULL;
		int numParams = 0;
		int retval;

		if (strlen(statement)<1) { /* Corner case */
			XST_mUNDEF(0);
			return;
		}

		if (items < 3) { /* No attribs, no arguments */
			/* Quick run via PQexec */
			retval = pg_quickexec(dbh, statement);
		}
		else { /* The normal, slower way */
			imp_sth_t *imp_sth;
			SV * sth = dbixst_bounce_method("prepare", 3);
			if (!SvROK(sth))
				XSRETURN_UNDEF;
			imp_sth = (imp_sth_t*)(DBIh_COM(sth));
			if (items > 3)
				if (!dbdxst_bind_params(sth, imp_sth, items-2, ax+2))
					XSRETURN_UNDEF;
			imp_sth->server_prepare = 1;
			imp_sth->onetime = 1; /* Overrides the above at actual PQexec* decision time */
			retval = dbd_st_execute(sth, imp_sth);
		}

		if (retval == 0)
			XST_mPV(0, "0E0");
		else if (retval < -1)
			XST_mUNDEF(0);
		else
			XST_mIV(0, retval);
}


void
_ping(dbh)
	SV * dbh
	CODE:
		ST(0) = sv_2mortal(newSViv(dbd_db_ping(dbh)));


void
getfd(dbh)
	SV * dbh
	CODE:
		int ret;
		D_imp_dbh(dbh);
		ret = dbd_db_getfd(dbh, imp_dbh);
		ST(0) = sv_2mortal( newSViv( ret ) );


void
pg_endcopy(dbh)
	SV * dbh
	CODE:
		ST(0) = (pg_db_endcopy(dbh)!=0) ? &sv_no : &sv_yes;


void
pg_notifies(dbh)
	SV * dbh
	CODE:
		D_imp_dbh(dbh);
		ST(0) = dbd_db_pg_notifies(dbh, imp_dbh);


void
pg_savepoint(dbh,name)
	SV * dbh
	char * name
	CODE:
		D_imp_dbh(dbh);
		if (DBIc_has(imp_dbh,DBIcf_AutoCommit) && DBIc_WARN(imp_dbh))
			warn("savepoint ineffective with AutoCommit enabled");
		ST(0) = (pg_db_savepoint(dbh, imp_dbh, name)!=0) ? &sv_yes : &sv_no;


void
pg_rollback_to(dbh,name)
	SV * dbh
	char * name
	CODE:
		D_imp_dbh(dbh);
		if (DBIc_has(imp_dbh,DBIcf_AutoCommit) && DBIc_WARN(imp_dbh))
			warn("rollback_to ineffective with AutoCommit enabled");
		ST(0) = (pg_db_rollback_to(dbh, imp_dbh, name)!=0) ? &sv_yes : &sv_no;


void
pg_release(dbh,name)
	SV * dbh
	char * name
	CODE:
		D_imp_dbh(dbh);
		if (DBIc_has(imp_dbh,DBIcf_AutoCommit) && DBIc_WARN(imp_dbh))
			warn("release ineffective with AutoCommit enabled");
		ST(0) = (pg_db_release(dbh, imp_dbh, name)!=0) ? &sv_yes : &sv_no;


void
lo_open(dbh, lobjId, mode)
	SV * dbh
	unsigned int lobjId
	int mode
	CODE:
		int ret = pg_db_lo_open(dbh, lobjId, mode);
	ST(0) = (-1 != ret) ? sv_2mortal(newSViv(ret)) : &sv_undef;


void
lo_close(dbh, fd)
	SV * dbh
	int fd
	CODE:
		ST(0) = (-1 != pg_db_lo_close(dbh, fd)) ? &sv_yes : &sv_no;


void
lo_read(dbh, fd, buf, len)
	SV * dbh
	int fd
	char * buf
	size_t len
	PREINIT:
		SV *bufsv = SvROK(ST(2)) ? SvRV(ST(2)) : ST(2);
		sv_setpvn(bufsv,"",0); /* Make sure we can grow it safely */
		int ret;
	CODE:
		buf = SvGROW(bufsv, len + 1);
		ret = pg_db_lo_read(dbh, fd, buf, len);
		if (ret > 0) {
			SvCUR_set(bufsv, ret);
			*SvEND(bufsv) = '\0';
			sv_setpvn(ST(2), buf, (unsigned)ret);
			SvSETMAGIC(ST(2));
		}
		ST(0) = (-1 != ret) ? sv_2mortal(newSViv(ret)) : &sv_undef;


void
lo_write(dbh, fd, buf, len)
	SV * dbh
	int fd
	char * buf
	size_t len
	CODE:
		int ret = pg_db_lo_write(dbh, fd, buf, len);
		ST(0) = (-1 != ret) ? sv_2mortal(newSViv(ret)) : &sv_undef;


void
lo_lseek(dbh, fd, offset, whence)
	SV * dbh
	int fd
	int offset
	int whence
	CODE:
		int ret = pg_db_lo_lseek(dbh, fd, offset, whence);
		ST(0) = (-1 != ret) ? sv_2mortal(newSViv(ret)) : &sv_undef;


void
lo_creat(dbh, mode)
	SV * dbh
	int mode
	CODE:
		int ret = pg_db_lo_creat(dbh, mode);
		ST(0) = (-1 != ret) ? sv_2mortal(newSViv(ret)) : &sv_undef;


void
lo_tell(dbh, fd)
	SV * dbh
	int fd
	CODE:
		int ret = pg_db_lo_tell(dbh, fd);
		ST(0) = (-1 != ret) ? sv_2mortal(newSViv(ret)) : &sv_undef;


void
lo_unlink(dbh, lobjId)
	SV * dbh
	unsigned int lobjId
	CODE:
		ST(0) = (-1 != pg_db_lo_unlink(dbh, lobjId)) ? &sv_yes : &sv_no;


void
lo_import(dbh, filename)
	SV * dbh
	char * filename
	CODE:
		unsigned int ret = pg_db_lo_import(dbh, filename);
		ST(0) = (ret!=0) ? sv_2mortal(newSViv((int)ret)) : &sv_undef;


void
lo_export(dbh, lobjId, filename)
	SV * dbh
	unsigned int lobjId
	char * filename
	CODE:
		ST(0) = (-1 != pg_db_lo_export(dbh, lobjId, filename)) ? &sv_yes : &sv_no;


void
pg_putline(dbh, buf)
	SV * dbh
	char * buf
	CODE:
		ST(0) = (pg_db_putline(dbh, buf)!=0) ? &sv_no : &sv_yes;


void
putline(dbh, buf)
	SV * dbh
	char * buf
	CODE:
		ST(0) = (pg_db_putline(dbh, buf)!=0) ? &sv_no : &sv_yes;

void
pg_getline(dbh, buf, len)
	PREINIT:
		SV * bufsv = SvROK(ST(1)) ? SvRV(ST(1)) : ST(1);
		sv_setpvn(bufsv,"",0); /* Make sure we can grow it safely */
	INPUT:
		SV * dbh
		unsigned int len
		char * buf = SvGROW(bufsv, 3);
	CODE:
		int ret;
		if (len > 3)
			buf = SvGROW(bufsv, len);
		ret = pg_db_getline(dbh, buf, (int)len);
		sv_setpv((SV*)ST(1), buf);
		SvSETMAGIC(ST(1));
		ST(0) = (-1 != ret) ? &sv_yes : &sv_no;


void
getline(dbh, buf, len)
	PREINIT:
		SV *bufsv = SvROK(ST(1)) ? SvRV(ST(1)) : ST(1);
		sv_setpvn(bufsv,"",0); /* Make sure we can grow it safely */
	INPUT:
		SV * dbh
		unsigned int len
		char * buf = SvGROW(bufsv, 3);
	CODE:
		int ret;
		if (len > 3)
			buf = SvGROW(bufsv, len);
		ret = pg_db_getline(dbh, buf, (int)len);
		sv_setpv((SV*)ST(1), buf);
		SvSETMAGIC(ST(1));
		ST(0) = (-1 != ret) ? &sv_yes : &sv_no;

void
endcopy(dbh)
	SV * dbh
	CODE:
		ST(0) = (-1 != pg_db_endcopy(dbh)) ? &sv_yes : &sv_no;

void
pg_server_trace(dbh,fh)
	SV * dbh
	FILE * fh
	CODE:
		pg_db_pg_server_trace(dbh,fh);

void
pg_server_untrace(dbh)
	SV * dbh
	CODE:
		pg_db_pg_server_untrace(dbh);

void
_pg_type_info (type_sv=Nullsv)
	SV* type_sv
	CODE:
	{
		int type_num = 0;
		sql_type_info_t *type_info;

		if (type_sv && SvOK(type_sv)) {
			if SvMAGICAL(type_sv)
				(void)mg_get(type_sv);
			type_info = pg_type_data(SvIV(type_sv));
			type_num = type_info ? type_info->type.sql : SQL_VARCHAR;
		}
		ST(0) = sv_2mortal( newSViv( type_num ) );
	}

# -- end of DBD::Pg::db


# ------------------------------------------------------------
# statement level interface PG specific
# ------------------------------------------------------------
MODULE = DBD::Pg	PACKAGE = DBD::Pg::st

void state(sth)
SV *sth;
	CODE:
		D_imp_sth(sth);
		D_imp_dbh_from_sth;
		ST(0) = strEQ(imp_dbh->sqlstate,"00000") ? &sv_no : newSVpv(imp_dbh->sqlstate, 5);

# end of Pg.xs
