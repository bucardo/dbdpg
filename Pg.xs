/*
  $Id$

  Copyright (c) 2000-2009 Greg Sabino Mullane and others: see the Changes file
  Portions Copyright (c) 1997-2000 Edmund Mergl
  Portions Copyright (c) 1994-1997 Tim Bunce

  You may distribute under the terms of either the GNU General Public
  License or the Artistic License, as specified in the Perl README file.

*/


#include "Pg.h"

#ifdef _MSC_VER
#define strncasecmp(a,b,c) _strnicmp((a),(b),(c))
#endif

MODULE = DBD::Pg	PACKAGE = DBD::Pg


I32
constant(name=Nullch)
	char *name
	PROTOTYPE:
	ALIAS:
	PG_ABSTIME            = 702
	PG_ABSTIMEARRAY       = 1023
	PG_ACLITEM            = 1033
	PG_ACLITEMARRAY       = 1034
	PG_ANY                = 2276
	PG_ANYARRAY           = 2277
	PG_ANYELEMENT         = 2283
	PG_ANYENUM            = 3500
	PG_ANYNONARRAY        = 2776
	PG_BIT                = 1560
	PG_BITARRAY           = 1561
	PG_BOOL               = 16
	PG_BOOLARRAY          = 1000
	PG_BOX                = 603
	PG_BOXARRAY           = 1020
	PG_BPCHAR             = 1042
	PG_BPCHARARRAY        = 1014
	PG_BYTEA              = 17
	PG_BYTEAARRAY         = 1001
	PG_CHAR               = 18
	PG_CHARARRAY          = 1002
	PG_CID                = 29
	PG_CIDARRAY           = 1012
	PG_CIDR               = 650
	PG_CIDRARRAY          = 651
	PG_CIRCLE             = 718
	PG_CIRCLEARRAY        = 719
	PG_CSTRING            = 2275
	PG_CSTRINGARRAY       = 1263
	PG_DATE               = 1082
	PG_DATEARRAY          = 1182
	PG_FLOAT4             = 700
	PG_FLOAT4ARRAY        = 1021
	PG_FLOAT8             = 701
	PG_FLOAT8ARRAY        = 1022
	PG_GTSVECTOR          = 3642
	PG_GTSVECTORARRAY     = 3644
	PG_INET               = 869
	PG_INETARRAY          = 1041
	PG_INT2               = 21
	PG_INT2ARRAY          = 1005
	PG_INT2VECTOR         = 22
	PG_INT2VECTORARRAY    = 1006
	PG_INT4               = 23
	PG_INT4ARRAY          = 1007
	PG_INT8               = 20
	PG_INT8ARRAY          = 1016
	PG_INTERNAL           = 2281
	PG_INTERVAL           = 1186
	PG_INTERVALARRAY      = 1187
	PG_LANGUAGE_HANDLER   = 2280
	PG_LINE               = 628
	PG_LINEARRAY          = 629
	PG_LSEG               = 601
	PG_LSEGARRAY          = 1018
	PG_MACADDR            = 829
	PG_MACADDRARRAY       = 1040
	PG_MONEY              = 790
	PG_MONEYARRAY         = 791
	PG_NAME               = 19
	PG_NAMEARRAY          = 1003
	PG_NUMERIC            = 1700
	PG_NUMERICARRAY       = 1231
	PG_OID                = 26
	PG_OIDARRAY           = 1028
	PG_OIDVECTOR          = 30
	PG_OIDVECTORARRAY     = 1013
	PG_OPAQUE             = 2282
	PG_PATH               = 602
	PG_PATHARRAY          = 1019
	PG_PG_ATTRIBUTE       = 75
	PG_PG_CLASS           = 83
	PG_PG_PROC            = 81
	PG_PG_TYPE            = 71
	PG_POINT              = 600
	PG_POINTARRAY         = 1017
	PG_POLYGON            = 604
	PG_POLYGONARRAY       = 1027
	PG_RECORD             = 2249
	PG_RECORDARRAY        = 2287
	PG_REFCURSOR          = 1790
	PG_REFCURSORARRAY     = 2201
	PG_REGCLASS           = 2205
	PG_REGCLASSARRAY      = 2210
	PG_REGCONFIG          = 3734
	PG_REGCONFIGARRAY     = 3735
	PG_REGDICTIONARY      = 3769
	PG_REGDICTIONARYARRAY = 3770
	PG_REGOPER            = 2203
	PG_REGOPERARRAY       = 2208
	PG_REGOPERATOR        = 2204
	PG_REGOPERATORARRAY   = 2209
	PG_REGPROC            = 24
	PG_REGPROCARRAY       = 1008
	PG_REGPROCEDURE       = 2202
	PG_REGPROCEDUREARRAY  = 2207
	PG_REGTYPE            = 2206
	PG_REGTYPEARRAY       = 2211
	PG_RELTIME            = 703
	PG_RELTIMEARRAY       = 1024
	PG_SMGR               = 210
	PG_TEXT               = 25
	PG_TEXTARRAY          = 1009
	PG_TID                = 27
	PG_TIDARRAY           = 1010
	PG_TIME               = 1083
	PG_TIMEARRAY          = 1183
	PG_TIMESTAMP          = 1114
	PG_TIMESTAMPARRAY     = 1115
	PG_TIMESTAMPTZ        = 1184
	PG_TIMESTAMPTZARRAY   = 1185
	PG_TIMETZ             = 1266
	PG_TIMETZARRAY        = 1270
	PG_TINTERVAL          = 704
	PG_TINTERVALARRAY     = 1025
	PG_TRIGGER            = 2279
	PG_TSQUERY            = 3615
	PG_TSQUERYARRAY       = 3645
	PG_TSVECTOR           = 3614
	PG_TSVECTORARRAY      = 3643
	PG_TXID_SNAPSHOT      = 2970
	PG_TXID_SNAPSHOTARRAY = 2949
	PG_UNKNOWN            = 705
	PG_UUID               = 2950
	PG_UUIDARRAY          = 2951
	PG_VARBIT             = 1562
	PG_VARBITARRAY        = 1563
	PG_VARCHAR            = 1043
	PG_VARCHARARRAY       = 1015
	PG_VOID               = 2278
	PG_XID                = 28
	PG_XIDARRAY           = 1011
	PG_XML                = 142
	PG_XMLARRAY           = 143

	PG_ASYNC              = 1
	PG_OLDQUERY_CANCEL    = 2
	PG_OLDQUERY_WAIT      = 4

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


SV*
quote(dbh, to_quote_sv, type_sv=Nullsv)
    SV* dbh
	SV* to_quote_sv
	SV* type_sv

	CODE:
	{
		D_imp_dbh(dbh);

		SvGETMAGIC(to_quote_sv);

		/* Null is always returned as "NULL", so we can ignore any type given */
		if (!SvOK(to_quote_sv)) {
			RETVAL = newSVpvn("NULL", 4);
		}
		else if (SvROK(to_quote_sv) && !SvAMAGIC(to_quote_sv)) {
			if (SvTYPE(SvRV(to_quote_sv)) != SVt_PVAV)
				croak("Cannot quote a reference");
			RETVAL = pg_stringify_array(to_quote_sv, ",", imp_dbh->pg_server_version, 1);
		}
		else {
			sql_type_info_t *type_info;
			char *quoted;
			const char *to_quote;
			STRLEN retlen=0;
			STRLEN len=0;

			/* If no valid type is given, we default to unknown */
			if (!type_sv || !SvOK(type_sv)) {
				type_info = pg_type_data(PG_UNKNOWN);
			}
			else {
				if SvMAGICAL(type_sv)
					(void)mg_get(type_sv);
				if (SvNIOK(type_sv)) {
					type_info = sql_type_data(SvIV(type_sv));
				}
				else {
					SV **svp;
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
					warn("Unknown type %" IVdf ", defaulting to UNKNOWN",SvIV(type_sv));
					type_info = pg_type_data(PG_UNKNOWN);
				}
			}

			/* At this point, type_info points to a valid struct, one way or another */

			if (SvMAGICAL(to_quote_sv))
				(void)mg_get(to_quote_sv);
				
			to_quote = SvPV(to_quote_sv, len);
			/* Need good debugging here */
			quoted = type_info->quote(to_quote, len, &retlen, imp_dbh->pg_server_version >= 80100 ? 1 : 0);
			RETVAL = newSVpvn(quoted, retlen);
			if (SvUTF8(to_quote_sv)) /* What about overloaded objects? */
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
		int retval;
		int asyncflag = 0;

		if (statement[0] == '\0') { /* Corner case */
			XST_mUNDEF(0);
			return;
		}

		if (attr && SvROK(attr) && SvTYPE(SvRV(attr)) == SVt_PVHV) {
			SV **svp;
			if ((svp = hv_fetch((HV*)SvRV(attr),"pg_async", 8, 0)) != NULL) {
			   asyncflag = (int)SvIV(*svp);
			}
		}
		if (items < 4) { /* No bind arguments */
			/* Quick run via PQexec */
			retval = pg_quickexec(dbh, statement, asyncflag);
		}
		else { /* We've got bind arguments, so we do the whole prepare/execute route */
			imp_sth_t *imp_sth;
			SV * const sth = dbixst_bounce_method("prepare", 3);
			if (!SvROK(sth))
				XSRETURN_UNDEF;
			imp_sth = (imp_sth_t*)(DBIh_COM(sth));
			if (!dbdxst_bind_params(sth, imp_sth, items-2, ax+2))
				XSRETURN_UNDEF;
			imp_sth->onetime = 1; /* Tells dbdimp.c not to bother preparing this */
			imp_sth->async_flag = asyncflag;
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
		ret = pg_db_getfd(imp_dbh);
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
		ST(0) = pg_db_pg_notifies(dbh, imp_dbh);


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
pg_lo_creat(dbh, mode)
	SV * dbh
	int mode
	CODE:
		const unsigned int ret = pg_db_lo_creat(dbh, mode);
		ST(0) = (ret > 0) ? sv_2mortal(newSVuv(ret)) : &sv_undef;

void
pg_lo_open(dbh, lobjId, mode)
	SV * dbh
	unsigned int lobjId
	int mode
	CODE:
		const int ret = pg_db_lo_open(dbh, lobjId, mode);
		ST(0) = (ret >= 0) ? sv_2mortal(newSViv(ret)) : &sv_undef;


void
pg_lo_write(dbh, fd, buf, len)
	SV * dbh
	int fd
	char * buf
	size_t len
	CODE:
		const int ret = pg_db_lo_write(dbh, fd, buf, len);
		ST(0) = (ret >= 0) ? sv_2mortal(newSViv(ret)) : &sv_undef;


void
pg_lo_read(dbh, fd, buf, len)
	SV * dbh
	int fd
	char * buf
	size_t len
	PREINIT:
		SV * const bufsv = SvROK(ST(2)) ? SvRV(ST(2)) : ST(2);
		int ret;
	CODE:
		sv_setpvn(bufsv,"",0); /* Make sure we can grow it safely */
		buf = SvGROW(bufsv, len + 1);
		ret = pg_db_lo_read(dbh, fd, buf, len);
		if (ret > 0) {
			SvCUR_set(bufsv, ret);
			*SvEND(bufsv) = '\0';
			sv_setpvn(ST(2), buf, (unsigned)ret);
			SvSETMAGIC(ST(2));
		}
		ST(0) = (ret >= 0) ? sv_2mortal(newSViv(ret)) : &sv_undef;


void
pg_lo_lseek(dbh, fd, offset, whence)
	SV * dbh
	int fd
	int offset
	int whence
	CODE:
		const int ret = pg_db_lo_lseek(dbh, fd, offset, whence);
		ST(0) = (ret >= 0) ? sv_2mortal(newSViv(ret)) : &sv_undef;


void
pg_lo_tell(dbh, fd)
	SV * dbh
	int fd
	CODE:
		const int ret = pg_db_lo_tell(dbh, fd);
		ST(0) = (ret >= 0) ? sv_2mortal(newSViv(ret)) : &sv_undef;


void
pg_lo_close(dbh, fd)
	SV * dbh
	int fd
	CODE:
		ST(0) = (pg_db_lo_close(dbh, fd) >= 0) ? &sv_yes : &sv_no;


void
pg_lo_unlink(dbh, lobjId)
	SV * dbh
	unsigned int lobjId
	CODE:
		ST(0) = (pg_db_lo_unlink(dbh, lobjId) >= 1) ? &sv_yes : &sv_no;


void
pg_lo_import(dbh, filename)
	SV * dbh
	char * filename
	CODE:
		const unsigned int ret = pg_db_lo_import(dbh, filename);
		ST(0) = (ret > 0) ? sv_2mortal(newSVuv(ret)) : &sv_undef;


void
pg_lo_export(dbh, lobjId, filename)
	SV * dbh
	unsigned int lobjId
	char * filename
	CODE:
		ST(0) = (pg_db_lo_export(dbh, lobjId, filename) >= 1) ? &sv_yes : &sv_no;


void
lo_creat(dbh, mode)
	SV * dbh
	int mode
	CODE:
		const unsigned int ret = pg_db_lo_creat(dbh, mode);
		ST(0) = (ret > 0) ? sv_2mortal(newSVuv(ret)) : &sv_undef;

void
lo_open(dbh, lobjId, mode)
	SV * dbh
	unsigned int lobjId
	int mode
	CODE:
		const int ret = pg_db_lo_open(dbh, lobjId, mode);
		ST(0) = (ret >= 0) ? sv_2mortal(newSViv(ret)) : &sv_undef;


void
lo_write(dbh, fd, buf, len)
	SV * dbh
	int fd
	char * buf
	size_t len
	CODE:
		const int ret = pg_db_lo_write(dbh, fd, buf, len);
		ST(0) = (ret >= 0) ? sv_2mortal(newSViv(ret)) : &sv_undef;


void
lo_read(dbh, fd, buf, len)
	SV * dbh
	int fd
	char * buf
	size_t len
	PREINIT:
		SV * const bufsv = SvROK(ST(2)) ? SvRV(ST(2)) : ST(2);
		int ret;
	CODE:
		sv_setpvn(bufsv,"",0); /* Make sure we can grow it safely */
		buf = SvGROW(bufsv, len + 1);
		ret = pg_db_lo_read(dbh, fd, buf, len);
		if (ret > 0) {
			SvCUR_set(bufsv, ret);
			*SvEND(bufsv) = '\0';
			sv_setpvn(ST(2), buf, (unsigned)ret);
			SvSETMAGIC(ST(2));
		}
		ST(0) = (ret >= 0) ? sv_2mortal(newSViv(ret)) : &sv_undef;


void
lo_lseek(dbh, fd, offset, whence)
	SV * dbh
	int fd
	int offset
	int whence
	CODE:
		const int ret = pg_db_lo_lseek(dbh, fd, offset, whence);
		ST(0) = (ret >= 0) ? sv_2mortal(newSViv(ret)) : &sv_undef;


void
lo_tell(dbh, fd)
	SV * dbh
	int fd
	CODE:
		const int ret = pg_db_lo_tell(dbh, fd);
		ST(0) = (ret >= 0) ? sv_2mortal(newSViv(ret)) : &sv_undef;


void
lo_close(dbh, fd)
	SV * dbh
	int fd
	CODE:
		ST(0) = (pg_db_lo_close(dbh, fd) >= 0) ? &sv_yes : &sv_no;


void
lo_unlink(dbh, lobjId)
	SV * dbh
	unsigned int lobjId
	CODE:
		ST(0) = (pg_db_lo_unlink(dbh, lobjId) >= 1) ? &sv_yes : &sv_no;


void
lo_import(dbh, filename)
	SV * dbh
	char * filename
	CODE:
		const unsigned int ret = pg_db_lo_import(dbh, filename);
		ST(0) = (ret > 0) ? sv_2mortal(newSVuv(ret)) : &sv_undef;


void
lo_export(dbh, lobjId, filename)
	SV * dbh
	unsigned int lobjId
	char * filename
	CODE:
		ST(0) = (pg_db_lo_export(dbh, lobjId, filename) >= 1) ? &sv_yes : &sv_no;


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
		SV *bufsv = SvROK(ST(1)) ? SvRV(ST(1)) : ST(1);
	INPUT:
		SV * dbh
		unsigned int len
		char * buf
	CODE:
		int ret;
		bufsv = SvROK(ST(1)) ? SvRV(ST(1)) : ST(1);
		sv_setpvn(bufsv,"",0); /* Make sure we can grow it safely */
		buf = SvGROW(bufsv, 3);
		if (len > 3)
			buf = SvGROW(bufsv, len);
		ret = pg_db_getline(dbh, bufsv, (int)len);
		sv_setpv((SV*)ST(1), buf);
		SvSETMAGIC(ST(1));
		ST(0) = (-1 != ret) ? &sv_yes : &sv_no;

I32
pg_getcopydata(dbh, dataline)
	INPUT:
		SV * dbh
	CODE:
		RETVAL = pg_db_getcopydata(dbh, SvROK(ST(1)) ? SvRV(ST(1)) : ST(1), 0);
	OUTPUT:
		RETVAL

I32
pg_getcopydata_async(dbh, dataline)
	INPUT:
		SV * dbh
	CODE:
		RETVAL = pg_db_getcopydata(dbh, SvROK(ST(1)) ? SvRV(ST(1)) : ST(1), 1);
	OUTPUT:
		RETVAL

I32
pg_putcopydata(dbh, dataline)
	INPUT:
		SV * dbh
		SV * dataline
	CODE:
		RETVAL = pg_db_putcopydata(dbh, dataline);
	OUTPUT:
		RETVAL

I32
pg_putcopyend(dbh)
	INPUT:
		SV * dbh
	CODE:
		RETVAL = pg_db_putcopyend(dbh);
	OUTPUT:
		RETVAL

void
getline(dbh, buf, len)
	PREINIT:
		SV *bufsv = SvROK(ST(1)) ? SvRV(ST(1)) : ST(1);
	INPUT:
		SV * dbh
		unsigned int len
		char * buf
	CODE:
		int ret;
		sv_setpvn(bufsv,"",0); /* Make sure we can grow it safely */
		buf = SvGROW(bufsv, 3);
		if (len > 3)
			buf = SvGROW(bufsv, len);
		ret = pg_db_getline(dbh, bufsv, (int)len);
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

		if (type_sv && SvOK(type_sv)) {
			sql_type_info_t *type_info;
			if SvMAGICAL(type_sv)
				(void)mg_get(type_sv);
			type_info = pg_type_data(SvIV(type_sv));
			type_num = type_info ? type_info->type.sql : SQL_VARCHAR;
		}
		ST(0) = sv_2mortal( newSViv( type_num ) );
	}

#if PGLIBVERSION >= 80000

void
pg_result(dbh)
	SV * dbh
	CODE:
		int ret;
		D_imp_dbh(dbh);
		ret = pg_db_result(dbh, imp_dbh);
		if (ret == 0)
			XST_mPV(0, "0E0");
		else if (ret < -1)
			XST_mUNDEF(0);
		else
			XST_mIV(0, ret);

void
pg_ready(dbh)
	SV *dbh
	CODE:
		D_imp_dbh(dbh);
		ST(0) = sv_2mortal(newSViv(pg_db_ready(dbh, imp_dbh)));

void
pg_cancel(dbh)
	SV *dbh
	CODE:
	D_imp_dbh(dbh);
	ST(0) = pg_db_cancel(dbh, imp_dbh) ? &sv_yes : &sv_no;

#endif


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

void
pg_ready(sth)
	SV *sth
	CODE:
		D_imp_sth(sth);
		D_imp_dbh_from_sth;
		ST(0) = sv_2mortal(newSViv(pg_db_ready(sth, imp_dbh)));

void
pg_cancel(sth)
	SV *sth
	CODE:
	D_imp_sth(sth);
	ST(0) = pg_db_cancel_sth(sth, imp_sth) ? &sv_yes : &sv_no;

#if PGLIBVERSION >= 80000

void
pg_result(sth)
	SV * sth
	CODE:
		int ret;
		D_imp_sth(sth);
		D_imp_dbh_from_sth;
		ret = pg_db_result(sth, imp_dbh);
		if (ret == 0)
			XST_mPV(0, "0E0");
		else if (ret < -1)
			XST_mUNDEF(0);
		else
			XST_mIV(0, ret);

#endif


# end of Pg.xs
