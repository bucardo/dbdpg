#include "quote.h"

#include "Pg.h"
#include "types.h"




#define TRUE 1
#define FALSE 0
/* For quoting/sql type mapping purposes this table only knows about
   the types that DBD::Pg knew about before.  The other tyeps are just
   here for returning the field type.

TODO:   - expand this for use with type_info() 
	- map all types to closest sql type.
	- set up quote functions for remaining types
	- autogeneratet this file.
*/

static sql_type_info_t pg_types[] = {
	{BOOLOID, "bool", TRUE, quote_bool, dequote_bool, {SQL_INTEGER}},
	{BYTEAOID, "bytea", TRUE, quote_bytea, dequote_bytea, {SQL_BINARY}},
	{CHAROID, "char", FALSE, quote_char, dequote_char, {0}},
	{NAMEOID, "name", FALSE, null_quote, null_dequote, {SQL_VARCHAR}},
	{INT8OID, "int8", TRUE, null_quote, null_dequote, {SQL_DOUBLE}},
	{INT2OID, "int2", TRUE, null_quote, null_dequote, {SQL_SMALLINT}},
	{INT2VECTOROID, "int28", FALSE, null_quote, null_dequote, {0}},
	{INT4OID, "int4", 2, null_quote, null_dequote, {SQL_INTEGER}},
	{REGPROCOID, "regproc", FALSE, null_quote, null_dequote, {0}},
	{TEXTOID, "text", TRUE, quote_varchar, dequote_varchar, {SQL_VARCHAR}},
	{OIDOID, "oid", TRUE, null_quote, null_dequote, {SQL_INTEGER}},
	{TIDOID, "tid", TRUE, null_quote, null_dequote, {SQL_INTEGER}},
	{XIDOID, "xid", TRUE, null_quote, null_dequote, {SQL_INTEGER}},
	{CIDOID, "cid", TRUE, null_quote, null_dequote, {SQL_INTEGER}},
	{OIDVECTOROID, "oid8", FALSE, null_quote, null_dequote, {0}},
	{POINTOID, "point", FALSE, null_quote, null_dequote, {0}},
	{LSEGOID, "lseg", FALSE, null_quote, null_dequote, {0}},
	{PATHOID, "path", FALSE, null_quote, null_dequote, {0}},
	{BOXOID, "box", FALSE, null_quote, null_dequote, {0}},
	{POLYGONOID, "polygon", FALSE, null_quote, null_dequote, {0}},
	{LINEOID, "line", FALSE, null_quote, null_dequote, {0}},
	{FLOAT4OID, "float4", TRUE, quote_char, dequote_char, {SQL_NUMERIC}},
	{FLOAT8OID, "float8", TRUE, null_quote,null_dequote, {SQL_REAL}},
	{ABSTIMEOID, "abstime", TRUE, null_quote, null_dequote, {0}},
	{RELTIMEOID, "reltime", TRUE, null_quote, null_dequote, {0}},
	{TINTERVALOID, "tinterval", TRUE, null_quote, null_dequote, {0}},
	{UNKNOWNOID, "unknown", FALSE, null_quote, null_dequote, {0}},
	{CIRCLEOID, "circle", FALSE, null_quote, null_dequote, {0}},
	{CASHOID, "money", TRUE, null_quote, null_dequote, {0}},
	{MACADDROID, "MAC address", TRUE, quote_varchar,dequote_varchar, {0}},
	{INETOID, "IP address", TRUE, null_quote, null_dequote, {0}},
	{CIDROID, "IP - cidr", TRUE, null_quote, null_dequote, {0}},
	{ACLITEMOID, "aclitem", FALSE, null_quote, null_dequote, {0}},
	{BPCHAROID, "bpchar", TRUE, quote_char, dequote_char, {SQL_CHAR}},
	{VARCHAROID, "varchar", TRUE, quote_varchar, dequote_varchar, {SQL_VARCHAR}},
	{DATEOID, "date", TRUE, null_quote, null_dequote, {0}},
	{TIMEOID, "time", TRUE, null_quote, null_dequote, {0}},
	{TIMESTAMPOID, "timestamp", TRUE, null_quote, null_dequote, {0}},
	{TIMESTAMPTZOID, "datetime", TRUE, null_quote, null_dequote, {0}},
	{INTERVALOID, "timespan", TRUE, null_quote, null_dequote, {0}},
	{TIMETZOID, "timestamptz", TRUE, null_quote, null_dequote, {0}},
	{BITOID, "bitstring", TRUE, null_quote, null_dequote, {0}},
	{VARBITOID, "vbitstring", TRUE, null_quote, null_dequote, {0}},
	{NUMERICOID, "numeric", TRUE, null_quote, null_dequote, {SQL_DECIMAL}},
	{REFCURSOROID, "refcursor", FALSE, null_quote, null_dequote, {0}},
	{REGPROCEDUREOID, "regprocedureoid", FALSE, null_quote, null_dequote, {0}},
	{REGOPEROID, "registeredoperator", FALSE, null_quote, null_dequote, {0}},
	{REGOPERATOROID, "registeroperator_args ", FALSE, null_quote, null_dequote, {0}},
	{REGCLASSOID, "regclass", FALSE, null_quote, null_dequote, {0}},
	{REGTYPEOID, "regtype", FALSE, null_quote, null_dequote, {0}},
	{RECORDOID, "record", FALSE, null_quote, null_dequote, {0}},
	{CSTRINGOID, "cstring", FALSE, null_quote, null_dequote, {0}},
	{ANYOID, "any", FALSE, null_quote, null_dequote, {0}},
	{ANYARRAYOID, "anyarray", FALSE, null_quote, null_dequote, {0}},
	{VOIDOID, "void", FALSE, null_quote, null_dequote, {0}},
	{TRIGGEROID, "trigger", FALSE, null_quote, null_dequote, {0}},
	{LANGUAGE_HANDLEROID, "languagehandle", FALSE, null_quote, null_dequote, {0}},
	{INTERNALOID, "internal", FALSE, null_quote, null_dequote, {0}},
	{OPAQUEOID, "opaque", FALSE, null_quote, null_dequote, {0}},
};

sql_type_info_t*
pg_type_data(sql_type)
	int sql_type;
{
	switch(sql_type) {

		case BOOLOID: 			return &pg_types[0];
		case BYTEAOID: 			return &pg_types[1];
		case CHAROID: 			return &pg_types[2];
		case NAMEOID: 			return &pg_types[3];
		case INT8OID: 			return &pg_types[4];
		case INT2OID: 			return &pg_types[5];
		case INT2VECTOROID: 		return &pg_types[6];
		case INT4OID: 			return &pg_types[7];
		case REGPROCOID: 		return &pg_types[8];
		case TEXTOID: 			return &pg_types[9];
		case OIDOID: 			return &pg_types[10];
		case TIDOID: 			return &pg_types[11];
		case XIDOID: 			return &pg_types[12];
		case CIDOID: 			return &pg_types[13];
		case OIDVECTOROID: 		return &pg_types[14];
		case POINTOID: 			return &pg_types[15];
		case LSEGOID: 			return &pg_types[16];
		case PATHOID: 			return &pg_types[17];
		case BOXOID: 			return &pg_types[18];
		case POLYGONOID: 		return &pg_types[19];
		case LINEOID: 			return &pg_types[20];
		case FLOAT4OID: 		return &pg_types[21];
		case FLOAT8OID: 		return &pg_types[22];
		case ABSTIMEOID: 		return &pg_types[23];
		case RELTIMEOID: 		return &pg_types[24];
		case TINTERVALOID: 		return &pg_types[25];
		case UNKNOWNOID: 		return &pg_types[26];
		case CIRCLEOID: 		return &pg_types[27];
		case CASHOID: 			return &pg_types[28];
		case MACADDROID: 		return &pg_types[29];
		case INETOID: 			return &pg_types[30];
		case CIDROID: 			return &pg_types[31];
		case ACLITEMOID: 		return &pg_types[32];
		case BPCHAROID: 		return &pg_types[33];
		case VARCHAROID: 		return &pg_types[34];
		case DATEOID: 			return &pg_types[35];
		case TIMEOID: 			return &pg_types[36];
		case TIMESTAMPOID: 		return &pg_types[37];
		case TIMESTAMPTZOID: 		return &pg_types[38];
		case INTERVALOID: 		return &pg_types[39];
		case TIMETZOID: 		return &pg_types[40];
		case BITOID: 			return &pg_types[41];
		case VARBITOID: 		return &pg_types[42];
		case NUMERICOID: 		return &pg_types[43];
		case REFCURSOROID: 		return &pg_types[44];
		case REGPROCEDUREOID: 		return &pg_types[45];
		case REGOPEROID: 		return &pg_types[46];
		case REGOPERATOROID: 		return &pg_types[47];
		case REGCLASSOID: 		return &pg_types[48];
		case REGTYPEOID: 		return &pg_types[49];
		case RECORDOID: 		return &pg_types[50];
		case CSTRINGOID: 		return &pg_types[51];
		case ANYOID: 			return &pg_types[52];
		case ANYARRAYOID: 		return &pg_types[53];
		case VOIDOID: 			return &pg_types[54];
		case TRIGGEROID: 		return &pg_types[55];
		case LANGUAGE_HANDLEROID: 	return &pg_types[56];
		case INTERNALOID: 		return &pg_types[57];
		case OPAQUEOID: 		return &pg_types[58];



		default:		return NULL;
	}
}




/*  This table only knows about the types that dbd_pg knew about before
    TODO: Put the rest of the sql types in here with mapping.
*/
static sql_type_info_t sql_types[] = {
	{SQL_VARCHAR, "SQL_VARCHAR", TRUE,quote_varchar, dequote_varchar, {VARCHAROID}},
	{SQL_CHAR, "SQL_CHAR", TRUE, quote_char, dequote_char, {BPCHAROID}},
	{SQL_NUMERIC, "SQL_NUMERIC", TRUE, null_quote, null_dequote, {FLOAT4OID}},
	{SQL_DECIMAL, "SQL_DECIMAL", TRUE, null_quote, null_dequote, {FLOAT4OID}},
	{SQL_INTEGER, "SQL_INTEGER", TRUE, null_quote, null_dequote, {INT4OID}},
	{SQL_SMALLINT, "SQL_SMALLINT", TRUE, null_quote, null_dequote, {INT2OID}},
	{SQL_FLOAT, "SQL_FLOAT", TRUE, null_quote, null_dequote, {FLOAT4OID}},
	{SQL_REAL, "SQL_REAL", TRUE, null_quote, null_dequote, {FLOAT8OID}},
	{SQL_DOUBLE, "SQL_DOUBLE", TRUE, null_quote, null_dequote, {INT8OID}},
	{SQL_BINARY, "SQL_BINARY", TRUE, quote_sql_binary, dequote_sql_binary, {BYTEAOID}},

};

sql_type_info_t*
sql_type_data(sql_type)
	int sql_type;
{
	switch(sql_type) {
		case SQL_VARCHAR:	return &sql_types[0];
		case SQL_CHAR:		return &sql_types[1];
		case SQL_NUMERIC:	return &sql_types[2];
		case SQL_DECIMAL:	return &sql_types[3];
		case SQL_INTEGER:	return &sql_types[4];
		case SQL_SMALLINT:	return &sql_types[5];
		case SQL_FLOAT:		return &sql_types[6];
		case SQL_REAL:		return &sql_types[7];
		case SQL_DOUBLE:	return &sql_types[8];
		case SQL_BINARY:	return &sql_types[9];
		default:		return NULL;
	}
}
