/*

   $Id$

   Copyright (c) 2003-2010 Greg Sabino Mullane and others: see the Changes file
   
   You may distribute under the terms of either the GNU General Public
   License or the Artistic License, as specified in the Perl README file.

*/

/*
    Please do not edit the C portions of this file directly. 
    It is automatically generated by the enclosed Perl script.
*/

#include "Pg.h"

static sql_type_info_t pg_types[] = {
 {PG_ABSTIMEARRAY      ,"_abstime"         ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_ACLITEMARRAY      ,"_aclitem"         ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_BITARRAY          ,"_bit"             ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_BOOLARRAY         ,"_bool"            ,1,',',"array_out"       ,quote_string,dequote_string,{0},3},
 {PG_BOXARRAY          ,"_box"             ,1,';',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_BPCHARARRAY       ,"_bpchar"          ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_BYTEAARRAY        ,"_bytea"           ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_CHARARRAY         ,"_char"            ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_CIDARRAY          ,"_cid"             ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_CIDRARRAY         ,"_cidr"            ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_CIRCLEARRAY       ,"_circle"          ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_CSTRINGARRAY      ,"_cstring"         ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_DATEARRAY         ,"_date"            ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_FLOAT4ARRAY       ,"_float4"          ,1,',',"array_out"       ,quote_string,dequote_string,{0},2},
 {PG_FLOAT8ARRAY       ,"_float8"          ,1,',',"array_out"       ,quote_string,dequote_string,{0},2},
 {PG_GTSVECTORARRAY    ,"_gtsvector"       ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_INETARRAY         ,"_inet"            ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_INT2ARRAY         ,"_int2"            ,1,',',"array_out"       ,quote_string,dequote_string,{0},1},
 {PG_INT2VECTORARRAY   ,"_int2vector"      ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_INT4ARRAY         ,"_int4"            ,1,',',"array_out"       ,quote_string,dequote_string,{0},1},
 {PG_INT8ARRAY         ,"_int8"            ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_INTERVALARRAY     ,"_interval"        ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_LINEARRAY         ,"_line"            ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_LSEGARRAY         ,"_lseg"            ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_MACADDRARRAY      ,"_macaddr"         ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_MONEYARRAY        ,"_money"           ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_NAMEARRAY         ,"_name"            ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_NUMERICARRAY      ,"_numeric"         ,1,',',"array_out"       ,quote_string,dequote_string,{0},2},
 {PG_OIDARRAY          ,"_oid"             ,1,',',"array_out"       ,quote_string,dequote_string,{0},1},
 {PG_OIDVECTORARRAY    ,"_oidvector"       ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_PATHARRAY         ,"_path"            ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_POINTARRAY        ,"_point"           ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_POLYGONARRAY      ,"_polygon"         ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_RECORDARRAY       ,"_record"          ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_REFCURSORARRAY    ,"_refcursor"       ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_REGCLASSARRAY     ,"_regclass"        ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_REGCONFIGARRAY    ,"_regconfig"       ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_REGDICTIONARYARRAY,"_regdictionary"   ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_REGOPERARRAY      ,"_regoper"         ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_REGOPERATORARRAY  ,"_regoperator"     ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_REGPROCARRAY      ,"_regproc"         ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_REGPROCEDUREARRAY ,"_regprocedure"    ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_REGTYPEARRAY      ,"_regtype"         ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_RELTIMEARRAY      ,"_reltime"         ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_TEXTARRAY         ,"_text"            ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_TIDARRAY          ,"_tid"             ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_TIMEARRAY         ,"_time"            ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_TIMESTAMPARRAY    ,"_timestamp"       ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_TIMESTAMPTZARRAY  ,"_timestamptz"     ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_TIMETZARRAY       ,"_timetz"          ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_TINTERVALARRAY    ,"_tinterval"       ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_TSQUERYARRAY      ,"_tsquery"         ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_TSVECTORARRAY     ,"_tsvector"        ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_TXID_SNAPSHOTARRAY,"_txid_snapshot"   ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_UUIDARRAY         ,"_uuid"            ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_VARBITARRAY       ,"_varbit"          ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_VARCHARARRAY      ,"_varchar"         ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_XIDARRAY          ,"_xid"             ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_XMLARRAY          ,"_xml"             ,1,',',"array_out"       ,quote_string,dequote_string,{0},0},
 {PG_ABSTIME           ,"abstime"          ,1,',',"abstimeout"      ,quote_string,dequote_string,{0},0},
 {PG_ACLITEM           ,"aclitem"          ,1,',',"aclitemout"      ,quote_string,dequote_string,{0},0},
 {PG_ANY               ,"any"              ,1,',',"any_out"         ,quote_string,dequote_string,{0},0},
 {PG_ANYARRAY          ,"anyarray"         ,1,',',"anyarray_out"    ,quote_string,dequote_string,{0},0},
 {PG_ANYELEMENT        ,"anyelement"       ,1,',',"anyelement_out"  ,quote_string,dequote_string,{0},0},
 {PG_ANYENUM           ,"anyenum"          ,1,',',"anyenum_out"     ,quote_string,dequote_string,{0},0},
 {PG_ANYNONARRAY       ,"anynonarray"      ,1,',',"anynonarray_out" ,quote_string,dequote_string,{0},0},
 {PG_BIT               ,"bit"              ,1,',',"bit_out"         ,quote_string,dequote_string,{0},0},
 {PG_BOOL              ,"bool"             ,1,',',"boolout"         ,quote_bool  ,dequote_bool  ,{SQL_BOOLEAN},3},
 {PG_BOX               ,"box"              ,1,';',"box_out"         ,quote_geom  ,dequote_string,{0},0},
 {PG_BPCHAR            ,"bpchar"           ,1,',',"bpcharout"       ,quote_string,dequote_char  ,{SQL_CHAR},0},
 {PG_BYTEA             ,"bytea"            ,1,',',"byteaout"        ,quote_bytea ,dequote_bytea ,{SQL_VARBINARY},0},
 {PG_CHAR              ,"char"             ,1,',',"charout"         ,quote_string,dequote_char  ,{SQL_CHAR},0},
 {PG_CID               ,"cid"              ,1,',',"cidout"          ,quote_string,dequote_string,{0},0},
 {PG_CIDR              ,"cidr"             ,1,',',"cidr_out"        ,quote_string,dequote_string,{0},0},
 {PG_CIRCLE            ,"circle"           ,1,',',"circle_out"      ,quote_circle,dequote_string,{0},0},
 {PG_CSTRING           ,"cstring"          ,1,',',"cstring_out"     ,quote_string,dequote_string,{0},0},
 {PG_DATE              ,"date"             ,1,',',"date_out"        ,quote_string,dequote_string,{SQL_TYPE_DATE},0},
 {PG_FLOAT4            ,"float4"           ,1,',',"float4out"       ,quote_float ,null_dequote  ,{0},2},
 {PG_FLOAT8            ,"float8"           ,1,',',"float8out"       ,quote_float ,null_dequote  ,{SQL_FLOAT},2},
 {PG_GTSVECTOR         ,"gtsvector"        ,1,',',"gtsvectorout"    ,quote_string,dequote_string,{0},0},
 {PG_INET              ,"inet"             ,1,',',"inet_out"        ,quote_string,dequote_string,{0},0},
 {PG_INT2              ,"int2"             ,1,',',"int2out"         ,quote_int   ,null_dequote  ,{SQL_SMALLINT},1},
 {PG_INT2VECTOR        ,"int2vector"       ,1,',',"int2vectorout"   ,quote_string,dequote_string,{0},0},
 {PG_INT4              ,"int4"             ,1,',',"int4out"         ,quote_int   ,null_dequote  ,{SQL_INTEGER},1},
 {PG_INT8              ,"int8"             ,1,',',"int8out"         ,quote_int   ,null_dequote  ,{SQL_BIGINT},0},
 {PG_INTERNAL          ,"internal"         ,1,',',"internal_out"    ,quote_string,dequote_string,{0},0},
 {PG_INTERVAL          ,"interval"         ,1,',',"interval_out"    ,quote_string,dequote_string,{0},0},
 {PG_LANGUAGE_HANDLER  ,"language_handler" ,1,',',"language_handler_out",quote_string,dequote_string,{0},0},
 {PG_LINE              ,"line"             ,1,',',"line_out"        ,quote_geom  ,dequote_string,{0},0},
 {PG_LSEG              ,"lseg"             ,1,',',"lseg_out"        ,quote_geom  ,dequote_string,{0},0},
 {PG_MACADDR           ,"macaddr"          ,1,',',"macaddr_out"     ,quote_string,dequote_string,{0},0},
 {PG_MONEY             ,"money"            ,1,',',"cash_out"        ,quote_string,dequote_string,{0},0},
 {PG_NAME              ,"name"             ,1,',',"nameout"         ,quote_name  ,null_dequote  ,{SQL_VARCHAR},0},
 {PG_NUMERIC           ,"numeric"          ,1,',',"numeric_out"     ,quote_float ,null_dequote  ,{SQL_DECIMAL},2},
 {PG_OID               ,"oid"              ,1,',',"oidout"          ,quote_int   ,null_dequote  ,{0},1},
 {PG_OIDVECTOR         ,"oidvector"        ,1,',',"oidvectorout"    ,quote_string,dequote_string,{0},0},
 {PG_OPAQUE            ,"opaque"           ,1,',',"opaque_out"      ,quote_string,dequote_string,{0},0},
 {PG_PATH              ,"path"             ,1,',',"path_out"        ,quote_path  ,dequote_string,{0},0},
 {PG_PG_ATTRIBUTE      ,"pg_attribute"     ,1,',',"record_out"      ,quote_string,dequote_string,{0},0},
 {PG_PG_CLASS          ,"pg_class"         ,1,',',"record_out"      ,quote_string,dequote_string,{0},0},
 {PG_PG_PROC           ,"pg_proc"          ,1,',',"record_out"      ,quote_string,dequote_string,{0},0},
 {PG_PG_TYPE           ,"pg_type"          ,1,',',"record_out"      ,quote_string,dequote_string,{0},0},
 {PG_POINT             ,"point"            ,1,',',"point_out"       ,quote_geom  ,dequote_string,{0},0},
 {PG_POLYGON           ,"polygon"          ,1,',',"poly_out"        ,quote_geom  ,dequote_string,{0},0},
 {PG_RECORD            ,"record"           ,1,',',"record_out"      ,quote_string,dequote_string,{0},0},
 {PG_REFCURSOR         ,"refcursor"        ,1,',',"textout"         ,quote_string,dequote_string,{0},0},
 {PG_REGCLASS          ,"regclass"         ,1,',',"regclassout"     ,quote_string,dequote_string,{0},0},
 {PG_REGCONFIG         ,"regconfig"        ,1,',',"regconfigout"    ,quote_string,dequote_string,{0},0},
 {PG_REGDICTIONARY     ,"regdictionary"    ,1,',',"regdictionaryout",quote_string,dequote_string,{0},0},
 {PG_REGOPER           ,"regoper"          ,1,',',"regoperout"      ,quote_string,dequote_string,{0},0},
 {PG_REGOPERATOR       ,"regoperator"      ,1,',',"regoperatorout"  ,quote_string,dequote_string,{0},0},
 {PG_REGPROC           ,"regproc"          ,1,',',"regprocout"      ,quote_string,dequote_string,{0},0},
 {PG_REGPROCEDURE      ,"regprocedure"     ,1,',',"regprocedureout" ,quote_string,dequote_string,{0},0},
 {PG_REGTYPE           ,"regtype"          ,1,',',"regtypeout"      ,quote_string,dequote_string,{0},0},
 {PG_RELTIME           ,"reltime"          ,1,',',"reltimeout"      ,quote_string,dequote_string,{0},0},
 {PG_SMGR              ,"smgr"             ,1,',',"smgrout"         ,quote_string,dequote_string,{0},0},
 {PG_TEXT              ,"text"             ,1,',',"textout"         ,quote_string,dequote_string,{SQL_LONGVARCHAR},0},
 {PG_TID               ,"tid"              ,1,',',"tidout"          ,quote_geom  ,dequote_string,{0},0},
 {PG_TIME              ,"time"             ,1,',',"time_out"        ,quote_string,dequote_string,{SQL_TYPE_TIME},0},
 {PG_TIMESTAMP         ,"timestamp"        ,1,',',"timestamp_out"   ,quote_string,dequote_string,{SQL_TIMESTAMP},0},
 {PG_TIMESTAMPTZ       ,"timestamptz"      ,1,',',"timestamptz_out" ,quote_string,dequote_string,{SQL_TYPE_TIMESTAMP_WITH_TIMEZONE},0},
 {PG_TIMETZ            ,"timetz"           ,1,',',"timetz_out"      ,quote_string,dequote_string,{0},0},
 {PG_TINTERVAL         ,"tinterval"        ,1,',',"tintervalout"    ,quote_string,dequote_string,{0},0},
 {PG_TRIGGER           ,"trigger"          ,1,',',"trigger_out"     ,quote_string,dequote_string,{0},0},
 {PG_TSQUERY           ,"tsquery"          ,1,',',"tsqueryout"      ,quote_string,dequote_string,{0},0},
 {PG_TSVECTOR          ,"tsvector"         ,1,',',"tsvectorout"     ,quote_string,dequote_string,{0},0},
 {PG_TXID_SNAPSHOT     ,"txid_snapshot"    ,1,',',"txid_snapshot_out",quote_string,dequote_string,{0},0},
 {PG_UNKNOWN           ,"unknown"          ,1,',',"unknownout"      ,quote_string,dequote_string,{0},0},
 {PG_UUID              ,"uuid"             ,1,',',"uuid_out"        ,quote_string,dequote_string,{0},0},
 {PG_VARBIT            ,"varbit"           ,1,',',"varbit_out"      ,quote_string,dequote_string,{0},0},
 {PG_VARCHAR           ,"varchar"          ,1,',',"varcharout"      ,quote_string,dequote_string,{SQL_VARCHAR},0},
 {PG_VOID              ,"void"             ,1,',',"void_out"        ,quote_string,dequote_string,{0},0},
 {PG_XID               ,"xid"              ,1,',',"xidout"          ,quote_string,dequote_string,{0},0},
 {PG_XML               ,"xml"              ,1,',',"xml_out"         ,quote_string,dequote_string,{0},0},
};

sql_type_info_t* pg_type_data(int sql_type)
{
	dTHX;

	switch(sql_type) {

		case PG_ABSTIMEARRAY:       return &pg_types[0];
		case PG_ACLITEMARRAY:       return &pg_types[1];
		case PG_BITARRAY:           return &pg_types[2];
		case PG_BOOLARRAY:          return &pg_types[3];
		case PG_BOXARRAY:           return &pg_types[4];
		case PG_BPCHARARRAY:        return &pg_types[5];
		case PG_BYTEAARRAY:         return &pg_types[6];
		case PG_CHARARRAY:          return &pg_types[7];
		case PG_CIDARRAY:           return &pg_types[8];
		case PG_CIDRARRAY:          return &pg_types[9];
		case PG_CIRCLEARRAY:        return &pg_types[10];
		case PG_CSTRINGARRAY:       return &pg_types[11];
		case PG_DATEARRAY:          return &pg_types[12];
		case PG_FLOAT4ARRAY:        return &pg_types[13];
		case PG_FLOAT8ARRAY:        return &pg_types[14];
		case PG_GTSVECTORARRAY:     return &pg_types[15];
		case PG_INETARRAY:          return &pg_types[16];
		case PG_INT2ARRAY:          return &pg_types[17];
		case PG_INT2VECTORARRAY:    return &pg_types[18];
		case PG_INT4ARRAY:          return &pg_types[19];
		case PG_INT8ARRAY:          return &pg_types[20];
		case PG_INTERVALARRAY:      return &pg_types[21];
		case PG_LINEARRAY:          return &pg_types[22];
		case PG_LSEGARRAY:          return &pg_types[23];
		case PG_MACADDRARRAY:       return &pg_types[24];
		case PG_MONEYARRAY:         return &pg_types[25];
		case PG_NAMEARRAY:          return &pg_types[26];
		case PG_NUMERICARRAY:       return &pg_types[27];
		case PG_OIDARRAY:           return &pg_types[28];
		case PG_OIDVECTORARRAY:     return &pg_types[29];
		case PG_PATHARRAY:          return &pg_types[30];
		case PG_POINTARRAY:         return &pg_types[31];
		case PG_POLYGONARRAY:       return &pg_types[32];
		case PG_RECORDARRAY:        return &pg_types[33];
		case PG_REFCURSORARRAY:     return &pg_types[34];
		case PG_REGCLASSARRAY:      return &pg_types[35];
		case PG_REGCONFIGARRAY:     return &pg_types[36];
		case PG_REGDICTIONARYARRAY: return &pg_types[37];
		case PG_REGOPERARRAY:       return &pg_types[38];
		case PG_REGOPERATORARRAY:   return &pg_types[39];
		case PG_REGPROCARRAY:       return &pg_types[40];
		case PG_REGPROCEDUREARRAY:  return &pg_types[41];
		case PG_REGTYPEARRAY:       return &pg_types[42];
		case PG_RELTIMEARRAY:       return &pg_types[43];
		case PG_TEXTARRAY:          return &pg_types[44];
		case PG_TIDARRAY:           return &pg_types[45];
		case PG_TIMEARRAY:          return &pg_types[46];
		case PG_TIMESTAMPARRAY:     return &pg_types[47];
		case PG_TIMESTAMPTZARRAY:   return &pg_types[48];
		case PG_TIMETZARRAY:        return &pg_types[49];
		case PG_TINTERVALARRAY:     return &pg_types[50];
		case PG_TSQUERYARRAY:       return &pg_types[51];
		case PG_TSVECTORARRAY:      return &pg_types[52];
		case PG_TXID_SNAPSHOTARRAY: return &pg_types[53];
		case PG_UUIDARRAY:          return &pg_types[54];
		case PG_VARBITARRAY:        return &pg_types[55];
		case PG_VARCHARARRAY:       return &pg_types[56];
		case PG_XIDARRAY:           return &pg_types[57];
		case PG_XMLARRAY:           return &pg_types[58];
		case PG_ABSTIME:            return &pg_types[59];
		case PG_ACLITEM:            return &pg_types[60];
		case PG_ANY:                return &pg_types[61];
		case PG_ANYARRAY:           return &pg_types[62];
		case PG_ANYELEMENT:         return &pg_types[63];
		case PG_ANYENUM:            return &pg_types[64];
		case PG_ANYNONARRAY:        return &pg_types[65];
		case PG_BIT:                return &pg_types[66];
		case PG_BOOL:               return &pg_types[67];
		case PG_BOX:                return &pg_types[68];
		case PG_BPCHAR:             return &pg_types[69];
		case PG_BYTEA:              return &pg_types[70];
		case PG_CHAR:               return &pg_types[71];
		case PG_CID:                return &pg_types[72];
		case PG_CIDR:               return &pg_types[73];
		case PG_CIRCLE:             return &pg_types[74];
		case PG_CSTRING:            return &pg_types[75];
		case PG_DATE:               return &pg_types[76];
		case PG_FLOAT4:             return &pg_types[77];
		case PG_FLOAT8:             return &pg_types[78];
		case PG_GTSVECTOR:          return &pg_types[79];
		case PG_INET:               return &pg_types[80];
		case PG_INT2:               return &pg_types[81];
		case PG_INT2VECTOR:         return &pg_types[82];
		case PG_INT4:               return &pg_types[83];
		case PG_INT8:               return &pg_types[84];
		case PG_INTERNAL:           return &pg_types[85];
		case PG_INTERVAL:           return &pg_types[86];
		case PG_LANGUAGE_HANDLER:   return &pg_types[87];
		case PG_LINE:               return &pg_types[88];
		case PG_LSEG:               return &pg_types[89];
		case PG_MACADDR:            return &pg_types[90];
		case PG_MONEY:              return &pg_types[91];
		case PG_NAME:               return &pg_types[92];
		case PG_NUMERIC:            return &pg_types[93];
		case PG_OID:                return &pg_types[94];
		case PG_OIDVECTOR:          return &pg_types[95];
		case PG_OPAQUE:             return &pg_types[96];
		case PG_PATH:               return &pg_types[97];
		case PG_PG_ATTRIBUTE:       return &pg_types[98];
		case PG_PG_CLASS:           return &pg_types[99];
		case PG_PG_PROC:            return &pg_types[100];
		case PG_PG_TYPE:            return &pg_types[101];
		case PG_POINT:              return &pg_types[102];
		case PG_POLYGON:            return &pg_types[103];
		case PG_RECORD:             return &pg_types[104];
		case PG_REFCURSOR:          return &pg_types[105];
		case PG_REGCLASS:           return &pg_types[106];
		case PG_REGCONFIG:          return &pg_types[107];
		case PG_REGDICTIONARY:      return &pg_types[108];
		case PG_REGOPER:            return &pg_types[109];
		case PG_REGOPERATOR:        return &pg_types[110];
		case PG_REGPROC:            return &pg_types[111];
		case PG_REGPROCEDURE:       return &pg_types[112];
		case PG_REGTYPE:            return &pg_types[113];
		case PG_RELTIME:            return &pg_types[114];
		case PG_SMGR:               return &pg_types[115];
		case PG_TEXT:               return &pg_types[116];
		case PG_TID:                return &pg_types[117];
		case PG_TIME:               return &pg_types[118];
		case PG_TIMESTAMP:          return &pg_types[119];
		case PG_TIMESTAMPTZ:        return &pg_types[120];
		case PG_TIMETZ:             return &pg_types[121];
		case PG_TINTERVAL:          return &pg_types[122];
		case PG_TRIGGER:            return &pg_types[123];
		case PG_TSQUERY:            return &pg_types[124];
		case PG_TSVECTOR:           return &pg_types[125];
		case PG_TXID_SNAPSHOT:      return &pg_types[126];
		case PG_UNKNOWN:            return &pg_types[127];
		case PG_UUID:               return &pg_types[128];
		case PG_VARBIT:             return &pg_types[129];
		case PG_VARCHAR:            return &pg_types[130];
		case PG_VOID:               return &pg_types[131];
		case PG_XID:                return &pg_types[132];
		case PG_XML:                return &pg_types[133];
		default: return NULL;
	}
}

static sql_type_info_t sql_types[] = {
 {SQL_BOOLEAN,"SQL_BOOLEAN",1,',', "none", quote_bool, dequote_bool, {PG_BOOL}, 3},
 {SQL_CHAR,"SQL_CHAR",1,',', "none", quote_string, dequote_char, {PG_BPCHAR}, 0},
 {SQL_VARBINARY,"SQL_VARBINARY",1,',', "none", quote_bytea, dequote_bytea, {PG_BYTEA}, 0},
 {SQL_CHAR,"SQL_CHAR",1,',', "none", quote_string, dequote_char, {PG_CHAR}, 0},
 {SQL_TYPE_DATE,"SQL_TYPE_DATE",1,',', "none", quote_string, dequote_string, {PG_DATE}, 0},
 {SQL_FLOAT,"SQL_FLOAT",1,',', "none", quote_float, null_dequote, {PG_FLOAT8}, 2},
 {SQL_DOUBLE,"SQL_DOUBLE",1,',', "none", quote_float, null_dequote, {PG_FLOAT8}, 2},
 {SQL_NUMERIC,"SQL_NUMERIC",1,',', "none", quote_float, null_dequote, {PG_FLOAT8}, 2},
 {SQL_REAL,"SQL_REAL",1,',', "none", quote_float, null_dequote, {PG_FLOAT8}, 2},
 {SQL_SMALLINT,"SQL_SMALLINT",1,',', "none", quote_int, null_dequote, {PG_INT2}, 1},
 {SQL_TINYINT,"SQL_TINYINT",1,',', "none", quote_int, null_dequote, {PG_INT2}, 1},
 {SQL_INTEGER,"SQL_INTEGER",1,',', "none", quote_int, null_dequote, {PG_INT4}, 1},
 {SQL_BIGINT,"SQL_BIGINT",1,',', "none", quote_int, null_dequote, {PG_INT8}, 0},
 {SQL_VARCHAR,"SQL_VARCHAR",1,',', "none", quote_name, null_dequote, {PG_NAME}, 0},
 {SQL_DECIMAL,"SQL_DECIMAL",1,',', "none", quote_float, null_dequote, {PG_NUMERIC}, 2},
 {SQL_LONGVARCHAR,"SQL_LONGVARCHAR",1,',', "none", quote_string, dequote_string, {PG_TEXT}, 0},
 {SQL_TYPE_TIME,"SQL_TYPE_TIME",1,',', "none", quote_string, dequote_string, {PG_TIME}, 0},
 {SQL_TIMESTAMP,"SQL_TIMESTAMP",1,',', "none", quote_string, dequote_string, {PG_TIMESTAMP}, 0},
 {SQL_TYPE_TIMESTAMP,"SQL_TYPE_TIMESTAMP",1,',', "none", quote_string, dequote_string, {PG_TIMESTAMP}, 0},
 {SQL_TYPE_TIMESTAMP_WITH_TIMEZONE,"SQL_TYPE_TIMESTAMP_WITH_TIMEZONE",1,',', "none", quote_string, dequote_string, {PG_TIMESTAMPTZ}, 0},
 {SQL_TYPE_TIME_WITH_TIMEZONE,"SQL_TYPE_TIME_WITH_TIMEZONE",1,',', "none", quote_string, dequote_string, {PG_TIMESTAMPTZ}, 0},
 {SQL_VARCHAR,"SQL_VARCHAR",1,',', "none", quote_string, dequote_string, {PG_VARCHAR}, 0},
};

sql_type_info_t* sql_type_data(int sql_type)
{
	dTHX;

	switch(sql_type) {
		case SQL_BOOLEAN:                      return &sql_types[0];
		case SQL_VARBINARY:                    return &sql_types[2];
		case SQL_CHAR:                         return &sql_types[3];
		case SQL_TYPE_DATE:                    return &sql_types[4];
		case SQL_FLOAT:                        return &sql_types[5];
		case SQL_DOUBLE:                       return &sql_types[6];
		case SQL_NUMERIC:                      return &sql_types[7];
		case SQL_REAL:                         return &sql_types[8];
		case SQL_SMALLINT:                     return &sql_types[9];
		case SQL_TINYINT:                      return &sql_types[10];
		case SQL_INTEGER:                      return &sql_types[11];
		case SQL_BIGINT:                       return &sql_types[12];
		case SQL_DECIMAL:                      return &sql_types[14];
		case SQL_LONGVARCHAR:                  return &sql_types[15];
		case SQL_TYPE_TIME:                    return &sql_types[16];
		case SQL_TIMESTAMP:                    return &sql_types[17];
		case SQL_TYPE_TIMESTAMP:               return &sql_types[18];
		case SQL_TYPE_TIMESTAMP_WITH_TIMEZONE: return &sql_types[19];
		case SQL_TYPE_TIME_WITH_TIMEZONE:      return &sql_types[20];
		case SQL_VARCHAR:                      return &sql_types[21];
		default: return NULL;
	}
}

/*
#!perl

## Autogenerate all type information and populate 
## all files referencing type information.

## You should only run this if you are developing DBD::Pg and 
## understand what this script does

## Usage: perl -x $0 "path-to-pgsql-source"

use strict;
use warnings;

my $arg = shift || die "Usage: $0 path-to-pgsql-source\n";

-d $arg or die qq{Sorry, but "$arg" is not a directory!\n};

my $file = "$arg/src/include/catalog/pg_type.h";
my $typefile = $file;

open my $fh, '<', $file or die qq{Could not open file "$file": $!\n};
my $maxlen = 1;
my %pgtype;
my $thisname = 0;
while(<$fh>) {
	s/FLOAT8PASSBYVAL/t/;
	s/FLOAT4PASSBYVAL/t/;
	if (/^DATA\(insert OID\s+=\s+(\d+)\s+\(\s+(\S+)\s+\S+ \S+\s+\S+\s+[t|f]\s+. . [tf] ([tf]) \\(\d+) (\d+)\s+(\d+) (\d+) (\S+) (\S+) (\S+) (\S+)/o) {
		my ($oid,$name,$typedef,$delim,$typrelid,$typelem,$typarray,$tin,$tout,$bin,$bout) =
			($1,$2,$3,chr(oct($4)),$5,$6,$7,$8,$9,$10,$11);
		die "Duplicated OID $oid!: $_\n" if exists $pgtype{$oid};
		$pgtype{$name} = {
			oid     => $oid,
			delim   => $delim,
			textin  => $tin,
			textout => $tout,
			binin   => $bin,
			binout  => $bout,
			quote   => 'quote_string',
			dequote => 'dequote_string',
			define  => 'PG_' . uc($name),
			sql     => 0,
			sqlc    => 0,
			svtype  => 0,
		};
		if ($name =~ /_/) {
			(my $basename = $name) =~ s/_//;
			if (exists $pgtype{$basename}) {
				$pgtype{$name}{delim} = $pgtype{$basename}{delim};
			}
		}
		length($name) > $maxlen and $maxlen = length($name);
		$thisname = $name;
		## Special hack for array types
		if ($tin =~ /^array/ and $name =~ /^_/) {
			$pgtype{$name}{define} = 'PG' . uc $name . 'ARRAY';
		}
	}
	elsif (/^DESCR\("(.+?)"/) {
		$pgtype{$thisname}{description} = $1;
	}
	elsif (/^DATA/) {
		die "Bad line at $. ->$_\n";
	}
}
close $fh or die qq{Could not close "$file": $!\n};

my ($oldfh,$newfh);

## Rewrite types.h
$file = 'types.h';
open $newfh, '>', "$file.tmp" or die qq{Could not create "$file.tmp": $!\n};

my $slashstar = '/' . '*';
my $starslash = '*' . '/';
print $newfh qq{$slashstar
Do not edit this file directly - it is generated by types.c
$starslash

typedef struct sql_type_info {
	int     type_id;
	char*   type_name;
	bool    bind_ok;
	char    array_delimeter;
	char*   arrayout;
	char*   (*quote)();
	void    (*dequote)();
	union   {
			int pg;
			int sql;
	} type;
	int     svtype;
} sql_type_info_t;

sql_type_info_t* pg_type_data(int);
sql_type_info_t* sql_type_data(int);

};

$maxlen += 5;
for (sort {
		$pgtype{$a}{define} =~ /ARRAY/ <=> $pgtype{$b}{define} =~ /ARRAY/
		or $pgtype{$a}{define} cmp $pgtype{$b}{define}
	} keys %pgtype) {
	printf $newfh "#define %${maxlen}s  $pgtype{$_}{oid}\n", $pgtype{$_}{define};
}

print $newfh "\n";
close $newfh or die qq{Could not close "$file.tmp": $!\n};
system("mv $file.tmp $file");
print "Wrote $file\n";

## Rewrite Pg.xs
$file = 'Pg.xs';
open $oldfh, '<', $file or die qq{Could not open "$file": $!\n};
open $newfh, '>', "$file.tmp" or die qq{Could not write to "$file.tmp": $!\n};
my $step = 0;
while (<$oldfh>) {
	if (0 == $step) {
		if (/ALIAS:/) {
			print $newfh $_;
			$step = 1;
			for (sort { $pgtype{$a}{define} cmp $pgtype{$b}{define} } keys %pgtype) {
				printf $newfh "\t%-${maxlen}s = $pgtype{$_}{oid}\n", $pgtype{$_}{define};
			}
			printf $newfh "\n\t%-${maxlen}s = 1\n", 'PG_ASYNC';
			printf $newfh "\t%-${maxlen}s = 2\n", 'PG_OLDQUERY_CANCEL';
			printf $newfh "\t%-${maxlen}s = 4\n\n", 'PG_OLDQUERY_WAIT';
			next;
		}
	}
	elsif (1 == $step) {
		next unless /CODE:/;
		$step = 2;
	}
	print $newfh $_;
}
close $newfh or die qq{Could not close "$file.tmp": $!\n};
close $oldfh  or die qq{Could not close "$file": $!\n};
system("mv $file.tmp $file");
print "Wrote $file\n";


## Rewrite Pg.pm
$file = 'Pg.pm';
open $oldfh, '<', $file or die qq{Could not open "$file": $!\n};
open $newfh, '>', "$file.tmp" or die qq{Could not write to "$file.tmp": $!\n};
$step = 0;
while (<$oldfh>) {
	if (0 == $step) {
		if (/pg_types/) {
			chomp;
			print $newfh $_;
			$step = 0;
			for (sort { $pgtype{$a}{define} cmp $pgtype{$b}{define} } keys %pgtype) {
				printf $newfh "%s$pgtype{$_}{define}", !($step++ % 5) ? "\n\t\t\t" : ' ';
			}
			print $newfh "\n";
			$step = 1;
			next;
		}
	}
	elsif (1 == $step) {
		next unless /\]/;
		$step = 2;
	}
	elsif (2 == $step) {
		if (/data types exported/) {
			print $newfh $_;
			$step = 0;
			for (sort { $pgtype{$a}{define} cmp $pgtype{$b}{define} } keys %pgtype) {
				printf $newfh "%s$pgtype{$_}{define}", !($step++ % 6) ? "\n " : ' ';
			}
			print $newfh "\n\n";
			$step = 3;
			next;
		}
	}
	elsif (3 == $step) {
		next unless /sticky/;
		$step = 4;
	}
	print $newfh $_;
}
close $newfh or die qq{Could not close "$file.tmp": $!\n};
close $oldfh or die qq{Could not close "$file": $!\n};
system("mv $file.tmp $file");
print "Wrote $file\n";

## Rewrite 01constants.t
$file = 't/01constants.t';
open $oldfh, '<', $file or die qq{Could not open "$file": $!\n};
open $newfh, '>', "$file.tmp" or die qq{Could not write to "$file.tmp": $!\n};
$step = 0;
while (<$oldfh>) {
	if (0 == $step) {
		if (/^is/) {
			for (sort { $pgtype{$a}{define} cmp $pgtype{$b}{define} } keys %pgtype) {
				printf $newfh qq{is (%-*s, %5s, '%s returns correct value');\n},
					3+$maxlen, $pgtype{$_}{define}, $pgtype{$_}{oid}, $pgtype{$_}{define};
			}
			print $newfh "\n";
			$step = 1;
			last;
		}
	}
	print $newfh $_;
}
close $newfh or die qq{Could not close "$file.tmp": $!\n};
close $oldfh or die qq{Could not close "$file": $!\n};
system("mv $file.tmp $file");
print "Wrote $file\n";

## Rewrite 99_pod.t
$file = 't/99_pod.t';
open $oldfh, '<', $file or die qq{Could not open "$file": $!\n};
open $newfh, '>', "$file.tmp" or die qq{Could not write to "$file.tmp": $!\n};
$step = 0;
while (<$oldfh>) {
	if (0 == $step) {
		if (/types.c/) {
			print $newfh $_;
			for (sort { $pgtype{$a}{define} cmp $pgtype{$b}{define} } keys %pgtype) {
				print $newfh "\t\t qr{$pgtype{$_}{define}},\n";
			}
			print $newfh "\n";
			$step = 1;
			next;
		}
	}
	elsif (1 == $step) {
		next unless /;/;
		$step = 2;
	}
	print $newfh $_;
}
close $newfh or die qq{Could not close "$file.tmp": $!\n};
close $oldfh or die qq{Could not close "$file": $!\n};
system("mv $file.tmp $file");
print "Wrote $file\n";


## Rewrite types.c
$file = 'types.c';
open $newfh, '>', "$file.tmp" or die qq{Could not write to "$file.tmp": $!\n};

print $newfh qq{$slashstar

   \$Id: types.c 11297 2008-05-20 21:52:34Z turnstep \$

   Copyright (c) 2003-2010 Greg Sabino Mullane and others: see the Changes file
   
   You may distribute under the terms of either the GNU General Public
   License or the Artistic License, as specified in the Perl README file.

$starslash

$slashstar
    Please do not edit the C portions of this file directly. 
    It is automatically generated by the enclosed Perl script.
$starslash

#include "Pg.h"

};

## Read in our DATA information to make things more specific
while (<DATA>) {
	last if /^__END__/;
	next unless /^[a-z]/o;
	chomp;
	my ($name,$q,$dq,$sql,$sqlc,$svtype) = split / +/ => $_;
	if (!exists $pgtype{$name}) {
		die qq{Type ($name) was not found in source file $typefile!\n};
	}
	$pgtype{$name}{quote}   = $q;
	$pgtype{$name}{dequote} = $dq;
	$pgtype{$name}{sql}     = $sql;
	$pgtype{$name}{sqlc}    = $sqlc;
	$pgtype{$name}{svtype}  = $svtype;
	## For arrays, we want to echo the base svtype
	if ($svtype and exists $pgtype{"_$name"} and $pgtype{"_$name"}{textin} =~ /array/) {
		$pgtype{"_$name"}{svtype} = $svtype;
	}
}


## Map all types into a sql_type_info structure

print $newfh "static sql_type_info_t pg_types[] = {\n";

my %pos;
my $item = 0;
for my $name (sort {$a cmp $b } keys %pgtype) {
	my $t = $pgtype{$name};
	my ($sqltype,$quote,$dequote) = (0,0,0);
	(my $sql = $t->{sql}) =~ s{^(\w+).*}{$1};

	printf $newfh qq! {%-*s,%-*s,%d,'%s',%-18s,%-12s,%-14s,\{%s\},%d\},\n!,
		$maxlen, $t->{define}, $maxlen-2, "\"$name\"", 1, $t->{delim}, "\"$t->{textout}\"", $t->{quote}, $t->{dequote}, $sql, $t->{svtype};
	$pos{$name} = $item++;
}

print $newfh "\};\n\n";

print $newfh
"sql_type_info_t* pg_type_data(int sql_type)
{
\tdTHX;

\tswitch(sql_type) {
\n";

for my $name (sort { $a cmp $b } keys %pgtype) {
	printf $newfh qq{\t\tcase %-*s return \&pg_types\[%d\];\n}, 1+$maxlen, "$pgtype{$name}{define}:", $pos{$name};
}

print $newfh "\t\tdefault: return NULL;\n\t\}\n\}\n\n";

print $newfh "static sql_type_info_t sql_types[] = \{\n";

undef %pos;
$item=0;
$maxlen = 1;
for my $name (sort { $a cmp $b } keys %pgtype) {
	next unless $pgtype{$name}{sql};
	for my $sql (split /\|/ => $pgtype{$name}{sql}) {
		## {SQL_VARCHAR, "SQL_VARCHAR", quote_string, dequote_string, {VARCHAROID}, DBDPG_TRUE },
		printf $newfh qq! {%s,"%s",1,',', "none", $pgtype{$name}{quote}, $pgtype{$name}{dequote}, \{$pgtype{$name}{define}\}, $pgtype{$name}{svtype}\},\n!, $sql, $sql;
		$maxlen = length $sql if length $sql > $maxlen;
		$pos{$sql} = $item++;
	}
}
print $newfh "\};\n\n";

print $newfh "sql_type_info_t* sql_type_data(int sql_type)\n\{\n\tdTHX;\n\n";

print $newfh "\tswitch(sql_type) \{\n";
for (sort { $pos{$a} <=> $pos{$b} } keys %pos) {
	printf $newfh qq{\t\tcase %-*s return \&sql_types\[%d\];\n}, 1+$maxlen, "$_:", $pos{$_};
}
print $newfh "\t\tdefault: return NULL;\n\t\}\n\}\n\n/" ."*\n";

seek(DATA,0,0);
1 while <DATA> !~ /!perl/;
print $newfh "#!perl\n";
while (<DATA>) { print $newfh $_; }
close($newfh) or die qq{Could not close "$file.tmp": $!\n};
system("mv $file.tmp $file");
print "Wrote $file\n";

exit;
__DATA__
## Format: for each type, there are 6 items, space separated:
## 1. The given name, from the Postgres source code (pg_type.h)
## 2. The function name we use to do the quoting, or 0 if we do not bind it
## 3. The function name we use for DE-quoting
## 4. The closest SQL_ datatype, or 0 if there is none. May be multiple, separated by |
## 5. Whether this is the one to use for reverse SQL_ type mapping
## 6. What type of SV we can put this in: 1: IV 2: NV

## Simple quoting (e.g. text) - wrap in single quotes, escape backslashes and apostrophes
## This is also the default action for types not specified here
varchar  quote_string  dequote_string  SQL_VARCHAR               1  0
text     quote_string  dequote_string  SQL_LONGVARCHAR           1  0
char     quote_string  dequote_char    SQL_CHAR                  0  0
bpchar   quote_string  dequote_char    SQL_CHAR                  1  0
cid      quote_string  dequote_string  0                         0  0

## Things that get special quoting
int2     quote_int     null_dequote    SQL_SMALLINT|SQL_TINYINT  1  1
int4     quote_int     null_dequote    SQL_INTEGER               1  1
int8     quote_int     null_dequote    SQL_BIGINT                1  0
float4   quote_float   null_dequote    0                         1  2
float8   quote_float   null_dequote    SQL_FLOAT|SQL_DOUBLE|SQL_NUMERIC|SQL_REAL 1  2
numeric  quote_float   null_dequote    SQL_DECIMAL               1  2
oid      quote_int     null_dequote    0                         0  1
name     quote_name    null_dequote    SQL_VARCHAR               0  0 ## XXX Wrong

## Boolean
bool     quote_bool    dequote_bool    SQL_BOOLEAN               1  3

## Geometric types
point    quote_geom    dequote_string  0                         0  0
line     quote_geom    dequote_string  0                         0  0
lseg     quote_geom    dequote_string  0                         0  0
box      quote_geom    dequote_string  0                         0  0
path     quote_path    dequote_string  0                         0  0
polygon  quote_geom    dequote_string  0                         0  0
circle   quote_circle  dequote_string  0                         0  0

## Similar enough to geometric types that we use the same quoting rules
tid      quote_geom    dequote_string  0                         0  0

## Binary - very different quoting rules
bytea    quote_bytea   dequote_bytea   SQL_VARBINARY             1  0

## Time and date
date        quote_string  dequote_string  SQL_TYPE_DATE                     1  0
time        quote_string  dequote_string  SQL_TYPE_TIME                     1  0
timestamp   quote_string  dequote_string  SQL_TIMESTAMP|SQL_TYPE_TIMESTAMP  1  0
timestamptz quote_string  dequote_string  SQL_TYPE_TIMESTAMP_WITH_TIMEZONE|SQL_TYPE_TIME_WITH_TIMEZONE       1  0

__END__

*/
