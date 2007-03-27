#ifndef DBDPGTYEPSH
#define DBDPGTYEPSH

typedef struct sql_type_info {
	int	type_id;
	char	*type_name;
	char* 	(*quote)();
	void	(*dequote)();
	union	{
			int pg;
			int sql;
	} type;
	bool	bind_ok;
} sql_type_info_t;

sql_type_info_t* pg_type_data(int);
sql_type_info_t* sql_type_data(int);

#define               ABSTIMEOID  702
#define               ACLITEMOID  1033
#define              ANYARRAYOID  2277
#define            ANYELEMENTOID  2283
#define                   ANYOID  2276
#define                   BITOID  1560
#define                  BOOLOID  16
#define                   BOXOID  603
#define                BPCHAROID  1042
#define                 BYTEAOID  17
#define                  CASHOID  790
#define                  CHAROID  18
#define                   CIDOID  29
#define                  CIDROID  650
#define                CIRCLEOID  718
#define               CSTRINGOID  2275
#define                  DATEOID  1082
#define           FLOAT4ARRAYOID  1021
#define                FLOAT4OID  700
#define                FLOAT8OID  701
#define                  INETOID  869
#define                  INT2OID  21
#define            INT2VECTOROID  22
#define             INT4ARRAYOID  1007
#define                  INT4OID  23
#define                  INT8OID  20
#define              INTERNALOID  2281
#define              INTERVALOID  1186
#define      LANGUAGE_HANDLEROID  2280
#define                  LINEOID  628
#define                  LSEGOID  601
#define               MACADDROID  829
#define                  NAMEOID  19
#define               NUMERICOID  1700
#define                   OIDOID  26
#define             OIDVECTOROID  30
#define                OPAQUEOID  2282
#define                  PATHOID  602
#define PG_ATTRIBUTE_RELTYPE_OID  75
#define     PG_CLASS_RELTYPE_OID  83
#define      PG_PROC_RELTYPE_OID  81
#define      PG_TYPE_RELTYPE_OID  71
#define                 POINTOID  600
#define               POLYGONOID  604
#define                RECORDOID  2249
#define             REFCURSOROID  1790
#define              REGCLASSOID  2205
#define           REGOPERATOROID  2204
#define               REGOPEROID  2203
#define          REGPROCEDUREOID  2202
#define               REGPROCOID  24
#define          REGTYPEARRAYOID  2211
#define               REGTYPEOID  2206
#define               RELTIMEOID  703
#define                  TEXTOID  25
#define                   TIDOID  27
#define                  TIMEOID  1083
#define             TIMESTAMPOID  1114
#define           TIMESTAMPTZOID  1184
#define                TIMETZOID  1266
#define             TINTERVALOID  704
#define               TRIGGEROID  2279
#define               UNKNOWNOID  705
#define                VARBITOID  1562
#define               VARCHAROID  1043
#define                  VOIDOID  2278
#define                   XIDOID  28
#define                   XMLOID  142

#endif
