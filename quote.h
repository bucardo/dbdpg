

#ifdef WIN32
#ifndef snprintf
#define snprintf _snprintf
#endif
#endif

char * null_quote(pTHX_ const char *string, STRLEN len, STRLEN *retlen, int estring);
char * quote_string(pTHX_ const char *string, STRLEN len, STRLEN *retlen, int estring);
char * quote_bytea(pTHX_ char *string, STRLEN len, STRLEN *retlen, int estring);
char * quote_sql_binary(pTHX_ char *string, STRLEN len, STRLEN *retlen, int estring);
char * quote_bool(pTHX_ const char *string, STRLEN len, STRLEN *retlen, int estring);
char * quote_integer(pTHX_ const char *string, STRLEN len, STRLEN *retlen, int estring);
char * quote_int(pTHX_ const char *string, STRLEN len, STRLEN *retlen, int estring);
char * quote_float(pTHX_ char *string, STRLEN len, STRLEN *retlen, int estring);
char * quote_name(pTHX_ const char *string, STRLEN len, STRLEN *retlen, int estring);
char * quote_geom(pTHX_ const char *string, STRLEN len, STRLEN *retlen, int estring);
char * quote_path(pTHX_ const char *string, STRLEN len, STRLEN *retlen, int estring);
char * quote_circle(pTHX_ const char *string, STRLEN len, STRLEN *retlen, int estring);
void dequote_char(pTHX_ const char *string, STRLEN *retlen, int estring);
void dequote_string(pTHX_ const char *string, STRLEN *retlen, int estring);
void dequote_bytea(pTHX_ char *string, STRLEN *retlen, int estring);
void dequote_sql_binary(pTHX_ char *string, STRLEN *retlen, int estring);
void dequote_bool(pTHX_ char *string, STRLEN *retlen, int estring);
void null_dequote(pTHX_ const char *string, STRLEN *retlen, int estring);
bool is_keyword(const char *string);
