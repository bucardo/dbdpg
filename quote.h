
#ifndef DBDQUOTEH
#define DBDQUOTEH
char * null_quote(char *string, STRLEN len, STRLEN *retlen);
char * quote_string(char *string, STRLEN len, STRLEN *retlen);
char * quote_bytea(unsigned char *string, STRLEN len, STRLEN *retlen);
char * quote_sql_binary(unsigned char *string, STRLEN len, STRLEN *retlen);
char * quote_bool(char *string, STRLEN len, STRLEN *retlen);
char * quote_integer(char *string, STRLEN len, STRLEN *retlen);
char * quote_geom(char *string, STRLEN len, STRLEN *retlen);
char * quote_path(char *string, STRLEN len, STRLEN *retlen);
char * quote_circle(char *string, STRLEN len, STRLEN *retlen);
void dequote_char(char *string, STRLEN *retlen);
void dequote_string(char *string, STRLEN *retlen);
void dequote_bytea(unsigned char *string, STRLEN *retlen);
void dequote_sql_binary(unsigned char *string, STRLEN *retlen);
void dequote_bool(char *string, STRLEN *retlen);
void null_dequote(char *string, STRLEN *retlen);
#endif /*DBDQUOTEH*/
