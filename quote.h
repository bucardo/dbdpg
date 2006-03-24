
#ifndef DBDQUOTEH
#define DBDQUOTEH
char * null_quote(char *string, STRLEN len, STRLEN *retlen);
char * quote_string(char *string, STRLEN len, STRLEN *retlen);
char * quote_bytea(unsigned char *string, STRLEN len, STRLEN *retlen);
char * quote_sql_binary(char *string, STRLEN len, STRLEN *retlen);
char * quote_bool(char *string, STRLEN len, STRLEN *retlen);
char * quote_integer(char *string, STRLEN len, STRLEN *retlen);
void dequote_char(char *string, STRLEN *retlen);
void dequote_string(char *string, STRLEN *retlen);
void dequote_bytea(char *string, STRLEN *retlen);
void dequote_sql_binary(char *string, STRLEN *retlen);
void dequote_bool(char *string, STRLEN *retlen);
void null_dequote(char *string, STRLEN *retlen);
#endif /*DBDQUOTEH*/
