
char * null_quote(const char *string, STRLEN len, STRLEN *retlen);
char * quote_string(const char *string, STRLEN len, STRLEN *retlen);
char * quote_bytea(char *string, STRLEN len, STRLEN *retlen);
char * quote_sql_binary(char *string, STRLEN len, STRLEN *retlen);
char * quote_bool(const char *string, STRLEN len, STRLEN *retlen);
char * quote_integer(const char *string, STRLEN len, STRLEN *retlen);
char * quote_geom(const char *string, STRLEN len, STRLEN *retlen);
char * quote_path(const char *string, STRLEN len, STRLEN *retlen);
char * quote_circle(const char *string, STRLEN len, STRLEN *retlen);
void dequote_char(const char *string, STRLEN *retlen);
void dequote_string(const char *string, STRLEN *retlen);
void dequote_bytea(char *string, STRLEN *retlen);
void dequote_sql_binary(char *string, STRLEN *retlen);
void dequote_bool(char *string, STRLEN *retlen);
void null_dequote(const char *string, STRLEN *retlen);
