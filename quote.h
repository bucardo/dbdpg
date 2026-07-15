
char * quote_string(pTHX_ const char *string, STRLEN len, STRLEN *new_length, const int supports_supports_estring);
char * quote_geometric(pTHX_ const char *string, STRLEN length, STRLEN *new_length, int supports_estring);
char * quote_bytea(pTHX_ const char *string, STRLEN length, STRLEN *new_length, int supports_estring);
char * quote_bool(pTHX_ const char *string, STRLEN length, STRLEN *new_length, int supports_estring);
char * quote_integer(pTHX_ const char *string, STRLEN length, STRLEN *new_length, int supports_estring);
char * quote_float(pTHX_ const char *string, STRLEN length, STRLEN *new_length, int supports_estring);
char * quote_identifier(pTHX_ const char *string, STRLEN length, STRLEN *new_length, int supports_estring);
void dequote_char(pTHX_ char *string, STRLEN *new_length);
void dequote_string(pTHX_ char *string, STRLEN *new_length);
void dequote_bytea(pTHX_ char *string, STRLEN *new_length);
void dequote_sql_binary(pTHX_ char *string, STRLEN *new_length);
void dequote_bool(pTHX_ char *string, STRLEN *new_length);
void null_dequote(pTHX_ char *string, STRLEN *new_length);
bool is_keyword(const char *string);
