/*

   Copyright (c) 2003-2020 Greg Sabino Mullane and others: see the Changes file

   You may distribute under the terms of either the GNU General Public
   License or the Artistic License, as specified in the Perl README file.

*/

#include "Pg.h"

#if defined (_WIN32) && !defined (strncasecmp)
int
strncasecmp(const char *s1, const char *s2, size_t n)
{
    while(n > 0
          && toupper((unsigned char)*s1) == toupper((unsigned char)*s2))
    {
        if(*s1 == '\0')
            return 0;
        s1++;
        s2++;
        n--;
    }
    if(n == 0)
        return 0;
    return toupper((unsigned char)*s1) - toupper((unsigned char)*s2);
}
#endif

/*
The 'estring' indicates if the server is capable of using the E'' syntax
In other words, is it 8.1 or better?
It must arrive as 0 or 1
*/

char * null_quote(pTHX_ const char *string, STRLEN len, STRLEN *retlen, int estring)
{
    char *result;

    New(0, result, len+1, char);
    strncpy(result,string,len);
    result[len]='\0';
    *retlen = len;
    return result;
}


char * quote_string(pTHX_ const char *string, STRLEN len, STRLEN *retlen, int estring)
{
    char * result;
    STRLEN oldlen = len;
    const char * const tmp = string;

    (*retlen) = 2;
    while (len > 0 && *string != '\0') {
        if (*string == '\'')
            (*retlen)++;
        else if (*string == '\\') {
            if (estring == 1)
                estring = 2;
            (*retlen)++;
        }
        (*retlen)++;
        string++;
        len--;
    }
    if (estring == 2)
        (*retlen)++;

    string = tmp;
    New(0, result, 1+(*retlen), char);
    if (estring == 2)
        *result++ = 'E';
    *result++ = '\'';

    len = oldlen;
    while (len > 0 && *string != '\0') {
        if (*string == '\'' || *string == '\\') {
                *result++ = *string;
        }
        *result++ = *string++;
        len--;
    }
    *result++ = '\'';
    *result = '\0';
    return result - (*retlen);
}


/* Quote a geometric constant. */
/* Note: we only verify correct characters here, not for 100% valid input */
/* Covers: points, lines, lsegs, boxes, polygons */
char * quote_geom(pTHX_ const char *string, STRLEN len, STRLEN *retlen, int estring)
{
    char * result;
    const char *tmp;

    len = 0; /* stops compiler warnings. Remove entirely someday */
    tmp = string;
    (*retlen) = 2;

    while (*string != '\0') {
        if (*string !=9 && *string != 32 && *string != '(' && *string != ')'
            && *string != '-' && *string != '+' && *string != '.'
            && *string != 'e' && *string != 'E'
            && *string != ',' && (*string < '0' || *string > '9'))
            croak("Invalid input for geometric type");
        (*retlen)++;
        string++;
    }
    string = tmp;
    New(0, result, 1+(*retlen), char);
    *result++ = '\'';

    while (*string != '\0') {
        *result++ = *string++;
    }
    *result++ = '\'';
    *result = '\0';
    return result - (*retlen);
}

/* Same as quote_geom, but also allows square brackets */
char * quote_path(pTHX_ const char *string, STRLEN len, STRLEN *retlen, int estring)
{
    char * result;
    const char * const tmp = string;

    len = 0; /* stops compiler warnings. Remove entirely someday */
    (*retlen) = 2;
    while (*string != '\0') {
        if (*string !=9 && *string != 32 && *string != '(' && *string != ')'
            && *string != '-' && *string != '+' && *string != '.'
            && *string != 'e' && *string != 'E'
            && *string != '[' && *string != ']'
            && *string != ',' && (*string < '0' || *string > '9'))
            croak("Invalid input for path type");
        (*retlen)++;
        string++;
    }
    string = tmp;
    New(0, result, 1+(*retlen), char);
    *result++ = '\'';

    while (*string != '\0') {
        *result++ = *string++;
    }
    *result++ = '\'';
    *result = '\0';
    return result - (*retlen);
}

/* Same as quote_geom, but also allows less than / greater than signs */
char * quote_circle(pTHX_ const char *string, STRLEN len, STRLEN *retlen, int estring)
{
    char * result;
    const char * const tmp = string;

    len = 0; /* stops compiler warnings. Remove entirely someday */
    (*retlen) = 2;

    while (*string != '\0') {
        if (*string !=9 && *string != 32 && *string != '(' && *string != ')'
            && *string != '-' && *string != '+' && *string != '.'
            && *string != 'e' && *string != 'E'
            && *string != '<' && *string != '>'
            && *string != ',' && (*string < '0' || *string > '9'))
            croak("Invalid input for circle type");
        (*retlen)++;
        string++;
    }
    string = tmp;
    New(0, result, 1+(*retlen), char);
    *result++ = '\'';

    while (*string != '\0') {
        *result++ = *string++;
    }
    *result++ = '\'';
    *result = '\0';
    return result - (*retlen);
}


char * quote_bytea(pTHX_ char *string, STRLEN len, STRLEN *retlen, int estring)
{
    char * result;
    STRLEN oldlen = len;

    /* For this one, always use the E'' format if we can */
    result = string;
    (*retlen) = 2;
    while (len > 0) {
        if (*string == '\'') {
            (*retlen) += 2;
        }
        else if (*string == '\\') {
            (*retlen) += 4;
        }
        else if (*string < 0x20 || *string > 0x7e) {
            (*retlen) += 5;
        }
        else {
            (*retlen)++;
        }
        string++;
        len--;
    }
    string = result;
    if (estring)
        (*retlen)++;

    New(0, result, 1+(*retlen), char);
    if (estring)
        *result++ = 'E';
    *result++ = '\'';

    len = oldlen;
    while (len > 0) {
        if (*string == '\'') { /* Single quote becomes double quotes */
            *result++ = *string;
            *result++ = *string++;
        }
        else if (*string == '\\') { /* Backslash becomes 4 backslashes */
            *result++ = *string;
            *result++ = *string++;
            *result++ = '\\';
            *result++ = '\\';
        }
        else if (*string < 0x20 || *string > 0x7e) {
            (void) sprintf((char *)result, "\\\\%03o", (unsigned char)*string++);
            result += 5;
        }
        else {
            *result++ = *string++;
        }
        len--;
    }
    *result++ = '\'';
    *result = '\0';

    return (char *)result - (*retlen);
}

char * quote_sql_binary(pTHX_ char *string, STRLEN len, STRLEN *retlen, int estring)
{
    /* We are going to return a quote_bytea() for backwards compat but
         we warn first */
    warn("Use of SQL_BINARY invalid in quote()");
    return quote_bytea(aTHX_ string, len, retlen, estring);
    
}

/* Return TRUE, FALSE, or throws an error */
char * quote_bool(pTHX_ const char *value, STRLEN len, STRLEN *retlen, int estring)
{
    char *result;
    
    /* Things that are true: t, T, 1, true, TRUE, 0E0, 0 but true */
    if (
        (1 == len && (0 == strncasecmp(value, "t", 1) || '1' == *value))
        ||
        (4 == len && 0 == strncasecmp(value, "true", 4))
        ||
        (3 == len && 0 == strncasecmp(value, "0e0", 3))
        ||
        (10 == len && 0 == strncasecmp(value, "0 but true", 10))
        ) {
        New(0, result, 5, char);
        strncpy(result,"TRUE\0",5);
        *retlen = 4;
        return result;
    }

    /* Things that are false: f, F, 0, false, FALSE, 0, zero-length string */
    if (
        (1 == len && (0 == strncasecmp(value, "f", 1) || '0' == *value))
        ||
        (5 == len && 0 == strncasecmp(value, "false", 5))
        ||
        (0 == len)
        ) {
        New(0, result, 6, char);
        strncpy(result,"FALSE\0",6);
        *retlen = 5;
        return result;
    }

    croak("Invalid boolean value");
    
}

char * quote_int(pTHX_ const char *string, STRLEN len, STRLEN *retlen, int estring)
{
    char * result;

    New(0, result, len+1, char);
    strcpy(result,string);
    *retlen = len;

    while (len > 0 && *string != '\0') {
        len--;
        if (isdigit(*string) || ' ' == *string || '+' == *string || '-' == *string) {
            string++;
            continue;            
        }
        croak("Invalid integer");
    }

    return result;
}

char * quote_float(pTHX_ char *string, STRLEN len, STRLEN *retlen, int estring)
{
    char * result;

    /* Empty string is always an error. Here for dumb compilers. */
    if (len<1)
        croak("Invalid float");

    result = (char*)string;
    *retlen = len;

    /* Allow some standard strings in */
    if (0 != strncasecmp(string, "NaN", 4)
        && 0 != strncasecmp(string, "Infinity", 9)
        && 0 != strncasecmp(string, "-Infinity", 10)) {
        while (len > 0 && *string != '\0') {
            len--;
            if (isdigit(*string)
                || '.' == *string
                || ' ' == *string
                || '+' == *string
                || '-' == *string
                || 'e' == *string
                || 'E' == *string) {
                string++;
                continue;            
            }
            croak("Invalid float");
        }
    }

    string = result;
    New(0, result, 1+(*retlen), char);
    strcpy(result,string);

    return result;
}

char * quote_name(pTHX_ const char *string, STRLEN len, STRLEN *retlen, int estring)
{
    char * result;
    const char *ptr;
    int nquotes = 0;
    int x;
    bool safe;

    /* We throw double quotes around the whole thing, if:
       1. It starts with anything other than [a-z_]
       OR
       2. It has characters other than [a-z_0-9]
       OR
       3. It is a reserved word (e.g. `user`)
    */

    /* 1. It starts with anything other than [a-z_] */
    safe = ((string[0] >= 'a' && string[0] <= 'z') || '_' == string[0]);

    /* 2. It has characters other than [a-z_0-9] (also count number of quotes) */    
    for (ptr = string; *ptr; ptr++) {

        char ch = *ptr;

        if (
            (ch < 'a' || ch > 'z')
            && 
            (ch < '0' || ch > '9')
            &&
            ch != '_') {
            safe = DBDPG_FALSE;
            if (ch == '"')
                nquotes++;
        }
    }

    /* 3. Is it a reserved word (e.g. `user`) */
    if (safe) {
        if (! is_keyword(string)) {
            New(0, result, len+1, char);
            strcpy(result,string);
            *retlen = len;
            return result;
        }
    }

    /* Need room for the string, the outer quotes, any inner quotes (which get doubled) and \0 */
    *retlen = len + 2 + nquotes;
    New(0, result, *retlen + 1, char);

    x=0;
    result[x++] = '"';
    for (ptr = string; *ptr; ptr++) {
        char ch = *ptr;
        result[x++] = ch;
        if (ch == '"')
            result[x++] = '"';
    }
    result[x++] = '"';
    result[x] = '\0';

    return result;
}

void dequote_char(pTHX_ const char *string, STRLEN *retlen, int estring)
{
    /* TODO: chop_blanks if requested */
    *retlen = strlen(string);
}


void dequote_string(pTHX_ const char *string, STRLEN *retlen, int estring)
{
    *retlen = strlen(string);
}



static void _dequote_bytea_escape(char *string, STRLEN *retlen, int estring)
{
    char *result;

    (*retlen) = 0;

    if (NULL != string) {
        result = string;

        while (*string != '\0') {
            (*retlen)++;
            if ('\\' == *string) {
                if ('\\' == *(string+1)) {
                    *result++ = '\\';
                    string +=2;
                }
                else if (
                         (*(string+1) >= '0' && *(string+1) <= '3') &&
                         (*(string+2) >= '0' && *(string+2) <= '7') &&
                         (*(string+3) >= '0' && *(string+3) <= '7'))
                    {
                        *result++ = (*(string+1)-'0')*64 + (*(string+2)-'0')*8 + (*(string+3)-'0');
                        string += 4;
                    }
                else { /* Invalid escape sequence - ignore the backslash */
                    (*retlen)--;
                    string++;
                }
            }
            else {
                *result++ = *string++;
            }
        }
        *result = '\0';
    }
}

static int _decode_hex_digit(char digit)
{
    if (digit >= '0' && digit <= '9')
        return digit - '0';
    if (digit >= 'a' && digit <= 'f')
        return 10 + digit - 'a';
    if (digit >= 'A' && digit <= 'F')
        return 10 + digit - 'A';

    return -1;
}

static void _dequote_bytea_hex(char *string, STRLEN *retlen, int estring)
{
    char *result;

    (*retlen) = 0;

    if (NULL != string) {
        result = string;

        while (*string != '\0') {
            int digit1, digit2;
            digit1 = _decode_hex_digit(*string);
            digit2 = _decode_hex_digit(*(string+1));
            if (digit1 >= 0 && digit2 >= 0) {
                *result++ = 16 * digit1 + digit2;
                (*retlen)++;
            }
            string += 2;
        }
        *result = '\0';
    }
}

void dequote_bytea(pTHX_ char *string, STRLEN *retlen, int estring)
{

    if (NULL != string) {
        if ('\\' == *string && 'x' == *(string+1))
            _dequote_bytea_hex(string, retlen, estring);
        else
            _dequote_bytea_escape(string, retlen, estring);
    }
}

/*
    This one is not used in PG, but since we have a quote_sql_binary,
    it might be nice to let people go the other way too. Say when talking
    to something that uses SQL_BINARY
 */
void dequote_sql_binary(pTHX_ char *string, STRLEN *retlen, int estring)
{

    /* We are going to return a dequote_bytea(), just in case */
    warn("Use of SQL_BINARY invalid in dequote()");
    dequote_bytea(aTHX_ string, retlen, estring);

    /* Put dequote_sql_binary function here at some point */
}



void dequote_bool(pTHX_ char *string, STRLEN *retlen, int estring)
{

    switch(*string){
    case 'f': *string = '0'; break;
    case 't': *string = '1'; break;
    default:
        croak("I do not know how to deal with %c as a bool", *string);
    }
    *retlen = 1;
}


void null_dequote(pTHX_ const char *string, STRLEN *retlen, int estring)
{
    *retlen = strlen(string);

}

bool is_keyword(const char *string)
{

    int max_keyword_length = 17;
    int keyword_len;
    int i;
    char word[64];

    keyword_len = (int)strlen(string);
    if (keyword_len > max_keyword_length || keyword_len > 64) {
        return DBDPG_FALSE;
    }

    /* Because of locale issues, we manually downcase A-Z only */
    for (i = 0; i < keyword_len; i++) {
        char ch = string[i];
        if (ch >= 'A' && ch <= 'Z')
            ch += 'a' - 'A';
        word[i] = ch;
    }
    word[keyword_len] = '\0';

    /* Check for each reserved word */
    if (0==strcmp(word, "abort")) return DBDPG_TRUE;
    if (0==strcmp(word, "absolute")) return DBDPG_TRUE;
    if (0==strcmp(word, "access")) return DBDPG_TRUE;
    if (0==strcmp(word, "action")) return DBDPG_TRUE;
    if (0==strcmp(word, "add")) return DBDPG_TRUE;
    if (0==strcmp(word, "admin")) return DBDPG_TRUE;
    if (0==strcmp(word, "after")) return DBDPG_TRUE;
    if (0==strcmp(word, "aggregate")) return DBDPG_TRUE;
    if (0==strcmp(word, "all")) return DBDPG_TRUE;
    if (0==strcmp(word, "also")) return DBDPG_TRUE;
    if (0==strcmp(word, "alter")) return DBDPG_TRUE;
    if (0==strcmp(word, "always")) return DBDPG_TRUE;
    if (0==strcmp(word, "analyse")) return DBDPG_TRUE;
    if (0==strcmp(word, "analyze")) return DBDPG_TRUE;
    if (0==strcmp(word, "and")) return DBDPG_TRUE;
    if (0==strcmp(word, "any")) return DBDPG_TRUE;
    if (0==strcmp(word, "array")) return DBDPG_TRUE;
    if (0==strcmp(word, "as")) return DBDPG_TRUE;
    if (0==strcmp(word, "asc")) return DBDPG_TRUE;
    if (0==strcmp(word, "assertion")) return DBDPG_TRUE;
    if (0==strcmp(word, "assignment")) return DBDPG_TRUE;
    if (0==strcmp(word, "asymmetric")) return DBDPG_TRUE;
    if (0==strcmp(word, "at")) return DBDPG_TRUE;
    if (0==strcmp(word, "attach")) return DBDPG_TRUE;
    if (0==strcmp(word, "attribute")) return DBDPG_TRUE;
    if (0==strcmp(word, "authorization")) return DBDPG_TRUE;
    if (0==strcmp(word, "backward")) return DBDPG_TRUE;
    if (0==strcmp(word, "before")) return DBDPG_TRUE;
    if (0==strcmp(word, "begin")) return DBDPG_TRUE;
    if (0==strcmp(word, "between")) return DBDPG_TRUE;
    if (0==strcmp(word, "bigint")) return DBDPG_TRUE;
    if (0==strcmp(word, "binary")) return DBDPG_TRUE;
    if (0==strcmp(word, "bit")) return DBDPG_TRUE;
    if (0==strcmp(word, "boolean")) return DBDPG_TRUE;
    if (0==strcmp(word, "both")) return DBDPG_TRUE;
    if (0==strcmp(word, "by")) return DBDPG_TRUE;
    if (0==strcmp(word, "cache")) return DBDPG_TRUE;
    if (0==strcmp(word, "call")) return DBDPG_TRUE;
    if (0==strcmp(word, "called")) return DBDPG_TRUE;
    if (0==strcmp(word, "cascade")) return DBDPG_TRUE;
    if (0==strcmp(word, "cascaded")) return DBDPG_TRUE;
    if (0==strcmp(word, "case")) return DBDPG_TRUE;
    if (0==strcmp(word, "cast")) return DBDPG_TRUE;
    if (0==strcmp(word, "catalog")) return DBDPG_TRUE;
    if (0==strcmp(word, "chain")) return DBDPG_TRUE;
    if (0==strcmp(word, "char")) return DBDPG_TRUE;
    if (0==strcmp(word, "character")) return DBDPG_TRUE;
    if (0==strcmp(word, "characteristics")) return DBDPG_TRUE;
    if (0==strcmp(word, "check")) return DBDPG_TRUE;
    if (0==strcmp(word, "checkpoint")) return DBDPG_TRUE;
    if (0==strcmp(word, "class")) return DBDPG_TRUE;
    if (0==strcmp(word, "close")) return DBDPG_TRUE;
    if (0==strcmp(word, "cluster")) return DBDPG_TRUE;
    if (0==strcmp(word, "coalesce")) return DBDPG_TRUE;
    if (0==strcmp(word, "collate")) return DBDPG_TRUE;
    if (0==strcmp(word, "collation")) return DBDPG_TRUE;
    if (0==strcmp(word, "column")) return DBDPG_TRUE;
    if (0==strcmp(word, "columns")) return DBDPG_TRUE;
    if (0==strcmp(word, "comment")) return DBDPG_TRUE;
    if (0==strcmp(word, "comments")) return DBDPG_TRUE;
    if (0==strcmp(word, "commit")) return DBDPG_TRUE;
    if (0==strcmp(word, "committed")) return DBDPG_TRUE;
    if (0==strcmp(word, "concurrently")) return DBDPG_TRUE;
    if (0==strcmp(word, "configuration")) return DBDPG_TRUE;
    if (0==strcmp(word, "conflict")) return DBDPG_TRUE;
    if (0==strcmp(word, "connection")) return DBDPG_TRUE;
    if (0==strcmp(word, "constraint")) return DBDPG_TRUE;
    if (0==strcmp(word, "constraints")) return DBDPG_TRUE;
    if (0==strcmp(word, "content")) return DBDPG_TRUE;
    if (0==strcmp(word, "continue")) return DBDPG_TRUE;
    if (0==strcmp(word, "conversion")) return DBDPG_TRUE;
    if (0==strcmp(word, "copy")) return DBDPG_TRUE;
    if (0==strcmp(word, "cost")) return DBDPG_TRUE;
    if (0==strcmp(word, "create")) return DBDPG_TRUE;
    if (0==strcmp(word, "cross")) return DBDPG_TRUE;
    if (0==strcmp(word, "csv")) return DBDPG_TRUE;
    if (0==strcmp(word, "cube")) return DBDPG_TRUE;
    if (0==strcmp(word, "current")) return DBDPG_TRUE;
    if (0==strcmp(word, "current_catalog")) return DBDPG_TRUE;
    if (0==strcmp(word, "current_date")) return DBDPG_TRUE;
    if (0==strcmp(word, "current_role")) return DBDPG_TRUE;
    if (0==strcmp(word, "current_schema")) return DBDPG_TRUE;
    if (0==strcmp(word, "current_time")) return DBDPG_TRUE;
    if (0==strcmp(word, "current_timestamp")) return DBDPG_TRUE;
    if (0==strcmp(word, "current_user")) return DBDPG_TRUE;
    if (0==strcmp(word, "cursor")) return DBDPG_TRUE;
    if (0==strcmp(word, "cycle")) return DBDPG_TRUE;
    if (0==strcmp(word, "data")) return DBDPG_TRUE;
    if (0==strcmp(word, "database")) return DBDPG_TRUE;
    if (0==strcmp(word, "day")) return DBDPG_TRUE;
    if (0==strcmp(word, "deallocate")) return DBDPG_TRUE;
    if (0==strcmp(word, "dec")) return DBDPG_TRUE;
    if (0==strcmp(word, "decimal")) return DBDPG_TRUE;
    if (0==strcmp(word, "declare")) return DBDPG_TRUE;
    if (0==strcmp(word, "default")) return DBDPG_TRUE;
    if (0==strcmp(word, "defaults")) return DBDPG_TRUE;
    if (0==strcmp(word, "deferrable")) return DBDPG_TRUE;
    if (0==strcmp(word, "deferred")) return DBDPG_TRUE;
    if (0==strcmp(word, "definer")) return DBDPG_TRUE;
    if (0==strcmp(word, "delete")) return DBDPG_TRUE;
    if (0==strcmp(word, "delimiter")) return DBDPG_TRUE;
    if (0==strcmp(word, "delimiters")) return DBDPG_TRUE;
    if (0==strcmp(word, "depends")) return DBDPG_TRUE;
    if (0==strcmp(word, "desc")) return DBDPG_TRUE;
    if (0==strcmp(word, "detach")) return DBDPG_TRUE;
    if (0==strcmp(word, "dictionary")) return DBDPG_TRUE;
    if (0==strcmp(word, "disable")) return DBDPG_TRUE;
    if (0==strcmp(word, "discard")) return DBDPG_TRUE;
    if (0==strcmp(word, "distinct")) return DBDPG_TRUE;
    if (0==strcmp(word, "do")) return DBDPG_TRUE;
    if (0==strcmp(word, "document")) return DBDPG_TRUE;
    if (0==strcmp(word, "domain")) return DBDPG_TRUE;
    if (0==strcmp(word, "double")) return DBDPG_TRUE;
    if (0==strcmp(word, "drop")) return DBDPG_TRUE;
    if (0==strcmp(word, "each")) return DBDPG_TRUE;
    if (0==strcmp(word, "else")) return DBDPG_TRUE;
    if (0==strcmp(word, "enable")) return DBDPG_TRUE;
    if (0==strcmp(word, "encoding")) return DBDPG_TRUE;
    if (0==strcmp(word, "encrypted")) return DBDPG_TRUE;
    if (0==strcmp(word, "end")) return DBDPG_TRUE;
    if (0==strcmp(word, "enum")) return DBDPG_TRUE;
    if (0==strcmp(word, "escape")) return DBDPG_TRUE;
    if (0==strcmp(word, "event")) return DBDPG_TRUE;
    if (0==strcmp(word, "except")) return DBDPG_TRUE;
    if (0==strcmp(word, "exclude")) return DBDPG_TRUE;
    if (0==strcmp(word, "excluding")) return DBDPG_TRUE;
    if (0==strcmp(word, "exclusive")) return DBDPG_TRUE;
    if (0==strcmp(word, "execute")) return DBDPG_TRUE;
    if (0==strcmp(word, "exists")) return DBDPG_TRUE;
    if (0==strcmp(word, "explain")) return DBDPG_TRUE;
    if (0==strcmp(word, "extension")) return DBDPG_TRUE;
    if (0==strcmp(word, "external")) return DBDPG_TRUE;
    if (0==strcmp(word, "extract")) return DBDPG_TRUE;
    if (0==strcmp(word, "false")) return DBDPG_TRUE;
    if (0==strcmp(word, "family")) return DBDPG_TRUE;
    if (0==strcmp(word, "fetch")) return DBDPG_TRUE;
    if (0==strcmp(word, "filter")) return DBDPG_TRUE;
    if (0==strcmp(word, "first")) return DBDPG_TRUE;
    if (0==strcmp(word, "float")) return DBDPG_TRUE;
    if (0==strcmp(word, "following")) return DBDPG_TRUE;
    if (0==strcmp(word, "for")) return DBDPG_TRUE;
    if (0==strcmp(word, "force")) return DBDPG_TRUE;
    if (0==strcmp(word, "foreign")) return DBDPG_TRUE;
    if (0==strcmp(word, "forward")) return DBDPG_TRUE;
    if (0==strcmp(word, "freeze")) return DBDPG_TRUE;
    if (0==strcmp(word, "from")) return DBDPG_TRUE;
    if (0==strcmp(word, "full")) return DBDPG_TRUE;
    if (0==strcmp(word, "function")) return DBDPG_TRUE;
    if (0==strcmp(word, "functions")) return DBDPG_TRUE;
    if (0==strcmp(word, "generated")) return DBDPG_TRUE;
    if (0==strcmp(word, "global")) return DBDPG_TRUE;
    if (0==strcmp(word, "grant")) return DBDPG_TRUE;
    if (0==strcmp(word, "granted")) return DBDPG_TRUE;
    if (0==strcmp(word, "greatest")) return DBDPG_TRUE;
    if (0==strcmp(word, "group")) return DBDPG_TRUE;
    if (0==strcmp(word, "grouping")) return DBDPG_TRUE;
    if (0==strcmp(word, "groups")) return DBDPG_TRUE;
    if (0==strcmp(word, "handler")) return DBDPG_TRUE;
    if (0==strcmp(word, "having")) return DBDPG_TRUE;
    if (0==strcmp(word, "header")) return DBDPG_TRUE;
    if (0==strcmp(word, "hold")) return DBDPG_TRUE;
    if (0==strcmp(word, "hour")) return DBDPG_TRUE;
    if (0==strcmp(word, "identity")) return DBDPG_TRUE;
    if (0==strcmp(word, "if")) return DBDPG_TRUE;
    if (0==strcmp(word, "ilike")) return DBDPG_TRUE;
    if (0==strcmp(word, "immediate")) return DBDPG_TRUE;
    if (0==strcmp(word, "immutable")) return DBDPG_TRUE;
    if (0==strcmp(word, "implicit")) return DBDPG_TRUE;
    if (0==strcmp(word, "import")) return DBDPG_TRUE;
    if (0==strcmp(word, "in")) return DBDPG_TRUE;
    if (0==strcmp(word, "include")) return DBDPG_TRUE;
    if (0==strcmp(word, "including")) return DBDPG_TRUE;
    if (0==strcmp(word, "increment")) return DBDPG_TRUE;
    if (0==strcmp(word, "index")) return DBDPG_TRUE;
    if (0==strcmp(word, "indexes")) return DBDPG_TRUE;
    if (0==strcmp(word, "inherit")) return DBDPG_TRUE;
    if (0==strcmp(word, "inherits")) return DBDPG_TRUE;
    if (0==strcmp(word, "initially")) return DBDPG_TRUE;
    if (0==strcmp(word, "inline")) return DBDPG_TRUE;
    if (0==strcmp(word, "inner")) return DBDPG_TRUE;
    if (0==strcmp(word, "inout")) return DBDPG_TRUE;
    if (0==strcmp(word, "input")) return DBDPG_TRUE;
    if (0==strcmp(word, "insensitive")) return DBDPG_TRUE;
    if (0==strcmp(word, "insert")) return DBDPG_TRUE;
    if (0==strcmp(word, "instead")) return DBDPG_TRUE;
    if (0==strcmp(word, "int")) return DBDPG_TRUE;
    if (0==strcmp(word, "integer")) return DBDPG_TRUE;
    if (0==strcmp(word, "intersect")) return DBDPG_TRUE;
    if (0==strcmp(word, "interval")) return DBDPG_TRUE;
    if (0==strcmp(word, "into")) return DBDPG_TRUE;
    if (0==strcmp(word, "invoker")) return DBDPG_TRUE;
    if (0==strcmp(word, "is")) return DBDPG_TRUE;
    if (0==strcmp(word, "isnull")) return DBDPG_TRUE;
    if (0==strcmp(word, "isolation")) return DBDPG_TRUE;
    if (0==strcmp(word, "join")) return DBDPG_TRUE;
    if (0==strcmp(word, "key")) return DBDPG_TRUE;
    if (0==strcmp(word, "label")) return DBDPG_TRUE;
    if (0==strcmp(word, "language")) return DBDPG_TRUE;
    if (0==strcmp(word, "large")) return DBDPG_TRUE;
    if (0==strcmp(word, "last")) return DBDPG_TRUE;
    if (0==strcmp(word, "lateral")) return DBDPG_TRUE;
    if (0==strcmp(word, "leading")) return DBDPG_TRUE;
    if (0==strcmp(word, "leakproof")) return DBDPG_TRUE;
    if (0==strcmp(word, "least")) return DBDPG_TRUE;
    if (0==strcmp(word, "left")) return DBDPG_TRUE;
    if (0==strcmp(word, "level")) return DBDPG_TRUE;
    if (0==strcmp(word, "like")) return DBDPG_TRUE;
    if (0==strcmp(word, "limit")) return DBDPG_TRUE;
    if (0==strcmp(word, "listen")) return DBDPG_TRUE;
    if (0==strcmp(word, "load")) return DBDPG_TRUE;
    if (0==strcmp(word, "local")) return DBDPG_TRUE;
    if (0==strcmp(word, "localtime")) return DBDPG_TRUE;
    if (0==strcmp(word, "localtimestamp")) return DBDPG_TRUE;
    if (0==strcmp(word, "location")) return DBDPG_TRUE;
    if (0==strcmp(word, "lock")) return DBDPG_TRUE;
    if (0==strcmp(word, "locked")) return DBDPG_TRUE;
    if (0==strcmp(word, "logged")) return DBDPG_TRUE;
    if (0==strcmp(word, "mapping")) return DBDPG_TRUE;
    if (0==strcmp(word, "match")) return DBDPG_TRUE;
    if (0==strcmp(word, "materialized")) return DBDPG_TRUE;
    if (0==strcmp(word, "maxvalue")) return DBDPG_TRUE;
    if (0==strcmp(word, "method")) return DBDPG_TRUE;
    if (0==strcmp(word, "minute")) return DBDPG_TRUE;
    if (0==strcmp(word, "minvalue")) return DBDPG_TRUE;
    if (0==strcmp(word, "mode")) return DBDPG_TRUE;
    if (0==strcmp(word, "month")) return DBDPG_TRUE;
    if (0==strcmp(word, "move")) return DBDPG_TRUE;
    if (0==strcmp(word, "name")) return DBDPG_TRUE;
    if (0==strcmp(word, "names")) return DBDPG_TRUE;
    if (0==strcmp(word, "national")) return DBDPG_TRUE;
    if (0==strcmp(word, "natural")) return DBDPG_TRUE;
    if (0==strcmp(word, "nchar")) return DBDPG_TRUE;
    if (0==strcmp(word, "new")) return DBDPG_TRUE;
    if (0==strcmp(word, "next")) return DBDPG_TRUE;
    if (0==strcmp(word, "no")) return DBDPG_TRUE;
    if (0==strcmp(word, "none")) return DBDPG_TRUE;
    if (0==strcmp(word, "not")) return DBDPG_TRUE;
    if (0==strcmp(word, "nothing")) return DBDPG_TRUE;
    if (0==strcmp(word, "notify")) return DBDPG_TRUE;
    if (0==strcmp(word, "notnull")) return DBDPG_TRUE;
    if (0==strcmp(word, "nowait")) return DBDPG_TRUE;
    if (0==strcmp(word, "null")) return DBDPG_TRUE;
    if (0==strcmp(word, "nullif")) return DBDPG_TRUE;
    if (0==strcmp(word, "nulls")) return DBDPG_TRUE;
    if (0==strcmp(word, "numeric")) return DBDPG_TRUE;
    if (0==strcmp(word, "object")) return DBDPG_TRUE;
    if (0==strcmp(word, "of")) return DBDPG_TRUE;
    if (0==strcmp(word, "off")) return DBDPG_TRUE;
    if (0==strcmp(word, "offset")) return DBDPG_TRUE;
    if (0==strcmp(word, "oids")) return DBDPG_TRUE;
    if (0==strcmp(word, "old")) return DBDPG_TRUE;
    if (0==strcmp(word, "on")) return DBDPG_TRUE;
    if (0==strcmp(word, "only")) return DBDPG_TRUE;
    if (0==strcmp(word, "operator")) return DBDPG_TRUE;
    if (0==strcmp(word, "option")) return DBDPG_TRUE;
    if (0==strcmp(word, "options")) return DBDPG_TRUE;
    if (0==strcmp(word, "or")) return DBDPG_TRUE;
    if (0==strcmp(word, "order")) return DBDPG_TRUE;
    if (0==strcmp(word, "ordinality")) return DBDPG_TRUE;
    if (0==strcmp(word, "others")) return DBDPG_TRUE;
    if (0==strcmp(word, "out")) return DBDPG_TRUE;
    if (0==strcmp(word, "outer")) return DBDPG_TRUE;
    if (0==strcmp(word, "over")) return DBDPG_TRUE;
    if (0==strcmp(word, "overlaps")) return DBDPG_TRUE;
    if (0==strcmp(word, "overlay")) return DBDPG_TRUE;
    if (0==strcmp(word, "overriding")) return DBDPG_TRUE;
    if (0==strcmp(word, "owned")) return DBDPG_TRUE;
    if (0==strcmp(word, "owner")) return DBDPG_TRUE;
    if (0==strcmp(word, "parallel")) return DBDPG_TRUE;
    if (0==strcmp(word, "parser")) return DBDPG_TRUE;
    if (0==strcmp(word, "partial")) return DBDPG_TRUE;
    if (0==strcmp(word, "partition")) return DBDPG_TRUE;
    if (0==strcmp(word, "passing")) return DBDPG_TRUE;
    if (0==strcmp(word, "password")) return DBDPG_TRUE;
    if (0==strcmp(word, "placing")) return DBDPG_TRUE;
    if (0==strcmp(word, "plans")) return DBDPG_TRUE;
    if (0==strcmp(word, "policy")) return DBDPG_TRUE;
    if (0==strcmp(word, "position")) return DBDPG_TRUE;
    if (0==strcmp(word, "preceding")) return DBDPG_TRUE;
    if (0==strcmp(word, "precision")) return DBDPG_TRUE;
    if (0==strcmp(word, "prepare")) return DBDPG_TRUE;
    if (0==strcmp(word, "prepared")) return DBDPG_TRUE;
    if (0==strcmp(word, "preserve")) return DBDPG_TRUE;
    if (0==strcmp(word, "primary")) return DBDPG_TRUE;
    if (0==strcmp(word, "prior")) return DBDPG_TRUE;
    if (0==strcmp(word, "privileges")) return DBDPG_TRUE;
    if (0==strcmp(word, "procedural")) return DBDPG_TRUE;
    if (0==strcmp(word, "procedure")) return DBDPG_TRUE;
    if (0==strcmp(word, "procedures")) return DBDPG_TRUE;
    if (0==strcmp(word, "program")) return DBDPG_TRUE;
    if (0==strcmp(word, "publication")) return DBDPG_TRUE;
    if (0==strcmp(word, "quote")) return DBDPG_TRUE;
    if (0==strcmp(word, "range")) return DBDPG_TRUE;
    if (0==strcmp(word, "read")) return DBDPG_TRUE;
    if (0==strcmp(word, "real")) return DBDPG_TRUE;
    if (0==strcmp(word, "reassign")) return DBDPG_TRUE;
    if (0==strcmp(word, "recheck")) return DBDPG_TRUE;
    if (0==strcmp(word, "recursive")) return DBDPG_TRUE;
    if (0==strcmp(word, "ref")) return DBDPG_TRUE;
    if (0==strcmp(word, "references")) return DBDPG_TRUE;
    if (0==strcmp(word, "referencing")) return DBDPG_TRUE;
    if (0==strcmp(word, "refresh")) return DBDPG_TRUE;
    if (0==strcmp(word, "reindex")) return DBDPG_TRUE;
    if (0==strcmp(word, "relative")) return DBDPG_TRUE;
    if (0==strcmp(word, "release")) return DBDPG_TRUE;
    if (0==strcmp(word, "rename")) return DBDPG_TRUE;
    if (0==strcmp(word, "repeatable")) return DBDPG_TRUE;
    if (0==strcmp(word, "replace")) return DBDPG_TRUE;
    if (0==strcmp(word, "replica")) return DBDPG_TRUE;
    if (0==strcmp(word, "reset")) return DBDPG_TRUE;
    if (0==strcmp(word, "restart")) return DBDPG_TRUE;
    if (0==strcmp(word, "restrict")) return DBDPG_TRUE;
    if (0==strcmp(word, "returning")) return DBDPG_TRUE;
    if (0==strcmp(word, "returns")) return DBDPG_TRUE;
    if (0==strcmp(word, "revoke")) return DBDPG_TRUE;
    if (0==strcmp(word, "right")) return DBDPG_TRUE;
    if (0==strcmp(word, "role")) return DBDPG_TRUE;
    if (0==strcmp(word, "rollback")) return DBDPG_TRUE;
    if (0==strcmp(word, "rollup")) return DBDPG_TRUE;
    if (0==strcmp(word, "routine")) return DBDPG_TRUE;
    if (0==strcmp(word, "routines")) return DBDPG_TRUE;
    if (0==strcmp(word, "row")) return DBDPG_TRUE;
    if (0==strcmp(word, "rows")) return DBDPG_TRUE;
    if (0==strcmp(word, "rule")) return DBDPG_TRUE;
    if (0==strcmp(word, "savepoint")) return DBDPG_TRUE;
    if (0==strcmp(word, "schema")) return DBDPG_TRUE;
    if (0==strcmp(word, "schemas")) return DBDPG_TRUE;
    if (0==strcmp(word, "scroll")) return DBDPG_TRUE;
    if (0==strcmp(word, "search")) return DBDPG_TRUE;
    if (0==strcmp(word, "second")) return DBDPG_TRUE;
    if (0==strcmp(word, "security")) return DBDPG_TRUE;
    if (0==strcmp(word, "select")) return DBDPG_TRUE;
    if (0==strcmp(word, "sequence")) return DBDPG_TRUE;
    if (0==strcmp(word, "sequences")) return DBDPG_TRUE;
    if (0==strcmp(word, "serializable")) return DBDPG_TRUE;
    if (0==strcmp(word, "server")) return DBDPG_TRUE;
    if (0==strcmp(word, "session")) return DBDPG_TRUE;
    if (0==strcmp(word, "session_user")) return DBDPG_TRUE;
    if (0==strcmp(word, "set")) return DBDPG_TRUE;
    if (0==strcmp(word, "setof")) return DBDPG_TRUE;
    if (0==strcmp(word, "sets")) return DBDPG_TRUE;
    if (0==strcmp(word, "share")) return DBDPG_TRUE;
    if (0==strcmp(word, "show")) return DBDPG_TRUE;
    if (0==strcmp(word, "similar")) return DBDPG_TRUE;
    if (0==strcmp(word, "simple")) return DBDPG_TRUE;
    if (0==strcmp(word, "skip")) return DBDPG_TRUE;
    if (0==strcmp(word, "smallint")) return DBDPG_TRUE;
    if (0==strcmp(word, "snapshot")) return DBDPG_TRUE;
    if (0==strcmp(word, "some")) return DBDPG_TRUE;
    if (0==strcmp(word, "sql")) return DBDPG_TRUE;
    if (0==strcmp(word, "stable")) return DBDPG_TRUE;
    if (0==strcmp(word, "standalone")) return DBDPG_TRUE;
    if (0==strcmp(word, "start")) return DBDPG_TRUE;
    if (0==strcmp(word, "statement")) return DBDPG_TRUE;
    if (0==strcmp(word, "statistics")) return DBDPG_TRUE;
    if (0==strcmp(word, "stdin")) return DBDPG_TRUE;
    if (0==strcmp(word, "stdout")) return DBDPG_TRUE;
    if (0==strcmp(word, "storage")) return DBDPG_TRUE;
    if (0==strcmp(word, "stored")) return DBDPG_TRUE;
    if (0==strcmp(word, "strict")) return DBDPG_TRUE;
    if (0==strcmp(word, "strip")) return DBDPG_TRUE;
    if (0==strcmp(word, "subscription")) return DBDPG_TRUE;
    if (0==strcmp(word, "substring")) return DBDPG_TRUE;
    if (0==strcmp(word, "support")) return DBDPG_TRUE;
    if (0==strcmp(word, "symmetric")) return DBDPG_TRUE;
    if (0==strcmp(word, "sysid")) return DBDPG_TRUE;
    if (0==strcmp(word, "system")) return DBDPG_TRUE;
    if (0==strcmp(word, "table")) return DBDPG_TRUE;
    if (0==strcmp(word, "tables")) return DBDPG_TRUE;
    if (0==strcmp(word, "tablesample")) return DBDPG_TRUE;
    if (0==strcmp(word, "tablespace")) return DBDPG_TRUE;
    if (0==strcmp(word, "temp")) return DBDPG_TRUE;
    if (0==strcmp(word, "template")) return DBDPG_TRUE;
    if (0==strcmp(word, "temporary")) return DBDPG_TRUE;
    if (0==strcmp(word, "text")) return DBDPG_TRUE;
    if (0==strcmp(word, "then")) return DBDPG_TRUE;
    if (0==strcmp(word, "ties")) return DBDPG_TRUE;
    if (0==strcmp(word, "time")) return DBDPG_TRUE;
    if (0==strcmp(word, "timestamp")) return DBDPG_TRUE;
    if (0==strcmp(word, "to")) return DBDPG_TRUE;
    if (0==strcmp(word, "trailing")) return DBDPG_TRUE;
    if (0==strcmp(word, "transaction")) return DBDPG_TRUE;
    if (0==strcmp(word, "transform")) return DBDPG_TRUE;
    if (0==strcmp(word, "treat")) return DBDPG_TRUE;
    if (0==strcmp(word, "trigger")) return DBDPG_TRUE;
    if (0==strcmp(word, "trim")) return DBDPG_TRUE;
    if (0==strcmp(word, "true")) return DBDPG_TRUE;
    if (0==strcmp(word, "truncate")) return DBDPG_TRUE;
    if (0==strcmp(word, "trusted")) return DBDPG_TRUE;
    if (0==strcmp(word, "type")) return DBDPG_TRUE;
    if (0==strcmp(word, "types")) return DBDPG_TRUE;
    if (0==strcmp(word, "unbounded")) return DBDPG_TRUE;
    if (0==strcmp(word, "uncommitted")) return DBDPG_TRUE;
    if (0==strcmp(word, "unencrypted")) return DBDPG_TRUE;
    if (0==strcmp(word, "union")) return DBDPG_TRUE;
    if (0==strcmp(word, "unique")) return DBDPG_TRUE;
    if (0==strcmp(word, "unknown")) return DBDPG_TRUE;
    if (0==strcmp(word, "unlisten")) return DBDPG_TRUE;
    if (0==strcmp(word, "unlogged")) return DBDPG_TRUE;
    if (0==strcmp(word, "until")) return DBDPG_TRUE;
    if (0==strcmp(word, "update")) return DBDPG_TRUE;
    if (0==strcmp(word, "user")) return DBDPG_TRUE;
    if (0==strcmp(word, "using")) return DBDPG_TRUE;
    if (0==strcmp(word, "vacuum")) return DBDPG_TRUE;
    if (0==strcmp(word, "valid")) return DBDPG_TRUE;
    if (0==strcmp(word, "validate")) return DBDPG_TRUE;
    if (0==strcmp(word, "validator")) return DBDPG_TRUE;
    if (0==strcmp(word, "value")) return DBDPG_TRUE;
    if (0==strcmp(word, "values")) return DBDPG_TRUE;
    if (0==strcmp(word, "varchar")) return DBDPG_TRUE;
    if (0==strcmp(word, "variadic")) return DBDPG_TRUE;
    if (0==strcmp(word, "varying")) return DBDPG_TRUE;
    if (0==strcmp(word, "verbose")) return DBDPG_TRUE;
    if (0==strcmp(word, "version")) return DBDPG_TRUE;
    if (0==strcmp(word, "view")) return DBDPG_TRUE;
    if (0==strcmp(word, "views")) return DBDPG_TRUE;
    if (0==strcmp(word, "volatile")) return DBDPG_TRUE;
    if (0==strcmp(word, "when")) return DBDPG_TRUE;
    if (0==strcmp(word, "where")) return DBDPG_TRUE;
    if (0==strcmp(word, "whitespace")) return DBDPG_TRUE;
    if (0==strcmp(word, "window")) return DBDPG_TRUE;
    if (0==strcmp(word, "with")) return DBDPG_TRUE;
    if (0==strcmp(word, "within")) return DBDPG_TRUE;
    if (0==strcmp(word, "without")) return DBDPG_TRUE;
    if (0==strcmp(word, "work")) return DBDPG_TRUE;
    if (0==strcmp(word, "wrapper")) return DBDPG_TRUE;
    if (0==strcmp(word, "write")) return DBDPG_TRUE;
    if (0==strcmp(word, "xml")) return DBDPG_TRUE;
    if (0==strcmp(word, "xmlattributes")) return DBDPG_TRUE;
    if (0==strcmp(word, "xmlconcat")) return DBDPG_TRUE;
    if (0==strcmp(word, "xmlelement")) return DBDPG_TRUE;
    if (0==strcmp(word, "xmlexists")) return DBDPG_TRUE;
    if (0==strcmp(word, "xmlforest")) return DBDPG_TRUE;
    if (0==strcmp(word, "xmlnamespaces")) return DBDPG_TRUE;
    if (0==strcmp(word, "xmlparse")) return DBDPG_TRUE;
    if (0==strcmp(word, "xmlpi")) return DBDPG_TRUE;
    if (0==strcmp(word, "xmlroot")) return DBDPG_TRUE;
    if (0==strcmp(word, "xmlserialize")) return DBDPG_TRUE;
    if (0==strcmp(word, "xmltable")) return DBDPG_TRUE;
    if (0==strcmp(word, "year")) return DBDPG_TRUE;
    if (0==strcmp(word, "yes")) return DBDPG_TRUE;
    if (0==strcmp(word, "zone")) return DBDPG_TRUE;

    /* We made it! */

    return DBDPG_FALSE;

}

/* end of quote.c */

/*
#!perl

## Autogenerate the list of reserved keywords

## You should only run this if you are developing DBD::Pg and 
## understand what this script does

## Usage: perl -x $0 "path-to-pgsql-source"

use strict;
use warnings;

my $arg = shift || die "Usage: $0 path-to-pgsql-source\n";

-d $arg or die qq{Sorry, but "$arg" is not a directory!\n};

my $file = "$arg/src/include/parser/kwlist.h";

open my $fh, '<', $file or die qq{Could not open file "$file": $!\n};
my @word;
my $maxlen = 10;
while (<$fh>) {
  next unless /^PG_KEYWORD\("(.+?)"/;
  ## We don't care what type of word it is - when in doubt, quote it!
  my $word = $1;
  push @word => $word;
  $maxlen = length $word if length $word > $maxlen;
}
close $fh or die qq{Could not close "$file": $!\n};

my $tempfile = 'quote.c.tmp';
open my $fh2, '>', $tempfile or die qq{Could not open "$tempfile": $!\n};
seek(DATA,0,0);
my $gotlist = 0;
while (<DATA>) {
  s/(int max_keyword_length =) \d+/$1 $maxlen/;
  if (!$gotlist) {
    if (/Check for each reserved word/) {
      $gotlist = 1;
      print $fh2 $_;
      for my $word (@word) {
        print $fh2 qq{    if (0==strcmp(word, "$word")) return DBDPG_TRUE;\n};
      }
      print $fh2 "\n";
      next;
    }
  }
  elsif (1==$gotlist) {
    if (/We made it/) {
      $gotlist = 2;
    }
    else {
      next;
    }
  }


  print $fh2 $_;
}

close $fh2 or die qq{Could not close "$tempfile": $!\n};

my $ofile = 'quote.c';
system("mv $tempfile $ofile");
print "Wrote $ofile\n";
exit;

__END__

 */

