/*

   Copyright (c) 2003-2024 Greg Sabino Mullane and others: see the Changes file

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
    const char *test_str;

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
    switch (keyword_len) {
    case 2:
      if (word[0] < 'n') {
         if (word[0] < 'd') {
            if (word[0] < 'b') {
               if (word[1] < 't') {
                  test_str = "as";
               } else {
                  test_str = "at";
               }
            } else {
               test_str = "by";
            }
         } else if (word[1] < 'o') {
            if (word[1] < 'n') {
               test_str = "if";
            } else {
               test_str = "in";
            }
         } else if (word[0] < 'i') {
            test_str = "do";
         } else {
            test_str = "is";
         }
      } else if (word[1] < 'o') {
         if (word[1] < 'n') {
            test_str = "of";
         } else {
            test_str = "on";
         }
      } else if (word[0] < 'o') {
         test_str = "no";
      } else if (word[0] < 't') {
         test_str = "or";
      } else {
         test_str = "to";
      }
      break;
    case 3:
      if (word[0] < 'n') {
         if (word[0] < 'c') {
            if (word[1] < 'n') {
               if (word[0] < 'b') {
                  if (word[1] < 'l') {
                     test_str = "add";
                  } else {
                     test_str = "all";
                  }
               } else {
                  test_str = "bit";
               }
            } else if (word[1] < 's') {
               if (word[2] < 'y') {
                  test_str = "and";
               } else {
                  test_str = "any";
               }
            } else {
               test_str = "asc";
            }
         } else if (word[0] < 'e') {
            if (word[0] < 'd') {
               test_str = "csv";
            } else if (word[1] < 'e') {
               test_str = "day";
            } else {
               test_str = "dec";
            }
         } else if (word[0] < 'i') {
            if (word[0] < 'f') {
               test_str = "end";
            } else {
               test_str = "for";
            }
         } else if (word[0] < 'k') {
            test_str = "int";
         } else {
            test_str = "key";
         }
      } else if (word[0] < 'r') {
         if (word[0] < 'o') {
            if (word[2] < 't') {
               if (word[2] < 'd') {
                  test_str = "nfc";
               } else {
                  test_str = "nfd";
               }
            } else if (word[1] < 'o') {
               test_str = "new";
            } else {
               test_str = "not";
            }
         } else if (word[1] < 'l') {
            test_str = "off";
         } else if (word[1] < 'u') {
            test_str = "old";
         } else {
            test_str = "out";
         }
      } else if (word[1] < 'm') {
         if (word[0] < 's') {
            test_str = "ref";
         } else if (word[0] < 'y') {
            test_str = "set";
         } else {
            test_str = "yes";
         }
      } else if (word[0] < 's') {
         test_str = "row";
      } else if (word[0] < 'x') {
         test_str = "sql";
      } else {
         test_str = "xml";
      }
      break;
    case 4:
      if (word[1] < 'n') {
         if (word[2] < 'm') {
            if (word[0] < 'r') {
               if (word[2] < 'k') {
                  if (word[0] < 'l') {
                     if (word[0] < 'e') {
                        test_str = "char";
                     } else {
                        test_str = "each";
                     }
                  } else if (word[0] < 'o') {
                     test_str = "left";
                  } else {
                     test_str = "oids";
                  }
               } else if (word[0] < 'n') {
                  if (word[0] < 'l') {
                     test_str = "call";
                  } else {
                     test_str = "like";
                  }
               } else if (word[3] < 'd') {
                  test_str = "nfkc";
               } else {
                  test_str = "nfkd";
               }
            } else if (word[3] < 'p') {
               if (word[0] < 't') {
                  if (word[3] < 'l') {
                     test_str = "read";
                  } else {
                     test_str = "real";
                  }
               } else if (word[0] < 'w') {
                  test_str = "then";
               } else {
                  test_str = "when";
               }
            } else if (word[0] < 'v') {
               if (word[0] < 't') {
                  test_str = "skip";
               } else {
                  test_str = "ties";
               }
            } else if (word[0] < 'y') {
               test_str = "view";
            } else {
               test_str = "year";
            }
         } else if (word[0] < 'n') {
            if (word[1] < 'e') {
               if (word[0] < 'd') {
                  if (word[3] < 't') {
                     test_str = "case";
                  } else {
                     test_str = "cast";
                  }
               } else if (word[0] < 'l') {
                  test_str = "data";
               } else {
                  test_str = "last";
               }
            } else if (word[0] < 'e') {
               if (word[0] < 'd') {
                  test_str = "also";
               } else {
                  test_str = "desc";
               }
            } else if (word[0] < 'k') {
               test_str = "else";
            } else {
               test_str = "keys";
            }
         } else if (word[0] < 't') {
            if (word[0] < 's') {
               if (word[1] < 'e') {
                  test_str = "name";
               } else {
                  test_str = "next";
               }
            } else if (word[1] < 'h') {
               test_str = "sets";
            } else {
               test_str = "show";
            }
         } else if (word[1] < 'i') {
            if (word[2] < 'x') {
               test_str = "temp";
            } else {
               test_str = "text";
            }
         } else if (word[0] < 'w') {
            test_str = "time";
         } else {
            test_str = "with";
         }
      } else if (word[0] < 'm') {
         if (word[0] < 'h') {
            if (word[0] < 'd') {
               if (word[2] < 's') {
                  if (word[1] < 'u') {
                     test_str = "copy";
                  } else {
                     test_str = "cube";
                  }
               } else if (word[0] < 'c') {
                  test_str = "both";
               } else {
                  test_str = "cost";
               }
            } else if (word[0] < 'f') {
               if (word[0] < 'e') {
                  test_str = "drop";
               } else {
                  test_str = "enum";
               }
            } else if (word[1] < 'u') {
               test_str = "from";
            } else {
               test_str = "full";
            }
         } else if (word[0] < 'j') {
            if (word[0] < 'i') {
               if (word[2] < 'u') {
                  test_str = "hold";
               } else {
                  test_str = "hour";
               }
            } else {
               test_str = "into";
            }
         } else if (word[0] < 'l') {
            if (word[1] < 's') {
               test_str = "join";
            } else {
               test_str = "json";
            }
         } else if (word[2] < 'c') {
            test_str = "load";
         } else {
            test_str = "lock";
         }
      } else if (word[2] < 'm') {
         if (word[0] < 'r') {
            if (word[0] < 'o') {
               if (word[0] < 'n') {
                  test_str = "mode";
               } else {
                  test_str = "null";
               }
            } else if (word[1] < 'v') {
               test_str = "only";
            } else {
               test_str = "over";
            }
         } else if (word[0] < 't') {
            if (word[1] < 'u') {
               test_str = "role";
            } else {
               test_str = "rule";
            }
         } else if (word[0] < 'u') {
            test_str = "trim";
         } else {
            test_str = "user";
         }
      } else if (word[0] < 't') {
         if (word[0] < 'r') {
            if (word[0] < 'n') {
               test_str = "move";
            } else {
               test_str = "none";
            }
         } else if (word[0] < 's') {
            test_str = "rows";
         } else {
            test_str = "some";
         }
      } else if (word[0] < 'w') {
         if (word[1] < 'y') {
            test_str = "true";
         } else {
            test_str = "type";
         }
      } else if (word[0] < 'z') {
         test_str = "work";
      } else {
         test_str = "zone";
      }
      break;
    case 5:
      if (word[0] < 'm') {
         if (word[1] < 'l') {
            if (word[0] < 'f') {
               if (word[0] < 'c') {
                  if (word[1] < 'e') {
                     if (word[1] < 'd') {
                        test_str = "abort";
                     } else {
                        test_str = "admin";
                     }
                  } else if (word[0] < 'b') {
                     test_str = "after";
                  } else {
                     test_str = "begin";
                  }
               } else if (word[1] < 'h') {
                  if (word[0] < 'd') {
                     test_str = "cache";
                  } else {
                     test_str = "depth";
                  }
               } else if (word[2] < 'e') {
                  test_str = "chain";
               } else {
                  test_str = "check";
               }
            } else if (word[2] < 'r') {
               if (word[1] < 'e') {
                  if (word[0] < 'l') {
                     test_str = "false";
                  } else {
                     test_str = "label";
                  }
               } else if (word[1] < 'i') {
                  test_str = "least";
               } else {
                  test_str = "limit";
               }
            } else if (word[0] < 'l') {
               if (word[1] < 'i') {
                  test_str = "fetch";
               } else {
                  test_str = "first";
               }
            } else if (word[1] < 'e') {
               test_str = "large";
            } else {
               test_str = "level";
            }
         } else if (word[0] < 'g') {
            if (word[1] < 'r') {
               if (word[0] < 'f') {
                  if (word[0] < 'c') {
                     test_str = "alter";
                  } else if (word[2] < 'o') {
                     test_str = "class";
                  } else {
                     test_str = "close";
                  }
               } else if (word[1] < 'o') {
                  test_str = "float";
               } else {
                  test_str = "force";
               }
            } else if (word[1] < 'v') {
               if (word[0] < 'c') {
                  test_str = "array";
               } else {
                  test_str = "cross";
               }
            } else if (word[0] < 'e') {
               test_str = "cycle";
            } else {
               test_str = "event";
            }
         } else if (word[2] < 'n') {
            if (word[1] < 'o') {
               if (word[1] < 'n') {
                  test_str = "ilike";
               } else {
                  test_str = "index";
               }
            } else if (word[0] < 'l') {
               test_str = "grant";
            } else {
               test_str = "local";
            }
         } else if (word[4] < 't') {
            if (word[0] < 'i') {
               test_str = "group";
            } else {
               test_str = "inner";
            }
         } else if (word[2] < 'p') {
            test_str = "inout";
         } else {
            test_str = "input";
         }
      } else if (word[4] < 'n') {
         if (word[0] < 't') {
            if (word[0] < 'q') {
               if (word[1] < 'o') {
                  if (word[1] < 'e') {
                     test_str = "match";
                  } else {
                     test_str = "merge";
                  }
               } else if (word[0] < 'o') {
                  test_str = "month";
               } else {
                  test_str = "owned";
               }
            } else if (word[0] < 's') {
               if (word[0] < 'r') {
                  test_str = "quote";
               } else {
                  test_str = "range";
               }
            } else if (word[1] < 'h') {
               test_str = "setof";
            } else if (word[1] < 'y') {
               test_str = "share";
            } else {
               test_str = "sysid";
            }
         } else if (word[1] < 'm') {
            if (word[2] < 'l') {
               if (word[0] < 'w') {
                  test_str = "table";
               } else {
                  test_str = "where";
               }
            } else if (word[3] < 'u') {
               test_str = "valid";
            } else {
               test_str = "value";
            }
         } else if (word[0] < 'w') {
            if (word[1] < 's') {
               test_str = "until";
            } else {
               test_str = "using";
            }
         } else if (word[0] < 'x') {
            test_str = "write";
         } else {
            test_str = "xmlpi";
         }
      } else if (word[0] < 'r') {
         if (word[2] < 'l') {
            if (word[0] < 'p') {
               if (word[0] < 'o') {
                  test_str = "nchar";
               } else {
                  test_str = "order";
               }
            } else if (word[1] < 'r') {
               test_str = "plans";
            } else {
               test_str = "prior";
            }
         } else if (word[0] < 'o') {
            if (word[1] < 'u') {
               test_str = "names";
            } else {
               test_str = "nulls";
            }
         } else if (word[1] < 'w') {
            test_str = "outer";
         } else {
            test_str = "owner";
         }
      } else if (word[0] < 't') {
         if (word[0] < 's') {
            if (word[1] < 'i') {
               test_str = "reset";
            } else {
               test_str = "right";
            }
         } else if (word[2] < 'd') {
            test_str = "start";
         } else if (word[2] < 'r') {
            test_str = "stdin";
         } else {
            test_str = "strip";
         }
      } else if (word[0] < 'u') {
         if (word[1] < 'y') {
            test_str = "treat";
         } else {
            test_str = "types";
         }
      } else if (word[0] < 'v') {
         test_str = "union";
      } else {
         test_str = "views";
      }
      break;
    case 6:
      if (word[1] < 'm') {
         if (word[0] < 'o') {
            if (word[3] < 'i') {
               if (word[0] < 'd') {
                  if (word[1] < 'i') {
                     if (word[1] < 'c') {
                        test_str = "absent";
                     } else {
                        test_str = "access";
                     }
                  } else if (word[0] < 'b') {
                     test_str = "always";
                  } else {
                     test_str = "binary";
                  }
               } else if (word[0] < 'g') {
                  if (word[2] < 't') {
                     test_str = "delete";
                  } else {
                     test_str = "detach";
                  }
               } else if (word[0] < 'h') {
                  test_str = "global";
               } else if (word[0] < 'm') {
                  test_str = "header";
               } else {
                  test_str = "method";
               }
            } else if (word[0] < 'f') {
               if (word[1] < 'e') {
                  if (word[0] < 'c') {
                     test_str = "action";
                  } else {
                     test_str = "called";
                  }
               } else if (word[1] < 'i') {
                  test_str = "before";
               } else {
                  test_str = "bigint";
               }
            } else if (word[0] < 'h') {
               if (word[1] < 'i') {
                  test_str = "family";
               } else {
                  test_str = "filter";
               }
            } else if (word[0] < 'l') {
               test_str = "having";
            } else if (word[0] < 'm') {
               test_str = "listen";
            } else {
               test_str = "minute";
            }
         } else if (word[2] < 'm') {
            if (word[2] < 'f') {
               if (word[0] < 't') {
                  if (word[1] < 'e') {
                     test_str = "scalar";
                  } else if (word[2] < 'c') {
                     test_str = "search";
                  } else {
                     test_str = "second";
                  }
               } else if (word[0] < 'v') {
                  test_str = "tables";
               } else {
                  test_str = "vacuum";
               }
            } else if (word[0] < 's') {
               if (word[1] < 'f') {
                  test_str = "object";
               } else {
                  test_str = "offset";
               }
            } else if (word[0] < 'v') {
               if (word[1] < 'e') {
                  test_str = "schema";
               } else {
                  test_str = "select";
               }
            } else {
               test_str = "values";
            }
         } else if (word[0] < 's') {
            if (word[2] < 't') {
               if (word[0] < 'r') {
                  test_str = "parser";
               } else {
                  test_str = "rename";
               }
            } else if (word[2] < 'v') {
               test_str = "return";
            } else {
               test_str = "revoke";
            }
         } else if (word[0] < 'w') {
            if (word[1] < 'e') {
               test_str = "scroll";
            } else if (word[1] < 'i') {
               test_str = "server";
            } else {
               test_str = "simple";
            }
         } else if (word[2] < 't') {
            test_str = "window";
         } else {
            test_str = "within";
         }
      } else if (word[4] < 'm') {
         if (word[2] < 'o') {
            if (word[0] < 'l') {
               if (word[0] < 'e') {
                  if (word[0] < 'd') {
                     test_str = "commit";
                  } else {
                     test_str = "domain";
                  }
               } else if (word[0] < 'i') {
                  test_str = "enable";
               } else {
                  test_str = "isnull";
               }
            } else if (word[0] < 'n') {
               if (word[2] < 'g') {
                  test_str = "locked";
               } else {
                  test_str = "logged";
               }
            } else if (word[0] < 'p') {
               test_str = "nullif";
            } else if (word[0] < 's') {
               test_str = "policy";
            } else {
               test_str = "stable";
            }
         } else if (word[0] < 'n') {
            if (word[0] < 'd') {
               if (word[2] < 't') {
                  test_str = "atomic";
               } else {
                  test_str = "attach";
               }
            } else if (word[0] < 'f') {
               test_str = "double";
            } else {
               test_str = "format";
            }
         } else if (word[0] < 's') {
            if (word[2] < 'w') {
               test_str = "notify";
            } else {
               test_str = "nowait";
            }
         } else if (word[1] < 'y') {
            if (word[2] < 'r') {
               test_str = "stored";
            } else {
               test_str = "strict";
            }
         } else {
            test_str = "system";
         }
      } else if (word[1] < 'r') {
         if (word[0] < 'o') {
            if (word[2] < 'p') {
               if (word[0] < 'i') {
                  test_str = "column";
               } else if (word[2] < 'l') {
                  test_str = "indent";
               } else {
                  test_str = "inline";
               }
            } else if (word[1] < 'n') {
               test_str = "import";
            } else {
               test_str = "insert";
            }
         } else if (word[0] < 'u') {
            if (word[0] < 'r') {
               test_str = "option";
            } else {
               test_str = "rollup";
            }
         } else if (word[1] < 'p') {
            test_str = "unique";
         } else {
            test_str = "update";
         }
      } else if (word[0] < 'f') {
         if (word[0] < 'e') {
            if (word[1] < 'u') {
               test_str = "create";
            } else {
               test_str = "cursor";
            }
         } else if (word[1] < 'x') {
            test_str = "escape";
         } else if (word[2] < 'i') {
            test_str = "except";
         } else {
            test_str = "exists";
         }
      } else if (word[0] < 'o') {
         if (word[0] < 'g') {
            test_str = "freeze";
         } else {
            test_str = "groups";
         }
      } else if (word[0] < 's') {
         test_str = "others";
      } else {
         test_str = "stdout";
      }
      break;
    case 7:
      if (word[0] < 'n') {
         if (word[2] < 'o') {
            if (word[2] < 'e') {
               if (word[0] < 'e') {
                  if (word[0] < 'd') {
                     if (word[5] < 'z') {
                        test_str = "analyse";
                     } else {
                        test_str = "analyze";
                     }
                  } else if (word[3] < 'l') {
                     test_str = "decimal";
                  } else {
                     test_str = "declare";
                  }
               } else if (word[0] < 'i') {
                  if (word[0] < 'g') {
                     test_str = "exclude";
                  } else {
                     test_str = "granted";
                  }
               } else if (word[0] < 'l') {
                  if (word[2] < 'd') {
                     test_str = "include";
                  } else {
                     test_str = "indexes";
                  }
               } else {
                  test_str = "leading";
               }
            } else if (word[0] < 'd') {
               if (word[2] < 'm') {
                  if (word[0] < 'c') {
                     test_str = "breadth";
                  } else if (word[3] < 'u') {
                     test_str = "collate";
                  } else {
                     test_str = "columns";
                  }
               } else if (word[2] < 'n') {
                  test_str = "comment";
               } else {
                  test_str = "content";
               }
            } else if (word[0] < 'e') {
               if (word[3] < 'i') {
                  test_str = "default";
               } else {
                  test_str = "definer";
               }
            } else if (word[0] < 'h') {
               test_str = "execute";
            } else if (word[0] < 'i') {
               test_str = "handler";
            } else {
               test_str = "inherit";
            }
         } else if (word[0] < 'e') {
            if (word[1] < 'i') {
               if (word[1] < 'e') {
                  if (word[2] < 't') {
                     test_str = "cascade";
                  } else {
                     test_str = "catalog";
                  }
               } else if (word[0] < 'd') {
                  test_str = "between";
               } else {
                  test_str = "depends";
               }
            } else if (word[0] < 'd') {
               if (word[0] < 'c') {
                  test_str = "boolean";
               } else if (word[1] < 'u') {
                  test_str = "cluster";
               } else {
                  test_str = "current";
               }
            } else if (word[3] < 'c') {
               test_str = "disable";
            } else {
               test_str = "discard";
            }
         } else if (word[2] < 't') {
            if (word[0] < 'i') {
               if (word[0] < 'f') {
                  test_str = "explain";
               } else if (word[3] < 'w') {
                  test_str = "foreign";
               } else {
                  test_str = "forward";
               }
            } else if (word[0] < 'm') {
               test_str = "instead";
            } else {
               test_str = "mapping";
            }
         } else if (word[0] < 'l') {
            if (word[0] < 'i') {
               test_str = "extract";
            } else if (word[2] < 'v') {
               test_str = "integer";
            } else {
               test_str = "invoker";
            }
         } else if (word[0] < 'm') {
            test_str = "lateral";
         } else {
            test_str = "matched";
         }
      } else if (word[1] < 'i') {
         if (word[3] < 'r') {
            if (word[0] < 's') {
               if (word[2] < 'l') {
                  if (word[2] < 'i') {
                     test_str = "recheck";
                  } else {
                     test_str = "reindex";
                  }
               } else if (word[2] < 'p') {
                  test_str = "release";
               } else if (word[4] < 'i') {
                  test_str = "replace";
               } else {
                  test_str = "replica";
               }
            } else if (word[0] < 'v') {
               if (word[0] < 'u') {
                  test_str = "schemas";
               } else {
                  test_str = "uescape";
               }
            } else if (word[1] < 'e') {
               test_str = "varchar";
            } else {
               test_str = "verbose";
            }
         } else if (word[1] < 'e') {
            if (word[2] < 's') {
               if (word[0] < 'v') {
                  test_str = "partial";
               } else {
                  test_str = "varying";
               }
            } else if (word[0] < 'p') {
               test_str = "natural";
            } else {
               test_str = "passing";
            }
         } else if (word[0] < 's') {
            if (word[2] < 's') {
               test_str = "refresh";
            } else if (word[2] < 't') {
               test_str = "restart";
            } else {
               test_str = "returns";
            }
         } else if (word[0] < 'v') {
            test_str = "session";
         } else {
            test_str = "version";
         }
      } else if (word[0] < 'r') {
         if (word[0] < 'p') {
            if (word[0] < 'o') {
               if (word[1] < 'u') {
                  if (word[3] < 'n') {
                     test_str = "nothing";
                  } else {
                     test_str = "notnull";
                  }
               } else {
                  test_str = "numeric";
               }
            } else if (word[1] < 'v') {
               test_str = "options";
            } else {
               test_str = "overlay";
            }
         } else if (word[2] < 'i') {
            if (word[1] < 'r') {
               test_str = "placing";
            } else {
               test_str = "prepare";
            }
         } else if (word[2] < 'o') {
            test_str = "primary";
         } else {
            test_str = "program";
         }
      } else if (word[1] < 'r') {
         if (word[0] < 'u') {
            if (word[0] < 's') {
               test_str = "routine";
            } else {
               test_str = "similar";
            }
         } else if (word[0] < 'w') {
            test_str = "unknown";
         } else if (word[0] < 'x') {
            test_str = "without";
         } else {
            test_str = "xmlroot";
         }
      } else if (word[0] < 't') {
         if (word[1] < 'u') {
            test_str = "storage";
         } else {
            test_str = "support";
         }
      } else if (word[0] < 'w') {
         if (word[2] < 'u') {
            test_str = "trigger";
         } else {
            test_str = "trusted";
         }
      } else {
         test_str = "wrapper";
      }
      break;
    case 8:
      if (word[0] < 'n') {
         if (word[1] < 'n') {
            if (word[0] < 'f') {
               if (word[0] < 'd') {
                  if (word[0] < 'b') {
                     test_str = "absolute";
                  } else if (word[0] < 'c') {
                     test_str = "backward";
                  } else {
                     test_str = "cascaded";
                  }
               } else if (word[2] < 's') {
                  if (word[3] < 'e') {
                     test_str = "defaults";
                  } else {
                     test_str = "deferred";
                  }
               } else if (word[1] < 'i') {
                  test_str = "database";
               } else {
                  test_str = "distinct";
               }
            } else if (word[0] < 'l') {
               if (word[0] < 'i') {
                  test_str = "finalize";
               } else if (word[1] < 'm') {
                  test_str = "identity";
               } else {
                  test_str = "implicit";
               }
            } else if (word[0] < 'm') {
               test_str = "language";
            } else if (word[1] < 'i') {
               test_str = "maxvalue";
            } else {
               test_str = "minvalue";
            }
         } else if (word[0] < 'f') {
            if (word[0] < 'd') {
               if (word[2] < 'n') {
                  if (word[2] < 'm') {
                     test_str = "coalesce";
                  } else {
                     test_str = "comments";
                  }
               } else if (word[3] < 't') {
                  test_str = "conflict";
               } else {
                  test_str = "continue";
               }
            } else if (word[0] < 'e') {
               test_str = "document";
            } else if (word[1] < 'x') {
               test_str = "encoding";
            } else {
               test_str = "external";
            }
         } else if (word[0] < 'i') {
            if (word[0] < 'g') {
               test_str = "function";
            } else if (word[2] < 'o') {
               test_str = "greatest";
            } else {
               test_str = "grouping";
            }
         } else if (word[0] < 'l') {
            if (word[2] < 't') {
               test_str = "inherits";
            } else {
               test_str = "interval";
            }
         } else {
            test_str = "location";
         }
      } else if (word[0] < 's') {
         if (word[1] < 'o') {
            if (word[0] < 'r') {
               if (word[0] < 'p') {
                  test_str = "national";
               } else if (word[2] < 's') {
                  test_str = "parallel";
               } else {
                  test_str = "password";
               }
            } else if (word[2] < 'l') {
               test_str = "reassign";
            } else if (word[2] < 's') {
               test_str = "relative";
            } else {
               test_str = "restrict";
            }
         } else if (word[1] < 'p') {
            if (word[0] < 'r') {
               test_str = "position";
            } else if (word[2] < 'u') {
               test_str = "rollback";
            } else {
               test_str = "routines";
            }
         } else if (word[0] < 'p') {
            if (word[1] < 'v') {
               test_str = "operator";
            } else {
               test_str = "overlaps";
            }
         } else if (word[3] < 's') {
            test_str = "prepared";
         } else {
            test_str = "preserve";
         }
      } else if (word[0] < 'u') {
         if (word[0] < 't') {
            if (word[1] < 'm') {
               if (word[2] < 'q') {
                  test_str = "security";
               } else {
                  test_str = "sequence";
               }
            } else if (word[1] < 'n') {
               test_str = "smallint";
            } else {
               test_str = "snapshot";
            }
         } else if (word[1] < 'r') {
            test_str = "template";
         } else if (word[2] < 'u') {
            test_str = "trailing";
         } else {
            test_str = "truncate";
         }
      } else if (word[1] < 'n') {
         if (word[0] < 'x') {
            if (word[2] < 'r') {
               test_str = "validate";
            } else {
               test_str = "variadic";
            }
         } else if (word[3] < 't') {
            test_str = "xmlparse";
         } else {
            test_str = "xmltable";
         }
      } else if (word[0] < 'v') {
         if (word[3] < 'o') {
            test_str = "unlisten";
         } else {
            test_str = "unlogged";
         }
      } else {
         test_str = "volatile";
      }
      break;
    case 9:
      if (word[4] < 'o') {
         if (word[0] < 'l') {
            if (word[0] < 'd') {
               if (word[0] < 'c') {
                  if (word[1] < 't') {
                     test_str = "aggregate";
                  } else {
                     test_str = "attribute";
                  }
               } else if (word[1] < 'o') {
                  test_str = "character";
               } else if (word[2] < 'm') {
                  test_str = "collation";
               } else {
                  test_str = "committed";
               }
            } else if (word[2] < 'm') {
               if (word[0] < 'i') {
                  test_str = "delimiter";
               } else if (word[2] < 'i') {
                  test_str = "increment";
               } else {
                  test_str = "initially";
               }
            } else if (word[0] < 'i') {
               test_str = "extension";
            } else if (word[1] < 's') {
               test_str = "immediate";
            } else {
               test_str = "isolation";
            }
         } else if (word[1] < 'r') {
            if (word[1] < 'e') {
               if (word[0] < 'v') {
                  if (word[3] < 't') {
                     test_str = "parameter";
                  } else {
                     test_str = "partition";
                  }
               } else {
                  test_str = "validator";
               }
            } else if (word[0] < 'n') {
               test_str = "localtime";
            } else if (word[0] < 's') {
               test_str = "normalize";
            } else {
               test_str = "sequences";
            }
         } else if (word[0] < 's') {
            if (word[2] < 'o') {
               if (word[4] < 'i') {
                  test_str = "preceding";
               } else {
                  test_str = "precision";
               }
            } else {
               test_str = "procedure";
            }
         } else if (word[1] < 'y') {
            test_str = "statement";
         } else {
            test_str = "symmetric";
         }
      } else if (word[0] < 'r') {
         if (word[0] < 'g') {
            if (word[1] < 'u') {
               if (word[0] < 'e') {
                  test_str = "assertion";
               } else if (word[0] < 'f') {
                  test_str = "encrypted";
               } else {
                  test_str = "following";
               }
            } else if (word[0] < 'f') {
               if (word[5] < 's') {
                  test_str = "excluding";
               } else {
                  test_str = "exclusive";
               }
            } else {
               test_str = "functions";
            }
         } else if (word[1] < 'm') {
            if (word[0] < 'l') {
               test_str = "generated";
            } else {
               test_str = "leakproof";
            }
         } else if (word[1] < 'n') {
            test_str = "immutable";
         } else if (word[2] < 't') {
            test_str = "including";
         } else {
            test_str = "intersect";
         }
      } else if (word[1] < 'm') {
         if (word[0] < 's') {
            if (word[2] < 't') {
               test_str = "recursive";
            } else {
               test_str = "returning";
            }
         } else if (word[0] < 't') {
            test_str = "savepoint";
         } else if (word[1] < 'i') {
            test_str = "temporary";
         } else {
            test_str = "timestamp";
         }
      } else if (word[0] < 'x') {
         if (word[0] < 't') {
            test_str = "substring";
         } else if (word[0] < 'u') {
            test_str = "transform";
         } else {
            test_str = "unbounded";
         }
      } else if (word[3] < 'e') {
         test_str = "xmlconcat";
      } else if (word[3] < 'f') {
         test_str = "xmlexists";
      } else {
         test_str = "xmlforest";
      }
      break;
    case 10:
      if (word[0] < 'n') {
         if (word[0] < 'd') {
            if (word[0] < 'c') {
               if (word[2] < 's') {
                  test_str = "asensitive";
               } else if (word[2] < 'y') {
                  test_str = "assignment";
               } else {
                  test_str = "asymmetric";
               }
            } else if (word[3] < 's') {
               if (word[1] < 'o') {
                  test_str = "checkpoint";
               } else {
                  test_str = "connection";
               }
            } else if (word[3] < 'v') {
               test_str = "constraint";
            } else {
               test_str = "conversion";
            }
         } else if (word[1] < 'i') {
            if (word[2] < 'f') {
               test_str = "deallocate";
            } else if (word[2] < 'l') {
               test_str = "deferrable";
            } else {
               test_str = "delimiters";
            }
         } else if (word[0] < 'e') {
            test_str = "dictionary";
         } else if (word[0] < 'j') {
            test_str = "expression";
         } else {
            test_str = "json_array";
         }
      } else if (word[0] < 'r') {
         if (word[0] < 'p') {
            if (word[0] < 'o') {
               test_str = "normalized";
            } else if (word[1] < 'v') {
               test_str = "ordinality";
            } else {
               test_str = "overriding";
            }
         } else if (word[2] < 'o') {
            test_str = "privileges";
         } else if (word[8] < 'e') {
            test_str = "procedural";
         } else {
            test_str = "procedures";
         }
      } else if (word[0] < 't') {
         if (word[0] < 's') {
            if (word[2] < 'p') {
               test_str = "references";
            } else {
               test_str = "repeatable";
            }
         } else if (word[3] < 't') {
            test_str = "standalone";
         } else {
            test_str = "statistics";
         }
      } else if (word[0] < 'w') {
         test_str = "tablespace";
      } else if (word[0] < 'x') {
         test_str = "whitespace";
      } else {
         test_str = "xmlelement";
      }
      break;
    case 11:
      if (word[0] < 'r') {
         if (word[0] < 'j') {
            if (word[0] < 'i') {
               if (word[2] < 'n') {
                  test_str = "compression";
               } else {
                  test_str = "constraints";
               }
            } else {
               test_str = "insensitive";
            }
         } else if (word[0] < 'p') {
            if (word[5] < 's') {
               test_str = "json_object";
            } else {
               test_str = "json_scalar";
            }
         } else {
            test_str = "publication";
         }
      } else if (word[2] < 'e') {
         if (word[0] < 'u') {
            if (word[1] < 'r') {
               test_str = "tablesample";
            } else {
               test_str = "transaction";
            }
         } else {
            test_str = "uncommitted";
         }
      } else if (word[0] < 's') {
         test_str = "referencing";
      } else if (word[0] < 'u') {
         test_str = "system_user";
      } else {
         test_str = "unencrypted";
      }
      break;
    case 12:
      if (word[0] < 'm') {
         if (word[8] < 'r') {
            if (word[1] < 'u') {
               test_str = "concurrently";
            } else {
               test_str = "current_date";
            }
         } else if (word[8] < 't') {
            test_str = "current_role";
         } else if (word[8] < 'u') {
            test_str = "current_time";
         } else {
            test_str = "current_user";
         }
      } else if (word[1] < 'm') {
         if (word[0] < 's') {
            test_str = "materialized";
         } else if (word[2] < 's') {
            test_str = "serializable";
         } else {
            test_str = "session_user";
         }
      } else if (word[0] < 'x') {
         test_str = "subscription";
      } else {
         test_str = "xmlserialize";
      }
      break;
    case 13:
      if (word[0] < 'j') {
         if (word[0] < 'c') {
            test_str = "authorization";
         } else {
            test_str = "configuration";
         }
      } else if (word[0] < 'x') {
         test_str = "json_arrayagg";
      } else if (word[3] < 'n') {
         test_str = "xmlattributes";
      } else {
         test_str = "xmlnamespaces";
      }
      break;
    case 14:
      if (word[4] < 'e') {
         if (word[5] < 's') {
            test_str = "json_objectagg";
         } else {
            test_str = "json_serialize";
         }
      } else if (word[0] < 'l') {
         test_str = "current_schema";
      } else {
         test_str = "localtimestamp";
      }
      break;
    case 15:
      if (word[1] < 'u') {
         test_str = "characteristics";
      } else {
         test_str = "current_catalog";
      }
      break;
    case 17:
      test_str = "current_timestamp";
      break;
    default:
        return DBDPG_FALSE;
    }
    if (0 == strcmp(word, test_str))
        return DBDPG_TRUE;

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
      print $fh2 generate_binary_search(\@word);
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

my $testfile= "t/01keywords.t";
open my $fh3, '<', $testfile or die "open($testfile): $!";
my @lines = <$fh3>;
my ($start, $end);
for (0..$#lines) {
  $start = $_ if $lines[$_] =~ /BEGIN GENERATED KEYWORDS/;
  $end = $_ if $lines[$_] =~ /END GENERATED KEYWORDS/;
}
if ($start && $end) {
  splice(@lines, $start+1, $end-$start-1, map "  $_\n", 'qw(', @word, ')');
} else {
  die "Can't find keyword comment markers in $testfile";
}
open my $fh4, '>', "$testfile.tmp" or die "open($testfile.tmp): $!";
print $fh4 @lines;
close $fh4 or die "close: $!";
system("mv $testfile.tmp $testfile");
print "Wrote $testfile\n";

exit;

sub generate_binary_search {
  my $words = shift;
  my $code = "    switch (keyword_len) {\n";
  my %len_map;
  for (@$words) {
    push @{$len_map{length $_}}, $_;
  }
  sub _binary_split {
    my $vals = shift;
    # Stop at length 1
    return qq{test_str = "$vals->[0]";}
      if @$vals == 1;
    # Find a character comparison that splits the list roughly in half.
    my ($best_i, $best_ch, $best_less);
    my $goal = .5 * scalar @$vals;
    for (my $i = 0; $i < length $vals->[0]; ++$i) {
      my %seen;
      for my $ch (grep !$seen{$_}++, map substr($_, $i, 1), @$vals) {
        my @less= grep substr($_, $i, 1) lt $ch, @$vals;
        ($best_i, $best_ch, $best_less) = ($i, $ch, \@less)
          if !defined $best_i || abs($goal - @less) < abs($goal - @$best_less);
      }
    }
    my %less = map +($_ => 1), @$best_less;
    my @less_src = _binary_split($best_less);
    my @ge_src = _binary_split([ grep !$less{$_}, @$vals ]);
    if (@ge_src > 1) {
      # combine "else { if"
      $ge_src[0] = '} else '.$ge_src[0];
    }
    return (
      "if (word[$best_i] < '$best_ch') {",
      (map "   $_", @less_src),
      (@ge_src > 1
        ? @ge_src
        : ( '} else {', (map "   $_", @ge_src), '}' )
      )
    );
  }
  for (sort { $a <=> $b } keys %len_map) {
    my @split_expr = _binary_split($len_map{$_});
    local $" = "\n      ";
    $code .= <<~C;
        case $_:
          @split_expr
          break;
    C
  }
  $code .= <<~C;
      default:
          return DBDPG_FALSE;
      }
      if (0 == strcmp(word, test_str))
          return DBDPG_TRUE;
  C
  return $code;
}

__END__

 */

