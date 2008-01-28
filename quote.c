/*

   $Id$

   Copyright (c) 2003-2008 Greg Sabino Mullane and others: see the Changes file

   You may distribute under the terms of either the GNU General Public
   License or the Artistic License, as specified in the Perl README file.

*/

#include "Pg.h"
#include "types.h"

char * null_quote(const char *string, STRLEN len, STRLEN *retlen)
{
	char *result;
	New(0, result, len+1, char);
	strncpy(result,string,len);
	result[len]='\0';
	*retlen = len;
	return result;
}


char * quote_string(const char *string, STRLEN len, STRLEN *retlen)
{
	char * result;
	STRLEN oldlen = len;

	const char * const tmp = string;

	(*retlen) = 2;
	while (len > 0 && *string != '\0') {
		if (*string == '\'' || *string == '\\') {
			(*retlen)++;
		}
		(*retlen)++;
		string++;
		len--;
	}
	string = tmp;
	New(0, result, 1+(*retlen), char);
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


char * quote_geom(const char *string, STRLEN len, STRLEN *retlen)
{
	char * result;
        const char *tmp;

	len = 0; /* stops compiler warnings. Remove entirely someday */
	tmp = string;
	(*retlen) = 2;
	while (*string != '\0') {
		if (*string !=9 && *string != 32 && *string != '(' && *string != ')'
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

char * quote_path(const char *string, STRLEN len, STRLEN *retlen)
{
	char * result;
	const char * const tmp = string;

	len = 0; /* stops compiler warnings. Remove entirely someday */
	(*retlen) = 2;
	while (*string != '\0') {
		if (*string !=9 && *string != 32 && *string != '(' && *string != ')'
			&& *string != ',' && *string != '[' && *string != ']'
			&& (*string < '0' || *string > '9'))
				croak("Invalid input for geometric path type");
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

char * quote_circle(const char *string, STRLEN len, STRLEN *retlen)
{
	char * result;
	const char * const tmp = string;

	len = 0; /* stops compiler warnings. Remove entirely someday */
	(*retlen) = 2;
	while (*string != '\0') {
		if (*string !=9 && *string != 32 && *string != '(' && *string != ')'
			&& *string != ',' && *string != '<' && *string != '>'
			&& (*string < '0' || *string > '9'))
				croak("Invalid input for geometric circle type");
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


char * quote_bytea(char *string, STRLEN len, STRLEN *retlen)
{
	char * result;
	STRLEN oldlen = len;

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
	New(0, result, 1+(*retlen), char);
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
			(void) snprintf((char *)result, 6, "\\\\%03o", *string++);
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

char * quote_sql_binary(char *string, STRLEN len, STRLEN *retlen)
{
	
	/* We are going to return a quote_bytea() for backwards compat but
		 we warn first */
	warn("Use of SQL_BINARY invalid in quote()");
	return quote_bytea(string, len, retlen);
	
}



char * quote_bool(const char *value, STRLEN len, STRLEN *retlen) 
{
	char *result;
	long int int_value;
	STRLEN	max_len=6;
	
	len = 0;
	if (isDIGIT(*(const char*)value)) {
		/* For now -- will go away when quote* take SVs */
		int_value = atoi(value);
	} else {
		int_value = 42; /* Not true, not false. Just is */
	}
	New(0, result, max_len, char);
	
	if (0 == int_value)
		strncpy(result,"FALSE\0",6);
	else if (1 == int_value)
		strncpy(result,"TRUE\0",5);
	else
		croak("Error: Bool must be either 1 or 0");
	
	*retlen = strlen(result);
	assert(*retlen+1 <= max_len);

	return result;
}



char * quote_integer(const char *value, STRLEN len, STRLEN *retlen) 
{
	char *result;
	STRLEN max_len=6;
        const int intval = *((const int*)value);
	len = 0;

	New(0, result, max_len, char);
	
	if (0 == intval)
		strncpy(result,"FALSE\0",6);
	else if (1 == intval)
		strncpy(result,"TRUE\0",5);
	
	*retlen = strlen(result);
	assert(*retlen+1 <= max_len);

	return result;
}



void dequote_char(const char *string, STRLEN *retlen)
{
	/* TODO: chop_blanks if requested */
	*retlen = strlen(string);
}


void dequote_string(const char *string, STRLEN *retlen)
{
	*retlen = strlen(string);
}



void dequote_bytea(char *string, STRLEN *retlen)
{
	char *result;

	(*retlen) = 0;

	if (NULL == string)
			return;

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
	return;
}



/*
	This one is not used in PG, but since we have a quote_sql_binary,
	it might be nice to let people go the other way too. Say when talking
	to something that uses SQL_BINARY
 */
void dequote_sql_binary(char *string, STRLEN *retlen)
{
	/* We are going to retun a dequote_bytea(), JIC */
	warn("Use of SQL_BINARY invalid in dequote()");
	dequote_bytea(string, retlen);
	return;
	/* Put dequote_sql_binary function here at some point */
}



void dequote_bool(char *string, STRLEN *retlen)
{
	switch(*string){
	case 'f': *string = '0'; break;
	case 't': *string = '1'; break;
	default:
		croak("I do not know how to deal with %c as a bool", *string);
	}
	*retlen = 1;
}



void null_dequote(const char *string, STRLEN *retlen)
{
	*retlen = strlen(string);
}

/* end of quote.c */
