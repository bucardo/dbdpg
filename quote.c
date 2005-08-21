/*

   $Id$

   Copyright (c) 2003-2005 PostgreSQL Global Development Group
   
   You may distribute under the terms of either the GNU General Public
   License or the Artistic License, as specified in the Perl README file.

*/

#include "Pg.h"
#include "types.h"



#define VAL(CH) ((CH) - '0')

/*
 *		PQunescapeBytea - converts the null terminated string representation
 *		of a bytea, strtext, into binary, filling a buffer. It returns a
 *		pointer to the buffer which is NULL on error, and the size of the
 *		buffer in retbuflen. The pointer may subsequently be used as an
 *		argument to the function free(3). It is the reverse of PQescapeBytea.
 *
 *		The following transformations are made:
 *		\'   == ASCII 39 == '
 *		\\   == ASCII 92 == \
 *		\ooo == a byte whose value = ooo (ooo is an octal number)
 *		\x   == x (x is any character not matched by the above transformations)
 *
 */
#if PGLIBVERSION < 70300

#define ISFIRSTOCTDIGIT(CH) ((CH) >= '0' && (CH) <= '3')
#define ISOCTDIGIT(CH) ((CH) >= '0' && (CH) <= '7')
#define OCTVAL(CH) ((CH) - '0')

unsigned char * PQunescapeBytea(const unsigned char *strtext, STRLEN *retbuflen);
unsigned char * PQunescapeBytea(const unsigned char *strtext, STRLEN *retbuflen)
{
	STRLEN strtextlen, buflen;
	unsigned char *buffer;
	unsigned int i,j;
	
	if (NULL == strtext)
		return NULL;

	strtextlen = strlen(strtext);

	/*
	 * Length of input is max length of output, but add one to avoid
	 * unportable malloc(0) if input is zero-length.
	 */
	New(0, buffer, strtextlen+1, unsigned char);
	if (NULL == buffer)
		return NULL;

	for (i = j = 0; i < strtextlen;) {
		switch (strtext[i]) {
		case '\\':
			i++;
			if (strtext[i] == '\\')
				buffer[j++] = strtext[i++];
			else {
				if ((ISFIRSTOCTDIGIT(strtext[i])) &&
						(ISOCTDIGIT(strtext[i + 1])) &&
						(ISOCTDIGIT(strtext[i + 2]))) {
					int			byte;

					byte = OCTVAL(strtext[i++]);
					byte = (byte << 3) + OCTVAL(strtext[i++]);
					byte = (byte << 3) + OCTVAL(strtext[i++]);
					buffer[j++] = byte;
				}
			}

			/*
			 * Note: if we see '\' followed by something that isn't a
			 * recognized escape sequence, we loop around having done
			 * nothing except advance i.  Therefore the something will
			 * be emitted as ordinary data on the next cycle. Corner
			 * case: '\' at end of string will just be discarded.
			 */
			break;

		default:
			buffer[j++] = strtext[i++];
			break;
		}
	}
	buflen = j;	/* buflen is the length of the dequoted data */

	/* Shrink the buffer to be no larger than necessary */
	/* +1 avoids unportable behavior when buflen==0 */
	Renew(buffer,buflen+1,unsigned char);

	if (NULL == buffer) {
		Safefree(buffer);
		return NULL;
	}
	
	*retbuflen = buflen;
	return buffer;
}
#endif


char * null_quote(string, len, retlen)
	char *string;
	STRLEN len;
	STRLEN *retlen;
{
	char *result;
	New(0, result, len+1, char);
	strncpy(result,string,len);
	result[len]='\0';
	*retlen = len;
	return result;
}


char * quote_string(string, len, retlen)
		 unsigned char * string;
		 STRLEN len;
		 STRLEN * retlen;
{
	char * result;
	STRLEN oldlen = len;

	result = string;
	(*retlen) = 2;
	while (len > 0 && *string != '\0') {
			if (*string == '\'' || *string == '\\') {
				(*retlen)++;
			}
			(*retlen)++;
			*string++;
			len--;
	}
	string = result;
	New(0, result, (*retlen), char);
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

char * quote_bytea(string, len, retlen)
		 unsigned char * string;
		 STRLEN len;
		 STRLEN * retlen;
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
		*string++;
		len--;
	}
	string = result;
	New(0, result, (*retlen), char);
	*result++ = '\'';
	len = oldlen;
	while (len > 0 && *string != '\0') {
		if (*string == '\'') {
			*result++ = *string;
			*result++ = *string++;
		}
		else if (*string == '\\') {
			*result++ = *string;
			*result++ = *string++;
			*result++ = '\\';
			*result++ = '\\';
		}
		else if (*string < 0x20 || *string > 0x7e) {
			(void) sprintf(result, "\\\\%03o", *string++);
			result += 5;
		}
		else {
			*result++ = *string++;
		}
		len--;
	}
	*result++ = '\'';
	*result = '\0';
	return result - (*retlen);
}

char *
quote_sql_binary( string, len, retlen)
		 unsigned char *string;
		 STRLEN	len;
		 STRLEN	*retlen;
{
	char *result;
	char *dest;
	STRLEN max_len = 0, i;
	
	/* We are going to return a quote_bytea() for backwards compat but
		 we warn first */
	warn("Use of SQL_BINARY invalid in quote()");
	return quote_bytea(string, len, retlen);
	
	/* Ignore the rest of this code until such time that we implement
		 A SQL_BINARY that quotes in the X'' Format */

	/* +4 == 3 for X''; 1 for \0 */
	max_len = len*2+4;
	New(0, result, max_len, char);
	
	
	dest = result;
	Copy((char)"X\'",dest++,2,char);
	
	for (i=0 ; i <= len ; ++i, dest+=2) {
		sprintf(dest, "%X", *(i+(char*)string));
	}
	
	strcat(dest, "\'");
	
	*retlen = strlen(result);
	assert(*retlen+1 <= max_len);

	return result;
}



char *
quote_bool(value, len, retlen) 
		 char *value;
		 STRLEN	len;
		 STRLEN	*retlen;
{
	char *result;
	long int int_value;
	STRLEN	max_len=6;
	
	len = 0;
	if (isDIGIT(*(char*)value)) {
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



char *
quote_integer(value, len, retlen) 
		 char *value;
		 STRLEN	len;
		 STRLEN	*retlen;
{
	char *result;
	STRLEN max_len=6;
	len = 0;

	New(0, result, max_len, char);
	
	if (0 == *((int*)value) )
		strncpy(result,"FALSE\0",6);
	if (1 == *((int*)value))
		strncpy(result,"TRUE\0",5);
	
	*retlen = strlen(result);
	assert(*retlen+1 <= max_len);

	return result;
}



void
dequote_char(string, retlen)
		 char *string;
		 STRLEN *retlen;
{
	/* TODO: chop_blanks if requested */
	*retlen = strlen(string);
}


void
dequote_varchar (string, retlen)
		 char *string;
		 STRLEN *retlen;
{
	*retlen = strlen(string);
}



void
dequote_bytea(string, retlen)
		 char *string;
		 STRLEN *retlen;
{
	char *s, *p;
	int c1,c2,c3;
	/* Stolen right from dbdquote. This probably should be cleaned up
		 & made more robust. Maybe later...
	*/
	s = string;
	p = string;
	while (*s) {
		if ('\\' == *s) {
			if ('\\' == *(s+1)) { /* double backslash */
				*p++ = '\\';
				s += 2;
				continue;
			} else if ( isDIGIT(c1=(*(s+1))) &&
									isDIGIT(c2=(*(s+2))) &&
									isDIGIT(c3=(*(s+3))) ) 
				{
					*p++ = (c1-'0') * 64 + (c2-'0') * 8 + (c3-'0');
					s += 4;
					continue;
				}
		}
		*p++ = *s++;
	}
	*retlen = (p-string);
}



/*
	This one is not used in PG, but since we have a quote_sql_binary,
	it might be nice to let people go the other way too. Say when talking
	to something that uses SQL_BINARY
 */
void
dequote_sql_binary (string, retlen)
		 char *string;
		 STRLEN *retlen;
{
	/* We are going to retun a dequote_bytea(), JIC */
	warn("Use of SQL_BINARY invalid in dequote()");
	dequote_bytea(string, retlen);
	return;
	/* Put dequote_sql_binary function here at some point */
}



void
dequote_bool (string, retlen)
		 char *string;
		 STRLEN *retlen;
{
	switch(*string){
	case 'f': *string = '0'; break;
	case 't': *string = '1'; break;
	default:
		croak("I do not know how to deal with %c as a bool", *string);
	}
	*retlen = 1;
}



void
null_dequote (string, retlen)
		 char *string;
		 STRLEN *retlen;
{
	*retlen = strlen(string);
}

/* end of quote.c */
