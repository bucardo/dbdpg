/*

   $Id$

   Copyright (c) 2003-2004 PostgreSQL Global Development Group
   
   You may distribute under the terms of either the GNU General Public
   License or the Artistic License, as specified in the Perl README file.

*/

#include "Pg.h"
#include "types.h"
#include <assert.h>

/* This section was stolen from libpq */
#ifndef HAVE_PQescapeString
STRLEN
PQescapeString(char *to, const char *from, STRLEN length)
{
	const char *source = from;
	char *target = to;
	unsigned int remaining = length;
	
	while (remaining > 0)
		{
			switch (*source)
				{
				case '\\':
					*target = '\\';
					target++;
					*target = '\\';
					/* target and remaining are updated below. */
					break;
					
				case '\'':
					*target = '\'';
					target++;
					*target = '\'';
					/* target and remaining are updated below. */
					break;
					
				case 0:
					remaining = 0;
					continue; /* exit the while loop */
					
				default:
					*target = *source;
					/* target and remaining are updated below. */
				}
			source++;
			target++;
			remaining--;
		}
	
	/* Write the terminating NUL character. */
	*target = '\0';
	
	return target - to;
}
#endif

/*
 *		PQescapeBytea	- converts from binary string to the
 *		minimal encoding necessary to include the string in an SQL
 *		INSERT statement with a bytea type column as the target.
 *
 *		The following transformations are applied
 *		'\0' == ASCII  0 == \\000
 *		'\'' == ASCII 39 == \'
 *		'\\' == ASCII 92 == \\\\
 *		anything >= 0x80 ---> \\ooo (where ooo is an octal expression)
 */
#ifndef HAVE_PQescapeBytea
unsigned char *
PQescapeBytea(const unsigned char *bintext, STRLEN binlen, STRLEN *bytealen)
{
	const unsigned char *vp;
	unsigned char *rp;
	unsigned char *result;
	STRLEN		i;
	STRLEN		len;
	
	/*
	 * empty string has 1 char ('\0')
	 */
	len = 1;
	
	vp = bintext;
	for (i = binlen; i > 0; i--, vp++)
		{
			if (0 == *vp || *vp >= 0x80)
				len += 5;			/* '5' is for '\\ooo' */
			else if ('\'' == *vp)
				len += 2;
			else if ('\\' == *vp)
				len += 4;
			else
				len++;
		}
	
	New(0, result, len, unsigned char);
	if (NULL == rp)
		return NULL;
	rp = result;
	
	vp = bintext;
	*bytealen = len;
	
	for (i = binlen; i > 0; i--, vp++)
		{
			if (0 == *vp || *vp >= 0x80)
				{
					(void) sprintf(rp, "\\\\%03o", *vp);
					rp += 5;
				}
			else if ('\'' == *vp)
				{
					rp[0] = '\\';
					rp[1] = '\'';
					rp += 2;
				}
			else if ('\\' == *vp)
				{
					rp[0] = '\\';
					rp[1] = '\\';
					rp[2] = '\\';
					rp[3] = '\\';
					rp += 4;
				}
			else
				*rp++ = *vp;
		}
	*rp = '\0';
	
	return result;
}
#endif


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
#ifndef HAVE_PQunescapeBytea
unsigned char *
PQunescapeBytea2(const unsigned char *strtext, STRLEN *retbuflen)
{
	STRLEN strtextlen, buflen, i, j;
	unsigned char *buffer, *tmpbuf;
	int byte;
	
	if (NULL == strtext) {
		return NULL;
	}
	
	strtextlen = strlen(strtext);
	/* will shrink, also we discover if strtext isn't NULL terminated */
	
	New(0, buffer, strtextlen, unsigned char);
	if (NULL == buffer)
		return NULL;
	
	for (i = j = buflen = 0; i < strtextlen;)
		{
			switch (strtext[i])
				{
				case '\\':
					i++;
					if ('\\' == strtext[i])
						buffer[j++] = strtext[i++];
					else
						{
							if ((isDIGIT(strtext[i])) &&
									(isDIGIT(strtext[i+1])) &&
									(isDIGIT(strtext[i+2])))
								{
									byte = VAL(strtext[i++]);
									byte = (byte << 3) + VAL(strtext[i++]);
									buffer[j++] = (byte << 3) + VAL(strtext[i++]);
								}
						}
					break;
					
				default:
					buffer[j++] = strtext[i++];
				}
		}
	buflen = j; /* buflen is the length of the unquoted data */
	Renew(buffer,buflen,char);
	
	if (!tmpbuf)
		{
			Safefree(buffer);
			return 0;
		}
	
	*retbuflen = buflen;
	return tmpbuf;
}







unsigned char *
PQunescapeBytea(unsigned char *strtext, STRLEN *retbuflen)
{
	STRLEN		buflen;
	unsigned char *buffer,
		*sp,
		*bp;
	unsigned int state = 0;
	
	if (NULL == strtext)
		return NULL;
	buflen = strlen(strtext);	/* will shrink, also we discover if
														 * strtext */
	New(0, buffer, buflen, unsigned char);
	if (NULL == buffer)
		return NULL;
	for (bp = buffer, sp = strtext; *sp != '\0'; bp++, sp++)
		{
			switch (state)
				{
				case 0:
					if ('\\' == *sp)
						state = 1;
					*bp = *sp;
					break;
				case 1:
					if ('\'' == *sp)	/* state=5 */
						{				/* replace \' with 39 */
							bp--;
							*bp = '\'';
							buflen--;
							state = 0;
						}
					else if ('\\' == *sp)	/* state=6 */
						{				/* replace \\ with 92 */
							bp--;
							*bp = '\\';
							buflen--;
							state = 0;
						}
					else
						{
							if (isDIGIT(*sp))
								state = 2;
							else
								state = 0;
							*bp = *sp;
						}
					break;
				case 2:
					if (isDIGIT(*sp))
						state = 3;
					else
						state = 0;
					*bp = *sp;
					break;
				case 3:
					if (isDIGIT(*sp))		/* state=4 */
						{
							unsigned int v;
							
							bp -= 3;
							sscanf(sp - 2, "%03o", &v);
							*bp = v;
							buflen -= 3;
							state = 0;
						}
					else
						{
							*bp = *sp;
							state = 0;
						}
					break;
				}
		}
	Renew(buffer,buflen,char);
	if (NULL == buffer)
		return NULL;
	
	*retbuflen = buflen;
	return buffer;
}
#endif



char *
null_quote(string, len, retlen)
	void *string;
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


char *
quote_varchar(string, len, retlen)
		 char *string;
		 STRLEN len;
		 STRLEN *retlen;
{
	STRLEN	outlen;
	char *result;

	New(0, result, len*2+3, char);
	outlen = PQescapeString(result+1, string, len);
	
	/* TODO: remalloc outlen */
	*result = '\'';
	outlen++;
	*(result+outlen)='\'';
	outlen++;
	*(result+outlen)='\0';
	*retlen = outlen;
	return result;
}

char *
quote_char(string, len, retlen)
		 void *string;
		 STRLEN len;
		 STRLEN *retlen;
{
	STRLEN	outlen;
	char *result;
	
	New(0, result, len*2+3, char);
	outlen = PQescapeString(result+1, string, len);
	
	/* TODO: remalloc outlen */
	*result = '\'';
	outlen++;
	*(result+outlen)='\'';
	outlen++;
	*(result+outlen)='\0';
	*retlen = outlen;

	return result;
}




char *
quote_bytea(string, len, retlen)
		 void* string;
		 STRLEN len;
		 STRLEN *retlen;
{
	char *result;
	STRLEN resultant_len = 0;
	unsigned char *intermead, *dest;
	
	intermead = PQescapeBytea(string, len, &resultant_len);
	New(0, result, resultant_len+2, char);
	
	
	dest = result;
	
	Copy("'", dest++,1,char);
	strcpy(dest,intermead);
	strcat(dest,"\'");
	
#ifdef HAVE_PQfreemem
	PQfreemem(intermead);
#else
	Safefree(intermead);
#endif
	*retlen=strlen(result);
	assert(*retlen+1 <= resultant_len+2);
	
	return result;
}

char *
quote_sql_binary( string, len, retlen)
		 void *string;
		 STRLEN	len;
		 STRLEN	*retlen;
{
	char *result;
	char *dest;
	STRLEN max_len = 0, i;
	
	/* We are going to retun a quote_bytea() for backwards compat but
		 we warn first */
	warn("Use of SQL_BINARY invalid in quote()");
	return quote_bytea(string, len, retlen);
	
	/* Ignore the rest of this code until such time that we implement
		 A SQL_BINARY that quotes in the X'' Format */

	/* +4 == 3 for X''; 1 for \0 */
	max_len = len*2+4;
	New(0, result, max_len, char);
	
	
	dest = result;
	Copy("X'",dest++,2,char);
	
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
		 void *value;
		 STRLEN	len;
		 STRLEN	*retlen;
{
	char *result;
	long int int_value;
	STRLEN	max_len=6;
	
	if (isDIGIT(*(char*)value)) {
 		/* For now -- will go away when quote* take SVs */
		int_value = atoi(value);
	} else {
		int_value = 42; /* Not true, not false. Just is */
	}
	New(0, result, max_len, char);
	
	if (0 == int_value)
		strcpy(result,"FALSE");
	else if (1 == int_value)
		strcpy(result,"TRUE");
	else
		croak("Error: Bool must be either 1 or 0");
	
	*retlen = strlen(result);
	assert(*retlen+1 <= max_len);

	return result;
}



char *
quote_integer(value, len, retlen) 
		 void *value;
		 STRLEN	len;
		 STRLEN	*retlen;
{
	char *result;
	STRLEN	max_len=6;
	
	New(0, result, max_len, char);
	
	if (0 == *((int*)value) )
		strcpy(result,"FALSE");
	if (1 == *((int*)value))
		strcpy(result,"TRUE");
	
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
