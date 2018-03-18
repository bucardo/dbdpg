#include <string.h>
#include "Pg.h"

/*
 * A Simple String library designed to be efficent for concat operations
 * and to grow memory as needed.
 * */

struct string_s {
	size_t length; /* amount of data we have stored in the string, points to null byte */
	size_t memory; /* amount of memory we have allocated */
	char *string;
};

typedef struct string_s string_t;

/* Returns a pointer to the null byte */
static char* string_end(string_t *str)
{
	return &(str->string[ str->length ]);
}

/* Returns the number of bytes free at the end of the string */
static size_t string_free(string_t *str)
{
	return str->memory - str->length;
}

/* Set the length of the string to 'bytes' */
static void string_realloc(string_t *str, size_t bytes)
{
	Renew(str->string, bytes, char);

	str->memory = bytes;
}

/* The number of characters in the string, not incl the null byte */
size_t string_len(const string_t *str)
{
	return str->length;
}

/* return a pointer to the string itself */
char* string_get(string_t *str)
{
	return str->string;
}

string_t* string_create(size_t size)
{
	string_t *str;

	if (size == 0) {
		size = 1;
	}

	New(0, str, 1, string_t);
	New(0, str->string, (sizeof(char) * size) , char);

	str->length = 0;
	str->memory = size;
	str->string[0] = '\0';

	return str;
}

void string_destroy(string_t *str)
{
	Safefree(str->string);
	Safefree(str);
}

void string_append_chr(string_t *str, const char *text)
{
	const size_t textlen = strlen(text);

	if(str->length + textlen + 1 > str->memory){
		string_realloc(str, (str->length + textlen + 1) * 2);
	}

	Copy(text, string_end(str), textlen, char);

	str->length = str->length + textlen;

	str->string[str->length] = '\0';
}

void string_append_byte(string_t *str, char byte)
{
	if(str->length + 1 > str->memory){
		string_realloc(str, (str->memory * 2));
	}

	*(string_end(str)) = byte;
	str->length++;
	str->string[str->length] = '\0';
}

/*
 * sprintf a number in a string, prefixed by a dollar character
 * */
void string_append_dint(string_t *str, int num)
{
	const size_t bytes_free = string_free(str);

	const int written = snprintf(string_end(str), bytes_free, "$%d", num);
	if(written < 0){
		/* This shouldn't ever happen */
		return;
	}

	if((size_t)written >= bytes_free) {
		string_realloc(str, str->memory * 2);
		string_append_dint(str, num);
		return;
	}

	str->length += written;
}
