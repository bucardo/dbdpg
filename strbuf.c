#include <string.h>
#include "Pg.h"
#include "strbuf.h"

/*
 * String buffer that automatically grows as needed
 */

struct strbuf_s {
    size_t length; /* number of characters in string (excluding null byte) */
    size_t memory; /* amount of allocated memory */
    char *string;
};

typedef struct strbuf_s strbuf_t;

/* Returns a pointer to the null byte */
static char* strbuf_end(strbuf_t *str)
{
    return str->string + str->length;
}

/* Reallocate string buffer to at least the given size in bytes */
static void strbuf_realloc(strbuf_t *str, size_t needed)
{
    if (needed == 0)
        needed = 1;

    if (needed <= str->memory)
        return;

    size_t newsize = (needed > SIZE_MAX / 2) ? needed : needed * 2;

    Renew(str->string, newsize, char);
    str->memory = newsize;
}

/* Returns pointer to the string itself */
const char* strbuf_get(const strbuf_t *str)
{
    return str->string;
}

/* Create a new strbuf object */
strbuf_t* strbuf_create(size_t size)
{
    strbuf_t *str;

    if (size == 0)
        size = 1;

    New(0, str, 1, strbuf_t);
    New(0, str->string, size, char);

    str->length = 0;
    str->memory = size;
    str->string[0] = '\0';

    return str;
}

void strbuf_destroy(strbuf_t *str)
{
    if (!str)
        return;

    Safefree(str->string);
    Safefree(str);
}

void strbuf_append_text(strbuf_t *str, const char *text)
{
    if (!text)
        croak("strbuf_append_text: text is NULL");

    const size_t textlen = strlen(text);

    if (textlen > SIZE_MAX - str->length - 1)
        croak("strbuf_append_text: text too large");

    const size_t needed = str->length + textlen + 1;

    if (needed > str->memory)
        strbuf_realloc(str, needed);

    Copy(text, strbuf_end(str), textlen, char);
    str->length += textlen;
    str->string[str->length] = '\0';
}

/* Append a number to a string, prefixed with a dollar sign */
void strbuf_append_dollar_placeholder(strbuf_t *str, int phnum)
{
    const size_t num_buf_size = 32; /* 10 digits + '$' + sign + null + lots of extra */

    if (str->length >= SIZE_MAX - num_buf_size)
        croak("strbuf_append_dollar_placeholder: string too large");

    if (str->memory < str->length + num_buf_size)
        strbuf_realloc(str, str->length + num_buf_size);

    const size_t avail = str->memory - str->length;
    const int written = snprintf(strbuf_end(str), avail, "$%d", phnum);

    if (written < 0 || (size_t)written >= avail)
        croak("strbuf_append_dollar_placeholder: problem writing string");

    str->length += (size_t) written;
    str->string[str->length] = '\0';
}
