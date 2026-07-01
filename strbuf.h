#ifndef STRBUF_H
#define STRBUF_H

#include <stddef.h>

typedef struct strbuf_s strbuf_t;

strbuf_t* strbuf_create(size_t characters);
void strbuf_destroy(strbuf_t *string);

/* Returns a pointer to the internal char*.  Do not free */
const char* strbuf_get(const strbuf_t *str);

void strbuf_append_text(strbuf_t *str, const char *text);
void strbuf_append_dollar_placeholder(strbuf_t *str, int num);

#endif
