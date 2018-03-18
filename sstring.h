#ifndef SSTRING_H
#define SSTRING_H

typedef void string_t;

/* The length of the string */
size_t string_len(const string_t *str);

string_t* string_create(size_t characters);
void string_destroy(string_t *string);

/* Returns a pointer to the internal char*.  Do not free */
char* string_get(string_t *str);

void string_append_chr(string_t *str, const char *text);
void string_append_dint(string_t *str, int num);
void string_append_byte(string_t *str, char byte);

#endif
