#ifndef DBDPGTYEPSH
#define DBDPGTYEPSH
#include "pg_typeOID.h"


/* TODO:  Add type_info stuff */
typedef struct sql_type_info {
	int	type_id;	/* 16 */
	char	*type_name;	/* bool */
	bool	bind_ok;	/* 1 */
	char* 	(*quote)();
	void	(*dequote)();	/* 0 if no need to dequote */
	union	{
			int pg;
			int sql;	/* closest SQL/PG_WHATEVER Type */
	} type;
} sql_type_info_t;

sql_type_info_t* pg_type_data(int);
sql_type_info_t* sql_type_data(int);

#endif /*DBDPGTYEPSH */

