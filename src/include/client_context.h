#ifndef CLIENT_CONTEXT_H
#define CLIENT_CONTEXT_H

#include "cs165_api.h"
#include "utils.h"
#include <string.h>

Table* lookup_table(char *name);

Column* lookup_column(Table* table, char *name);

GeneralizedColumn* lookup_variables(char* db_name, char* table_name, char* column_name, char* name, ClientContext* client_context);

void add_context(Result* result, ClientContext* client_context, char* name);

#endif
