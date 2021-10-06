#ifndef CLIENT_CONTEXT_H
#define CLIENT_CONTEXT_H

#include "cs165_api.h"

Table* lookup_table(char *name);

Column* lookup_column(Table* table, char *name);

GeneralizedColumn* lookup_variables(char *name, ClientContext* client_context);

void add_context(Result* result, ClientContext* client_context, char* name);

#endif
