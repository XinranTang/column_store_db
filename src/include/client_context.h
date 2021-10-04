#ifndef CLIENT_CONTEXT_H
#define CLIENT_CONTEXT_H

#include "cs165_api.h"

Table* lookup_table(char *name);

Column* lookup_column(Table* table, char *name);

Table* lookup_variables(char *name);

#endif
