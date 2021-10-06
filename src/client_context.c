#include "client_context.h"
/* This is an example of a function you will need to
 * implement in your catalogue. It takes in a string (char *)
 * and outputs a pointer to a table object. Similar methods
 * will be needed for columns and databases. How you choose
 * to implement the method is up to you.
 * 
 */
Table* lookup_table(char *name) {
	for (size_t i = 0; i < current_db->tables_size; i++) {
        if (strcmp(current_db->tables[i].name, name) == 0) {
            return &(current_db->tables[i]);
        }
    }
	return NULL;
}

/**
*  Getting started hint:
* 		What other entities are context related (and contextual with respect to what scope in your design)?
* 		What else will you define in this file?
**/
Column* lookup_column(Table* table, char *name) {
	// void pattern for 'using' a variable to prevent compiler unused variable warning
	for (size_t i = 0; i < table->col_count; i++) {
		if (strcmp(table->columns[i].name, name) == 0) {
			printf("Found column name: %s\n",table->columns[i].name);
			return &(table->columns[i]);
		}
	}
	return NULL;
}

GeneralizedColumn* lookup_variables(char *name, ClientContext* client_context) {
	GeneralizedColumn* generalized_column;
	if (strchr(name, '.')) {
        // TODO: check if this malloc has been freed
        generalized_column = malloc(sizeof(GeneralizedColumn));
        // fully qualified column name
        char* db_name = strtok(name, ".");
        char* table_name = strtok(NULL, ".");
        char* column_name = strtok(NULL, ".");
        if (strcmp(db_name, current_db->name) != 0) {
            free(generalized_column);
            return NULL;
        }
        generalized_column->column_type = COLUMN;
        generalized_column->column_pointer.column = lookup_column(lookup_table(table_name), column_name);
		if (!generalized_column->column_pointer.column) {
			free(generalized_column);
			return NULL;
		}
    } else {
        // find specified positions vector in client context
        // TODO: extract find_context function later
        for (int i = 0; i < client_context->chandles_in_use; i++) {
            if (strcmp(client_context->chandle_table[i].name, name) == 0) {
                generalized_column = &client_context->chandle_table[i].generalized_column;
                break;
            }
        }
    }
	return generalized_column;
}

void add_context(Result* result, ClientContext* client_context, char* name) {
// check if client context is full
    if (client_context->chandles_in_use == client_context->chandle_slots) {
        cs165_log(stdout, "client context has no available slots, expand chandle table.\n");
        client_context->chandle_slots *= 2;
        client_context->chandle_table = realloc(client_context->chandle_table, client_context->chandle_slots * sizeof(GeneralizedColumnHandle));
    }
    // convert positions to generalized column
    GeneralizedColumnHandle* chandle_table = &client_context->chandle_table[client_context->chandles_in_use++];
    strcpy(chandle_table->name, name);
    // convert positions to generalized column
    // set column type to RESULT
    chandle_table->generalized_column.column_type = RESULT;
    // insert fetch data into Result*
    chandle_table->generalized_column.column_pointer.result = result;
}