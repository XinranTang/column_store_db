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

void* lookup_variables(char *name) {
	// void pattern for 'using' a variable to prevent compiler unused variable warning
	(void) name;

	return NULL;
}

void add_context() {

}