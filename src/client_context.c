#include "client_context.h"
#include <string.h>
#include <stdio.h>

// the hash function uses djb2 hash function
size_t HASH = 5381;
size_t hash_function(char* name) {
    int c;
    size_t hash = HASH;
    while ((c = *name++))
        hash = ((hash << 5) + hash) + c;
    return hash;
}

GeneralizedColumnHandle* create_column_handle(char* name) {
    GeneralizedColumnHandle* handle = malloc(sizeof(GeneralizedColumnHandle));
    strcpy(handle->name, name);
    handle->next = NULL;
    return handle;
}

void free_column_handle(GeneralizedColumnHandle* handle) {
    // free(handle->generalized_column); // TODO: how to free a generalized column?
    if (handle->generalized_column.column_type == RESULT) {
        free(handle->generalized_column.column_pointer.result->payload);
        free(handle->generalized_column.column_pointer.result);
    }
        // TODO: check how to free column
    else {
        free(handle->generalized_column.column_pointer.column->data);
        free(handle->generalized_column.column_pointer.column);
    }
    free(handle);
}

void allocate(ClientContext** context, size_t size) {
    *context = malloc(sizeof(struct ClientContext));
    ClientContext* context_pointer = *context;
    context_pointer->chandle_slots = size;
    context_pointer->chandles_in_use = 0;
    context_pointer->chandle_table = malloc(size * sizeof(GeneralizedColumnHandle*));
    for (int i = 0; i < context_pointer->chandle_slots; i++) {
        context_pointer->chandle_table[i] = NULL;
    }
}


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
			//printf("Found column name: %s\n",table->columns[i].name);
			return &(table->columns[i]);
		}
	}
	return NULL;
}

// TODO: check initialized slots
GeneralizedColumn* get_context(ClientContext* client_context, char* name) {
    GeneralizedColumnHandle* root = client_context->chandle_table[hash_function(name) % client_context->chandle_slots];
    while (root != NULL) {
        if (strcmp(root->name, name) == 0) {
            return &root->generalized_column;
        }
        root = root->next;
    }
    return NULL;
}


GeneralizedColumn* lookup_variables(char* db_name, char* table_name, char* column_name, char* name, ClientContext* client_context) {
	GeneralizedColumn* generalized_column;

	if (db_name) {
        // if db_name != NULL: find column
        // TODO: check if this malloc has been freed
        generalized_column = malloc(sizeof(GeneralizedColumn));

        if (strcmp(db_name, current_db->name) != 0) {
            free(generalized_column);
            return NULL;
        }
        // TODO: also store column in client context?
        generalized_column->column_type = COLUMN;
        generalized_column->column_pointer.column = lookup_column(lookup_table(table_name), column_name);
		if (!generalized_column->column_pointer.column) {
			free(generalized_column);
			return NULL;
		}
    } else {
        // find specified positions vector in client context
        // TODO: extract find_context function later
        generalized_column = get_context(client_context, name);
    }
	return generalized_column;
}


void add_context(Result* result, ClientContext* client_context, char* name) {
    size_t index = hash_function(name) % client_context->chandle_slots;

    if (client_context->chandle_table[index] == NULL) {
        GeneralizedColumnHandle* handle = create_column_handle(name);
        handle->generalized_column.column_type = RESULT;
        handle->generalized_column.column_pointer.result = result;
        client_context->chandle_table[index] = handle;
        client_context->chandles_in_use++;
    } else {
        GeneralizedColumnHandle* root = client_context->chandle_table[index];
        while (root != NULL) {
            if (strcmp(root->name, name) == 0) {
                // free memory
                if (root->generalized_column.column_type == RESULT) {
                    free(root->generalized_column.column_pointer.result->payload);
                    free(root->generalized_column.column_pointer.result);
                }
                // TODO: check how to free column
                else if (root->generalized_column.column_type == COLUMN) {
                    free(root->generalized_column.column_pointer.column->data);
                    free(root->generalized_column.column_pointer.column);
                }
                break;
            }
            root = root->next;
        }
        if (root == NULL) {
            GeneralizedColumnHandle* handle = create_column_handle(name);
            handle->generalized_column.column_type = RESULT;
            handle->generalized_column.column_pointer.result = result;
            handle->next = client_context->chandle_table[index];
            client_context->chandle_table[index] = handle;
            client_context->chandles_in_use++;
        } else {
            root->generalized_column.column_type = RESULT;
            // insert fetch data into Result*
            root->generalized_column.column_pointer.result = result;
        }
    }

}

// TODO: check
void deallocate(ClientContext* client_context) {
    // printf("execute_deallocate\n");
    for (int i = 0; i < client_context->chandle_slots;i++) {
        GeneralizedColumnHandle* current = client_context->chandle_table[i];
        GeneralizedColumnHandle* prev = current;
        while(current!=NULL) {
            if (current->next!= NULL) {
            }
            current = current->next;
            
            free_column_handle(prev);
            prev = current;
        }
        if (prev != NULL) {
            free(prev);
        }
    }
    free(client_context->chandle_table);
    free(client_context);
}