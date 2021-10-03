#include <string.h>
#include "cs165_api.h"
#include "message.h"
#include "utils.h"

#define DB_MAX_TABLE_CAPACITY 1
// In this class, there will always be only one active database at a time
Db *current_db;


/* 
 * create a table object. The Status object can be used to return
 * to the caller that there was an error in table creation
 */
Table* create_table(Db* db, const char* name, size_t num_columns, Status *ret_status) {
	// printf("123456");
	// Check if current db has table_size == table_capacity
	if(current_db->tables_size == current_db->tables_capacity){
				// expand capacity and reallocate memory
		current_db->tables_capacity *= 2;
		current_db->tables = realloc(current_db->tables, current_db->tables_capacity * sizeof(Table));
		// printf("befores");
		cs165_log(stdout,"%d",current_db->tables_capacity);
		// printf("134s");
	}
	// Create new table using the pre-allocated memory
	Table* table = &(current_db->tables[current_db->tables_size]);
	current_db->tables_size++;
	// Initialized table should have both 0 cols and 0 rows
	table->col_count = 0;
	table->table_length = 0;
	table->col_capacity = num_columns;
	// Allocate memory for columns
	table->columns = malloc(num_columns * sizeof(Column));
	strcpy(table->name, name);

	// TODO: update catalog file for database

	ret_status->code=OK;
	return table;
}

/* 
 * create a database
 */
Status create_db(const char* db_name) {
	struct Status ret_status;
	// if current database exists, return OBJECT_ALREADY_EXISTS error
	if(current_db != NULL){
		ret_status.code = ERROR;
		ret_status.error_message = "Database already exiests.";
		return ret_status;
	}

	// create new database
	current_db = malloc(sizeof(*current_db));
	// create new db attributes
	strcpy(current_db->name, db_name);
	current_db->tables_capacity = DB_MAX_TABLE_CAPACITY;
	current_db->tables_size = 0;
	current_db->tables = malloc(current_db->tables_capacity * sizeof(Table));
	// TODO: create catalog file for database

	// set return status code and message
	ret_status.code = OK;
	return ret_status;
}

/*
 * create a column
 */
Column* create_column(Table *table, char *name, bool sorted, Status *ret_status) {
	// Check if current table has col num == col capacity
	if(table->col_count == table->col_capacity){
		cs165_log(stdout,"%d %d",table->col_count , table->col_capacity );
		ret_status->code = ERROR;
		ret_status->error_message = "Maximum column capacity exceeded.";
		return NULL;
	}
	// Create new column using the pre-allocated memory
	Column* column = &(table->columns[table->col_count]);
	table->col_count++;
	// Assign values to column attributes
	strcpy(column->name, name);
	column->sorted = sorted;

	// TODO: insert into catalog file the column metadata

	// set return status code and message
	ret_status->code = OK;
	return NULL;
}