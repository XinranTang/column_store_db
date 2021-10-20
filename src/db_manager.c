#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include "cs165_api.h"
#include "message.h"
#include "utils.h"
#include "common.h"
#include "persist.h"

#define DB_MAX_TABLE_CAPACITY 10
#define TABLE_INIT_LENGTH_CAPACITY 10
// In this class, there will always be only one active database at a time
Db *current_db;


/* 
 * create a table object. The Status object can be used to return
 * to the caller that there was an error in table creation
 */
Table* create_table(Db* db, const char* name, size_t num_columns, Status *ret_status) {
	if (strcmp(current_db->name, db->name) != 0) {
		return NULL;
	}
	// Check if current db has table_size == table_capacity
	if(current_db->tables_size == current_db->tables_capacity){
				// expand capacity and reallocate memory
		current_db->tables_capacity *= 2;
		current_db->tables = realloc(current_db->tables, current_db->tables_capacity * sizeof(Table));
		cs165_log(stdout,"%d\n",current_db->tables_capacity);
	}
	// Create new table using the pre-allocated memory
	//printf("Table Size: %ld\n",current_db->tables_size);
	Table* table = &(current_db->tables[current_db->tables_size]);
	current_db->tables_size++;
	// Initialized table should have both 0 cols and 0 rows
	table->col_count = 0;
	table->table_length = 0;
	table->col_capacity = num_columns;
	table->table_length_capacity = TABLE_INIT_LENGTH_CAPACITY;
	// Allocate memory for columns
	table->columns = malloc(num_columns * sizeof(Column));
	strcpy(table->name, name);
	//for (size_t i = 0; i < current_db->tables_size; i++) {
	//	printf("%s\n", current_db->tables[i].name);
	//}
	// TODO: update catalog file for database

	ret_status->code=OK;
	return table;
}

/* 
 * create a database
 */
Status create_db(const char* db_name) {
	struct Status ret_status;
	// In the current design, if current database exists, the current database will be cleared in order to create a new one
	// for the persisted database data, since we are only using one database, the persisted data will just be backed up on the disk
	if(current_db != NULL){
		// ret_status.code = ERROR;
		// ret_status.error_message = "Database already exiests.";
		// return ret_status;
		persist_database();

		char column_path[MAX_COLUMN_PATH];
		column_path[0] = 'r';
		column_path[1] = 'm';
		column_path[2] = ' ';
		column_path[3] = '-';
		column_path[4] = 'r';
		column_path[5] = ' ';
		strcat(column_path, COLUMN_PATH);

		system(column_path);
		
		free_database();
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
		cs165_log(stdout,"Expand table column capacity from %d to %d\n",table->col_count , table->col_capacity );
		ret_status->code = ERROR;
		ret_status->error_message = "Maximum column capacity exceeded.";
		return NULL;
	}
	//printf("Create column in table %s\n", table->name);
	// Create new column using the pre-allocated memory
	Column* column = &(table->columns[table->col_count]);
	table->col_count++;
	// Assign values to column attributes
	strcpy(column->name, name);
	column->sorted = sorted;
	column->length = table->table_length;
	// set return status code and message
	ret_status->code = OK;
	return NULL;
}
