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
#include "btree.h"

#define DB_MAX_TABLE_CAPACITY 10
#define TABLE_INIT_LENGTH_CAPACITY 10
// In this class, there will always be only one active database at a time
Db *current_db;

/* 
 * create a table object. The Status object can be used to return
 * to the caller that there was an error in table creation
 */
Table *create_table(Db *db, const char *name, size_t num_columns, Status *ret_status)
{
	if (strcmp(current_db->name, db->name) != 0)
	{
		return NULL;
	}
	// Check if current db has table_size == table_capacity
	if (current_db->tables_size == current_db->tables_capacity)
	{
		// expand capacity and reallocate memory
		current_db->tables_capacity *= 2;
		current_db->tables = realloc(current_db->tables, current_db->tables_capacity * sizeof(Table));
		cs165_log(stdout, "%d\n", current_db->tables_capacity);
	}
	// Create new table using the pre-allocated memory
	//printf("Table Size: %ld\n",current_db->tables_size);
	Table *table = &(current_db->tables[current_db->tables_size]);
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

	ret_status->code = OK;
	return table;
}

/* 
 * create a database
 */
Status create_db(const char *db_name)
{
	struct Status ret_status;
	// In the current design, if current database exists, the current database will be cleared in order to create a new one
	// for the persisted database data, since we are only using one database, the persisted data will just be backed up on the disk
	if (current_db != NULL)
	{
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
Column *create_column(Table *table, char *name, bool sorted, Status *ret_status)
{
	// Check if current table has col num == col capacity
	if (table->col_count == table->col_capacity)
	{
		cs165_log(stdout, "Expand table column capacity from %d to %d\n", table->col_count, table->col_capacity);
		ret_status->code = ERROR;
		ret_status->error_message = "Maximum column capacity exceeded.";
		return NULL;
	}
	//printf("Create column in table %s\n", table->name);
	// Create new column using the pre-allocated memory
	Column *column = &(table->columns[table->col_count]);
	table->col_count++;
	// Assign values to column attributes
	strcpy(column->name, name);
	column->sorted = sorted;
	column->length = table->table_length;
	// set return status code and message
	ret_status->code = OK;
	return NULL;
}

/*
 * create a index
 */
void create_index(Table *table, Column *column, bool sorted, bool btree, bool clustered, Status *ret_status)
{
	column->sorted = sorted|btree;
	column->btree = btree;
	column->clustered = clustered;
	if ((sorted|btree) && clustered) {
		for (size_t i = 0; i < table->col_count; i++) {
			table->columns[i].clustered = true;
		}
	} else if ((sorted|btree) && !clustered) {
		// TODO: free column_index, values and positions
		ColumnIndex* column_index = malloc(sizeof(ColumnIndex));
		column_index->values = malloc(table->table_length_capacity * sizeof(int));
		column_index->indexes = malloc(table->table_length_capacity * sizeof(size_t));
		column->index = column_index;
	}
	// set return status code and message
	ret_status->code = OK;
	return NULL;
}

void quick_sort(Table *table, size_t primary_column, size_t start, size_t end) {
	if (start >= end) return;
	size_t i = start;
	size_t j = end;
	int pivots[table->col_count];
	for (size_t c = 0; c < table->col_count; c++) {
		pivots[c] = table->columns[c].data[start];
	}
	while (i < j) {
		while (i < j && table->columns[primary_column].data[j] >= pivots[primary_column]) j--;
		while (i < j && table->columns[primary_column].data[i] <= pivots[primary_column]) i--;
		if (i < j) {
			int tmp[table->col_count];
			for (size_t c = 0; c < table->col_count; c++) {
				tmp[c] = table->columns[c].data[i];
			}
			for (size_t c = 0; c < table->col_count; c++) {
				table->columns[c].data[i] = table->columns[c].data[j];
			}
			for (size_t c = 0; c < table->col_count; c++) {
				table->columns[c].data[j] = tmp[c];
			}
		}
	}
	for (size_t c = 0; c < table->col_count; c++) {
		table->columns[c].data[start] = table->columns[c].data[i];
	}
	for (size_t c = 0; c < table->col_count; c++) {
		table->columns[c].data[i] = pivots[c];
	}
	quick_sort(table, primary_column, start, i - 1);
	quick_sort(table, primary_column, i + 1, end);
}

void quick_sort_index(ColumnIndex * index, size_t start, size_t end) {
	if (start >= end) return;
	size_t i = start;
	size_t j = end;
	int pivot = index->values[start];
	while (i < j) {
		while (i < j && index->values[j] >= pivot) j--;
		while (i < j && index->values[i] <= pivot) i++;
		if (i < j) {
			int tmp_val = index->values[i];
			index->values[i] = index->values[j];
			index->values[j] = tmp_val;
			size_t tmp_idx = index->indexes[i];
			index->indexes[i] = index->indexes[j];
			index->indexes[j] = tmp_idx;
		}
	}
	index->values[start] = index->values[i];
	index->values[i] = pivot;
	size_t tmp_idx = index->indexes[start];
	index->indexes[start] = index->values[i];
	index->values[i] = tmp_idx;
	quick_sort_index(index, start, i - 1);
	quick_sort_index(index, i + 1, end);
}

void build_column_index(Column * column) {
	if (!column->index) {
		cs165_log(stderr, "Column index not crated.");
		return;
	}
	ColumnIndex *index = column->index;
	for (size_t i = 0; i < column->length; i++) {
		index->values[i] = column->data[i];
		index->indexes[i] = i;
	}
	quick_sort_index(index, 0, column->length - 1);
}

void build_primary_index(Table *table, size_t primary_column_index) {
	// propagate the order of primary column to the whole table
	quick_sort(table, primary_column_index, 0, table->table_length - 1);
	if (table->columns[primary_column_index].btree) {
		// create indexes for primary column
		size_t *indexes = malloc(table->columns[primary_column_index].length * sizeof(size_t));
		for (size_t i = 0; i < table->columns[primary_column_index].length; i++) {
			indexes[i] = i;
		}
		table->columns[primary_column_index].btree_root = create_btree(table->columns[primary_column_index].data, indexes, table->columns[primary_column_index].length);
		free(indexes);
	}
}

void build_secondary_index(Table *table, Column * primary_column, bool btree, bool sorted) {
	// TODO: persist btree
	// TODO: persist indexes
	build_column_index(primary_column);
	if (btree) {
		primary_column->btree_root = create_btree(primary_column->index->values, primary_column->index->indexes, primary_column->length);
	}
}