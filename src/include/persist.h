#include "cs165_api.h"
#include "common.h"
#include "utils.h"
#include "btree.h"
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <stdio.h>

int load_database();

int map_column(Table* table, Column* column);

int syncing_column (Column* column, Table* table);

// void map_context(char* intermediate, int context_capacity, void* data);

int persist_database();

int persist_table(Table* current_table, FILE* fp);

int persist_column(Table* current_table, Column* current_column);

int persist_index(char* table_name, char* column_name, Column* current_column);

int load_index(char* table_name, char* column_name, Column* current_column);

int free_database();