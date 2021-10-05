#include "cs165_api.h"
#include "common.h"
#include "utils.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <stdio.h>

int load_database();

int map_column(Table* table, Column* column);

// void map_context(char* intermediate, int context_capacity, void* data);

int persist_database();

int persist_table(Table* current_table);

int persist_column(Table* current_table, Column* current_column);

// int persist_context(size_t* intermediate_data, int context_capacity);

int free_database();