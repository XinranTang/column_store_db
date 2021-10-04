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

int persist_database();

int persist_table(Table* current_table);

int persist_column(Table* current_table, Column* current_column);

int free_database();