#include "persist.h"

int load_database() {
    // create database path if not exist
    struct stat st = {0};
    if (stat(CS165_DATABASE_PATH, &st) == -1) {
        mkdir(CS165_DATABASE_PATH, 0600);
    }
    // open the database catalog file
    FILE* fp = fopen(DB_PATH, "rb");
    int return_flag = 0;
    if (!fp) {
        cs165_log(stdout, "Failed to open database catalog\n");
        return_flag = -1;
        return return_flag;
    }
    // load database
    current_db = malloc(sizeof(Db));
    fread(current_db, sizeof(Db), 1, fp);
    fclose(fp);
    // open the database catalog file
    fp = fopen(TABLE_PATH, "rb");
    if (!fp) {
        cs165_log(stdout, "Failed to open tables catalog\n");
        return_flag = -1;
        return return_flag;
    }
    // load tables
    current_db->tables = malloc(current_db->tables_capacity * sizeof(Table));
    for (size_t i = 0; i < current_db->tables_size; i++) {
        Table* current_table = &(current_db->tables[i]);
        fread(current_table, sizeof(Table), 1, fp);
        current_table->columns = malloc(current_table->col_capacity * sizeof(Column));
        for (size_t j = 0; j < current_table->col_count; j++) {
            Column* current_column = &(current_table->columns[j]);
            fread(current_column, sizeof(Column), 1, fp);
            
            map_column(current_table, current_column);
            // printf("Column length: %s, %ld\n",current_column->name, current_column->length);
        }
    }
    // printf("Loading database, sizeof table is %ld\n", current_db->tables_size);
    // for (size_t i = 0 ; i < current_db->tables_size; i++) {
    // printf("Loading db, table name: %s\n",current_db->tables[i].name);
    // printf("Loading db, table name: %ld\n",current_db->tables[i].table_length);
    // }
    fclose(fp);
    return return_flag;
}

int map_column(Table* table, Column* column) {
    // create column base path if not exist
    struct stat st = {0};
    if (stat(COLUMN_PATH, &st) == -1) {
        mkdir(COLUMN_PATH, 0600);
    }
    // mmap column data to file
	int fd;
	int result;
    // concat column path for table
	char column_path[MAX_COLUMN_PATH];
	strcpy(column_path, COLUMN_PATH);
    strcat(column_path, table->name);
    strcat(column_path, PATH_SEP);

    // create column path for table if not exist
    if (stat(column_path, &st) == -1) {
        mkdir(column_path, 0600);
    }
    // concat column path for column page
	strcat(column_path, column->name);
    strcat(column_path, ".data");
	fd = open(column_path, O_RDWR | O_CREAT, (mode_t)0600);
	if (fd == -1) {
		cs165_log(stdout, "Cannot create column page file %s\n", column_path);
		return -1;
	}
	result = lseek(fd, table->table_length_capacity * sizeof(int) - 1, SEEK_SET);
	if (result == -1) {
		cs165_log(stdout, "Cannot lseek in column page file %s\n", column_path);
		return -1;
	}
	result = write(fd, "", 1);
	if (result == -1) {
		cs165_log(stdout, "Cannot write zero-byte at the end of column page file %s\n", column_path);
		return -1;
	}
	column->data = mmap(0, table->table_length_capacity * sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	if (column->data == MAP_FAILED) {
		close(fd);
		cs165_log(stdout, "Memory mapping failed column page file %s\n", column_path);
		return -1;
	}
    if (msync(column->data, table->table_length_capacity * sizeof(int), MS_SYNC) == -1) {
        cs165_log(stdout, "Memory syncing the file %s failed.\n", column_path);
        return -1;
    }
	// TODO: further check: closing the file descriptor does not unmap the region
	close(fd);
    return 0;
}

int syncing_column (Column* column, Table* table) {
    // printf("Syncing column %s \n",column->name);
    char column_path[MAX_COLUMN_PATH];
	strcpy(column_path, COLUMN_PATH);
    strcat(column_path, table->name);
    strcat(column_path, PATH_SEP);
    if (msync(column->data, table->table_length_capacity * sizeof(int), MS_SYNC) == -1) {
        cs165_log(stdout, "Memory syncing the file %s failed.\n", column_path);
        return -1;
    }
    return 0;
}

int persist_database() {
    // open the database catalog file if exists
    // otherwise, create a new catalog file
    FILE* fp = fopen(DB_PATH, "wb");
    // check if fopen() is successful
    int return_flag = 0;
    if (!fp) {
        cs165_log(stdout, "Failed to open/create database catalog\n");
        return_flag = -1;
        return return_flag;
    }
    fwrite(current_db, sizeof(Db), 1, fp);
    fclose(fp);

    fp = fopen(TABLE_PATH, "wb");
    if (!fp) {
        cs165_log(stdout, "Failed to open/create tables catalog\n");
        return_flag = -1;
        return return_flag;
    }
    for (size_t i = 0; i < current_db->tables_size; i++) {
        Table* current_table = &(current_db->tables[i]);
        fwrite(current_table, sizeof(Table), 1, fp);
        for (size_t j = 0; j < current_table->col_count; j++) {
            Column* current_column = &(current_table->columns[j]);
            //printf("Persist column %s %ld", current_column->name, current_column->length);
            fwrite(current_column, sizeof(Column), 1, fp);
        }
    }
    fclose(fp);
    return return_flag;
}

int persist_table(Table* current_table) {
    FILE* fp = fopen(TABLE_PATH, "wb");
    int return_flag = 0;
    if (!fp) {
        cs165_log(stdout, "Failed to open/create tables catalog\n");
        return_flag = -1;
        return return_flag;
    }
 
    fwrite(current_table, sizeof(Table), 1, fp);
    for (size_t j = 0; j < current_table->col_count; j++) {
        Column* current_column = &(current_table->columns[j]);
        fwrite(current_column, sizeof(Column), 1, fp);
        return_flag = persist_column(current_table, current_column);
    }
    
    fclose(fp);
    return return_flag;
}

// int persist_context(size_t* intermediate_data, int context_capacity) {
//     if (msync(intermediate_data, context_capacity * sizeof(int), MS_SYNC) == -1) {
//         cs165_log(stdout, "Memory syncing the context file failed.\n");
//         return -1;
//     }
//     if (munmap(intermediate_data, context_capacity * sizeof(int)) == -1) {
//         cs165_log(stdout, "Unmapping the context file failed.\n");
//         return -1;
//     }
//     return 0;
// }

int persist_column(Table* current_table, Column* current_column) {
    // unmap column file
    char column_path[MAX_COLUMN_PATH];
	strcpy(column_path, COLUMN_PATH);
	strcat(column_path, current_column->name);
    strcat(column_path, ".data");
    if (msync(current_column->data, current_table->table_length_capacity * sizeof(int), MS_SYNC) == -1) {
        cs165_log(stdout, "Memory syncing the file %s failed.\n", column_path);
        return -1;
    }
    if (munmap(current_column->data, current_table->table_length_capacity * sizeof(int)) == -1) {
        cs165_log(stdout, "Unmapping the file %s failed.\n", column_path);
        return -1;
    }
    return 0;
}

int free_database() {
    for (size_t i = 0; i < current_db->tables_size; i++) {
        Table* current_table = &(current_db->tables[i]);
        free(current_table->columns);
    }
    free(current_db->tables);
    free(current_db);
    return 0;
}