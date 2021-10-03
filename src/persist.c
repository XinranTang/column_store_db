#include "persist.h"

int load_database() {
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
        }
    }
    fclose(fp);
    return return_flag;
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
    }
    
    fclose(fp);
    return return_flag;
}

int free_database() {
    for (size_t i = 0; i < current_db->tables_size; i++) {
        Table* current_table = &(current_db->tables[i]);
        for (size_t j = 0; j < current_table->col_count; j++) {
            Column* current_column = &(current_table->columns[j]);
            free(current_column->data);
        }
        free(current_table->columns);
    }
    free(current_db->tables);
    free(current_db);
    return 0;
}