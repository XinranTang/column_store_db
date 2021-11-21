#include "persist.h"

int load_database()
{
    // create database path if not exist
    struct stat st = {0};
    if (stat(CS165_DATABASE_PATH, &st) == -1)
    {
        mkdir(CS165_DATABASE_PATH, 0600);
    }
    // open the database catalog file
    FILE *fp = fopen(DB_PATH, "rb");
    int return_flag = 0;
    if (!fp)
    {
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
    if (!fp)
    {
        cs165_log(stdout, "Failed to open tables catalog\n");
        return_flag = -1;
        return return_flag;
    }
    // load tables
    current_db->tables = malloc(current_db->tables_capacity * sizeof(Table));
    for (size_t i = 0; i < current_db->tables_size; i++)
    {
        Table *current_table = &(current_db->tables[i]);
        fread(current_table, sizeof(Table), 1, fp);
        current_table->columns = malloc(current_table->col_capacity * sizeof(Column));
        for (size_t j = 0; j < current_table->col_count; j++)
        {
            Column *current_column = &(current_table->columns[j]);
            fread(current_column, sizeof(Column), 1, fp);
            if (current_column->clustered)
            {
                current_column->histogram = malloc(sizeof(Histogram));
                fread(current_column->histogram, sizeof(Histogram), 1, fp);
            }
            if ((!current_column->clustered) && (current_column->btree || current_column->sorted))
            {
                current_column->histogram = malloc(sizeof(Histogram));
                fread(current_column->histogram, sizeof(Histogram), 1, fp);
                current_column->index = malloc(sizeof(ColumnIndex));
                fread(current_column->index, sizeof(ColumnIndex), 1, fp);
                load_index(current_table->name, current_column->name, current_column);
            }
            map_column(current_table, current_column);
            if (current_column->btree)
            {
                current_column->btree_root = load_btree(current_table->name, current_column->name);
            }
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

int map_column(Table *table, Column *column)
{
    // create column base path if not exist
    struct stat st = {0};
    if (stat(COLUMN_PATH, &st) == -1)
    {
        mkdir(COLUMN_PATH, 0600);
    }
    // mmap column data to file
    // TODO: map indexes
    // column file to indexes
    //
    int fd;
    int result;
    // concat column path for table
    char column_path[MAX_COLUMN_PATH];
    strcpy(column_path, COLUMN_PATH);
    strcat(column_path, table->name);
    strcat(column_path, PATH_SEP);

    // create column path for table if not exist
    if (stat(column_path, &st) == -1)
    {
        mkdir(column_path, 0600);
    }
    // concat column path for column page
    strcat(column_path, column->name);
    strcat(column_path, ".data");
    fd = open(column_path, O_RDWR | O_CREAT, (mode_t)0600);
    if (fd == -1)
    {
        cs165_log(stdout, "Cannot create column page file %s\n", column_path);
        return -1;
    }
    result = lseek(fd, table->table_length_capacity * sizeof(int) - 1, SEEK_SET);
    if (result == -1)
    {
        cs165_log(stdout, "Cannot lseek in column page file %s\n", column_path);
        return -1;
    }
    result = write(fd, "", 1);
    if (result == -1)
    {
        cs165_log(stdout, "Cannot write zero-byte at the end of column page file %s\n", column_path);
        return -1;
    }
    column->data = mmap(0, table->table_length_capacity * sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (column->data == MAP_FAILED)
    {
        close(fd);
        cs165_log(stdout, "Memory mapping failed column page file %s\n", column_path);
        return -1;
    }
    if (msync(column->data, table->table_length_capacity * sizeof(int), MS_SYNC) == -1)
    {
        cs165_log(stdout, "Memory syncing the file %s failed.\n", column_path);
        return -1;
    }
    // TODO: further check: closing the file descriptor does not unmap the region
    close(fd);
    return 0;
}

int syncing_column(Column *column, Table *table)
{
    // printf("Syncing column %s \n",column->name);
    char column_path[MAX_COLUMN_PATH];
    strcpy(column_path, COLUMN_PATH);
    strcat(column_path, table->name);
    strcat(column_path, PATH_SEP);
    if (msync(column->data, table->table_length_capacity * sizeof(int), MS_SYNC) == -1)
    {
        cs165_log(stdout, "Memory syncing the file %s failed.\n", column_path);
        return -1;
    }
    return 0;
}

int persist_database()
{
    // open the database catalog file if exists
    // otherwise, create a new catalog file
    // write database metadata
    FILE *fp = fopen(DB_PATH, "wb");
    // check if fopen() is successful
    int return_flag = 0;
    if (!fp)
    {
        cs165_log(stdout, "Failed to open/create database catalog\n");
        return_flag = -1;
        return return_flag;
    }
    fwrite(current_db, sizeof(Db), 1, fp);
    fclose(fp);
    // write tables metadata
    fp = fopen(TABLE_PATH, "wb");
    if (!fp)
    {
        cs165_log(stdout, "Failed to open/create tables catalog\n");
        return_flag = -1;
        return return_flag;
    }
    for (size_t i = 0; i < current_db->tables_size; i++)
    {
        Table *current_table = &(current_db->tables[i]);
        if (persist_table(current_table, fp) == -1)
        {
            cs165_log(stdout, "Failed to persist dummy tables catalog\n");
            return_flag = -1;
            return return_flag;
        }
    }
    fclose(fp);
    return return_flag;
}

int persist_table(Table *current_table, FILE *fp)
{
    int return_flag = 0;
    fwrite(current_table, sizeof(Table), 1, fp);
    for (size_t j = 0; j < current_table->col_count; j++)
    {
        Column *current_column = &(current_table->columns[j]);
        fwrite(current_column, sizeof(Column), 1, fp);
        if (current_column->clustered)
        {
            fwrite(current_column->histogram, sizeof(Histogram), 1, fp);
        }
        if ((!current_column->clustered) && (current_column->btree || current_column->sorted))
        {
            fwrite(current_column->histogram, sizeof(Histogram), 1, fp);
            fwrite(current_column->index, sizeof(ColumnIndex), 1, fp);
            persist_index(current_table->name, current_column->name, current_column);
        }
        if (current_column->btree)
        {
            persist_btree(current_column->btree_root, current_table->name, current_column->name);
        }
    }

    for (size_t j = 0; j < current_table->col_count; j++)
    {
        return_flag = persist_column(current_table, &(current_table->columns[j]));
    }
    return return_flag;
}

int load_index(char *table_name, char *column_name, Column *current_column)
{
    char index_path[MAX_COLUMN_PATH];
    int return_flag = 0;
    strcpy(index_path, COLUMN_PATH);
    strcat(index_path, table_name);
    strcat(index_path, PATH_SEP);
    strcat(index_path, column_name);
    strcat(index_path, ".idx");
    // write tables metadata
    FILE *fp = fopen(index_path, "rb");
    if (!fp)
    {
        return_flag = -1;
        return return_flag;
    }
    current_column->index->values = malloc(current_column->length * sizeof(int));
    current_column->index->positions = malloc(current_column->length * sizeof(size_t));
    fread(current_column->index->values, sizeof(int), current_column->length, fp);
    fread(current_column->index->positions, sizeof(size_t), current_column->length, fp);
    fclose(fp);
    return return_flag;
}

int persist_index(char *table_name, char *column_name, Column *current_column)
{
    char index_path[MAX_COLUMN_PATH];
    int return_flag = 0;
    strcpy(index_path, COLUMN_PATH);
    struct stat st = {0};
    strcat(index_path, table_name);
    strcat(index_path, PATH_SEP);
    // create btree path for table if not exist
    if (stat(index_path, &st) == -1)
    {
        mkdir(index_path, 0600);
    }
    strcat(index_path, column_name);
    strcat(index_path, ".idx");
    // write tables metadata
    FILE *fp = fopen(index_path, "wb");
    if (!fp)
    {
        return_flag = -1;
        return return_flag;
    }
    fwrite(current_column->index->values, sizeof(int), current_column->length, fp);
    fwrite(current_column->index->positions, sizeof(size_t), current_column->length, fp);
    fclose(fp);
    return return_flag;
}

int persist_column(Table *current_table, Column *current_column)
{
    // unmap column file
    char column_path[MAX_COLUMN_PATH];
    strcpy(column_path, COLUMN_PATH);
    strcat(column_path, current_table->name);
    strcat(column_path, PATH_SEP);

    strcat(column_path, current_column->name);
    strcat(column_path, ".data");
    if (msync(current_column->data, current_table->table_length_capacity * sizeof(int), MS_SYNC) == -1)
    {
        cs165_log(stdout, "Memory syncing the file %s failed.\n", column_path);
        return -1;
    }
    if (munmap(current_column->data, current_table->table_length_capacity * sizeof(int)) == -1)
    {
        cs165_log(stdout, "Unmapping the file %s failed.\n", column_path);
        return -1;
    }
    return 0;
}

int free_database()
{
    for (size_t i = 0; i < current_db->tables_size; i++)
    {
        Table *current_table = &(current_db->tables[i]);
        for (size_t j = 0; j < current_table->col_count; j++)
        {
            Column *current_column = &(current_table->columns[j]);
            if (current_column->clustered)
            {
                free(current_column->histogram);
            }
            if ((!current_column->clustered) && (current_column->btree || current_column->sorted))
            {
                free(current_column->histogram);
                free(current_column->index->values);
                free(current_column->index->positions);
                free(current_column->index);
            }
            if (current_column->btree)
            {
                deallocate_btree(current_column->btree_root);
            }
        }
        free(current_table->columns);
    }
    free(current_db->tables);
    free(current_db);
    return 0;
}