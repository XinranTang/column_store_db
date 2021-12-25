/** server.c
 * CS165 Fall 2021
 *
 * This file provides a basic unix socket implementation for a server
 * used in an interactive client-server database.
 * The client should be able to send messages containing queries to the
 * server.  When the server receives a message, it must:
 * 1. Respond with a status based on the query (OK, UNKNOWN_QUERY, etc.)
 * 2. Process any appropriate queries, if applicable.
 * 3. Return the query response to the client.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <unistd.h>
#include <pthread.h>

#include "threadpool.h"
#include "common.h"
#include "parse.h"
#include "persist.h"
#include "cs165_api.h"
#include "message.h"
#include "utils.h"
#include "client_context.h"
#include "hash_table.h"

#define DEFAULT_QUERY_BUFFER_SIZE 1024
#define DEFAULT_TABLE_LENGTH 5000000
#define THREAD 32
#define QUEUE 256
#define CACHE_SIZE_THRESHOLD 1000 // for 32 KB L1 cache to store the hash table
#define NUM_PARTITIONS 7          // = 32KB / 4KB - 1 = 7
threadpool_t *pool;
int tasks = 0, done = 0;
size_t mutex_k=0;
pthread_mutex_t lock;
pthread_mutex_t grace_hash_lock;
typedef struct thread_args
{
    DbOperator *query;
    message *send_message;
    Partition *partitions;
    Partition *partitionR;
    Partition *partitionL;
    size_t *resL;
    size_t *resR;
    size_t len;
    int *column;
    int p_div;
} thread_args;

void execute_create(DbOperator *query, message *send_message)
{
    if (query->operator_fields.create_operator.create_type == _DB)
    {
        if (create_db(query->operator_fields.create_operator.name).code == OK)
        {
            send_message->status = OK_DONE;
            return;
        }
        else
        {
            send_message->status = OBJECT_ALREADY_EXISTS;
            return;
        }
    }
    else if (query->operator_fields.create_operator.create_type == _TABLE)
    {
        Status create_status;
        create_table(query->operator_fields.create_operator.db,
                     query->operator_fields.create_operator.name,
                     query->operator_fields.create_operator.col_count,
                     &create_status);
        if (create_status.code != OK)
        {
            send_message->status = EXECUTION_ERROR;
            return;
        }
        send_message->status = OK_DONE;
    }
    else if (query->operator_fields.create_operator.create_type == _COLUMN)
    {
        Status create_status;
        create_column(query->operator_fields.create_operator.table,
                      query->operator_fields.create_operator.name,
                      &create_status);
        if (create_status.code != OK)
        {
            send_message->status = EXECUTION_ERROR;
            return;
        }
        send_message->status = OK_DONE;
    }
    else if (query->operator_fields.create_operator.create_type == _INDEX)
    {
        Status create_status;
        create_index(
            query->operator_fields.create_operator.column,
            query->operator_fields.create_operator.sorted,
            query->operator_fields.create_operator.btree,
            query->operator_fields.create_operator.clustered,
            &create_status);
        if (create_status.code != OK)
        {
            send_message->status = EXECUTION_ERROR;
            return;
        }
        send_message->status = OK_DONE;
    }
}

void execute_insert(DbOperator *query, message *send_message)
{
    Table *insert_table = query->operator_fields.insert_operator.table;
    if (!insert_table)
    {
        send_message->status = OBJECT_NOT_FOUND;
        return;
    }
    // increase # of rows
    insert_table->table_length++;
    // if # of rows is larger than table length capacity, expand capacity
    if (insert_table->table_length > insert_table->table_length_capacity)
    {
        // reallocate column data sizes
        for (size_t i = 0; i < insert_table->col_count; i++)
        {
            // sync and unmap before expand capacity
            if (persist_column(insert_table, &insert_table->columns[i]) == -1)
            {
                cs165_log(stdout, "Memory syncing and unmapping failed.\n");
                send_message->status = EXECUTION_ERROR;
                return;
            }
        }
        insert_table->table_length_capacity = insert_table->table_length_capacity * 2;
        for (size_t i = 0; i < insert_table->col_count; i++)
        {
            // sync and unmap before expand capacity
            if (map_column(insert_table, &insert_table->columns[i]) == -1)
            {
                cs165_log(stdout, "Memory mapping failed.\n");
                send_message->status = EXECUTION_ERROR;
                return;
            }
        }
    }
    int *insert_values = query->operator_fields.insert_operator.values;
    Column *columns = insert_table->columns;
    if (!columns)
    {
        send_message->status = OBJECT_NOT_FOUND;
        return;
    }

    for (size_t i = 0; i < insert_table->col_count; i++)
    {
        Column *current_column = &(insert_table->columns[i]);
        // current_column->data = realloc(current_column, insert_table->table_length * sizeof(int));
        current_column->data[insert_table->table_length - 1] = insert_values[i];
        // syncing_column(current_column, insert_table);
        // increase the length of current_column
        current_column->length++;
    }

    send_message->status = OK_DONE;
}
// TODO: modify to add index
void execute_load(DbOperator *query, message *send_message)
{

    message load_message_header;
    message load_recv_message;

    // file_name is assigned to payload
    load_message_header.status = OK_WAIT_FOR_RESPONSE;
    load_message_header.payload = malloc(strlen(query->operator_fields.load_operator.file_name) + 1);
    strcpy(load_message_header.payload, query->operator_fields.load_operator.file_name);
    load_message_header.length = strlen(load_message_header.payload);
    // send load message header to client
    if (send(query->client_fd, &(load_message_header), sizeof(message), 0) == -1)
    {
        free(load_message_header.payload);
        send_message->status = EXECUTION_ERROR;
        return;
    }
    // send load message payload to client
    if (send(query->client_fd, load_message_header.payload, load_message_header.length, 0) == -1)
    {
        free(load_message_header.payload);
        send_message->status = EXECUTION_ERROR;
        return;
    }
    free(load_message_header.payload);

    // start receiving data from the client
    char meta_buffer[DEFAULT_QUERY_BUFFER_SIZE];
    // receive load schema: first line in load file
    // TODO: only handling one table for one file
    int len = 0;

    if ((len = recv(query->client_fd, &(load_recv_message), sizeof(message), 0)) > 0)
    {
        if (load_recv_message.status != OK_WAIT_FOR_RESPONSE)
        {
            cs165_log(stdout, "Error on the client side. Return.\n");
            send_message->status = OBJECT_NOT_FOUND;
            return;
        }

        if ((len = recv(query->client_fd, &(meta_buffer), DEFAULT_QUERY_BUFFER_SIZE, 0)) > 0)
        {
            char *metadata = strtok(meta_buffer, ",");
            char *db_name = strtok(metadata, ".");
            char *table_name = strtok(NULL, ".");

            if (!db_name || !table_name)
            {
                // check data schema
                cs165_log(stdout, "Incorrect data schema.\n");
                send_message->status = OBJECT_NOT_FOUND;
                load_message_header.status = OBJECT_NOT_FOUND;
                // TODO: have not checked if send fails
                send(query->client_fd, &(load_message_header), sizeof(load_message_header), 0);
                return;
            }
            // find the table for the load file
            if (strcmp(db_name, current_db->name) != 0)
            {
                cs165_log(stdout, "Cannot find database.\n");
                send_message->status = OBJECT_NOT_FOUND;
                load_message_header.status = OBJECT_NOT_FOUND;
                // TODO: have not checked if send fails
                send(query->client_fd, &(load_message_header), sizeof(load_message_header), 0);
                return;
            }
            Table *current_table = lookup_table(table_name);
            if (!current_table)
            {
                send_message->status = OBJECT_NOT_FOUND;
                load_message_header.status = OBJECT_NOT_FOUND;
                // TODO: have not checked if send fails
                send(query->client_fd, &(load_message_header), sizeof(load_message_header), 0);
                return;
            }

            // receive file_size from client
            size_t file_size[3];
            load_message_header.status = OK_WAIT_FOR_RESPONSE;
            send(query->client_fd, &(load_message_header), sizeof(load_message_header), 0);
            recv(query->client_fd, &(file_size), 3 * sizeof(size_t), 0);
            file_size[1] = (file_size[0] / current_table->col_count) / sizeof(int);
            // set table length capacity for current table
            current_table->table_length_capacity = file_size[1] * 2;
            for (size_t i = 0; i < current_table->col_count; i++)
            {
                if (map_column(current_table, &current_table->columns[i]) == -1)
                {
                    send_message->status = EXECUTION_ERROR;
                    load_message_header.status = EXECUTION_ERROR;
                    send(query->client_fd, &(load_message_header), sizeof(load_message_header), 0);
                    return;
                }
            }
            load_message_header.status = OK_WAIT_FOR_RESPONSE;
            send(query->client_fd, &(load_message_header), sizeof(load_message_header), 0);

            char *raw_data = malloc(file_size[0]); // TODO: check file size TODO: free
            char *raw_data_ptr = raw_data;
            size_t remain_data = file_size[0] - file_size[2];

            //printf("recv %d bytes, remaining %ld bytes \n", 0, remain_data);
            while ((remain_data >= DEFAULT_QUERY_BUFFER_SIZE) && ((len = recv(query->client_fd, raw_data_ptr, DEFAULT_QUERY_BUFFER_SIZE, 0)) > 0))
            {

                remain_data -= len;
                raw_data_ptr += len;
                //printf("recv %d bytes, remaining %ld bytes \n", len,remain_data);
            }
            if ((remain_data > 0) && ((len = recv(query->client_fd, raw_data_ptr, remain_data, 0)) > 0))
            {

                remain_data -= len;
                raw_data_ptr += len;
                //printf("recv %d bytes, remaining %ld bytes \n", len,remain_data);
            }
            printf("File size: %ld\n", file_size[0]);

            size_t row = 0;
            // start receiving data body from the client
            // start loading line by line
            char *ptr, *save1 = NULL;

            ptr = strtok_r(raw_data, "\n", &save1);

            while (ptr != NULL && ptr < raw_data_ptr)
            {
                row++;
                char *data, *save2 = NULL;
                data = strtok_r(ptr, ",", &save2);
                current_table->table_length++;
                for (size_t column = 0; column < current_table->col_count; column++)
                {
                    current_table->columns[column].data[row - 1] = atoi(data);
                    data = strtok_r(NULL, ",", &save2);
                }
                ptr = strtok_r(NULL, "\n", &save1);
            }
            free(raw_data);

            // record the primary indexed column if exists
            // 1. find first primary index column
            // 2. build primary index
            // 3. build secondary index
            size_t primary_index_column = 0;
            // flag: used to record if found primary index column
            bool flag = false;
            // set length of column and find primary indexed column
            for (size_t i = 0; i < current_table->col_count; i++)
            {
                current_table->columns[i].length = current_table->table_length;
                if (current_table->columns[i].clustered)
                {
                    if (flag)
                    {
                        current_table->columns[i].clustered = false;
                    }
                    else
                    {
                        primary_index_column = i;
                        flag = true;
                    }
                }
                // syncing_column(&current_table->columns[i], current_table);
            }
            // process primary index
            // the primary index column is sorted, and the order is propagated to the whole table
            if (flag)
            {
                build_primary_index(current_table, primary_index_column);
            }
            // process secondary indexes
            for (size_t i = 0; i < current_table->col_count; i++)
            {
                if (flag & (i == primary_index_column))
                    continue;
                if (current_table->columns[i].btree | current_table->columns[i].sorted)
                {
                    build_secondary_index(&current_table->columns[i], current_table->columns[i].btree);
                }
            }
            load_message_header.status = OK_DONE;
            send(query->client_fd, &load_message_header, sizeof(message), 0);
        }
    }

    send_message->status = OK_DONE;
}

void execute_select(DbOperator *query, message *send_message)
{
    // TODO: do we need to modify TWO_COLUMN select to use indexing?
    if (query->operator_fields.select_operator.select_type == TWO_COLUMN)
    {
        Result *position_vector = lookup_variables(NULL, NULL, NULL, query->operator_fields.select_operator.position_vector, query->context)->column_pointer.result;
        Result *value_vector = lookup_variables(NULL, NULL, NULL, query->operator_fields.select_operator.value_vector, query->context)->column_pointer.result;
        int low = query->operator_fields.select_operator.low;
        int high = query->operator_fields.select_operator.high;
        if (position_vector->data_type == FLOAT)
        {
            send_message->status = INCORRECT_FORMAT;
        }
        else
        {
            if (value_vector->data_type == INT)
            {
                // map context file
                size_t *select_data = malloc(value_vector->num_tuples * sizeof(size_t));

                size_t index = 0;
                size_t *positions = position_vector->payload;
                int *values = value_vector->payload;
                for (size_t i = 0; i < position_vector->num_tuples; i++)
                {
                    // select_data[index] = positions[i];
                    // index += (values[i] >= low) & (values[i] <= high);
                    if (values[i] >= low && values[i] <= high)
                        select_data[index++] = positions[i];
                }

                // insert selected positions to client context
                ClientContext *client_context = query->context;
                Result *result = malloc(sizeof(Result));
                result->data_type = LONG;
                result->num_tuples = index;
                result->payload = select_data;
                add_context(result, client_context, query->operator_fields.select_operator.intermediate);
            }
            else if (value_vector->data_type == LONG)
            {
                // map context file
                size_t *select_data = malloc(value_vector->num_tuples * sizeof(size_t));

                size_t index = 0;
                size_t *positions = position_vector->payload;
                long *values = value_vector->payload;
                for (size_t i = 0; i < position_vector->num_tuples; i++)
                {
                    // select_data[index] = positions[i];
                    // index += (values[i] >= low) & (values[i] <= high);
                    if (values[i] >= low && values[i] <= high)
                        select_data[index++] = positions[i];
                }

                // insert selected positions to client context
                ClientContext *client_context = query->context;
                Result *result = malloc(sizeof(Result));
                result->data_type = LONG;
                result->num_tuples = index;
                result->payload = select_data;
                add_context(result, client_context, query->operator_fields.select_operator.intermediate);
            }
            else
            {
                // map context file
                size_t *select_data = malloc(value_vector->num_tuples * sizeof(size_t));

                size_t index = 0;
                size_t *positions = position_vector->payload;
                float *values = value_vector->payload;
                for (size_t i = 0; i < position_vector->num_tuples; i++)
                {
                    // select_data[index] = positions[i];
                    // index += (values[i] >= low) & (values[i] <= high);
                    if (values[i] >= low && values[i] <= high)
                        select_data[index++] = positions[i];
                }
                // insert selected positions to client context
                ClientContext *client_context = query->context;
                Result *result = malloc(sizeof(Result));
                result->data_type = LONG;
                result->num_tuples = index;
                result->payload = select_data;
                add_context(result, client_context, query->operator_fields.select_operator.intermediate);
            }
        }
    }
    else
    {
        Column *column = query->operator_fields.select_operator.column;
        int low = query->operator_fields.select_operator.low;
        int high = query->operator_fields.select_operator.high;
        // map context file
        size_t *select_data = malloc(query->operator_fields.select_operator.column_length * sizeof(size_t));
        size_t index = 0;
        // if primary index
        if (column->clustered)
        {
            ColumnSelectType column_select_type = optimize(column, low, high);
            if (column_select_type == RANDOM_ACCESS)
            {
                if (column->btree)
                {
                    long index_low = search_position(column->btree_root, low);
                    size_t index_high = search_position(column->btree_root, high);
                    // search_position returns the position that is greater or equal to target
                    while ((index_low >= 0) && (column->data[index_low] >= low))
                    {
                        index_low--;
                    }
                    while (column->data[index_low] < low)
                    {
                        index_low++;
                    }
                    while ((index_high < column->length) && (column->data[index_high] <= high))
                    {
                        index_high++;
                    }
                    while (column->data[index_high] > high)
                    {
                        index_high--;
                    }
                    for (size_t i = index_low; (i <= index_high) && (i < column->length); i++)
                    {
                        select_data[index++] = i;
                    }
                }
                else
                { // sorted non btree primary index
                    size_t index_low = binary_search_index(column->data, column->length, low);
                    size_t index_high = binary_search_index(column->data, column->length, high);
                    // search_index returns the index that is greater or equal to target
                    while (column->data[index_low] == low)
                    {
                        index_low--;
                    }
                    while (column->data[index_low] < low)
                    {
                        index_low++;
                    }
                    while (column->data[index_high] == high)
                    {
                        index_high++;
                    }
                    while (column->data[index_high] > high)
                    {
                        index_high--;
                    }
                    for (size_t i = index_low; i <= index_high; i++)
                    {
                        select_data[index++] = i;
                    }
                }
            }
            else
            {
                goto sequential_select;
            }
        }
        else if (column->btree)
        {
            ColumnSelectType column_select_type = optimize(column, low, high);
            if (column_select_type == RANDOM_ACCESS)
            {
                long index_low = search_position(column->btree_root, low);
                size_t index_high = search_position(column->btree_root, high);
                // print_btree(column->btree_root, 0);
                // search_position returns the position that is greater or equal to target
                while ((index_low >= 0) && (column->index->values[index_low] >= low))
                {
                    index_low--;
                }
                while (column->index->values[index_low] < low)
                {
                    index_low++;
                }

                while ((index_high < column->length) && (column->index->values[index_high] <= high))
                {
                    index_high++;
                }
                while (column->index->values[index_high] > high)
                {
                    index_high--;
                }
                for (size_t i = index_low; (i <= index_high) && (i < column->length); i++)
                {
                    select_data[index++] = column->index->positions[i];
                }
            }
            else
            {
                goto sequential_select;
            }
        }
        else if (column->sorted)
        {
            ColumnSelectType column_select_type = optimize(column, low, high);
            if (column_select_type == RANDOM_ACCESS)
            {
                size_t index_low = binary_search_index(column->index->values, column->length, low);
                size_t index_high = binary_search_index(column->index->values, column->length, high);
                // search_index returns the index that is greater or equal to target
                while (column->index->values[index_low] == low)
                {
                    index_low--;
                }
                while (column->index->values[index_low] < low)
                {
                    index_low++;
                }

                while (column->index->values[index_high] == high)
                {
                    index_high++;
                }
                while (column->index->values[index_high] > high)
                {
                    index_high--;
                }
                for (size_t i = index_low; i <= index_high; i++)
                {
                    select_data[index++] = column->index->positions[i];
                }
                qsort(select_data, index, sizeof(size_t), int_cmp);
            }
            else
            {
                goto sequential_select;
            }
        }
        else
        {
        sequential_select:
            for (size_t i = 0; i < query->operator_fields.select_operator.column_length; i++)
            {
                // select_data[index] = i;
                // index += (column->data[i] >= low) & (column->data[i] <= high);
                if (column->data[i] >= low && column->data[i] <= high)
                    select_data[index++] = i;
            }
        }

        // insert selected positions to client context
        ClientContext *client_context = query->context;
        Result *result = malloc(sizeof(Result));
        result->data_type = LONG;
        result->num_tuples = index;
        result->payload = select_data;
        add_context(result, client_context, query->operator_fields.select_operator.intermediate);
    }

    if (query->context->batch_mode)
    {
        pthread_mutex_lock(&lock);
        // Record the number of successful tasks completed
        done++;
        pthread_mutex_unlock(&lock);
    }
    send_message->status = OK_DONE;
}

// TODO: add message to indicate select status
void batch_execute_select(void *args)
{
    thread_args *arguments = (thread_args *)args;
    DbOperator *query = arguments->query;
    message *send_message = arguments->send_message;
    if (query->operator_fields.select_operator.select_type == TWO_COLUMN)
    {

        Result *position_vector = lookup_variables(NULL, NULL, NULL, query->operator_fields.select_operator.position_vector, query->context)->column_pointer.result;
        Result *value_vector = lookup_variables(NULL, NULL, NULL, query->operator_fields.select_operator.value_vector, query->context)->column_pointer.result;
        int low = query->operator_fields.select_operator.low;
        int high = query->operator_fields.select_operator.high;
        if (position_vector->data_type == FLOAT)
        {
            pthread_mutex_lock(&lock);
            send_message->status = INCORRECT_FORMAT;
            pthread_mutex_unlock(&lock);
        }
        else
        {
            if (value_vector->data_type == INT)
            {
                // map context file
                size_t *select_data = malloc(value_vector->num_tuples * sizeof(size_t));

                size_t index = 0;
                size_t *positions = position_vector->payload;
                int *values = value_vector->payload;
                for (size_t i = 0; i < position_vector->num_tuples; i++)
                {
                    select_data[index] = positions[i];
                    index += (values[i] >= low) & (values[i] <= high);
                }

                // insert selected positions to client context
                ClientContext *client_context = query->context;
                Result *result = malloc(sizeof(Result));
                result->data_type = LONG;
                result->num_tuples = index;
                result->payload = select_data;
                add_context(result, client_context, query->operator_fields.select_operator.intermediate);
            }
            else if (value_vector->data_type == LONG)
            {
                // map context file
                size_t *select_data = malloc(value_vector->num_tuples * sizeof(size_t));

                size_t index = 0;
                size_t *positions = position_vector->payload;
                long *values = value_vector->payload;
                for (size_t i = 0; i < position_vector->num_tuples; i++)
                {
                    select_data[index] = positions[i];
                    index += (values[i] >= low) & (values[i] <= high);
                }

                // insert selected positions to client context
                ClientContext *client_context = query->context;
                Result *result = malloc(sizeof(Result));
                result->data_type = LONG;
                result->num_tuples = index;
                result->payload = select_data;
                add_context(result, client_context, query->operator_fields.select_operator.intermediate);
            }
            else
            {
                // map context file
                size_t *select_data = malloc(value_vector->num_tuples * sizeof(size_t));

                size_t index = 0;
                size_t *positions = position_vector->payload;
                float *values = value_vector->payload;
                for (size_t i = 0; i < position_vector->num_tuples; i++)
                {
                    select_data[index] = positions[i];
                    index += (values[i] >= low) & (values[i] <= high);
                }
                // insert selected positions to client context
                ClientContext *client_context = query->context;
                Result *result = malloc(sizeof(Result));
                result->data_type = LONG;
                result->num_tuples = index;
                result->payload = select_data;
                add_context(result, client_context, query->operator_fields.select_operator.intermediate);
            }
        }
    }
    else
    {
        Column *column = query->operator_fields.select_operator.column;
        int low = query->operator_fields.select_operator.low;
        int high = query->operator_fields.select_operator.high;
        // map context file
        size_t *select_data = malloc(query->operator_fields.select_operator.column_length * sizeof(size_t));

        size_t index = 0;

        for (size_t i = 0; i < query->operator_fields.select_operator.column_length; i++)
        {
            select_data[index] = i;
            index += (column->data[i] >= low) & (column->data[i] <= high);
        }

        // insert selected positions to client context
        ClientContext *client_context = query->context;
        Result *result = malloc(sizeof(Result));
        result->data_type = LONG;
        result->num_tuples = index;
        result->payload = select_data;
        add_context(result, client_context, query->operator_fields.select_operator.intermediate);
    }

    if (query->context->batch_mode)
    {
        pthread_mutex_lock(&lock);
        // Record the number of successful tasks completed
        done++;
        pthread_mutex_unlock(&lock);
    }
    // free db operator
    free(query);
    // free arguments
    free(arguments);
}

void execute_fetch(DbOperator *query, message *send_message)
{
    Column *column = query->operator_fields.fetch_operator.column;
    // char* intermediate = query->operator_fields.fetch_operator.intermediate; // Error!!!!!! strcpy for intermediate

    size_t *positions;
    size_t positions_len;
    // find specified positions vector in client context
    // TODO: extract find_context function later
    ClientContext *client_context = query->context;

    GeneralizedColumn *generalized_column = lookup_variables(NULL, NULL, NULL, query->operator_fields.fetch_operator.positions, client_context);
    positions = generalized_column->column_pointer.result->payload;
    positions_len = generalized_column->column_pointer.result->num_tuples;

    // return if variable not found
    if (!positions)
    {
        cs165_log(stdout, "Variable not found in variable pool.");
        send_message->status = EXECUTION_ERROR;
        return;
    }

    // allocate memory for context file
    int *fetch_data = malloc(positions_len * sizeof(int));
    // fetch data
    for (size_t i = 0; i < positions_len; i++)
    {
        fetch_data[i] = column->data[positions[i]];
    }
    Result *result = malloc(sizeof(Result));
    result->data_type = INT;
    result->num_tuples = positions_len;
    result->payload = fetch_data;
    add_context(result, client_context, query->operator_fields.fetch_operator.intermediate);
    send_message->status = OK_DONE;
}

void execute_aggregate(DbOperator *query, message *send_message)
{
    ClientContext *client_context = query->context;
    AggregateType agg_type = query->operator_fields.aggregate_operator.aggregate_type;
    Result *result = malloc(sizeof(Result));
    if (agg_type == SUM || agg_type == AVG)
    {
        GeneralizedColumn *gc1 = query->operator_fields.aggregate_operator.gc1;
        if (gc1->column_type == RESULT)
        { // result
            if (gc1->column_pointer.result->data_type == FLOAT)
            {
                result->data_type = DOUBLE;

                double *result_data = malloc(1 * sizeof(double));
                *result_data = 0.0;

                float *payload = (float *)gc1->column_pointer.result->payload;
                for (size_t i = 0; i < gc1->column_pointer.result->num_tuples; i++)
                {
                    *result_data += payload[i];
                }
                if (agg_type == AVG)
                    *result_data = gc1->column_pointer.result->num_tuples != 0 ? *result_data / gc1->column_pointer.result->num_tuples : 0;
                result->payload = result_data;
                result->num_tuples = 1;
            }
            else if (gc1->column_pointer.result->data_type == INT)
            {

                if (agg_type == AVG)
                {
                    result->data_type = DOUBLE;

                    double *result_data = malloc(1 * sizeof(double));
                    *result_data = 0.0;
                    int *payload = (int *)gc1->column_pointer.result->payload;
                    for (size_t i = 0; i < gc1->column_pointer.result->num_tuples; i++)
                    {
                        *result_data += payload[i];
                    }
                    *result_data = gc1->column_pointer.result->num_tuples != 0 ? *result_data / gc1->column_pointer.result->num_tuples : 0;
                    result->payload = result_data;
                    result->num_tuples = 1;
                }
                else
                { // agg_type == SUM
                    result->data_type = LONG;

                    long *result_data = malloc(1 * sizeof(long));
                    *result_data = 0;

                    int *payload = (int *)gc1->column_pointer.result->payload;
                    for (size_t i = 0; i < gc1->column_pointer.result->num_tuples; i++)
                    {
                        *result_data += payload[i];
                    }
                    result->payload = result_data;
                    result->num_tuples = 1;
                }
            }
            else
            { // LONG
                result->data_type = LONG;

                if (agg_type == AVG)
                {
                    result->data_type = DOUBLE;

                    long result_data = 0;
                    double *avg_result = malloc(1 * sizeof(double));
                    long *payload = (long *)gc1->column_pointer.result->payload;
                    for (size_t i = 0; i < gc1->column_pointer.result->num_tuples; i++)
                    {
                        result_data += payload[i];
                    }
                    if (gc1->column_pointer.result->num_tuples != 0)
                    {
                        *avg_result = result_data * 1.0 / (1000 * gc1->column_pointer.result->num_tuples);
                        *avg_result *= 1000;
                    }
                    else
                    {
                        *avg_result = 0;
                    }
                    result->payload = avg_result;
                    result->num_tuples = 1;
                }
                else
                { // agg_type == SUM
                    result->data_type = LONG;

                    long *result_data = malloc(1 * sizeof(long));
                    *result_data = 0;
                    long *payload = (long *)gc1->column_pointer.result->payload;
                    for (size_t i = 0; i < gc1->column_pointer.result->num_tuples; i++)
                    {
                        *result_data += payload[i];
                    }
                    result->payload = result_data;
                    result->num_tuples = 1;
                }
            }
        }
        else
        { // column
            if (agg_type == AVG)
            {
                result->data_type = FLOAT;

                float *result_data = malloc(1 * sizeof(float));
                *result_data = 0.0;
                for (size_t i = 0; i < gc1->column_pointer.column->length; i++)
                {
                    *result_data += gc1->column_pointer.column->data[i];
                }
                *result_data = *result_data / gc1->column_pointer.column->length;
                result->payload = result_data;
                result->num_tuples = 1;
            }
            else
            { // agg_type == SUM
                result->data_type = LONG;

                long *result_data = malloc(1 * sizeof(long));
                *result_data = 0;
                for (size_t i = 0; i < gc1->column_pointer.column->length; i++)
                {
                    *result_data += gc1->column_pointer.column->data[i];
                }
                result->payload = result_data;
                result->num_tuples = 1;
            }
            free(gc1);
        }
    }
    else if (agg_type == ADD || agg_type == SUB)
    {
        GeneralizedColumn *gc1 = query->operator_fields.aggregate_operator.gc1;
        GeneralizedColumn *gc2 = query->operator_fields.aggregate_operator.gc2;

        if (gc1->column_type == RESULT)
        {
            if (gc2->column_type == RESULT)
            { // RESULT RESULT
                Result *data1 = gc1->column_pointer.result;
                Result *data2 = gc2->column_pointer.result;
                if (data1->num_tuples != data2->num_tuples)
                {
                    send_message->status = INCORRECT_FORMAT;
                    return;
                }
                if (data1->data_type == INT)
                { // int
                    if (data2->data_type == INT)
                    { // int int
                        long *payload = malloc(data1->num_tuples * sizeof(long));
                        int *payload1 = data1->payload;
                        int *payload2 = data2->payload;
                        for (size_t i = 0; i < data1->num_tuples; i++)
                        {
                            payload[i] = agg_type == ADD ? payload1[i] + payload2[i] : payload1[i] - payload2[i];
                        }
                        result->num_tuples = data1->num_tuples;
                        result->data_type = LONG;
                        result->payload = payload;
                    }
                    else if (data2->data_type == LONG)
                    { // int long
                        long *payload = malloc(data1->num_tuples * sizeof(long));
                        int *payload1 = data1->payload;
                        long *payload2 = data2->payload;
                        for (size_t i = 0; i < data1->num_tuples; i++)
                        {
                            payload[i] = agg_type == ADD ? payload1[i] + payload2[i] : payload1[i] - payload2[i];
                        }
                        result->num_tuples = data1->num_tuples;
                        result->data_type = LONG;
                        result->payload = payload;
                    }
                    else
                    { // int= float
                        float *payload = malloc(data1->num_tuples * sizeof(float));
                        int *payload1 = data1->payload;
                        float *payload2 = data2->payload;
                        for (size_t i = 0; i < data1->num_tuples; i++)
                        {
                            payload[i] = agg_type == ADD ? payload1[i] + payload2[i] : payload1[i] - payload2[i];
                        }
                        result->num_tuples = data1->num_tuples;
                        result->data_type = FLOAT;
                        result->payload = payload;
                    }
                }
                else if (data1->data_type == LONG)
                { // long
                    if (data2->data_type == INT)
                    { // long int
                        long *payload = malloc(data1->num_tuples * sizeof(long));
                        long *payload1 = data1->payload;
                        int *payload2 = data2->payload;
                        for (size_t i = 0; i < data1->num_tuples; i++)
                        {
                            payload[i] = agg_type == ADD ? payload1[i] + payload2[i] : payload1[i] - payload2[i];
                        }
                        result->num_tuples = data1->num_tuples;
                        result->data_type = LONG;
                        result->payload = payload;
                    }
                    else if (data2->data_type == LONG)
                    { // long long
                        long *payload = malloc(data1->num_tuples * sizeof(long));
                        long *payload1 = data1->payload;
                        long *payload2 = data2->payload;
                        for (size_t i = 0; i < data1->num_tuples; i++)
                        {
                            payload[i] = agg_type == ADD ? payload1[i] + payload2[i] : payload1[i] - payload2[i];
                        }
                        result->num_tuples = data1->num_tuples;
                        result->data_type = LONG;
                        result->payload = payload;
                    }
                    else
                    { // int/long float
                        float *payload = malloc(data1->num_tuples * sizeof(float));
                        long *payload1 = data1->payload;
                        float *payload2 = data2->payload;
                        for (size_t i = 0; i < data1->num_tuples; i++)
                        {
                            payload[i] = agg_type == ADD ? payload1[i] + payload2[i] : payload1[i] - payload2[i];
                        }
                        result->num_tuples = data1->num_tuples;
                        result->data_type = FLOAT;
                        result->payload = payload;
                    }
                }
                else
                { // float
                    if (data2->data_type == INT)
                    { // float int
                        float *payload = malloc(data1->num_tuples * sizeof(float));
                        float *payload1 = data1->payload;
                        int *payload2 = data2->payload;
                        for (size_t i = 0; i < data1->num_tuples; i++)
                        {
                            payload[i] = agg_type == ADD ? payload1[i] + payload2[i] : payload1[i] - payload2[i];
                        }
                        result->num_tuples = data1->num_tuples;
                        result->data_type = FLOAT;
                        result->payload = payload;
                    }
                    else if (data2->data_type == LONG)
                    { // float long
                        float *payload = malloc(data1->num_tuples * sizeof(float));
                        float *payload1 = data1->payload;
                        long *payload2 = data2->payload;
                        for (size_t i = 0; i < data1->num_tuples; i++)
                        {
                            payload[i] = agg_type == ADD ? payload1[i] + payload2[i] : payload1[i] - payload2[i];
                        }
                        result->num_tuples = data1->num_tuples;
                        result->data_type = FLOAT;
                        result->payload = payload;
                    }
                    else
                    { // float float
                        float *payload = malloc(data1->num_tuples * sizeof(float));
                        float *payload1 = data1->payload;
                        float *payload2 = data2->payload;
                        for (size_t i = 0; i < data1->num_tuples; i++)
                        {
                            payload[i] = agg_type == ADD ? payload1[i] + payload2[i] : payload1[i] - payload2[i];
                        }
                        result->num_tuples = data1->num_tuples;
                        result->data_type = FLOAT;
                        result->payload = payload;
                    }
                }
            }
            else
            { // RESULT COLUMN
                Result *data1 = gc1->column_pointer.result;
                Column *data2 = gc2->column_pointer.column;
                if (data1->num_tuples != data2->length)
                {
                    send_message->status = INCORRECT_FORMAT;
                    return;
                }
                if (data1->data_type == INT)
                { // int int
                    long *payload = malloc(data1->num_tuples * sizeof(long));
                    int *payload1 = data1->payload;
                    int *payload2 = data2->data;
                    for (size_t i = 0; i < data1->num_tuples; i++)
                    {
                        payload[i] = agg_type == ADD ? payload1[i] + payload2[i] : payload1[i] - payload2[i];
                    }
                    result->num_tuples = data1->num_tuples;
                    result->data_type = LONG;
                    result->payload = payload;
                }
                else if (data1->data_type == LONG)
                { // long int
                    long *payload = malloc(data1->num_tuples * sizeof(long));
                    long *payload1 = data1->payload;
                    int *payload2 = data2->data;
                    for (size_t i = 0; i < data1->num_tuples; i++)
                    {
                        payload[i] = agg_type == ADD ? payload1[i] + payload2[i] : payload1[i] - payload2[i];
                    }
                    result->num_tuples = data1->num_tuples;
                    result->data_type = LONG;
                    result->payload = payload;
                }
                else
                { // float int
                    float *payload = malloc(data1->num_tuples * sizeof(float));
                    float *payload1 = data1->payload;
                    int *payload2 = data2->data;
                    for (size_t i = 0; i < data1->num_tuples; i++)
                    {
                        payload[i] = agg_type == ADD ? payload1[i] + payload2[i] : payload1[i] - payload2[i];
                    }
                    result->num_tuples = data1->num_tuples;
                    result->data_type = FLOAT;
                    result->payload = payload;
                }
                free(gc2);
            }
        }
        else
        { // COLUMN RESULT
            if (gc2->column_type == RESULT)
            {
                Column *data1 = gc1->column_pointer.column;
                Result *data2 = gc2->column_pointer.result;
                if (data1->length != data2->num_tuples)
                {
                    send_message->status = INCORRECT_FORMAT;
                    return;
                }
                if (data2->data_type == INT)
                { // int int
                    long *payload = malloc(data2->num_tuples * sizeof(long));
                    int *payload1 = data1->data;
                    int *payload2 = data2->payload;
                    for (size_t i = 0; i < data2->num_tuples; i++)
                    {
                        payload[i] = agg_type == ADD ? payload1[i] + payload2[i] : payload1[i] - payload2[i];
                    }
                    result->num_tuples = data2->num_tuples;
                    result->data_type = LONG;
                    result->payload = payload;
                }
                else if (data2->data_type == LONG)
                { // int long
                    long *payload = malloc(data2->num_tuples * sizeof(long));
                    int *payload1 = data1->data;
                    long *payload2 = data2->payload;
                    for (size_t i = 0; i < data2->num_tuples; i++)
                    {
                        payload[i] = agg_type == ADD ? payload1[i] + payload2[i] : payload1[i] - payload2[i];
                    }
                    result->num_tuples = data2->num_tuples;
                    result->data_type = LONG;
                    result->payload = payload;
                }
                else
                { // int float
                    float *payload = malloc(data2->num_tuples * sizeof(float));
                    int *payload1 = data1->data;
                    float *payload2 = data2->payload;
                    for (size_t i = 0; i < data2->num_tuples; i++)
                    {
                        payload[i] = agg_type == ADD ? payload1[i] + payload2[i] : payload1[i] - payload2[i];
                    }
                    result->num_tuples = data2->num_tuples;
                    result->data_type = FLOAT;
                    result->payload = payload;
                }
                free(gc1);
            }
            else
            { // COLUMN COLUMN
                Column *data1 = gc1->column_pointer.column;
                Column *data2 = gc2->column_pointer.column;
                if (data1->length != data2->length)
                {
                    send_message->status = INCORRECT_FORMAT;
                    return;
                }
                // int int
                long *payload = malloc(data2->length * sizeof(long));
                int *payload1 = data1->data;
                int *payload2 = data2->data;
                for (size_t i = 0; i < data2->length; i++)
                {
                    payload[i] = agg_type == ADD ? payload1[i] + payload2[i] : payload1[i] - payload2[i];
                }
                result->num_tuples = data2->length;
                result->data_type = LONG;
                result->payload = payload;
                free(gc1);
                free(gc2);
            }
        }
    }
    else if (agg_type == MAX || agg_type == MIN)
    {
        if (query->operator_fields.aggregate_operator.variable_number == 1)
        {
            GeneralizedColumn *gc1 = query->operator_fields.aggregate_operator.gc1;
            if (gc1->column_type == RESULT)
            {
                Result *data1 = gc1->column_pointer.result;
                if (data1->data_type == INT)
                {
                    int *result_data = malloc(1 * sizeof(int));
                    if (agg_type == MAX)
                    {
                        *result_data = -__INT_MAX__ - 1;
                        int *payload = data1->payload;
                        for (size_t i = 0; i < data1->num_tuples; i++)
                        {
                            *result_data = payload[i] > *result_data ? payload[i] : *result_data;
                        }
                    }
                    else
                    { // M_I_N
                        *result_data = __INT_MAX__;
                        int *payload = data1->payload;
                        for (size_t i = 0; i < data1->num_tuples; i++)
                        {
                            *result_data = payload[i] < *result_data ? payload[i] : *result_data;
                        }
                    }
                    result->data_type = INT;
                    result->payload = result_data;
                    result->num_tuples = 1;
                }
                else if (data1->data_type == LONG)
                {
                    long *result_data = malloc(1 * sizeof(long));
                    if (agg_type == MAX)
                    {
                        *result_data = -__INT_MAX__ - 1;
                        long *payload = data1->payload;
                        for (size_t i = 0; i < data1->num_tuples; i++)
                        {
                            *result_data = payload[i] > *result_data ? payload[i] : *result_data;
                        }
                    }
                    else
                    { // M_I_N
                        *result_data = __INT_MAX__;
                        long *payload = data1->payload;
                        for (size_t i = 0; i < data1->num_tuples; i++)
                        {
                            *result_data = payload[i] < *result_data ? payload[i] : *result_data;
                        }
                    }
                    result->data_type = LONG;
                    result->payload = result_data;
                    result->num_tuples = 1;
                }
                else
                { // float
                    float *result_data = malloc(1 * sizeof(float));
                    if (agg_type == MAX)
                    {
                        *result_data = -__INT_MAX__ - 1;
                        float *payload = data1->payload;
                        for (size_t i = 0; i < data1->num_tuples; i++)
                        {
                            *result_data = payload[i] > *result_data ? payload[i] : *result_data;
                        }
                    }
                    else
                    { // M_I_N
                        *result_data = __INT_MAX__;
                        float *payload = data1->payload;
                        for (size_t i = 0; i < data1->num_tuples; i++)
                        {
                            *result_data = payload[i] < *result_data ? payload[i] : *result_data;
                        }
                    }
                    result->data_type = FLOAT;
                    result->payload = result_data;
                    result->num_tuples = 1;
                }
            }
            else
            { // COLUMN int
                Column *data1 = gc1->column_pointer.column;
                int *result_data = malloc(1 * sizeof(int));
                if (agg_type == MAX)
                {
                    *result_data = -__INT_MAX__ - 1;
                    int *payload = data1->data;
                    for (size_t i = 0; i < data1->length; i++)
                    {
                        *result_data = payload[i] > *result_data ? payload[i] : *result_data;
                    }
                }
                else
                { // M_I_N
                    *result_data = __INT_MAX__;
                    int *payload = data1->data;
                    for (size_t i = 0; i < data1->length; i++)
                    {
                        *result_data = payload[i] < *result_data ? payload[i] : *result_data;
                    }
                }
                result->data_type = INT;
                result->payload = result_data;
                result->num_tuples = 1;
                free(gc1);
            }
        }
        else
        { // 2 generalized columns
        }
    }
    else
    {
        send_message->status = INCORRECT_FORMAT;
        free(result);
        return;
    }
    add_context(result, client_context, query->operator_fields.aggregate_operator.intermediate);
}
void grace_hash_join_partition(void *args)
{
    thread_args *arguments = (thread_args *)args;
    for (size_t j = 0; j < arguments->len; j++)
    {
        int index_p = arguments->column[j] / arguments->p_div;
        arguments->partitions[index_p].values[arguments->partitions[index_p].p_len] = arguments->column[j];
        arguments->partitions[index_p].positions[arguments->partitions[index_p].p_len] = j;
        arguments->partitions[index_p].p_len++;
        if (arguments->partitions[index_p].p_len >= arguments->partitions[index_p].p_capacity)
        {
            // reallocate and increment size by twice
            arguments->partitions[index_p].p_capacity *= 2;
            arguments->partitions[index_p].values = realloc(arguments->partitions[index_p].values, arguments->partitions[index_p].p_capacity * sizeof(int));
            arguments->partitions[index_p].positions = realloc(arguments->partitions[index_p].positions, arguments->partitions[index_p].p_capacity * sizeof(size_t));
        }
    }
    pthread_mutex_lock(&lock);
    // record the number of successful tasks completed
    done++;
    pthread_mutex_unlock(&lock);
    // free arguments
    free(arguments);
}
void grace_hash_join(void *args)
{
    thread_args *arguments = (thread_args *)args;
    // always hash on the small column
    hashtable *ht = malloc(sizeof(struct hashtable));
    // compare and swap
    if (arguments->partitionR->p_len <= arguments->partitionL->p_len)
    {
        size_t res[arguments->partitionR->p_len];
        allocate_ht(ht, arguments->partitionR->p_len );
        for (size_t j = 0; j < arguments->partitionR->p_len; j++)
        {
            put_ht(ht, arguments->partitionR->values[j], arguments->partitionR->positions[j]);
        }
        for (size_t t = 0; t < arguments->partitionL->p_len; t++)
        {
            size_t res_len = get_ht(ht, arguments->partitionL->values[t], res);
            for (size_t j = 0; j < res_len; j++)
            {
                pthread_mutex_lock(&grace_hash_lock);
                arguments->resL[mutex_k] = arguments->partitionL->positions[t];
                arguments->resR[mutex_k++] = res[j];
                pthread_mutex_unlock(&grace_hash_lock);
            }
        }
    }
    else
    {
        size_t res[arguments->partitionL->p_len];
        allocate_ht(ht, arguments->partitionL->p_len);
        for (size_t j = 0; j < arguments->partitionL->p_len; j++)
        {
            put_ht(ht, arguments->partitionL->values[j], arguments->partitionL->positions[j]);
        }
        for (size_t t = 0; t < arguments->partitionR->p_len; t++)
        {
            size_t res_len = get_ht(ht, arguments->partitionR->values[t], res);
            for (size_t j = 0; j < res_len; j++)
            {
                pthread_mutex_lock(&grace_hash_lock);
                arguments->resR[mutex_k] = arguments->partitionR->positions[t];
                arguments->resL[mutex_k++] = res[j];
                pthread_mutex_unlock(&grace_hash_lock);
            }
        }
    }
    deallocate_ht(ht);
    pthread_mutex_lock(&lock);
    // record the number of successful tasks completed
    done++;
    pthread_mutex_unlock(&lock);
    // free arguments
    free(arguments);
}
void execute_join(DbOperator *query, message *send_message)
{
    ClientContext *client_context = query->context;
    Result *f1 = query->operator_fields.join_operator.f1;
    Result *f2 = query->operator_fields.join_operator.f2;
    Result *p1 = query->operator_fields.join_operator.p1;
    Result *p2 = query->operator_fields.join_operator.p2;

    int *L = (int *)f1->payload;
    int *R = (int *)f2->payload;
    size_t *posL = (size_t *)p1->payload;
    size_t *posR = (size_t *)p2->payload;
    size_t lenL = p1->num_tuples;
    size_t lenR = p2->num_tuples;
    size_t *resL = malloc(lenR * lenL * sizeof(size_t));
    size_t *resR = malloc(lenR * lenL * sizeof(size_t));
    size_t k = 0;

    if (query->operator_fields.join_operator.joinType == NESTED_LOOP_JOIN)
    {

        for (size_t i = 0; i < lenL; i++)
        {
            for (size_t j = 0; j < lenR; j++)
            {
                if (L[i] == R[j])
                {
                    resL[k] = posL[i];
                    resR[k++] = posR[j];
                }
            }
        }
    }
    else
    { // HASH JOIN
        if (lenL <= CACHE_SIZE_THRESHOLD || lenR <= CACHE_SIZE_THRESHOLD)
        {
            // always hash on the small column
            hashtable *ht = malloc(sizeof(struct hashtable));
            // compare and swap
            if (lenR <= lenL)
            {
                size_t res[lenR];
                allocate_ht(ht, lenR);
                for (size_t j = 0; j < lenR; j++)
                {
                    put_ht(ht, R[j], posR[j]);
                }
                for (size_t i = 0; i < lenL; i++)
                {
                    size_t res_len = get_ht(ht, L[i], res);
                    for (size_t j = 0; j < res_len; j++)
                    {
                        resL[k] = posL[i];
                        resR[k++] = res[j];
                    }
                }
            }
            else
            {
                size_t res[lenL];
                allocate_ht(ht, lenL);
                for (size_t j = 0; j < lenL; j++)
                {
                    put_ht(ht, L[j], posL[j]);
                }
                for (size_t i = 0; i < lenR; i++)
                {
                    size_t res_len = get_ht(ht, R[i], res);
                    for (size_t j = 0; j < res_len; j++)
                    {
                        resR[k] = posR[i];
                        resL[k++] = res[j];
                    }
                }
            }
            deallocate_ht(ht);
        }
        else
        {
            // GRACE HASH JOIN
            // 1. find ranges
            // find max value in L
            int max;
            max = L[0];
            for (int t = 1; t < lenL; t++)
            {
                if (L[t] > max)
                    max = L[t];
            }
            int p_div = max / (NUM_PARTITIONS - 1); // #_p = value / p_div
            size_t capacity = lenL / NUM_PARTITIONS;
            // TODO: free partitions
            Partition *partitionsL = malloc(NUM_PARTITIONS * sizeof(Partition));
            Partition *partitionsR = malloc(NUM_PARTITIONS * sizeof(Partition));
            for (int i = 0; i < NUM_PARTITIONS; i++)
            {
                partitionsL[i].p_capacity = capacity;
                partitionsL[i].p_len = 0;
                partitionsL[i].values = malloc(capacity * sizeof(int));
                partitionsL[i].positions = malloc(capacity * sizeof(size_t));
                partitionsR[i].p_capacity = capacity;
                partitionsR[i].p_len = 0;
                partitionsR[i].values = malloc(capacity * sizeof(int));
                partitionsR[i].positions = malloc(capacity * sizeof(size_t));
            }

            // 2. build partitions
            tasks = 0;
            done = 0;
            pthread_mutex_init(&lock, NULL);
            if ((pool = threadpool_create(2, QUEUE, 0)) == NULL)
            {
                send_message->status = EXECUTION_ERROR;
                return;
            }
            thread_args *argsL = malloc(sizeof(thread_args));
            argsL->partitions = partitionsL;
            argsL->len = lenL;
            argsL->column = L;
            argsL->p_div = p_div;
            argsL->send_message = send_message;
            if (threadpool_add(pool, &grace_hash_join_partition, (void *)argsL, 0) == 0)
            {
                pthread_mutex_lock(&lock);
                tasks++;
                pthread_mutex_unlock(&lock);
            }
            else
            {
                cs165_log(stdout, "Start pool failed\n");
                send_message->status = EXECUTION_ERROR;
                return;
            }
            thread_args *argsR = malloc(sizeof(thread_args));
            argsR->partitions = partitionsR;
            argsR->len = lenR;
            argsR->column = R;
            argsR->p_div = p_div;
            argsR->send_message = send_message;
            if (threadpool_add(pool, &grace_hash_join_partition, (void *)argsR, 0) == 0)
            {
                pthread_mutex_lock(&lock);
                tasks++;
                pthread_mutex_unlock(&lock);
            }
            else
            {
                cs165_log(stdout, "Start pool failed\n");
                send_message->status = EXECUTION_ERROR;
                return;
            }

            while (done < tasks)
            {
                sleep(0.01);
            }
            pthread_mutex_init(&grace_hash_lock, NULL);

            for (int i = 0; i < NUM_PARTITIONS; i++)
            {
                thread_args *args = malloc(sizeof(thread_args));
                args->partitionR = &partitionsR[i];
                args->partitionL = &partitionsL[i];
                args->resR = resR;
                args->resL = resL;

                if (threadpool_add(pool, &grace_hash_join, (void *)args, 0) == 0)
                {
                    pthread_mutex_lock(&lock);
                    tasks++;
                    pthread_mutex_unlock(&lock);
                }
                else
                {
                    cs165_log(stdout, "Start pool failed\n");
                    send_message->status = EXECUTION_ERROR;
                    return;
                }
            }
            pthread_mutex_destroy(&grace_hash_lock);
            pthread_mutex_destroy(&lock);
        }
    }

    Result *resultL = malloc(sizeof(Result));
    resultL->data_type = LONG;
    resultL->num_tuples = k;
    resultL->payload = resL;
    add_context(resultL, client_context, query->operator_fields.join_operator.l_name);
    Result *resultR = malloc(sizeof(Result));
    resultR->data_type = LONG;
    resultR->num_tuples = k;
    resultR->payload = resR;
    add_context(resultR, client_context, query->operator_fields.join_operator.r_name);

    send_message->status = OK_DONE;
}

void execute_print(DbOperator *query, message *send_message)
{
    // find specified positions vector in client context
    ClientContext *client_context = query->context;
    void *print_data;
    size_t print_len;
    DataType data_type;
    Result **results = malloc(query->operator_fields.print_operator.number_intermediates * sizeof(Result *));
    char **intermediates = query->operator_fields.print_operator.intermediates;
    for (size_t j = 0; j < query->operator_fields.print_operator.number_intermediates; j++)
    {
        char *intermediate = intermediates[j];
        GeneralizedColumn *generalized_column = lookup_variables(NULL, NULL, NULL, intermediate, client_context);
        results[j] = generalized_column->column_pointer.result;
    }
    print_len = results[0]->num_tuples;

    char *print_chars = malloc(32 * query->operator_fields.print_operator.number_intermediates * print_len * sizeof(char));
    char *print_chars_ptr = print_chars;

    int len;
    for (size_t i = 0; i < print_len; i++)
    {
        for (size_t j = 0; j < query->operator_fields.print_operator.number_intermediates; j++)
        {
            print_data = results[j]->payload;
            data_type = results[j]->data_type;
            if (data_type == INT)
            {
                int *print_int = (int *)print_data;
                if (j == 0)
                    sprintf(print_chars_ptr, "%d", print_int[i]);
                else
                    sprintf(print_chars_ptr, ",%d", print_int[i]);
                // printf("int %d",print_int[i]);
            }
            else if (data_type == LONG)
            {
                size_t *print_long = (size_t *)print_data;
                if (j == 0)
                    sprintf(print_chars_ptr, "%ld", print_long[i]);
                else
                    sprintf(print_chars_ptr, ",%ld", print_long[i]);
                // printf("long %ld",print_long[i]);
            }
            else if (data_type == FLOAT)
            {
                float *print_float = (float *)print_data;
                if (j == 0)
                    sprintf(print_chars_ptr, "%.2f", print_float[i]);
                else
                    sprintf(print_chars_ptr, ",%.2f", print_float[i]);
                // printf("float %.2f", print_float[i]);
            }
            else if (data_type == DOUBLE)
            {
                double *print_double = (double *)print_data;
                if (j == 0)
                    sprintf(print_chars_ptr, "%.2lf", print_double[i]);
                else
                    sprintf(print_chars_ptr, ",%.2lf", print_double[i]);
            }
            print_chars_ptr = strlen(print_chars) + print_chars;
        }
        // send(query->client_fd, &print_chars, sizeof(print_chars), 0);
        sprintf(print_chars_ptr, "\n");
        print_chars_ptr++;
    }

    // TODO: here send_buffer/payload is not assigned
    size_t str_len = strlen(print_chars);
    send_message->length = str_len;
    send_message->status = OK_WAIT_FOR_RESPONSE;
    send(query->client_fd, send_message, sizeof(message), 0);
    size_t offset = 0;
    while (offset < str_len)
    {
        len = send(query->client_fd, print_chars + offset, DEFAULT_QUERY_BUFFER_SIZE, 0);
        offset += len;
        // printf("sent %ld, remain %ld\n", offset, str_len - offset);
    }
    // Just print for checking on server side
    send_message->status = OK_PRINT;
    free(print_chars);
    free(results);
}

// TODO: check batch start
void execute_batch_start(ClientContext *client_context, message *send_message)
{
    // set batch mode to true
    client_context->batch_mode = true;

    // Initialize mutex
    pthread_mutex_init(&lock, NULL);
    if ((pool = threadpool_create(THREAD, QUEUE, 0)) == NULL)
    {
        send_message->status = EXECUTION_ERROR;
        return;
    }

    // allocate size in client context to store db operators
    // client_context->batch_queries = malloc(HANDLE_MAX_SIZE * sizeof(DbOperator*));
    send_message->status = OK_DONE;
}

// server: Thread pool, take query on the fly
//      queue for clients (bonus)
// client: ClientContext
void execute_batch_select(DbOperator *query, message *send_message)
{
    thread_args *args = malloc(sizeof(thread_args));
    args->query = query;
    args->send_message = send_message;
    if (threadpool_add(pool, &batch_execute_select, (void *)args, 0) == 0)
    {
        pthread_mutex_lock(&lock);
        tasks++;
        pthread_mutex_unlock(&lock);
    }
    else
    {
        cs165_log(stdout, "Start pool failed\n");
        send_message->status = EXECUTION_ERROR;
        return;
    }
    send_message->status = BATCH_WAIT;
    free(args);
}
// TODO: check batch end
void execute_batch_end(ClientContext *client_context, message *send_message)
{
    while (tasks > done)
    {
        sleep(0.01); // TODO: change sleep time
    }
    // At this point, destroy the thread pool, 0 for immediate_shutdown
    if (threadpool_destroy(pool, 0) != 0)
    {
        cs165_log(stdout, "Destroy pool failed\n");
        send_message->status = EXECUTION_ERROR; // TODO: check how to deal with pool destruction error
        return;
    }
    client_context->batch_mode = false;
    pthread_mutex_destroy(&lock);
    // free(client_context->batch_queries);
    send_message->status = OK_DONE;
}

void execute_shutdown(ClientContext *client_context, message *send_message)
{
    // free client context
    deallocate(client_context);
    // TODO: move the following executions to server side
    persist_database();
    free_database();
    send_message->status = OK_SHUTDOWN;
}
/** execute_DbOperator takes as input the DbOperator and executes the query.
 * This should be replaced in your implementation (and its implementation possibly moved to a different file).
 * It is currently here so that you can verify that your server and client can send messages.
 * 
 * Getting started hints: 
 *      What are the structural attributes of a `query`?
 *      How will you interpret different queries?
 *      How will you ensure different queries invoke different execution paths in your code?
 **/
char *execute_DbOperator(DbOperator *query, message *send_message)
{
    //////////////////////////////////
    // TODO: Solve memory leak
    //////////////////////////////////
    // there is a small memory leak here (when combined with other parts of your database.)
    // as practice with something like valgrind and to develop intuition on memory leaks, find and fix the memory leak.
    if (!query)
    {
        return "165";
    }
    if (query && query->type == CREATE)
    {
        execute_create(query, send_message);
    }
    else if (query && query->type == INSERT)
    {
        execute_insert(query, send_message);
        free(query->operator_fields.insert_operator.values);
    }
    else if (query && query->type == LOAD)
    {
        execute_load(query, send_message);
    }
    else if (query && query->type == SELECT && query->context->batch_mode == true)
    {
        execute_batch_select(query, send_message);
    }
    else if (query && query->type == SELECT && query->context->batch_mode == false)
    {
        execute_select(query, send_message);
    }
    else if (query && query->type == FETCH)
    {
        execute_fetch(query, send_message);
    }
    else if (query && query->type == AGGREGATE)
    {
        execute_aggregate(query, send_message);
    }
    else if (query && query->type == PRINT)
    {
        execute_print(query, send_message);
        free(query->operator_fields.print_operator.intermediates);
    }
    else if (query && query->type == SHUTDOWN)
    {
        execute_shutdown(query->context, send_message);
    }
    else if (query && query->type == BATCH_START)
    {
        execute_batch_start(query->context, send_message);
    }
    else if (query && query->type == BATCH_END)
    {
        execute_batch_end(query->context, send_message);
    }
    else if (query && query->type == JOIN)
    {
        execute_join(query, send_message);
    }
    return "165";
}

/**
 * handle_client(client_socket)
 * This is the execution routine after a client has connected.
 * It will continually listen for messages from the client and execute queries.
 **/
void handle_client(int client_socket)
{
    int done = 0;
    int length = 0;

    log_info("Connected to socket: %d.\n", client_socket);

    // Create two messages, one from which to read and one from which to receive
    message send_message;
    message recv_message;

    // create the client context here
    ClientContext *client_context;
    allocate(&client_context, CONTEXT_CAPACIRY);
    client_context->batch_mode = false;

    // Continually receive messages from client and execute queries.
    // 1. Parse the command
    // 2. Handle request if appropriate
    // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
    // 4. Send response to the request.
    do
    {
        length = recv(client_socket, &recv_message, sizeof(message), 0);
        if (length < 0)
        {
            log_err("Client connection closed!\n");
            break;
        }
        else if (length == 0)
        {
            done = 1;
        }

        if (!done)
        {
            char recv_buffer[recv_message.length + 1];
            length = recv(client_socket, recv_buffer, recv_message.length, 0);
            recv_message.payload = recv_buffer;
            recv_message.payload[recv_message.length] = '\0';
            /////////////////////////
            // Important workflow
            /////////////////////////
            // 1. Parse command
            //    Query string is converted into a request for an database operator
            DbOperator *query = parse_command(recv_message.payload, &send_message, client_socket, client_context);

            // 2. Handle request
            //    Corresponding database operator is exsecuted over the query
            char *result = execute_DbOperator(query, &send_message);
            // TODO: Check
            // free DbOperator
            if (send_message.status != BATCH_WAIT)
            {
                free(query);
            }
            if (send_message.status == OK_PRINT)
            {
                continue;
            }
            if (send_message.status == OK_SHUTDOWN)
            {
                // send(client_socket, &(send_message), sizeof(message), 0);
                exit(0);
            }
            send_message.length = strlen(result);
            char send_buffer[send_message.length + 1];
            strcpy(send_buffer, result);
            send_message.payload = send_buffer;
            send_message.status = OK_WAIT_FOR_RESPONSE;

            // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
            if (send(client_socket, &(send_message), sizeof(message), 0) == -1)
            {
                log_err("Failed to send message.");
                // TODO: move the following executions to server side
                // persist_database();
                // free_database();
                // exit(1);
            }
            // 4. Send response to the request
            if (send(client_socket, result, send_message.length, 0) == -1)
            {
                log_err("Failed to send message.");
                // exit(1);
            }
        }
    } while (!done);

    deallocate(client_context);

    log_info("Connection closed at socket %d!\n", client_socket);
    close(client_socket);
}

/**
 * setup_server()
 *
 * This sets up the connection on the server side using unix sockets.
 * Returns a valid server socket fd on success, else -1 on failure.
 **/
int setup_server()
{
    int server_socket;
    size_t len;
    struct sockaddr_un local;

    cs165_log(stdout, "Attempting to setup server...\n");

    if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1)
    {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    local.sun_family = AF_UNIX;
    strncpy(local.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    unlink(local.sun_path);

    /*
    int on = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0)
    {
        log_err("L%d: Failed to set socket as reusable.\n", __LINE__);
        return -1;
    }
    */

    len = strlen(local.sun_path) + sizeof(local.sun_family) + 1;
    if (bind(server_socket, (struct sockaddr *)&local, len) == -1)
    {
        log_err("L%d: Socket failed to bind.\n", __LINE__);
        return -1;
    }

    if (listen(server_socket, 5) == -1)
    {
        log_err("L%d: Failed to listen on socket.\n", __LINE__);
        return -1;
    }

    return server_socket;
}

// Currently this main will setup the socket and accept a single client.
// After handling the client, it will exit.
// You WILL need to extend this to handle MULTIPLE concurrent clients
// and remain running until it receives a shut-down command.

// Getting Started Hints:
//      How will you extend main to handle multiple concurrent clients?
//      Is there a maximum number of concurrent client connections you will allow?
//      What aspects of siloes or isolation are maintained in your design? (Think `what` is shared between `whom`?)
int main(void)
{
    int db = load_database();
    if (db < 0)
    {
        cs165_log(stdout, "No current database ...\n");
    }

    int server_socket = setup_server();
    if (server_socket < 0)
    {
        exit(1);
    }

    log_info("Waiting for a connection %d ...\n", server_socket);

    struct sockaddr_un remote;
    socklen_t t = sizeof(remote);
    int client_socket = 0;

    // TODO: handle multiple clients
    while ((client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)))
    {
        log_info("Connection accepted.\n", server_socket);
        handle_client(client_socket);
    }
    if (client_socket == -1)
    {
        log_err("L%d: Failed to accept a new connection.\n", __LINE__);
        // TODO: move the following executions to server side
        persist_database();
        free_database();
        exit(0);
    }
    return 0;
}
