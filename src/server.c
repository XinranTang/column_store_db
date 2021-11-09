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

#define DEFAULT_QUERY_BUFFER_SIZE 1024
#define THREAD 32
#define QUEUE 256
threadpool_t *pool;
int tasks = 0, done = 0;
pthread_mutex_t lock;

typedef struct thread_args
{
    DbOperator *query;
    message *send_message;
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
                      query->operator_fields.create_operator.sorted,
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
        create_index(query->operator_fields.create_operator.table,
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

            // receive number_lines from client
            size_t number_lines;
            load_message_header.status = OK_WAIT_FOR_RESPONSE;
            send(query->client_fd, &(load_message_header), sizeof(load_message_header), 0);
            recv(query->client_fd, &(number_lines), sizeof(size_t), 0);
            printf("Number of lines to load: %ld", number_lines);
            // set table length capacity for current table
            current_table->table_length_capacity = (number_lines - 1) * 2;
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

            char buffer[DEFAULT_QUERY_BUFFER_SIZE];
            size_t column = 0;
            size_t row = 0;
            // start receiving data body from the client
            // start loading line by line
            while ((len = recv(query->client_fd, &(buffer), DEFAULT_QUERY_BUFFER_SIZE, 0)) > 0)
            {
                if ((strncmp(buffer, "break", 5)) == 0)
                {
                    printf("break:%s", buffer);
                    break;
                }
                row++;
                current_table->table_length++;
                column = 0;
                char *data = strtok(buffer, ",");
                while (data)
                {
                    current_table->columns[column].data[row - 1] = atoi(data);
                    data = strtok(NULL, ",");
                    column++;
                }
            }
            // record the primary indexed column if exists
            size_t primary_index_column = -1;
            // set length of column and find primary indexed column
            for (size_t i = 0; i < current_table->col_count; i++)
            {
                current_table->columns[i].length = current_table->table_length;
                if (current_table->columns[i].clustered)
                {
                    if (primary_index_column != -1)
                    {
                        current_table->columns[i].clustered = false;
                    }
                    else
                    {
                        primary_index_column = i;
                    }
                }
                // syncing_column(&current_table->columns[i], current_table);
            }
            // process primary index
            // the primary index column is sorted, and the order is propagated to the whole table
            if (primary_index_column != -1)
            {
                build_primary_index(current_table, primary_index_column);
            }
            // process secondary indexes
            for (size_t i = 0; i < current_table->col_count; i++)
            {
                if (i == primary_index_column)
                    continue;
                if (current_table->columns[i].btree | current_table->columns[i].sorted)
                {
                    build_secondary_index(current_table, &current_table->columns[i], current_table->columns[i].btree, current_table->columns[i].sorted);
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

        for (size_t i = 0; i < query->operator_fields.select_operator.column_length; i++)
        {
            if (column->data[i] >= low && column->data[i] <= high)
                select_data[index++] = i;
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

        for (size_t i = 0; i < query->operator_fields.select_operator.column_length; i++)
        {
            if (column->data[i] >= low && column->data[i] <= high)
                select_data[index++] = i;
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
                result->data_type = FLOAT;

                float *result_data = malloc(1 * sizeof(float));
                *result_data = 0.0;

                float *payload = (float *)gc1->column_pointer.result->payload;
                for (size_t i = 0; i < gc1->column_pointer.result->num_tuples; i++)
                {
                    *result_data += payload[i];
                }
                if (agg_type == AVG)
                    *result_data = *result_data / gc1->column_pointer.result->num_tuples;
                result->payload = result_data;
                result->num_tuples = 1;
            }
            else if (gc1->column_pointer.result->data_type == INT)
            {

                if (agg_type == AVG)
                {
                    result->data_type = FLOAT;

                    float *result_data = malloc(1 * sizeof(float));
                    *result_data = 0.0;
                    int *payload = (int *)gc1->column_pointer.result->payload;
                    for (size_t i = 0; i < gc1->column_pointer.result->num_tuples; i++)
                    {
                        *result_data += payload[i];
                    }
                    *result_data = *result_data / gc1->column_pointer.result->num_tuples;
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
                    *avg_result = result_data * 1.0 / (1000 * gc1->column_pointer.result->num_tuples);
                    *avg_result *= 1000;
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
                            if (payload[i] > *result_data)
                                *result_data = payload[i];
                        }
                    }
                    else
                    { // M_I_N
                        *result_data = __INT_MAX__;
                        int *payload = data1->payload;
                        for (size_t i = 0; i < data1->num_tuples; i++)
                        {
                            if (payload[i] < *result_data)
                                *result_data = payload[i];
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
                            if (payload[i] > *result_data)
                                *result_data = payload[i];
                        }
                    }
                    else
                    { // M_I_N
                        *result_data = __INT_MAX__;
                        long *payload = data1->payload;
                        for (size_t i = 0; i < data1->num_tuples; i++)
                        {
                            if (payload[i] < *result_data)
                                *result_data = payload[i];
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
                            if (payload[i] > *result_data)
                                *result_data = payload[i];
                        }
                    }
                    else
                    { // M_I_N
                        *result_data = __INT_MAX__;
                        float *payload = data1->payload;
                        for (size_t i = 0; i < data1->num_tuples; i++)
                        {
                            if (payload[i] < *result_data)
                                *result_data = payload[i];
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
                        if (payload[i] > *result_data)
                            *result_data = payload[i];
                    }
                }
                else
                { // M_I_N
                    *result_data = __INT_MAX__;
                    int *payload = data1->data;
                    for (size_t i = 0; i < data1->length; i++)
                    {
                        if (payload[i] < *result_data)
                            *result_data = payload[i];
                    }
                }
                result->data_type = INT;
                result->payload = result_data;
                result->num_tuples = 1;
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

    // TODO: here send_buffer/payload is not assigned
    send_message->length = query->operator_fields.print_operator.number_intermediates;
    send_message->status = OK_WAIT_FOR_RESPONSE;
    char break_signal[32 * send_message->length];
    send_message->payload = break_signal;
    send(query->client_fd, send_message, sizeof(message), 0);
    for (size_t i = 0; i < print_len; i++)
    {
        char print_chars[32 * send_message->length];
        memset(print_chars, '\0', 32 * send_message->length);
        for (size_t j = 0; j < query->operator_fields.print_operator.number_intermediates; j++)
        {
            print_data = results[j]->payload;
            data_type = results[j]->data_type;
            if (data_type == INT)
            {
                int *print_int = (int *)print_data;
                if (j == 0)
                    sprintf(strlen(print_chars) + print_chars, "%d", print_int[i]);
                else
                    sprintf(strlen(print_chars) + print_chars, ",%d", print_int[i]);
                // printf("int %d",print_int[i]);
            }
            else if (data_type == LONG)
            {
                size_t *print_long = (size_t *)print_data;
                if (j == 0)
                    sprintf(strlen(print_chars) + print_chars, "%ld", print_long[i]);
                else
                    sprintf(strlen(print_chars) + print_chars, ",%ld", print_long[i]);
                // printf("long %ld",print_long[i]);
            }
            else if (data_type == FLOAT)
            {
                float *print_float = (float *)print_data;
                if (j == 0)
                    sprintf(strlen(print_chars) + print_chars, "%.2f", print_float[i]);
                else
                    sprintf(strlen(print_chars) + print_chars, ",%.2f", print_float[i]);
                // printf("float %.2f", print_float[i]);
            }
            else if (data_type == DOUBLE)
            {
                double *print_double = (double *)print_data;
                if (j == 0)
                    sprintf(strlen(print_chars) + print_chars, "%.2lf", print_double[i]);
                else
                    sprintf(strlen(print_chars) + print_chars, ",%.2lf", print_double[i]);
            }
        }
        send(query->client_fd, &print_chars, sizeof(print_chars), 0);
    }

    memset(break_signal, '\0', 32 * send_message->length);
    strcpy(break_signal, "break");
    send(query->client_fd, &break_signal, strlen(break_signal) + 1, 0);
    // Just print for checking on server side
    send_message->status = OK_PRINT;
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
}
// TODO: check batch end
void execute_batch_end(ClientContext *client_context, message *send_message)
{
    while (tasks > done)
    {
        // printf("Sleep %d %d\n", tasks, done);
        sleep(0.01); // TODO: change sleep time
        // printf("Sleep ENd %d %d\n", tasks, done);
    }
    // At this point, destroy the thread pool, 0 for immediate_shutdown
    if (threadpool_destroy(pool, 0) != 0)
    {
        cs165_log(stdout, "Destroy pool failed\n");
        send_message->status = EXECUTION_ERROR; // TODO: check how to deal with pool destruction error
        return;
    }
    client_context->batch_mode = false;
    // free(client_context->batch_queries);
    send_message->status = OK_DONE;
}

void execute_shutdown(ClientContext *client_context)
{
    // free client context
    deallocate(client_context);
    // TODO: move the following executions to server side
    persist_database();
    free_database();
    // TODO: send message to client side
    exit(0);
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
        execute_shutdown(query->context);
    }
    else if (query && query->type == BATCH_START)
    {
        execute_batch_start(query->context, send_message);
    }
    else if (query && query->type == BATCH_END)
    {
        execute_batch_end(query->context, send_message);
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
            return;
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
                free(query);
            if (send_message.status == OK_PRINT)
                continue;
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
//
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
        exit(1);
    }
    return 0;
}
