////////////////////////////
// TODO: change code to let server waiting for another client instead of shutting down
////////////////////////////
/** server.c
 * CS165 Fall 2018
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
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>

#include "common.h"
#include "parse.h"
#include "persist.h"
#include "cs165_api.h"
#include "message.h"
#include "utils.h"
#include "client_context.h"


#define DEFAULT_QUERY_BUFFER_SIZE 1024

void execute_create(DbOperator* query, message* send_message) {
    if(query->operator_fields.create_operator.create_type == _DB) {
            if (create_db(query->operator_fields.create_operator.name).code == OK) {
                send_message->status = OK_DONE;
                return;
            } else {
                send_message->status = OBJECT_ALREADY_EXISTS;
                return;
            }
        }
        else if(query->operator_fields.create_operator.create_type == _TABLE) {
            Status create_status;
            create_table(query->operator_fields.create_operator.db, 
                query->operator_fields.create_operator.name, 
                query->operator_fields.create_operator.col_count, 
                &create_status);
            if (create_status.code != OK) {
                send_message->status = EXECUTION_ERROR;
                return;
            }
            send_message->status = OK_DONE;
        }else if(query->operator_fields.create_operator.create_type == _COLUMN) {
            Status create_status;
            create_column(query->operator_fields.create_operator.table,
                query->operator_fields.create_operator.name,
                query->operator_fields.create_operator.sorted,
                &create_status);
            if (create_status.code != OK) {
                send_message->status = EXECUTION_ERROR;
                return;
            }
            send_message->status = OK_DONE;
        }
}

void execute_insert(DbOperator* query, message* send_message) {
    Table* insert_table = query->operator_fields.insert_operator.table;
    if (!insert_table) {
        send_message->status = OBJECT_NOT_FOUND;
        return;
    }
    // increase # of rows
    insert_table->table_length++;
    // if # of rows is larger than table length capacity, expand capacity
    if (insert_table->table_length > insert_table->table_length_capacity) {
        // reallocate column data sizes
        for (size_t i = 0; i < insert_table->col_count; i++) {
            // sync and unmap before expand capacity
            if (persist_column(insert_table, &insert_table->columns[i]) == -1) {
                cs165_log(stdout, "Memory syncing and unmapping failed.\n");
                send_message->status = EXECUTION_ERROR;
                return;
            }
        }
        insert_table->table_length_capacity = insert_table->table_length_capacity * 2;
        for (size_t i = 0; i < insert_table->col_count; i++) {
            // sync and unmap before expand capacity
            if (map_column(insert_table, &insert_table->columns[i]) == -1) {
                cs165_log(stdout, "Memory mapping failed.\n");
                send_message->status = EXECUTION_ERROR;
                return;
            }
        }
    }
    int* insert_values = query->operator_fields.insert_operator.values;
    Column* columns = insert_table->columns;
    if (!columns) {
        send_message->status = OBJECT_NOT_FOUND;
        return;
    }

    for (size_t i = 0; i < insert_table->col_count; i++) {
        Column* current_column = &(insert_table->columns[i]);
        current_column->data = realloc(current_column, insert_table->table_length * sizeof(int));
        current_column->data[insert_table->table_length-1] = insert_values[i];
    }
    send_message->status = OK_DONE;
}

void execute_load(DbOperator* query, message* send_message) {
    // load data from file
    FILE* fp = fopen(query->operator_fields.load_operator.file_name, "r");
    
    // if file not exists
    if (!fp) {
        cs165_log(stdout, "Cannot open file.\n");
        // cs165_log(stdout,query->operator_fields.load_operator.file_name );
        send_message->status = FILE_NOT_FOUND;
        return;
    }
    // count how many lines to load
    int ch, number_lines = 0;
    do {
        ch = fgetc(fp);
        if (ch == '\n') number_lines++;
    }while (ch != EOF);
    if(ch != '\n' && number_lines != 0) number_lines++;
    fclose(fp);

    // read header metadata
    fp = fopen(query->operator_fields.load_operator.file_name, "r");
    // buffer to store data from file
    char buffer[DEFAULT_QUERY_BUFFER_SIZE];
    // read file
    fgets(buffer, DEFAULT_QUERY_BUFFER_SIZE, fp);
    // header file stores the database metadata
    char* metadata = strtok(buffer, ",");
    char* db_name = strtok(metadata, ".");
    char* table_name = strtok(NULL, ".");
    // check data schema
    if (!db_name || !table_name) {
        cs165_log(stdout, "Incorrect data schema.\n");
        send_message->status = INCORRECT_FORMAT;
    }
    // find the table for the load file
    if (strcmp(db_name, current_db->name) != 0) {
        cs165_log(stdout, "Cannot find database.\n");
        send_message->status = OBJECT_NOT_FOUND;
        return;
    }
    Table* current_table = lookup_table(table_name);
    if (!current_table) {
        send_message->status = OBJECT_NOT_FOUND;
        return;
    }
    // set table length capacity for current table
    current_table->table_length_capacity = (number_lines - 1) * 2;
    for (size_t i = 0; i < current_table->col_count; i++) {
        if (map_column(current_table, &current_table->columns[i]) == -1) {
            send_message->status = EXECUTION_ERROR;
            return;
        }
    }
    // TODO: match column pointers with column names in header
    //
    // load data from file and insert them into current database
    size_t row = 0;
    size_t column = 0;
    // loop through each row in the file
    while (fgets(buffer, DEFAULT_QUERY_BUFFER_SIZE, fp)) {
        row++;
        column = 0;
        // if (row > current_table->table_length_capacity) {
        //     // reallocate column data sizes
        //     for (size_t i = 0; i < current_table->col_count; i++) {
        //         // sync and unmap before expand capacity
        //         if (persist_column(current_table, &current_table->columns[i]) == -1) {
        //             cs165_log(stdout, "Memory syncing and unmapping failed.\n");
        //         }
        //         current_table->columns[i].data = realloc(current_table->columns[i].data, sizeof(int) * current_table->table_length);
        //     }
        //     current_table->table_length_capacity = current_table->table_length_capacity * 2;
        // }
        char* data =strtok(buffer, ",");
        while (data) {
            current_table->columns[column].data[row-1] = atoi(data);
            data = strtok(NULL, ",");
            column++;
        }
    }
    fclose(fp);
    send_message->status = OK_DONE;
}

void execute_select(DbOperator* query, message* send_message) {
    Column* column = &query->operator_fields.select_operator.column;
    int low = query->operator_fields.select_operator.low;
    int hight = query->operator_fields.select_operator.high;
    int fd;
	int result;
	int select_data[query->operator_fields.select_operator.column_length];
    // MAX_COLUMN_PATH value is also used here for context path
    char context_path[MAX_COLUMN_PATH];
    // create context path if not exist
    struct stat st = {0};
    if (stat(CONTEXT_PATH, &st) == -1) {
        mkdir(CONTEXT_PATH, 0700);
    }
    // TODO: TODAY
	strcpy(context_path, CONTEXT_PATH);
	strcat(context_path, column->name);
    strcat(context_path, ".context");
	fd = open(context_path, O_RDWR | O_CREAT | O_TRUNC, (mode_t)0600);
	if (fd == -1) {
		cs165_log(stdout, "Cannot create column page file %s\n", context_path);
		return -1;
	}
	result = lseek(fd, query->operator_fields.select_operator.column_length * sizeof(int) - 1, SEEK_SET);
	if (result == -1) {
		cs165_log(stdout, "Cannot lseek in column page file %s\n", context_path);
		return NULL;
	}
	result = write(fd, "", 1);
	if (result == -1) {
		cs165_log(stdout, "Cannot write zero-byte at the end of column page file %s\n", context_path);
		return NULL;
	}
	column->data = mmap(0, query->operator_fields.select_operator.column_length * sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	if (column->data == MAP_FAILED) {
		close(fd);
		cs165_log(stdout, "Memory mapping failed column page file %s\n", context_path);
		return NULL;
	}
	// TODO: further check: closing the file descriptor does not unmap the region
	close(fd);
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
char* execute_DbOperator(DbOperator* query, message* send_message) {
    //////////////////////////////////
    // TODO: Solve memory leak
    //////////////////////////////////
    // there is a small memory leak here (when combined with other parts of your database.)
    // as practice with something like valgrind and to develop intuition on memory leaks, find and fix the memory leak. 
    if(!query)
    {
        return "165";
    }
    if(query && query->type == CREATE) {
        execute_create(query, send_message);
    } else if (query && query->type == INSERT) {
        execute_insert(query, send_message);
    } else if (query && query->type == LOAD) {
        execute_load(query, send_message);
    } else if (query && query->type == SELECT) {
        execute_select(query, send_message);
    }
    free(query);
    return "165";
}

/**
 * handle_client(client_socket)
 * This is the execution routine after a client has connected.
 * It will continually listen for messages from the client and execute queries.
 **/
void handle_client(int client_socket) {
    int done = 0;
    int length = 0;

    log_info("Connected to socket: %d.\n", client_socket);

    // Create two messages, one from which to read and one from which to receive
    message send_message;
    message recv_message;

    // create the client context here
    ClientContext* client_context = NULL;

    // Continually receive messages from client and execute queries.
    // 1. Parse the command
    // 2. Handle request if appropriate
    // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
    // 4. Send response to the request.
    do {
        length = recv(client_socket, &recv_message, sizeof(message), 0);
        if (length < 0) {
            log_err("Client connection closed!\n");
            exit(1);
        } else if (length == 0) {
            done = 1;
        }

        if (!done) {
            char recv_buffer[recv_message.length + 1];
            length = recv(client_socket, recv_buffer, recv_message.length,0);
            recv_message.payload = recv_buffer;
            recv_message.payload[recv_message.length] = '\0';
            /////////////////////////
            // Important workflow
            /////////////////////////
            // 1. Parse command
            //    Query string is converted into a request for an database operator
            DbOperator* query = parse_command(recv_message.payload, &send_message, client_socket, client_context);

            // 2. Handle request
            //    Corresponding database operator is exsecuted over the query
            char* result = execute_DbOperator(query, &send_message);
            
            send_message.length = strlen(result);
            char send_buffer[send_message.length + 1];
            strcpy(send_buffer, result);
            send_message.payload = send_buffer;
            send_message.status = OK_WAIT_FOR_RESPONSE;
            
            // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
            if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                log_err("Failed to send message.");
                exit(1);
            }

            // 4. Send response to the request
            if (send(client_socket, result, send_message.length, 0) == -1) {
                log_err("Failed to send message.");
                exit(1);
            }
        }
    } while (!done);

    log_info("Connection closed at socket %d!\n", client_socket);
    close(client_socket);
    persist_database();
    free_database();
}

/**
 * setup_server()
 *
 * This sets up the connection on the server side using unix sockets.
 * Returns a valid server socket fd on success, else -1 on failure.
 **/
int setup_server() {
    int server_socket;
    size_t len;
    struct sockaddr_un local;

    log_info("Attempting to setup server...\n");

    if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
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
    if (bind(server_socket, (struct sockaddr *)&local, len) == -1) {
        log_err("L%d: Socket failed to bind.\n", __LINE__);
        return -1;
    }

    if (listen(server_socket, 5) == -1) {
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
    int server_socket = setup_server();
    if (server_socket < 0) {
        exit(1);
    }

    int db = load_database();
    if (db < 0) {
        log_info("No current database %d ...\n", server_socket);
    }
    log_info("Waiting for a connection %d ...\n", server_socket);

    struct sockaddr_un remote;
    socklen_t t = sizeof(remote);
    int client_socket = 0;

    // TODO: handle multiple clients
    if ((client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)) == -1) {
        log_err("L%d: Failed to accept a new connection.\n", __LINE__);
        exit(1);
    }

    handle_client(client_socket);
    

    return 0;
}
