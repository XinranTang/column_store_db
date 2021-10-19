/* This line at the top is necessary for compilation on the lab machine and many other Unix machines.
Please look up _XOPEN_SOURCE for more details. As well, if your code does not compile on the lab
machine please look into this as a a source of error. */
#define _XOPEN_SOURCE

/**
 * client.c
 *  CS165 Fall 2018
 *
 * This file provides a basic unix socket implementation for a client
 * used in an interactive client-server database.
 * The client receives input from stdin and sends it to the server.
 * No pre-processing is done on the client-side.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "common.h"
#include "message.h"
#include "utils.h"

#define DEFAULT_STDIN_BUFFER_SIZE 1024

/**
 * connect_client()
 *
 * This sets up the connection on the client side using unix sockets.
 * Returns a valid client socket fd on success, else -1 on failure.
 *
 **/
int connect_client() {
    int client_socket;
    int len;
    struct sockaddr_un remote;

    // log_info("-- Attempting to connect...\n");

    if ((client_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    remote.sun_family = AF_UNIX;
    strncpy(remote.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    len = strlen(remote.sun_path) + sizeof(remote.sun_family) + 1;
    if (connect(client_socket, (struct sockaddr *)&remote, len) == -1) {
        log_err("client connect failed: \n");
        return -1;
    }

    // log_info("-- Client connected at socket: %d.\n", client_socket);
    return client_socket;
}

/**
 * Getting Started Hint:
 *      What kind of protocol or structure will you use to deliver your results from the server to the client?
 *      What kind of protocol or structure will you use to interpret results for final display to the user?
 *      
**/
int main(void)
{
    int client_socket = connect_client();
    if (client_socket < 0) {
        exit(1);
    }

    message send_message;
    message recv_message;

    // Always output an interactive marker at the start of each command if the
    // input is from stdin. Do not output if piped in from file or from other fd
    char* prefix = "";
    if (isatty(fileno(stdin))) {
        prefix = "db_client > ";
    }

    char *output_str = NULL;
    int len = 0;

    // Continuously loop and wait for input. At each iteration:
    // 1. output interactive marker
    // 2. read from stdin until eof.
    char read_buffer[DEFAULT_STDIN_BUFFER_SIZE];
    send_message.payload = read_buffer;
    send_message.status = 0;

    while (printf("%s", prefix), output_str = fgets(read_buffer,
           DEFAULT_STDIN_BUFFER_SIZE, stdin), !feof(stdin)) {
        if (output_str == NULL) {
            log_err("fgets failed.\n");
            break;
        }

        // Only process input that is greater than 1 character.
        // Convert to message and send the message and the
        // payload directly to the server.
        send_message.length = strlen(read_buffer);

        // TODO: check here
        // I don't know how to receive print() result using message struct, since it only supports char array
        // So I seperate the case that a print() query is sent from the client
        if (strncmp(read_buffer, "print", 5) == 0) {
            // Send the message_header, which tells server payload size
            if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                log_err("Failed to send message header.\n");
                exit(1);
            }

            // Send the payload (query) to server
            if (send(client_socket, send_message.payload, send_message.length, 0) == -1) {
                log_err("Failed to send query payload.\n");
                exit(1);
            }

            // Always wait for server response (even if it is just an OK message)
            if ((len = recv(client_socket, &(recv_message), sizeof(message), 0)) > 0) {
                // printf("Received 1\n");
                if ((recv_message.status == OK_WAIT_FOR_RESPONSE) || (recv_message.status == OK_DONE)) {
                    // Calculate number of bytes in response package
                    int num_bytes = 32 * recv_message.length;
                    char payload[num_bytes];
                    // size_t num_print = recv_message.length;
                    // Receive the payload and print it out
                    while ((len = recv(client_socket, payload, num_bytes, 0)) > 0) {
                        if (payload[0] == 1){
                            // printf(" break;\n");
                            break;
                        }
                        int i = 0;
                        for (; i < num_bytes; i ++) {
                            if (!payload[i]) {
                                payload[i] = '\0';
                            }
                        }
                        printf("%s\n", payload);
                    }
                }
            }
        } else if (strncmp(read_buffer, "load", 4) == 0) {
            // Send the message_header, which tells server payload size
            if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                log_err("Failed to send message header.\n");
                exit(1);
            }

            // Send the payload (query) to server
            if (send(client_socket, send_message.payload, send_message.length, 0) == -1) {
                log_err("Failed to send query payload.\n");
                exit(1);
            }
            // Always wait for server response (even if it is just an OK message)
            if ((len = recv(client_socket, &(recv_message), sizeof(message), 0)) > 0) {
                if (recv_message.status == OK_WAIT_FOR_RESPONSE) {
                    char* file_name = malloc((recv_message.length+1) * sizeof(char));
                    recv(client_socket, file_name, recv_message.length, 0);
                    file_name[recv_message.length] = '\0';
                    // printf("%s\n",file_name);
                    FILE* fp = fopen(file_name, "r");
                    
                    // if file not exists
                    if (!fp) {
                        cs165_log(stdout, "Cannot open file.\n");
                        free(file_name);
                        send_message.status = FILE_NOT_FOUND;
                        send(client_socket, &(send_message), sizeof(message), 0);
                        exit(1);
                    }
                    // count how many lines to load
                    int ch = 0;
                    size_t number_lines;
                    number_lines = 0;
                    do {
                        ch = fgetc(fp);
                        if (ch == '\n') number_lines++;
                    } while (ch != EOF);
                    if(ch != '\n' && number_lines != 0) number_lines++;
                    fclose(fp);

                    // start sending file data
                    send_message.status = OK_WAIT_FOR_RESPONSE;
                    send(client_socket, &(send_message), sizeof(message), 0);
                    // read header metadata
                    fp = fopen(file_name, "r");
                    free(file_name);
                    // buffer to store data from file
                    char buffer[DEFAULT_STDIN_BUFFER_SIZE];
                    // read file
                    fgets(buffer, DEFAULT_STDIN_BUFFER_SIZE, fp);
                    send(client_socket, &buffer, DEFAULT_STDIN_BUFFER_SIZE, 0);

                    if ((len = recv(client_socket, &(recv_message), sizeof(message), 0)) > 0) {
                        if (recv_message.status == OK_WAIT_FOR_RESPONSE) {
                            send(client_socket, &number_lines, sizeof(size_t), 0);
                            recv(client_socket, &(recv_message), sizeof(message), 0);
                            if (recv_message.status == OK_WAIT_FOR_RESPONSE) {
                                // TODO: match column pointers with column names in header
                                //
                                // load data from file and insert them into current database
                                
                                // loop through each row in the file
                                while (fgets(buffer, DEFAULT_STDIN_BUFFER_SIZE, fp)) {
                                    send_message.status = OK_WAIT_FOR_RESPONSE;
                                    send(client_socket,&send_message, sizeof(message),0);
                                    send(client_socket, &buffer, DEFAULT_STDIN_BUFFER_SIZE, 0);
                                }
                                fclose(fp);
                                send_message.status = OK_DONE;
                                send(client_socket,&send_message, sizeof(message),0);
                            }
                        } 
                    } else {
                        log_err("Failed to receive message header.\n");
                        exit(1);
                    }
                }
            }
        } else if (send_message.length > 1) {
            // printf("received 2\n");
            // Send the message_header, which tells server payload size
            if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                log_err("Failed to send message header.\n");
                exit(1);
            }

            // Send the payload (query) to server
            if (send(client_socket, send_message.payload, send_message.length, 0) == -1) {
                log_err("Failed to send query payload.\n");
                exit(1);
            }

            // Always wait for server response (even if it is just an OK message)
            if ((len = recv(client_socket, &(recv_message), sizeof(message), 0)) > 0) {
                if ((recv_message.status == OK_WAIT_FOR_RESPONSE || recv_message.status == OK_DONE) &&
                    (int) recv_message.length > 0) {
                    // Calculate number of bytes in response package
                    int num_bytes = (int) recv_message.length;
                    char payload[num_bytes + 1];

                    // Receive the payload and print it out
                    if ((len = recv(client_socket, payload, num_bytes, 0)) > 0) {
                        payload[num_bytes] = '\0';
                        // if(strncmp(payload, "165", 3) != 0) printf("%s\n", payload);
                    }
                }
            }
            else {
                if (len < 0) {
                    // log_err("Failed to receive message.\n");
                }
                else {
		            // log_info("-- Server closed connection\n");
		        }
                exit(1);
            }
        }
    }
    close(client_socket);
    return 0;
}
