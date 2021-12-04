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
#include <strings.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/stat.h>
#include <sys/sendfile.h>
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

int connect_client()
{
    int client_socket;
    int len;
    struct sockaddr_un remote;

    // log_info("-- Attempting to connect...\n");

    if ((client_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1)
    {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    remote.sun_family = AF_UNIX;
    strncpy(remote.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    len = strlen(remote.sun_path) + sizeof(remote.sun_family) + 1;
    if (connect(client_socket, (struct sockaddr *)&remote, len) == -1)
    {
        log_err("client connect failed\n");
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
    int ch = 0;
    struct stat st;
    size_t file_size[3];
    char *prefix = "";
    char *output_str = NULL;
    int len = 0;
    long offset = 0;
    char read_buffer[DEFAULT_STDIN_BUFFER_SIZE];
    size_t remain_data;
    int client_socket = connect_client();
    if (client_socket < 0)
    {
        exit(1);
    }

    message send_message;
    message recv_message;

    // Always output an interactive marker at the start of each command if the
    // input is from stdin. Do not output if piped in from file or from other fd

    if (isatty(fileno(stdin)))
    {
        prefix = "db_client > ";
    }

    // Continuously loop and wait for input. At each iteration:
    // 1. output interactive marker
    // 2. read from stdin until eof.

    send_message.payload = read_buffer;
    send_message.status = 0;

    while (printf("%s", prefix), output_str = fgets(read_buffer, DEFAULT_STDIN_BUFFER_SIZE, stdin), !feof(stdin))
    {
        if (output_str == NULL)
        {
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
        if (strncmp(read_buffer, "print", 5) == 0)
        {

            // Send the message_header, which tells server payload size
            if (send(client_socket, &(send_message), sizeof(message), 0) == -1)
            {
                log_err("Failed to send message header.\n");
                exit(1);
            }

            // Send the payload (query) to server
            if (send(client_socket, send_message.payload, send_message.length, 0) == -1)
            {
                log_err("Failed to send query payload.\n");
                exit(1);
            }

            // Always wait for server response (even if it is just an OK message)
            if ((len = recv(client_socket, &(recv_message), sizeof(message), 0)) > 0)
            {
                            // printf("waiting...\n");
                if ((recv_message.status == OK_WAIT_FOR_RESPONSE) || (recv_message.status == OK_DONE))
                {
                                // printf("waiting...\n");
                    // Always wait for server response (even if it is just an OK message)

                    // Calculate number of bytes in response package
                    size_t print_num_bytes = recv_message.length;
                    char * print_payload;
                    if (print_num_bytes * sizeof(char) < DEFAULT_STDIN_BUFFER_SIZE) {
                        print_payload = malloc(DEFAULT_STDIN_BUFFER_SIZE);
                    } else {
                        print_payload = malloc(print_num_bytes * sizeof(char));
                    }
                    size_t print_offset = 0;
                                                    // printf("recv %ld, total %ld\n",print_offset, print_num_bytes);
                    // size_t num_print = recv_message.length;
                    // Receive the payload and print it out
                    while (print_offset < print_num_bytes && ((len = recv(client_socket, print_payload + print_offset, DEFAULT_STDIN_BUFFER_SIZE, 0)) > 0) )
                    {
                        print_offset += len;
                                // printf("recv %ld, remain %ld\n",print_offset, print_num_bytes - print_offset);
                        // printf("%s\n", payload);
                        // memset(payload, '\0', 32 * recv_message.length);
                    }
                    print_payload[print_num_bytes - 1] = '\0';
                    printf("%s\n", print_payload);
                    free(print_payload);
                }
            }
        }
        else if (strncmp(read_buffer, "load", 4) == 0)
        {
            // Send the message_header, which tells server payload size
            if (send(client_socket, &(send_message), sizeof(message), 0) == -1)
            {
                log_err("Failed to send message header.\n");
                exit(1);
            }

            // Send the payload (query) to server
            if (send(client_socket, send_message.payload, send_message.length, 0) == -1)
            {
                log_err("Failed to send query payload.\n");
                exit(1);
            }
            // Always wait for server response (even if it is just an OK message)
            if ((len = recv(client_socket, &(recv_message), sizeof(message), 0)) > 0)
            {
                if (recv_message.status == OK_WAIT_FOR_RESPONSE)
                {
                    char *file_name = malloc((recv_message.length + 1) * sizeof(char));
                    recv(client_socket, file_name, recv_message.length, 0);
                    file_name[recv_message.length] = '\0';
                    FILE *fp = fopen(file_name, "r");

                    // if file not exists
                    if (!fp)
                    {
                        cs165_log(stdout, "Cannot open file %s.\n", file_name);
                        free(file_name);
                        send_message.status = FILE_NOT_FOUND;
                        send(client_socket, &(send_message), sizeof(message), 0);
                        exit(1);
                    }
                    // count how many lines to load

                    fstat(fileno(fp), &st);

                    do
                    {
                        ch = fgetc(fp);
                        if (ch == '\n')
                            file_size[1]++;
                    } while (ch != EOF);
                    if (ch != '\n' && file_size[1] != 0)
                        file_size[1]++;
                    file_size[0] = st.st_size;
                    file_size[1]--;
                    remain_data = file_size[0];
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

                    len = send(client_socket, &buffer, DEFAULT_STDIN_BUFFER_SIZE, 0);
                    offset = strlen(buffer);
                    file_size[2] = offset;
                    remain_data -= offset;
                    if ((len = recv(client_socket, &(recv_message), sizeof(message), 0)) > 0)
                    {
                        if (recv_message.status == OK_WAIT_FOR_RESPONSE)
                        {
                            send(client_socket, &file_size, 3 * sizeof(size_t), 0);

                            recv(client_socket, &(recv_message), sizeof(message), 0);
                            if (recv_message.status == OK_WAIT_FOR_RESPONSE)
                            {
                                // TODO: match column pointers with column names in header
                                //
                                // load data from file and insert them into current database

                                // send file
                                while (((len = sendfile(client_socket, fileno(fp), &offset, DEFAULT_STDIN_BUFFER_SIZE)) > 0) && remain_data > 0)
                                {
                                    remain_data -= len;
                                    // printf("sent %d bytes, offset is now %ld, remaining %ld bytes \n", len, offset, remain_data);
                                }
                                fclose(fp);
                            }
                            // printf("sent %d bytes, offset is now %ld, remaining %ld bytes \n", len, offset, remain_data);
                        }
                    }
                    else
                    {
                        log_err("Failed to receive message header.\n");
                        exit(1);
                    }
                    // Always wait for server response (even if it is just an OK message)
                    if ((len = recv(client_socket, &(recv_message), sizeof(message), 0)) > 0)
                    {
                        if ((recv_message.status == OK_WAIT_FOR_RESPONSE || recv_message.status == OK_DONE) &&
                            (int)recv_message.length > 0)
                        {
                            // Calculate number of bytes in response package
                            int num_bytes = (int)recv_message.length;
                            char payload[num_bytes + 1];

                            // Receive the payload and print it out
                            if ((len = recv(client_socket, payload, num_bytes, 0)) > 0)
                            {
                                payload[num_bytes] = '\0';
                                // if(strncmp(payload, "165", 3) != 0) printf("%s\n", payload);
                            }
                        }
                    }
                }
            }
        }
        else if (send_message.length > 1)
        {
            // Send the message_header, which tells server payload size
            if (send(client_socket, &(send_message), sizeof(message), 0) == -1)
            {
                log_err("Failed to send message header.\n");
                exit(1);
            }

            // Send the payload (query) to server
            if (send(client_socket, send_message.payload, send_message.length, 0) == -1)
            {
                log_err("Failed to send query payload.\n");
                exit(1);
            }

            // Always wait for server response (even if it is just an OK message)
            if ((len = recv(client_socket, &(recv_message), sizeof(message), 0)) > 0)
            {
                if ((recv_message.status == OK_WAIT_FOR_RESPONSE || recv_message.status == OK_DONE) &&
                    (int)recv_message.length > 0)
                {
                    // Calculate number of bytes in response package
                    int num_bytes = (int)recv_message.length;
                    char payload[num_bytes + 1];

                    // Receive the payload and print it out
                    if ((len = recv(client_socket, payload, num_bytes, 0)) > 0)
                    {
                        payload[num_bytes] = '\0';
                        // if(strncmp(payload, "165", 3) != 0) printf("%s\n", payload);
                    }
                }
            }
            else
            {
                if (len < 0)
                {
                    // log_err("Failed to receive message.\n");
                }
                else
                {
                    // log_info("-- Server closed connection\n");
                }
                exit(1);
            }
        }
    }
    close(client_socket);
    return 0;
}
