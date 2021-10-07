/* 
 * This file contains methods necessary to parse input from the client.
 * Mostly, functions in parse.c will take in string input and map these
 * strings into database operators. This will require checking that the
 * input from the client is in the correct format and maps to a valid
 * database operator.
 */

#define _DEFAULT_SOURCE
#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>
#include "cs165_api.h"
#include "parse.h"
#include "utils.h"
#include "client_context.h"

/**
 * Takes a pointer to a string.
 * This method returns the original string truncated to where its first comma lies.
 * In addition, the original string now points to the first character after that comma.
 * This method destroys its input.
 **/

char* next_token(char** tokenizer, message_status* status) {
    char* token = strsep(tokenizer, ",");
    if (token == NULL) {
        *status= INCORRECT_FORMAT;
    }
    return token;
}

char* sep_token(char** tokenizer, char* s, message_status* status) {
    char* token = strsep(tokenizer, s);
    if (token ==  NULL) {
        *status= INCORRECT_FORMAT;
    }
    return token;
}

/**
 * This method takes in a string representing the arguments to create a column.
 * It parses those arguments, checks that they are valid, and creates a column.
 * create(col,"class",awesomebase.grades)
 **/


DbOperator* parse_create_col(char* create_arguments) {
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* column_name = next_token(create_arguments_index, &status);
    char* table_and_db = next_token(create_arguments_index, &status);
    bool sorted = false; // TODO: get sorted value from arguments

    // Get the table name free of quotation marks
    column_name = trim_quotes(column_name);
    // seperate db name and table name
    char* db_name = sep_token(&table_and_db, ".", &status);
    char* table_name = sep_token(&table_and_db, ".", &status);
    
        // not enough arguments
    if (status == INCORRECT_FORMAT) {
        return NULL;
    }

    // read and chop off last char, which should be a ')'
    int last_char = strlen(table_name) - 1;
    if (table_name[last_char] != ')') {
        return NULL;
    }
    // replace the ')' with a null terminating character. 
    table_name[last_char] = '\0';
    // check that the database argument is the current active database
    if (!current_db || strcmp(current_db->name, db_name) != 0) {
        cs165_log(stdout, "query unsupported. Bad db name\n");
        return NULL; //QUERY_UNSUPPORTED
    }
    // // TODO: turn the string sorted into an boolean, and check that the input is valid.
    // bool sorted = atoi(sorted);
    Table* current_table;
    for (size_t i = 0; i < current_db->tables_size; i++) {
        if (strcmp(current_db->tables[i].name, table_name) == 0) {
            current_table = &(current_db->tables[i]);
            break;
        }
    }
    if (!current_table) {
        cs165_log(stdout, "query unsupported. Bad table name\n");
        return NULL;
    }

    // make create dbo for table
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = CREATE;
    dbo->operator_fields.create_operator.create_type = _COLUMN;
    strcpy(dbo->operator_fields.create_operator.name, column_name);
    dbo->operator_fields.create_operator.db = current_db;
    dbo->operator_fields.create_operator.table = current_table;
    dbo->operator_fields.create_operator.sorted = sorted;
    return dbo;
}

/**
 * This method takes in a string representing the arguments to create a table.
 * It parses those arguments, checks that they are valid, and creates a table.
 **/


DbOperator* parse_create_tbl(char* create_arguments) {
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* table_name = next_token(create_arguments_index, &status);
    char* db_name = next_token(create_arguments_index, &status);
    char* col_cnt = next_token(create_arguments_index, &status);

    // not enough arguments
    if (status == INCORRECT_FORMAT) {
        return NULL;
    }
    // Get the table name free of quotation marks
    table_name = trim_quotes(table_name);
    // read and chop off last char, which should be a ')'
    int last_char = strlen(col_cnt) - 1;
    if (col_cnt[last_char] != ')') {
        return NULL;
    }
    // replace the ')' with a null terminating character. 
    col_cnt[last_char] = '\0';
    // check that the database argument is the current active database
    if (!current_db || strcmp(current_db->name, db_name) != 0) {
        cs165_log(stdout, "query unsupported. Bad db name\n");
        return NULL; //QUERY_UNSUPPORTED
    }
    // turn the string column count into an integer, and check that the input is valid.
    int column_cnt = atoi(col_cnt);
    if (column_cnt < 1) {
        return NULL;
    }
    // make create dbo for table
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = CREATE;
    dbo->operator_fields.create_operator.create_type = _TABLE;
    strcpy(dbo->operator_fields.create_operator.name, table_name);
    dbo->operator_fields.create_operator.db = current_db;
    dbo->operator_fields.create_operator.col_count = column_cnt;
    return dbo;
}

/**
 * This method takes in a string representing the arguments to create a database.
 * It parses those arguments, checks that they are valid, and creates a database.
 **/


DbOperator* parse_create_db(char* create_arguments) {
    char *token;
    token = strsep(&create_arguments, ",");
    // not enough arguments if token is NULL
    if (token == NULL) {
        return NULL;
    } else {
        // create the database with given name
        char* db_name = token;

        // trim quotes and check for finishing parenthesis.
        db_name = trim_quotes(db_name);
        int last_char = strlen(db_name) - 1;
        if (last_char < 0 || db_name[last_char] != ')') {
            return NULL;
        }
        // replace final ')' with null-termination character.
        db_name[last_char] = '\0';

        token = strsep(&create_arguments, ",");
        if (token != NULL) {
            return NULL;
        }
        // make create operator. 
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = CREATE;
        dbo->operator_fields.create_operator.create_type = _DB;
        strcpy(dbo->operator_fields.create_operator.name, db_name);
        return dbo;
    }
}

/**
 * parse_create parses a create statement and then passes the necessary arguments off to the next function
 **/
DbOperator* parse_create(char* create_arguments) {
    message_status mes_status;
    DbOperator* dbo = NULL;
    char *tokenizer_copy, *to_free;
    // Since strsep destroys input, we create a copy of our input. 
    tokenizer_copy = to_free = malloc((strlen(create_arguments)+1) * sizeof(char));
    char *token;
    strcpy(tokenizer_copy, create_arguments);
    // check for leading parenthesis after create. 
    if (strncmp(tokenizer_copy, "(", 1) == 0) {
        tokenizer_copy++;
        // token stores first argument. Tokenizer copy now points to just past first ","
        token = next_token(&tokenizer_copy, &mes_status);
        if (mes_status == INCORRECT_FORMAT) {
            free(to_free);
            return NULL;
        } else {
            // pass off to next parse function. 
            if (strcmp(token, "db") == 0) {
                dbo = parse_create_db(tokenizer_copy);
            } else if (strcmp(token, "tbl") == 0) {
                dbo = parse_create_tbl(tokenizer_copy);
            } else if (strcmp(token, "col") == 0) {
                dbo = parse_create_col(tokenizer_copy);
            } else {
                mes_status = UNKNOWN_COMMAND;
            }
        }
    } else {
        mes_status = UNKNOWN_COMMAND;
    }
    free(to_free);
    return dbo;
}

/**
 * parse_insert reads in the arguments for a create statement and 
 * then passes these arguments to a database function to insert a row.
 **/

DbOperator* parse_insert(char* query_command, message* send_message) {
    unsigned int columns_inserted = 0;
    char* token = NULL;
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;
        // parse table input
        char* table_name = next_token(command_index, &send_message->status);
        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }
        // lookup the table and make sure it exists. 
        // TODO: implement lookup table
        table_name = strtok(table_name, ".");
        table_name = strtok(NULL, ".");
        Table* insert_table = lookup_table(table_name);
        if (insert_table == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }
        // make insert operator. 
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = INSERT;
        dbo->operator_fields.insert_operator.table = insert_table;
        dbo->operator_fields.insert_operator.values = malloc(sizeof(int) * insert_table->col_count);
        // parse inputs until we reach the end. Turn each given string into an integer. 
        while ((token = strsep(command_index, ",")) != NULL) {
            int insert_val = atoi(token);
            dbo->operator_fields.insert_operator.values[columns_inserted] = insert_val;
            columns_inserted++;
        }
        // check that we received the correct number of input values
        if (columns_inserted != insert_table->col_count) {
            send_message->status = INCORRECT_FORMAT;
            free (dbo);
            return NULL;
        } 
        printf("Parse relational insert returnDBO\n");
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

DbOperator* parse_load(char* query_command, message* send_message) {
    query_command = trim_quotes(trim_parenthesis(query_command));
    cs165_log(stdout, "Loading: %s\n", query_command);
    // make create dbo
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = LOAD;
    strcpy(dbo->operator_fields.load_operator.file_name, query_command);

    return dbo;

}

DbOperator* parse_fetch(char* intermediate, char* query_command, message* send_message) {
    char* token = NULL;
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;
        // parse table input
        char* name = next_token(command_index, &send_message->status);
        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }
        char* db_name = strtok(name, ".");
        char* table_name = strtok(NULL, ".");
        char* column_name = strtok(NULL, ".");
        if (strcmp(db_name, current_db->name) != 0) {
            send_message->status = OBJECT_NOT_FOUND;
        }

        // lookup the table and make sure it exists. 
        // TODO: implement lookup table
        Table* fetch_table = lookup_table(table_name);
        if (fetch_table == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }

        Column* fetch_column = lookup_column(fetch_table, column_name);
        // make insert operator. 
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = FETCH;
        strcpy(dbo->operator_fields.fetch_operator.intermediate,intermediate);
        dbo->operator_fields.fetch_operator.column = fetch_column;
        // parse inputs until we reach the end. Turn each given string into an integer. 
        if ((token = strsep(command_index, ",")) != NULL) {
            int last_char = strlen(token) - 1;
            if (last_char < 0 || token[last_char] != ')') {
                free(dbo);
                return NULL;
            }
            // replace final ')' with null-termination character.
            token[last_char] = '\0';
            dbo->operator_fields.fetch_operator.positions = token;   
        } else {
            send_message->status = OBJECT_NOT_FOUND;
            free(dbo);
            return NULL;
        }
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

DbOperator* parse_select(char* intermediate, char* query_command, message* send_message) {
    char* token = NULL;
    char *tokenizer_copy, *to_free;
    // Since strsep destroys input, we create a copy of our input. 
    tokenizer_copy = to_free = malloc((strlen(query_command)+1) * sizeof(char));
    strcpy(tokenizer_copy, query_command);
    // check for leading '('
    if (strncmp(tokenizer_copy, "(", 1) == 0) {
        tokenizer_copy++;
        size_t intermediate_number = 1;
        for (size_t i = 0; tokenizer_copy[i]; i++) {
            if (tokenizer_copy[i] == ',') intermediate_number++;
        }
        if (intermediate_number == 3) {
            // parse table input
            char* name = next_token(&tokenizer_copy, &send_message->status);
            if (send_message->status == INCORRECT_FORMAT) {
                free(to_free);
                return NULL;
            }
            char* db_name = strtok(name, ".");
            char* table_name = strtok(NULL, ".");
            char* column_name = strtok(NULL, ".");
            // cs165_log(stdout, "%s, %s, %s, %s\n", db_name, table_name, column_name,current_db->name);
            if (strcmp(db_name, current_db->name) != 0) {
                send_message->status = OBJECT_NOT_FOUND;
            }
            // cs165_log(stdout, "%s, %s, %s\n", db_name, table_name, column_name);
            // lookup the table and make sure it exists. 
            Table* select_table = lookup_table(table_name);
            if (select_table == NULL) {
                send_message->status = OBJECT_NOT_FOUND;
                free(to_free);
                return NULL;
            }

            Column* select_column = lookup_column(select_table, column_name);
            // make insert operator. 
            DbOperator* dbo = malloc(sizeof(DbOperator));
            dbo->type = SELECT;
            strcpy(dbo->operator_fields.select_operator.intermediate, intermediate);
            dbo->operator_fields.select_operator.column = select_column;
            dbo->operator_fields.select_operator.column_length = select_table->table_length;
            // parse inputs until we reach the end. Turn each given string into an integer. 
            if ((token = strsep(&tokenizer_copy, ",")) != NULL) {
                if (strcmp(token, "null") == 0) dbo->operator_fields.select_operator.low = -__INT_MAX__-1;
                else dbo->operator_fields.select_operator.low = atoi(token);     
            } else {
                send_message->status = OBJECT_NOT_FOUND;
                free(dbo);
                free(to_free);
                return NULL;
            }
            if ((token = strsep(&tokenizer_copy, ",")) != NULL) {
                int last_char = strlen(token) - 1;
                if (last_char < 0 || token[last_char] != ')') {
                    free(dbo);
                    free(to_free);
                    return NULL;
                }
                // replace final ')' with null-termination character.
                token[last_char] = '\0';

                if (strcmp(token, "null") == 0) dbo->operator_fields.select_operator.high = __INT_MAX__;
                else dbo->operator_fields.select_operator.high = atoi(token) - 1;   
                printf("Just for check %s %d %d\n",token,  dbo->operator_fields.select_operator.low, dbo->operator_fields.select_operator.high);
            } else {
                send_message->status = OBJECT_NOT_FOUND;
                free(dbo);
                free(to_free);
                return NULL;
            }
            free(to_free);
            return dbo;
        } else { // 4
        printf("4 vars %s\n", tokenizer_copy);
            // parse table input
            char* name1 = next_token(&tokenizer_copy, &send_message->status);
            char* name2 = next_token(&tokenizer_copy, &send_message->status);
            if (send_message->status == INCORRECT_FORMAT) {
                free(to_free);
                return NULL;
            }
            // TODO: check the type of high and low
            // make insert operator. 
            DbOperator* dbo = malloc(sizeof(DbOperator));
            dbo->type = SELECT;
            strcpy(dbo->operator_fields.select_operator.intermediate, intermediate);
            dbo->operator_fields.select_operator.position_vector = name1;
            dbo->operator_fields.select_operator.value_vector = name2;

            // parse inputs until we reach the end. Turn each given string into an integer. 
            if ((token = strsep(&tokenizer_copy, ",")) != NULL) {
                if (strcmp(token, "null") == 0) dbo->operator_fields.select_operator.low = -__INT_MAX__-1;
                else dbo->operator_fields.select_operator.low = atoi(token);     
            } else {
                send_message->status = OBJECT_NOT_FOUND;
                free(dbo);
                free(to_free);
                return NULL;
            }
            if ((token = strsep(&tokenizer_copy, ",")) != NULL) {
                int last_char = strlen(token) - 1;
                if (last_char < 0 || token[last_char] != ')') {
                    free(dbo);
                    free(to_free);
                    return NULL;
                }
                // replace final ')' with null-termination character.
                token[last_char] = '\0';

                if (strcmp(token, "null") == 0) dbo->operator_fields.select_operator.high = __INT_MAX__;
                else dbo->operator_fields.select_operator.high = atoi(token) - 1;   
                printf("Just for check %s %d %d\n",token,  dbo->operator_fields.select_operator.low, dbo->operator_fields.select_operator.high);
            } else {
                send_message->status = OBJECT_NOT_FOUND;
                free(dbo);
                free(to_free);
                return NULL;
            }
            free(to_free);
            return dbo;
        }
        
    } else {
        send_message->status = UNKNOWN_COMMAND;
        free(to_free);
        return NULL;
    }
}

DbOperator* parse_aggregate(char* intermediate, char* query_command, AggregateType aggregate_type, message* send_message, ClientContext* client_context) {
    char *tokenizer_copy, *to_free;
    // Since strsep destroys input, we create a copy of our input. 
    tokenizer_copy = to_free = malloc((strlen(query_command)+1) * sizeof(char));
    strcpy(tokenizer_copy, query_command);
    // check for leading '('
    if (strncmp(tokenizer_copy, "(", 1) == 0) {
        tokenizer_copy++;
        tokenizer_copy = trim_parenthesis(tokenizer_copy);
        size_t intermediate_number = 1;
        for (size_t i = 0; tokenizer_copy[i]; i++) {
            if (tokenizer_copy[i] == ',') intermediate_number++;
        }
        if (intermediate_number == 1) {
            // SUM, AVG, MAX, MIN
 
            char* raw_intermediate = next_token(&tokenizer_copy, &send_message->status);

            if (send_message->status == INCORRECT_FORMAT) {
                free(to_free);
                return NULL;
            }
            char* db_name = NULL;
            char* table_name = NULL;
            char* column_name = NULL;
            if (strchr(raw_intermediate, '.') ) {
                db_name = strtok(raw_intermediate, ".");
                table_name = strtok(NULL, ".");
                column_name = strtok(NULL, ".");
                raw_intermediate = column_name;
            }
            GeneralizedColumn* generalized_column = lookup_variables(db_name, table_name, column_name, raw_intermediate, client_context);
            if (!generalized_column) {
                send_message->status = INCORRECT_FORMAT;
                free(to_free);
                return NULL;
            }
            DbOperator* dbo = malloc(sizeof(DbOperator));
            dbo->type = AGGREGATE;
            strcpy(dbo->operator_fields.aggregate_operator.intermediate, intermediate);
            dbo->operator_fields.aggregate_operator.aggregate_type = aggregate_type;
                        printf("Type Number: %d\n", dbo->operator_fields.aggregate_operator.aggregate_type);
            dbo->operator_fields.aggregate_operator.variable_number = intermediate_number;
            dbo->operator_fields.aggregate_operator.gc1 = generalized_column;
            free(to_free);
            return dbo;
        } else if (intermediate_number == 2) {
            // ADD, SUB, MAX(with position), MIN(with position)
            char* raw_intermediate1 = next_token(&tokenizer_copy, &send_message->status);
            char* raw_intermediate2 = next_token(&tokenizer_copy, &send_message->status);
            printf("Raw intermediates %s  %s", raw_intermediate1, raw_intermediate2);
            char* db_name1 = NULL;
            char* table_name1 = NULL;
            char* column_name1 = NULL;
            if (strchr(raw_intermediate1, '.') ) {
                db_name1 = strtok(raw_intermediate1, ".");
                table_name1 = strtok(NULL, ".");
                column_name1 = strtok(NULL, ".");
                raw_intermediate1 = column_name1;
            }
            char* db_name2 = NULL;
            char* table_name2 = NULL;
            char* column_name2 = NULL;
            if (strchr(raw_intermediate2, '.') ) {
                db_name2 = strtok(raw_intermediate2, ".");
                table_name2 = strtok(NULL, ".");
                column_name2 = strtok(NULL, ".");
                raw_intermediate2 = column_name2;
            }
            GeneralizedColumn* generalized_column1 = lookup_variables(db_name1, table_name1, column_name1, raw_intermediate1, client_context);
            if (!generalized_column1) {
                send_message->status = INCORRECT_FORMAT;
                free(to_free);
                return NULL;
            }
            GeneralizedColumn* generalized_column2 = lookup_variables(db_name2, table_name2, column_name2, raw_intermediate2, client_context);
            if (!generalized_column2) {
                send_message->status = INCORRECT_FORMAT;
                free(to_free);
                return NULL;
            }
            DbOperator* dbo = malloc(sizeof(DbOperator));
            dbo->type = AGGREGATE;
            strcpy(dbo->operator_fields.aggregate_operator.intermediate, intermediate);
            dbo->operator_fields.aggregate_operator.aggregate_type = aggregate_type;
            dbo->operator_fields.aggregate_operator.variable_number = intermediate_number;
            dbo->operator_fields.aggregate_operator.gc1 = generalized_column1;
            dbo->operator_fields.aggregate_operator.gc2 = generalized_column2;
            printf("Type Number: %d\n", dbo->operator_fields.aggregate_operator.aggregate_type);
            free(to_free);
            return dbo;
        } else {
            send_message->status = INCORRECT_FORMAT;
            free(to_free);
            return NULL;
        }   
    } else {
        send_message->status = UNKNOWN_COMMAND;
        free(to_free);
        return NULL;
    }
}

DbOperator* parse_print(char* query_command, message* send_message) {
    char* token = NULL;
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;
        // parse table input
        char* name = next_token(command_index, &send_message->status);
        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }
        char* intermediate = strtok(name, ")");
        
        // make print operator. 
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = PRINT;
        strcpy(dbo->operator_fields.print_operator.intermediate,intermediate);
        // printf("Prinet: intermediate name is %s\n", dbo->operator_fields.print_operator.intermediate);
        send_message->status = OK_DONE;
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

DbOperator* parse_shutdown(message* send_message) {
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = SHUTDOWN;
    send_message->status = OK_DONE;
    return dbo;
}
/**
 * parse_command takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns a db_operator.
 * 
 * Getting Started Hint:
 *      What commands are currently supported for parsing in the starter code distribution?
 *      How would you add a new command type to parse? 
 *      What if such command requires multiple arguments?
 **/
DbOperator* parse_command(char* query_command, message* send_message, int client_socket, ClientContext* context) {
    // a second option is to malloc the dbo here (instead of inside the parse commands). Either way, you should track the dbo
    // and free it when the variable is no longer needed. 
    DbOperator *dbo = NULL; // = malloc(sizeof(DbOperator));

    if (strncmp(query_command, "--", 2) == 0) {
        send_message->status = OK_DONE;
        // The -- signifies a comment line, no operator needed.  
        return NULL;
    }

    char *equals_pointer = strchr(query_command, '=');
    char *handle = query_command;
    if (equals_pointer != NULL) {
        // handle exists, store here. 
        *equals_pointer = '\0';
        cs165_log(stdout, "FILE HANDLE: %s\n", handle);
        query_command = ++equals_pointer;
    } else {
        handle = NULL;
    }

    cs165_log(stdout, "QUERY: %s\n", query_command);

    // by default, set the status to acknowledge receipt of command,
    //   indication to client to now wait for the response from the server.
    //   Note, some commands might want to relay a different status back to the client.
    send_message->status = OK_WAIT_FOR_RESPONSE;
    query_command = trim_whitespace(query_command);
    // check what command is given. 
    if (strncmp(query_command, "create", 6) == 0) {
        query_command += 6;
        dbo = parse_create(query_command);
        if(dbo == NULL){
            send_message->status = INCORRECT_FORMAT;
        }
        else{
            send_message->status = OK_DONE;
        }
    } else if (strncmp(query_command, "relational_insert", 17) == 0) {
        query_command += 17;
        cs165_log(stdout, "%s\n", query_command);
        cs165_log(stdout, "%s\n", query_command);
        dbo = parse_insert(query_command, send_message);
    } else if (strncmp(query_command, "load", 4) == 0) {
        query_command += 4;
        dbo = parse_load(query_command, send_message);
    // TODO: add other queries
    } else if (strncmp(query_command, "fetch", 5) == 0) {
        query_command += 5;
        cs165_log(stdout, "%s, %s\n", handle, query_command);
        dbo = parse_fetch(handle, query_command, send_message);
            // TODO: bug here !
    } else if (strncmp(query_command, "select", 6) == 0) {
        query_command += 6;
        cs165_log(stdout, "%s, %s\n", handle, query_command);
        dbo = parse_select(handle, query_command, send_message);
    } else if (strncmp(query_command, "avg", 3) == 0) {
        query_command += 3;
        dbo = parse_aggregate(handle, query_command, AVG, send_message, context);
    } else if (strncmp(query_command, "sum", 3) == 0) {
        query_command += 3;
        dbo = parse_aggregate(handle, query_command, SUM, send_message, context);
    } else if (strncmp(query_command, "add", 3) == 0) {
        query_command += 3;
        dbo = parse_aggregate(handle, query_command, ADD, send_message, context);
    } else if (strncmp(query_command, "sub", 3) == 0) {
        query_command += 3;
        dbo = parse_aggregate(handle, query_command, SUB, send_message, context);
    } else if (strncmp(query_command, "min", 3) == 0) {
        query_command += 3;
        dbo = parse_aggregate(handle, query_command, MIN, send_message, context);
    } else if (strncmp(query_command, "max", 3) == 0) {
        query_command += 3;
        dbo = parse_aggregate(handle, query_command, MAX, send_message, context);
    } else if (strncmp(query_command, "print", 5) == 0) {
        query_command += 5;
        dbo = parse_print(query_command, send_message);
    } else if (strncmp(query_command, "shutdown", 8) == 0) {
        dbo = parse_shutdown(send_message);
    }
    
    if (dbo == NULL) {
        return dbo;
    }
    dbo->client_fd = client_socket;
    dbo->context = context;
    return dbo;
}
