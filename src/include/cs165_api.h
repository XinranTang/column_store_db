

/* BREAK APART THIS API (TODO MYSELF) */
/* PLEASE UPPERCASE ALL THE STUCTS */

/*
Copyright (c) 2015 Harvard University - Data Systems Laboratory (DASLab)
Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#ifndef CS165_H
#define CS165_H

#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>

// Limits the size of a name in our database to 64 characters
#define MAX_SIZE_NAME 64
#define HANDLE_MAX_SIZE 64
#define CONTEXT_CAPACIRY 103
#define MAX_COLUMN_PATH 256
#define NUM_BINS 64
#define SELECTIVITY_THRES 0.6

/**
 * EXTRA
 * DataType
 * Flag to mark what type of data is held in the struct.
 * You can support additional types by including this enum and using void*
 * in place of int* in db_operator simliar to the way IndexType supports
 * additional types.
 **/

typedef enum DataType
{
    INT,
    LONG, // can be used to declare size_t type
    FLOAT,
    DOUBLE,
} DataType;

typedef enum ColumnSelectType
{
    SEQUENTIAL,
    RANDOM_ACCESS,
} ColumnSelectType;

struct Comparator;

typedef struct ColumnIndex
{
    int *values;
    size_t *positions;
} ColumnIndex;

typedef struct Histogram
{
    int values[NUM_BINS];
    size_t counts[NUM_BINS];
} Histogram;

typedef struct BTNode
{
    int num_values;
    bool isLeaf;
    int *values; // => values
    size_t *positions;
    struct BTNode **children;
} BTNode;

typedef struct Partition
{
    size_t p_capacity;
    size_t p_len;
    int *values;
    size_t *positions;
} Partition;

typedef struct Column
{
    char name[MAX_SIZE_NAME];
    int *data;
    size_t length;
    bool sorted;
    bool btree;
    bool clustered;
    // You will implement column indexes later.
    // void *index;
    ColumnIndex *index;
    BTNode *btree_root;
    Histogram *histogram;
} Column;

/**
 * table
 * Defines a table structure, which is composed of multiple columns.
 * We do not require you to dynamically manage the size of your tables,
 * although you are free to append to the struct if you would like to (i.e.,
 * include a size_t table_size).
 * name, the name associated with the table. table names must be unique
 *     within a database, but tables from different databases can have the same
 *     name.
 * - col_count, the number of columns in the table
 * - columns this is the pointer to an array of columns contained in the table.
 * - table_length, the size of the columns in the table.
 **/

typedef struct Table
{
    char name[MAX_SIZE_NAME];
    Column *columns;
    size_t col_count;
    size_t table_length;
    size_t col_capacity;
    size_t table_length_capacity;
} Table;

/**
 * db
 * Defines a database structure, which is composed of multiple tables.
 * - name: the name of the associated database.
 * - tables: the pointer to the array of tables contained in the db.
 * - tables_size: the size of the array holding table objects
 * - tables_capacity: the amount of pointers that can be held in the currently allocated memory slot
 **/

typedef struct Db
{
    char name[MAX_SIZE_NAME];
    Table *tables;
    size_t tables_size;
    size_t tables_capacity;
} Db;

/**
 * Error codes used to indicate the outcome of an API call
 **/
typedef enum StatusCode
{
    /* The operation completed successfully */
    OK,
    /* There was an error with the call. */
    ERROR,
} StatusCode;

// status declares an error code and associated message
typedef struct Status
{
    StatusCode code;
    char *error_message;
} Status;

// Defines a comparator flag between two values.
typedef enum ComparatorType
{
    NO_COMPARISON = 0,
    LESS_THAN = 1,
    GREATER_THAN = 2,
    EQUAL = 4,
    LESS_THAN_OR_EQUAL = 5,
    GREATER_THAN_OR_EQUAL = 6
} ComparatorType;

/*
 * Declares the type of a result column, 
 which includes the number of tuples in the result, the data type of the result, and a pointer to the result data
 */
typedef struct Result
{
    size_t num_tuples;
    DataType data_type;
    void *payload;
} Result;

/*
 * an enum which allows us to differentiate between columns and results
 */
typedef enum GeneralizedColumnType
{
    RESULT,
    COLUMN
} GeneralizedColumnType;
/*
 * a union type holding either a column or a result struct
 */
typedef union GeneralizedColumnPointer
{
    Result *result;
    Column *column;
} GeneralizedColumnPointer;

/*
 * unifying type holding either a column or a result
 */
typedef struct GeneralizedColumn
{
    GeneralizedColumnType column_type;
    GeneralizedColumnPointer column_pointer;
} GeneralizedColumn;

/*
 * used to refer to a column in our client context
 */

typedef struct GeneralizedColumnHandle
{                                         // bucket
    char name[MAX_SIZE_NAME];             // key
    GeneralizedColumn generalized_column; // value TODO: check if freed or not
    struct GeneralizedColumnHandle *next;
} GeneralizedColumnHandle;

/*
 * holds the information necessary to refer to generalized columns (results or columns)
 */
typedef struct ClientContext
{ // hashtables
    GeneralizedColumnHandle **chandle_table;
    int chandles_in_use; // hashtable->length
    int chandle_slots;   // hashtable->size
    bool batch_mode;     // true: in batch_mode
    // TODO: handle multiple clients
    // int num_batch_queries;
} ClientContext;

/**
 * comparator
 * A comparator defines a comparison operation over a column. 
 **/
typedef struct Comparator
{
    long int p_low;  // used in equality and ranges.
    long int p_high; // used in range compares.
    GeneralizedColumn *gen_col;
    ComparatorType type1;
    ComparatorType type2;
    char *handle;
} Comparator;

/*
 * tells the databaase what type of operator this is
 */
typedef enum OperatorType
{
    CREATE,
    INSERT,
    LOAD,
    FETCH,
    SELECT,
    AGGREGATE,
    PRINT,
    SHUTDOWN,
    BATCH_START,
    BATCH_END,
    JOIN,
} OperatorType;

typedef enum CreateType
{
    _DB,
    _TABLE,
    _COLUMN,
    _INDEX,
} CreateType;

typedef enum AggregateType
{
    AVG,
    SUM,
    ADD,
    SUB,
    MIN,
    MAX,
} AggregateType;

typedef enum SelectType
{
    ONE_COLUMN,
    TWO_COLUMN,
} SelectType;

typedef enum JoinType
{
    NESTED_LOOP_JOIN,
    HASH_JOIN,
    GRACE_HASH_JOIN,
} JoinType;
/*
 * necessary fields for creation
 * "create_type" indicates what kind of object you are creating. 
 * For example, if create_type == _DB, the operator should create a db named <<name>> 
 * if create_type = _TABLE, the operator should create a table named <<name>> with <<col_count>> columns within db <<db>>
 * if create_type = = _COLUMN, the operator should create a column named <<name>> within table <<table>>
 */
typedef struct CreateOperator
{
    CreateType create_type;
    char name[MAX_SIZE_NAME];
    Db *db;
    Table *table;
    Column *column;
    int col_count;
    bool sorted;
    bool btree;
    bool clustered;
} CreateOperator;

/*
 * necessary fields for insertion
 */
typedef struct InsertOperator
{
    Table *table;
    int *values;
} InsertOperator;

/*
 * necessary fields for insertion
 */
typedef struct LoadOperator
{
    char file_name[MAX_COLUMN_PATH];
} LoadOperator;

typedef struct SelectOperator
{
    Column *column;
    size_t column_length;
    char position_vector[MAX_SIZE_NAME];
    char value_vector[MAX_SIZE_NAME];
    char intermediate[MAX_SIZE_NAME];
    SelectType select_type;
    int low;
    int high;
} SelectOperator;

typedef struct FetchOperator
{
    Column *column;
    char intermediate[MAX_SIZE_NAME];
    char *positions;
} FetchOperator;

typedef struct PrintOperator
{
    char **intermediates;
    size_t number_intermediates;
} PrintOperator;

typedef struct AggregateOperator
{
    AggregateType aggregate_type;
    char intermediate[MAX_SIZE_NAME];
    int variable_number;
    GeneralizedColumn *gc1;
    GeneralizedColumn *gc2;
} AggregateOperator;

typedef struct JoinOperator
{
    char l_name[MAX_SIZE_NAME];
    char r_name[MAX_SIZE_NAME];
    Result* f1;
    Result* f2;
    Result* p1;
    Result* p2;
    JoinType joinType;
} JoinOperator;
/*
 * union type holding the fields of any operator
 */
typedef union OperatorFields
{
    CreateOperator create_operator;
    InsertOperator insert_operator;
    LoadOperator load_operator;
    SelectOperator select_operator;
    FetchOperator fetch_operator;
    PrintOperator print_operator;
    AggregateOperator aggregate_operator;
    JoinOperator join_operator;
} OperatorFields;

/*
 * DbOperator holds the following fields:
 * type: the type of operator to perform (i.e. insert, select, ...)
 * operator fields: the fields of the operator in question
 * client_fd: the file descriptor of the client that this operator will return to
 * context: the context of the operator in question. This context holds the local results of the client in question.
 */
typedef struct DbOperator
{
    OperatorType type;
    OperatorFields operator_fields;
    int client_fd;
    ClientContext *context;
} DbOperator;

// extern declares current_db as global variable without any memory assigned to it
extern Db *current_db;

/* 
 * Use this command to see if databases that were persisted start up properly. If files
 * don't load as expected, this can return an error. 
 */
Status db_startup();

Status create_db(const char *db_name);

Table *create_table(Db *db, const char *name, size_t num_columns, Status *status);

Column *create_column(Table *table, char *name, Status *ret_status);

SelectType optimize(Column *column, int low, int high);

void create_index(Column *column, bool sorted, bool btree, bool clustered, Status *ret_status);

void build_primary_index(Table *table, size_t primary_column_index);

void build_secondary_index(Column *primary_column, bool btree);

long binary_search(int* array, long l, long r, int x);

Status shutdown_server();

char **execute_db_operator(DbOperator *query);

void db_operator_free(DbOperator *query);

#endif /* CS165_H */
