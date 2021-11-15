#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include "cs165_api.h"
#include "utils.h"
#include "common.h"

#define MAX_KEYS 1024

BTNode *create_btree(int *values, size_t *positions, size_t num_nodes);

BTNode *initialize_btree(void);

void deallocate_btree(BTNode *root);

BTNode *insert_btree(BTNode *root, int value, size_t position);

size_t search_index(BTNode *root, int value);

BTNode *search_leaf(BTNode *root, int value);

int binary_search_value(int *values, int n, int value);

int binary_search_index(int *values, int n, int value);

BTNode *insert_btree_full(BTNode *root, int value, size_t position);

BTNode *insert_non_full_btree(BTNode *root, int value, size_t position);

void split_node(BTNode *node, BTNode *parent);

int persist_btree(BTNode *root, char* table_name, char* column_name);

BTNode *load_btree(char* table_name, char* column_name);