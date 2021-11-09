#include "cs165_api.h"

#define MAX_KEYS 1024

BTNode *create_btree(int *keys, size_t *values, size_t num_nodes);

BTNode *initialize_btree(void);

void deallocate_btree(BTNode *root);

size_t search_index(BTNode *root, int key);

BTNode *search_leaf(BTNode *root, int key);

void insert_btree(BTNode *root, int key, size_t value);