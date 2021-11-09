#include <stdio.h>
#include "btree.h"

BTNode *create_btree(int *keys, size_t *values, size_t num_nodes)
{
    BTNode *root = initialize_btree();
    for (size_t i = 0; i < num_nodes; i++)
    {
        insert_btree(root, keys[i], values[i]);
    }
    return root;
}

BTNode *initialize_btree(void)
{
    BTNode *root = malloc(sizeof(BTNode));
    root->isLeaf = false;
    root->num_keys = 0;
    root->keys = malloc(MAX_KEYS * sizeof(int));
    root->children = malloc(MAX_KEYS * sizeof(BTNode));
    return root;
}

void deallocate_btree(BTNode *root)
{
    if (root->isLeaf)
    {
        free(root->keys);
        free(root);
        return;
    }

    for (size_t i = 0; i < root->num_keys; i++)
    {
        deallocate_btree(&root->children[i]);
    }
    free(root->keys);
    free(root->indexes);
    free(root->children);
    return;
}

int binary_search_key(int *keys, int n, int key)
{
    //          35,            50
    // 12, 20        35,37         50, 59, 61
    // binary_search_key(, 2, 12):
    // low:   -1 -1
    // high:   2  0 return 0
    // middle: 0
    // binary_search_key(, 2, 35):
    // low:   -1
    // high:   2  0
    // middle: 0  return 0
    // binary_search_key(, 2, 37):
    // low:   -1  0 0
    // high:   2  2 1 return 1
    // middle: 0  1
    // binary_search_key(, 2, 50):
    // low:   -1  0
    // high:   2  2
    // middle: 0  1 return 2
    // binary_search_key(, 2, 58):
    // low:   -1  0  1
    // high:   2  2  2 return 2
    // middle: 0  1
    int low;
    int high;
    int middle;
    low = -1;
    high = n;
    while (low + 1 < high)
    {
        middle = (low + high) / 2;
        if (keys[middle] == key)
        {
            return middle + 1;
        }
        else if (keys[middle] < key)
        {
            low = middle;
        }
        else
        {
            high = middle;
        }
    }
    return high;
}

size_t binary_search_index(int *keys, int *indexes, int n, int key)
{
    //          35,            50
    // 12, 20        35,37         50, 59, 61
    // 0, 1          2, 3          4, 5, 6
    // binary_search_key(, 2, 13):
    // low:   -1  0  0
    // high:   2  2  1 return 1
    // middle: 0  1
    int low;
    int high;
    int middle;
    low = -1;
    high = n;
    while (low + 1 < high)
    {
        middle = (low + high) / 2;
        if (keys[middle] == key)
        {
            return indexes[middle + 1];
        }
        else if (keys[middle] < key)
        {
            low = middle;
        }
        else
        {
            high = middle;
        }
    }
    return indexes[high];// the return value is always the index of value that is greater or equal to key
}

size_t search_index(BTNode *root, int key)
{
    int pos;
    // if empty tree, return -1
    if (root->num_keys == 0)
    {
        return -1;
    }
    /* look for smallest position that key fits below */
    pos = binary_search_key(root->keys, root->num_keys, key);
    if (root->children[pos].isLeaf) // if searching a leaf node, return the index (size_t)
    {
        return binary_search_index(root->keys, root->indexes, root->num_keys, key);
    }
    else
    {
        return search_index(&root->children[pos], key);
    }
}

BTNode *search_leaf(BTNode *root, int key) {
    int pos;
    // if empty tree, return -1
    if (root->num_keys == 0)
    {
        return NULL;
    }
    pos = binary_search_key(root->keys, root->num_keys, key);
    if (root->children[pos].isLeaf) // if searching a leaf node, return the index (size_t)
    {
        return root;
    }
    else
    {
        return search_leaf(&root->children[pos], key);
    }
}

void insert_btree(BTNode *root, int key, size_t value)
{
    BTNode *leaf = search_leaf(root, key);
    if (leaf == NULL) {
        
    } else {

    }
}