#include "cs165_api.h"
#ifndef CS165_HASH_TABLE // This is a header guard. It prevents the header from being included more than once.
#define CS165_HASH_TABLE  

typedef int keyType;
typedef size_t valType;
// define the linked list as entryies in hashtable
typedef struct bucket{
    keyType key;
    valType value;
    struct bucket *next;
} bucket;

typedef struct hashtable {
// define the components of the hash table here (e.g. the array, bookkeeping for number of elements, etc)
    int length;
    int size;
    struct bucket** entries;
} hashtable;


int allocate_ht(hashtable* newht, size_t size);
int put_ht(hashtable* ht, keyType key, valType value);
size_t get_ht(hashtable* ht, keyType key, valType* res);
int erase_ht(hashtable* ht, keyType key);
int deallocate_ht(hashtable* ht);
int deallocate_ht_inner(hashtable* ht);
#endif
