#ifndef CS165_HASH_TABLE // This is a header guard. It prevents the header from being included more than once.
#define CS165_HASH_TABLE  

typedef char* keyType;
typedef int valType;
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


int allocate(hashtable** ht, size_t size);
int put(hashtable* ht, keyType key, valType value);
valType get(hashtable* ht, keyType key);
int erase(hashtable* ht, keyType key);
int deallocate(hashtable* ht);

#endif
