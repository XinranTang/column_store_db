#include <stdlib.h>
#include <stdio.h>

#include "hash_table.h"
#include "cs165_api.h"

size_t HASH = 5381;
size_t hash_function(char* name) {
    int c;
    size_t hash = HASH;
    while (c = *name++)
        hash = ((hash << 5) + hash) + c;
    return hash;
}

// Initialize the components of a hashtable.
// The size parameter is the expected number of elements to be inserted.
// This method returns an error code, 0 for success and -1 otherwise (e.g., if the parameter passed to the method is not null, if malloc fails, etc).
// @author Xinran Tang
int allocate(hashtable** ht, int size) {
    *ht = malloc(sizeof(struct hashtable));
    struct hashtable* newht = *ht;
    newht -> size = size;
    newht -> factor = size;
    newht -> entries = (struct bucket*) malloc(size * sizeof(struct bucket));
    return 0;
}

// This method inserts a key-value pair into the hash table.
// It returns an error code, 0 for success and -1 otherwise (e.g., if malloc is called and fails).
// @author Xinran Tang
int put(hashtable* ht, keyType key, valType value) {
    struct bucket *root = &(ht->entries[key % ht->factor]);
    // create a new bucket
    struct bucket *current;
    current = (struct bucket*) malloc(sizeof(*current));
    current -> key = key;
    current -> value = value;
    current -> next = root -> next;
    // put new bucket after root bucket
    root -> next = current;
    return 0;
}

// This method retrieves entries with a matching key and stores the corresponding values in the
// values array. The size of the values array is given by the parameter
// num_values. If there are more matching entries than num_values, they are not
// stored in the values array to avoid a buffer overflow. The function returns
// the number of matching entries using the num_results pointer. If the value of num_results is greater than
// num_values, the caller can invoke this function again (with a larger buffer)
// to get values that it missed during the first call. 
// This method returns an error code, 0 for success and -1 otherwise (e.g., if the hashtable is not allocated).
// @author Xinran Tang
int get(hashtable* ht, keyType key, valType *values, int num_values, int* num_results) {
   *num_results = 0;
    struct bucket* root = &(ht->entries[key % ht->factor]);
    while(root->next != NULL){
        root = root->next;
        if(root->key == key){
            if((*num_results)<num_values){
                values[(*num_results)++] = root->value;
            }else{
                (*num_results)++;
            }
        }
    }
    return 0;
}

// This method erases all key-value pairs with a given key from the hash table.
// It returns an error code, 0 for success and -1 otherwise (e.g., if the hashtable is not allocated).
// @author Xinran Tang
int erase(hashtable* ht, keyType key) {
    struct bucket* root = &(ht->entries[key % ht->factor]);
    struct bucket* prev;
    while(root->next != NULL){
        prev = root;
        root = root->next;
        if(root->key == key){
            prev->next = root->next;
            struct bucket* current = root;
            free(current);
            root = prev;
        }
    }
    return 0;
}

// This method frees all memory occupied by the hash table.
// It returns an error code, 0 for success and -1 otherwise.
// @author Xinran Tang
int deallocate(hashtable* ht) {
    for(int i = 0;i<ht->size;i++){
        struct bucket* prev = &(ht->entries[i]);
        struct bucket* next = prev->next;
        while(next!=NULL){
            prev = next;
            next = next->next;
            struct bucket* current = prev;
            free(current);
        }
    }
    free(ht->entries);
    free(ht);
    return 0;
}
