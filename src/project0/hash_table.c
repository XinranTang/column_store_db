#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "hash_table.h"
#include "cs165_api.h"

// the hash function uses djb2 hash function
size_t HASH = 5381;
size_t hash_function(char* name) {
    int c;
    size_t hash = HASH;
    while ((c = *name++))
        hash = ((hash << 5) + hash) + c;
    return hash;
}

bucket* create_bucket(keyType name, valType value) {
    bucket* bucket = malloc(sizeof(struct bucket));
    bucket->key = malloc(sizeof(name));
    // bucket->value = malloc(sizeof(valType));
    bucket->value = value;
    strcpy(bucket->key, name);
    return bucket;
}

void free_bucket(bucket* bucket) {
    free(bucket->key);
    // TODO: free bucket value
    // free(bucket->value);
    free(bucket);
}
// Initialize the components of a hashtable.
// The size parameter is the expected number of elements to be inserted.
// This method returns an error code, 0 for success and -1 otherwise (e.g., if the parameter passed to the method is not null, if malloc fails, etc).
// @author Xinran Tang
int allocate(hashtable** ht, size_t size) {
    *ht = malloc(sizeof(struct hashtable));
    struct hashtable* newht = *ht;
    newht->size = size;
    newht->length = 0;
    newht->entries = (struct bucket**) malloc(size * sizeof(struct bucket*));
    for (int i = 0; i < newht->size; i++) {
        newht->entries[i] = NULL;
    }
    return 0;
}

// This method inserts a key-value pair into the hash table.
// It returns an error code, 0 for success and -1 otherwise (e.g., if malloc is called and fails).
// @author Xinran Tang
int put(hashtable* ht, keyType key, valType value) { // O(1) for put
    size_t index = hash_function(key) % ht->size;
    // create a new bucket
    struct bucket* new_bucket = create_bucket(key, value);
    // put new bucket after root bucket
    if (ht->entries[index] == NULL) {
        ht->entries[index] = new_bucket;
    } else {
        struct bucket* root = ht->entries[index];
        new_bucket->next = root;
        ht->entries[index] = new_bucket;
    }
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
// int get(hashtable* ht, keyType key, valType *values, size_t num_values, size_t* num_results) { // O(K) for get, K is the length of entry
//    *num_results = 0;
//     struct bucket* root = ht->entries[hash_function(key) % ht->size];
//     while(root != NULL){
//         if(root->key == key){
//             if((*num_results)<num_values){
//                 values[(*num_results)++] = root->value;
//             }else{
//                 (*num_results)++;
//             }
//         }
//         root = root->next;
//     }
//     return 0;
// }
valType get(hashtable* ht, keyType key) { // O(K) for get, K is the length of entry
    struct bucket* root = ht->entries[hash_function(key) % ht->size];
    while(root != NULL){

  // int num_results = 0;
  printf("root value %d \n", root->value);
        if(strcmp(root->key, key) == 0){
            return root->value;
        }
        root = root->next;
    }
    return -1;
}
// This method erases all key-value pairs with a given key from the hash table.
// It returns an error code, 0 for success and -1 otherwise (e.g., if the hashtable is not allocated).
// @author Xinran Tang
int erase(hashtable* ht, keyType key) {
    size_t index = hash_function(key) % ht->size;
    struct bucket* root = ht->entries[index];
    struct bucket* prev = NULL;
    while(root != NULL){
        //   printf("root value %d \n", root->value);
        if(strcmp(root->key, key) == 0){
              printf("erase root value %d \n", root->value);
            if (prev == NULL) {
                ht->entries[index] = root->next;
            } else {
                prev->next = root->next;
            }
            struct bucket* current = root;
            root = root->next;
            free(current);
            
        }
    }
    return 0;
}

// This method frees all memory occupied by the hash table.
// It returns an error code, 0 for success and -1 otherwise.
// @author Xinran Tang
int deallocate(hashtable* ht) {
    for(int i = 0;i<ht->size;i++){
        struct bucket* prev = ht->entries[i];
        struct bucket* next = prev;
        while(next!=NULL){
            prev = next;
            next = next->next;
            struct bucket* current = prev;
            free_bucket(current);
        }
    }
    free(ht->entries);
    free(ht);
    return 0;
}
