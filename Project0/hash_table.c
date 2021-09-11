#include <stdlib.h>
#include <stdio.h>

#include "hash_table.h"


// Initialize the components of a hashtable.
// The size parameter is the expected number of elements to be inserted.
// This method returns an error code, 0 for success and -1 otherwise (e.g., if the parameter passed to the method is not null, if malloc fails, etc).
int allocate(hashtable** ht, int size) {
    // The next line tells the compiler that we know we haven't used the variable
    // yet so don't issue a warning. You should remove this line once you use
    // the parameter.
    *ht = malloc(sizeof(struct hashtable));
    struct hashtable* newht = *ht;
    newht->size = size;
    newht->factor = size;
    newht->entries = (struct node*) malloc(size * sizeof(struct node));

    return 0;
}

// This method inserts a key-value pair into the hash table.
// It returns an error code, 0 for success and -1 otherwise (e.g., if malloc is called and fails).
int put(hashtable* ht, keyType key, valType value) {
    struct node *current;
    current = (struct node*) malloc(sizeof(*current));
    current->key = key;
    current->value = value;
    current->next = NULL;

    struct node *root = &(ht->entries[key % ht->factor]);
    while(root->next!=NULL){
        root = root->next;
    }
    root->next = current;
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
int get(hashtable* ht, keyType key, valType *values, int num_values, int* num_results) {
    *num_results = 0;
    struct node* root = &(ht->entries[key % ht->factor]);
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
int erase(hashtable* ht, keyType key) {
    struct node* root = &(ht->entries[key % ht->factor]);
    struct node* prev;
    while(root->next != NULL){
        prev = root;
        root = root->next;
        if(root->key == key){
            prev->next = root->next;
            struct node* current = root;
            free(current);
            root = prev;
        }
    }
    return 0;
}

// This method frees all memory occupied by the hash table.
// It returns an error code, 0 for success and -1 otherwise.
int deallocate(hashtable* ht) {
    // This line tells the compiler that we know we haven't used the variable
    // yet so don't issue a warning. You should remove this line once you use
    // the parameter.
    for(int i = 0;i<ht->size;i++){
        struct node* prev = &(ht->entries[i]);
        struct node* next = prev->next;
        while(next!=NULL){
            prev = next;
            next = next->next;
            struct node* current = prev;
            free(current);
        }
    }
    free(ht->entries);
    free(ht);

    return 0;
}
