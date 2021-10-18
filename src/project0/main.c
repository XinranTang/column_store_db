#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "hash_table.h"

// This is where you can implement your own tests for the hash table
// implementation. 
int main(void) {

  hashtable *ht = NULL;
  int size = 10;
  allocate(&ht, size);

  char key[4];
  strcpy(key, "a-1");
  int value = -1;

  put(ht, key, value);
  put(ht, "a0",0);
  put(ht,"a100",100);
  put(ht,"a-1001",-1001);
  // int num_values = 1;


  valType values = get(ht, key);
  // int num_results = 0;
  printf("value is %d \n", values);
  values = get(ht,"a0");
  // int num_results = 0;
  printf("value is %d \n", values);
 values = get(ht, "a-1001");
  // int num_results = 0;
  printf("value is %d \n", values);
  values = get(ht, "a100");
  // int num_results = 0;
  printf("value is %d \n", values);
  // free(values);

  erase(ht, "a-1");
 values = get(ht, "a-1");
  // int num_results = 0;
  printf("value is %d \n", values);
  deallocate(ht);
  return 0;
}
