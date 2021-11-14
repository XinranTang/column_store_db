#include "btree.h"

BTNode *create_btree(int *values, size_t *positions, size_t num_nodes)
{
    BTNode *root = initialize_btree();
    for (size_t i = 0; i < num_nodes; i++)
    {
        insert_btree(root, values[i], positions[i]);
    }
    return root;
}

BTNode *initialize_btree(void)
{
    BTNode *root = malloc(sizeof(BTNode));
    root->isLeaf = true;
    root->num_values = 0;
    root->values = malloc(MAX_KEYS * sizeof(int));
    root->positions = malloc(MAX_KEYS * sizeof(size_t));
    // root->children = malloc(MAX_KEYS * sizeof(BTNode));
    return root;
}

void deallocate_btree(BTNode *root)
{

    if (root->isLeaf)
    {
        free(root->values);
        free(root->positions);
        free(root);
        return;
    }
    for (int i = 0; i <= root->num_values; i++)
    {
        deallocate_btree(root->children[i]);
    }
    free(root->values);
    free(root->positions);
    free(root->children);
    return;
}

int binary_search_value(int *values, int n, int value)
{
    // given an array of values, search the index of child that should contain the target value
    //          35,            50
    // 12, 20        35,37         50, 59, 61
    // binary_search_value(, 2, 12):
    // low:   -1 -1
    // high:   2  0 return 0
    // middle: 0
    // binary_search_value(, 2, 35):
    // low:   -1
    // high:   2  0
    // middle: 0  return 0
    // binary_search_value(, 2, 37):
    // low:   -1  0 0
    // high:   2  2 1 return 1
    // middle: 0  1
    // binary_search_value(, 2, 49):
    // low:   -1  0  0
    // high:   2  2  1 return 1
    // middle: 0  1
    // binary_search_value(, 2, 50):
    // low:   -1  0
    // high:   2  2
    // middle: 0  1 return 2
    // binary_search_value(, 2, 58):
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
        if (values[middle] == value)
        {
            return middle + 1;
        }
        else if (values[middle] < value)
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

int binary_search_index(int *values, int n, int value)
{
    // given an array of values and an array of positions, return the index associated with the target value
    // the return result is always the index of value that is greater or equal to value
    //          35,            50
    // 12, 20        35,37         50, 59, 61
    // 0, 1          2, 3          4, 5, 6
    // binary_search_index(, 2, 13):
    // low:   -1  0  0
    // high:   2  2  1 return 1
    // middle: 0  1
    // binary_search_index(, 2, 20):
    // low:   -1  0
    // high:   2  2
    // middle: 0  1 return 1
    // binary_search_index(, 3, 58):
    // low:   -1  -1  0
    // high:   3  1   1 return 1
    // middle: 1  0 
    // low:   -1  0  1 (, 2, 49)
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
        if (values[middle] == value)
        {
            return middle;
        }
        else if (values[middle] < value)
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

size_t search_index(BTNode *root, int value)
{
    // given a root b-tree node, search for the position associated with the target value
    int pos;
    // if empty tree, return -1
    if (root->num_values == 0)
    {
        return -1;
    }
    /* look for smallest position that value fits below */
    
    if (root->isLeaf) // if searching a leaf node, return the index (size_t)
    {
        pos = binary_search_index(root->values, root->num_values, value);
        return root->positions[pos];
    }
    else
    {
        pos = binary_search_value(root->values, root->num_values, value);
        return search_index(root->children[pos], value);
    }
}
BTNode *search_leaf(BTNode *root, int value) 
{
    // TODO: add search queue
    int pos;
    // if empty tree, return root
    if (root->num_values == 0)
    {
        return root;
    }
    pos = binary_search_index(root->values, root->num_values, value);
    if (root->children[pos]->isLeaf)
    {
        return root->children[pos];
    }
    else
    {
        return search_leaf(root->children[pos], value);
    }
}
// SearchQueueNode *search_leaf(BTNode *root, int value, SearchQueueNode *current_queue_node) {
//     // TODO: add search queue
//     int pos;
//     // if empty tree, return root
//     if (root->num_values == 0)
//     {
//         current_queue_node->next = malloc(sizeof(SearchQueueNode));
//         current_queue_node->next->content = root;
//         current_queue_node->next->prev = current_queue_node;
//         return current_queue_node->next;
//     }
//     pos = binary_search_key(root->values, root->num_values, value);
//     current_queue_node->next = malloc(sizeof(SearchQueueNode));
//     current_queue_node->next->content = &root->children[pos];
//     current_queue_node->next->prev = current_queue_node;
//     if (root->children[pos].isLeaf)
//     {
//         return current_queue_node->next;
//     }
//     else
//     {
//         return search_leaf(&root->children[pos], value, current_queue_node->next);
//     }
// }

void split_node(BTNode *node, BTNode *parent) 
{ // index: the index in children where the new_node is to be inserted
    BTNode *new_node = initialize_btree();
    if (node->isLeaf) {
        // split the old leaf and move 1 key to the new parent
        new_node->num_values = node->num_values / 2;
        int j = 0;
        for (int i = node->num_values - node->num_values / 2; i < node->num_values; i++) {
            new_node->values[j] = node->values[i];
            new_node->positions[j++] = node->positions[i];
        }
        node->num_values -= node->num_values / 2; // TODO: check whether to -1
            // Since this node is going to have a new child,
        // create space of new child
        int i = parent->num_values;
        for (; i > 0 && (parent->values[i-1] >= new_node->values[0]); i--)
        {
            parent->children[i+1] = parent->children[i];
            parent->values[i] = parent->values[i-1];
        }
        parent->children[i+1] = new_node;
        parent->values[i] = new_node->values[0];
        parent->num_values++;
    } else { // node is non-leaf
        new_node->num_values = (node->num_values + 1) / 2 - 1;
        new_node->isLeaf = false;
        new_node->children = malloc((MAX_KEYS + 1) * sizeof(BTNode *));
        int j = 0;
        for (int i = node->num_values - (node->num_values + 1) / 2 + 1; i < node->num_values; i++) {
            new_node->values[j] = node->values[i];
            new_node->children[j++] = node->children[i];
        }
        new_node->children[j] = node->children[node->num_values];
        node->num_values = node->num_values - (node->num_values + 1) / 2; // TODO: check whether to -1
            // Since this node is going to have a new child,
        // create space of new child
        int i = parent->num_values;
        for (; i > 0 && (parent->values[i-1] >= node->values[(node->num_values + 1) / 2]); i--)
        {
            parent->children[i+1] = parent->children[i];
            parent->values[i] = parent->values[i-1];
        }
        parent->children[i+1] = new_node;
        parent->values[i] = node->values[(node->num_values + 1) / 2];
        parent->num_values++;
    }
    

}

BTNode *insert_non_full_btree(BTNode *root, int value, size_t position) 
{

    // If this is a leaf node
    if (root->isLeaf)
    {
        int pos = binary_search_index(root->values, root->num_values, value);
        for (int i = root->num_values; i > pos; i--) {
            root->positions[i] = root->positions[i-1];
            root->values[i] = root->values[i-1];
        }
        root->values[pos] = value;
        root->positions[pos] = position;
        root->num_values++;
    }
    else // If this node is not leaf
    {
        int pos= binary_search_value(root->values, root->num_values, value);
            // pos is the index of children that value can be inserted
        if (root->children[pos]->num_values == MAX_KEYS) {
            split_node(root->children[pos], root);
            pos= binary_search_value(root->values, root->num_values, value);
        }
        // printf("pos to insert: %d\n", pos);
        root->children[pos] = insert_non_full_btree(root->children[pos], value, position);
    }
    return root;
}

// The main function that inserts a new key in this B-Tree, return the root after insertion
BTNode *insert_btree_full(BTNode *root, int value, size_t position)
{
    // If root is full, then tree grows in height
    if (root->num_values == MAX_KEYS)
    {
        // Allocate memory for new root
        BTNode *new_root = initialize_btree();
        new_root->isLeaf = false;
        new_root->children = malloc((MAX_KEYS + 1) * sizeof(BTNode*));
        for (int i = 0; i <= MAX_KEYS; i++) {
            new_root->children[i] = NULL;
        }
        // make old root as child of new root
        new_root->children[0] = root;
        split_node(root, new_root);
        // new root has two children now.  Decide which of the two children is going to have new key
        int i = 0; // insert to the first child
        if (new_root->values[0] <= value) {
            i++; // insert to the second child
        }
        new_root->children[i] = insert_non_full_btree(new_root->children[i], value, position);
        // return new root
        return new_root;
    }
    else { // If root is not full, call insertNonFull for root
        root = insert_non_full_btree(root, value, position);
        return root;
    }
}

BTNode *insert_btree(BTNode *root, int value, size_t position) 
{
    return insert_btree_full(root, value, position);
}

void print_btree(BTNode *root, int level) {
    if (root == NULL) return;
    printf("%d isLeaf: %d num_values: %d\n", level, root->isLeaf, root->num_values);
        printf("- values\n");
    for (int i = 0; i < root->num_values; i++) {
        printf("%d ", root->values[i]);
    }
    printf("\n");
    if (root->isLeaf) {
                printf("- positions\n");
        for (int i = 0; i < root->num_values; i++) {
            printf("%ld ", root->positions[i]);
        }
        printf("\n");
    } else {
        for (int i = 0; i <= root->num_values; i++) {
            print_btree(root->children[i], level + 1);
        }
    }
}

int persist_btree_inner(BTNode *root, int level, FILE* fp) {
    if (root == NULL) return 0;
    int return_flag = 0;
    fwrite(root, sizeof(BTNode), 1, fp);
    fwrite(root->values, root->num_values * sizeof(int), 1, fp);
    if (root->isLeaf) {
        fwrite(root->positions, root->num_values * sizeof(size_t), 1, fp);
    } else {
        for (int i = 0; i <= root->num_values; i++) {
            return_flag = persist_btree_inner(root->children[i], level + 1, fp);
        }
    }
    return return_flag;
}

int persist_btree(BTNode *root, char* table_name, char* column_name) {
    char btree_path[MAX_COLUMN_PATH];
    int return_flag = 0;
	strcpy(btree_path, BTREE_PATH);
    // create btree base path if not exist
    struct stat st = {0};
    if (stat(BTREE_PATH, &st) == -1) {
        mkdir(BTREE_PATH, 0600);
    }
    strcat(btree_path, table_name);
    strcat(btree_path, PATH_SEP);
    // create btree path for table if not exist
    if (stat(btree_path, &st) == -1) {
        mkdir(btree_path, 0600);
    }
	strcat(btree_path, column_name);
    strcat(btree_path, ".btree");
    // write tables metadata
    FILE* fp = fopen(btree_path, "wb");
    if (!fp) {
        return_flag = -1;
        return return_flag;
    }
    return_flag = persist_btree_inner(root, 0, fp);
    fclose(fp);
    return return_flag;
}

BTNode *load_btree_inner(int level, FILE* fp) {
    BTNode *root = malloc(sizeof(BTNode));
    fread(root, sizeof(BTNode), 1, fp);
    root->values = malloc(MAX_KEYS * sizeof(int));
    root->positions = malloc(MAX_KEYS * sizeof(size_t));
    fread(root->values, root->num_values * sizeof(int), 1, fp);
    if (root->isLeaf) {
        root->positions = malloc(MAX_KEYS * sizeof(size_t));
        fread(root->positions, root->num_values * sizeof(size_t), 1, fp);
    } else {
        root->children = malloc((MAX_KEYS + 1) * sizeof(BTNode *));
        for (int i = 0; i <= root->num_values; i++) {
            root->children[i] = load_btree_inner(level + 1, fp);
        }
    }
    return root;
}

BTNode* load_btree(char* table_name, char* column_name) {
    char btree_path[MAX_COLUMN_PATH];
	strcpy(btree_path, BTREE_PATH);
    // create btree base path if not exist
    struct stat st = {0};
    if (stat(BTREE_PATH, &st) == -1) {
        mkdir(BTREE_PATH, 0600);
    }
    strcat(btree_path, table_name);
    strcat(btree_path, PATH_SEP);
    // create btree path for table if not exist
    if (stat(btree_path, &st) == -1) {
        mkdir(btree_path, 0600);
    }
	strcat(btree_path, column_name);
    strcat(btree_path, ".btree");
    // write tables metadata
    FILE* fp = fopen(btree_path, "rb");
    if (!fp) {
        return NULL;
    }
    BTNode *root = load_btree_inner(0, fp);
    fclose(fp);
    return root;
}


void test_insert_sequential() {
        BTNode *node = initialize_btree();
   // 1:1
    node = insert_btree(node, 1, 1);
    print_btree(node, 0);
    printf("====================================\n\n");
    // 1:1 2:2
    node = insert_btree(node, 2, 2);
    print_btree(node, 0);
    printf("====================================\n\n");
    //      2
    // 1:1     2:2 3:3
    node = insert_btree(node, 3, 3);
    print_btree(node, 0);
    printf("====================================\n\n");
    //      2
    node = insert_btree(node, 4, 4);
    print_btree(node, 0);
    printf("====================================\n\n");
    node = insert_btree(node, 5, 5);
    print_btree(node, 0);
    printf("====================================\n\n");
    node = insert_btree(node, 6, 6);
    print_btree(node, 0);
    printf("====================================\n\n");
    node = insert_btree(node, 7, 7);
    print_btree(node, 0);
    printf("====================================\n\n");
    node = insert_btree(node, 8, 8);
    print_btree(node, 0);
    printf("====================================\n\n");
    node = insert_btree(node, 9, 9);
    print_btree(node, 0);
    printf("====================================\n\n");
    node = insert_btree(node, 10, 10);
    print_btree(node, 0);
    printf("====================================\n\n");
    node = insert_btree(node, 11, 11);
    print_btree(node, 0);
    printf("====================================\n\n");
    node = insert_btree(node, 12, 12);
    print_btree(node, 0);
    printf("====================================\n\n");
    node = insert_btree(node, 13, 13);
    print_btree(node, 0);
    printf("====================================\n\n");
    node = insert_btree(node, 14, 14);
    print_btree(node, 0);
    printf("====================================\n\n");
    node = insert_btree(node, 15, 15);
    print_btree(node, 0);
    printf("====================================\n\n");
    deallocate_btree(node);
}

void test_insert_non_sequential() {
        BTNode *node = initialize_btree();
    printf("====================================\n\n");
    node = insert_btree(node, 13, 13);
    print_btree(node, 0);
    printf("====================================\n\n");
    node = insert_btree(node, 10, 10);
    print_btree(node, 0);
    printf("====================================\n\n");
    // 1:1 2:2
    node = insert_btree(node, 2, 2);
    print_btree(node, 0);
    printf("====================================\n\n");
   // 1:1
    node = insert_btree(node, 1, 1);
    print_btree(node, 0);
    printf("====================================\n\n");

    node = insert_btree(node, 6, 6);
    print_btree(node, 0);
    printf("====================================\n\n");
    node = insert_btree(node, 14, 14);
    print_btree(node, 0);
    printf("====================================\n\n");
    node = insert_btree(node, 15, 15);
    print_btree(node, 0);
    printf("====================================\n\n");
    node = insert_btree(node, 7, 7);
    print_btree(node, 0);
//     //      2
//     // 1:1     2:2 3:3
    printf("====================================\n\n");
    node = insert_btree(node, 3, 3);
    print_btree(node, 0);
    printf("====================================\n\n");
    node = insert_btree(node, 5, 5);
    // print_btree(node, 0);
    printf("====================================\n\n");
//     //      2
    node = insert_btree(node, 4, 4);
    print_btree(node, 0);
    printf("====================================\n\n");
    node = insert_btree(node, 8, 8);
    print_btree(node, 0);
    printf("====================================\n\n");
    node = insert_btree(node, 11, 11);
    print_btree(node, 0);
    printf("====================================\n\n");
    node = insert_btree(node, 9, 9);
    print_btree(node, 0);
    printf("====================================\n\n");
    node = insert_btree(node, 12, 12);
    print_btree(node, 0);
        deallocate_btree(node);

}
void test_search() {
    BTNode *node = initialize_btree();
    BTNode *node_non_sequential = initialize_btree();
// 1:1
    node = insert_btree(node, 1, 1);
    print_btree(node, 0);
    printf("====================================\n\n");
    // 1:1 2:2
    node = insert_btree(node, 2, 2);
    print_btree(node, 0);
    printf("====================================\n\n");
    //      2
    // 1:1     2:2 3:3
    node = insert_btree(node, 3, 3);
    print_btree(node, 0);
    printf("====================================\n\n");
    //      2
    node = insert_btree(node, 4, 4);
    print_btree(node, 0);
    printf("====================================\n\n");
    node = insert_btree(node, 5, 5);
    print_btree(node, 0);
    printf("====================================\n\n");
    node = insert_btree(node, 6, 6);
    print_btree(node, 0);
    printf("====================================\n\n");
    node = insert_btree(node, 7, 7);
    print_btree(node, 0);
    printf("====================================\n\n");
    node = insert_btree(node, 8, 8);
    print_btree(node, 0);
    printf("====================================\n\n");
    node = insert_btree(node, 9, 9);
    print_btree(node, 0);
    printf("====================================\n\n");
    node = insert_btree(node, 10, 10);
    print_btree(node, 0);
    printf("====================================\n\n");
    node = insert_btree(node, 11, 11);
    print_btree(node, 0);
    printf("====================================\n\n");
    node = insert_btree(node, 12, 12);
    print_btree(node, 0);
    printf("====================================\n\n");
    node = insert_btree(node, 13, 13);
    print_btree(node, 0);
    printf("====================================\n\n");
    node = insert_btree(node, 14, 14);
    print_btree(node, 0);
    printf("====================================\n\n");
    node = insert_btree(node, 15, 15);
    print_btree(node, 0);
    printf("====================================\n\n");

        node_non_sequential = insert_btree(node_non_sequential, 13, 13);

    printf("====================================\n\n");
    node_non_sequential = insert_btree(node_non_sequential, 10, 10);

    printf("====================================\n\n");
    // 1:1 2:2
    node_non_sequential = insert_btree(node_non_sequential, 2, 2);

    printf("====================================\n\n");
   // 1:1
    node_non_sequential = insert_btree(node_non_sequential, 1, 1);

    printf("====================================\n\n");

    node_non_sequential = insert_btree(node_non_sequential, 6, 6);

    printf("====================================\n\n");
    node_non_sequential = insert_btree(node_non_sequential, 14, 14);
 
    printf("====================================\n\n");
    node_non_sequential = insert_btree(node_non_sequential, 15, 15);

    printf("====================================\n\n");
    node_non_sequential = insert_btree(node_non_sequential, 7, 7);

//     //      2
//     // 1:1     2:2 3:3
    printf("====================================\n\n");
    node_non_sequential = insert_btree(node_non_sequential, 3, 3);

    printf("====================================\n\n");
    node_non_sequential = insert_btree(node_non_sequential, 5, 5);

    printf("====================================\n\n");
//     //      2
    node_non_sequential = insert_btree(node_non_sequential, 4, 4);

    printf("====================================\n\n");
    node_non_sequential = insert_btree(node_non_sequential, 8, 8);
    printf("====================================\n\n");
    node_non_sequential = insert_btree(node_non_sequential, 11, 11);
    printf("====================================\n\n");
    node_non_sequential = insert_btree(node_non_sequential, 9, 9);
    printf("====================================\n\n");
    node_non_sequential = insert_btree(node_non_sequential, 12, 12);
    for (int i = 1; i <= 15; i++) {
        size_t l = search_index(node, i);
        size_t r = search_index(node_non_sequential, i);
        printf("%d %ld == %ld ?: %d\n", i, l, r, l == r);
    }
    deallocate_btree(node);
    deallocate_btree(node_non_sequential);
}

int main(){

    // test_insert_sequential();
    // test_insert_non_sequential();
    // test_search();
    BTNode *node = initialize_btree();
   // 1:1
    node = insert_btree(node, 1, 1);
    // 1:1 2:2
    node = insert_btree(node, 2, 2);
    //      2
    // 1:1     2:2 3:3
    node = insert_btree(node, 3, 3);
    //      2
    node = insert_btree(node, 4, 4);
    node = insert_btree(node, 5, 5);
    node = insert_btree(node, 6, 6);
    node = insert_btree(node, 7, 7);
    node = insert_btree(node, 8, 8);
    node = insert_btree(node, 9, 9);
    node = insert_btree(node, 10, 10);
    node = insert_btree(node, 11, 11);
    node = insert_btree(node, 12, 12);
    node = insert_btree(node, 13, 13);
    node = insert_btree(node, 14, 14);
    node = insert_btree(node, 15, 15);
    persist_btree(node, "tbl1", "col1");
    deallocate_btree(node);
    BTNode *new_node = load_btree("tbl1","col1");
    printf("1***************************\n");
    print_btree(new_node, 0);
    printf("2***************************\n");
    deallocate_btree(new_node);
}
// BTNode *insert_btree(BTNode *root, int value, size_t position)
// {
//     // insert value, position pair into a existing tree, return the tree root
//     // TODO: free memory
//     SearchQueueNode *search_queue = malloc(sizeof(SearchQueueNode));
//     SearchQueueNode *search_result = search_leaf(root, value, search_queue);
//     BTNode *leaf = search_result->content;
//     if (leaf->num_values < MAX_KEYS) {
//         int pos = binary_search_index(leaf->values, leaf->num_values, value);
//         for (int i = leaf->num_values; i > pos; i--) {
//             leaf->values[i] = leaf->values[i-1];
//             leaf->positions[i] = leaf->positions[i-1];
//         }
//         leaf->values[pos] = value;
//         leaf->positions[pos] = position;
//     } else { // split
//         split(search_result);
//     }
// }
