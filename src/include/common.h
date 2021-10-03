// This file includes shared constants and other values.
#ifndef COMMON_H__
#define COMMON_H__

// define the socket path if not defined. 
// note on windows we want this to be written to a docker container-only path
#ifndef SOCK_PATH
#define SOCK_PATH "cs165_unix_socket"
#endif

#ifndef DB_PATH
#define DB_PATH "database.txt"
#endif

#ifndef TABLE_PATH
#define TABLE_PATH "tables.txt"
#endif

#endif  // COMMON_H__
