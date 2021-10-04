// This file includes shared constants and other values.
#ifndef COMMON_H__
#define COMMON_H__

// define the socket path if not defined. 
// note on windows we want this to be written to a docker container-only path
#ifndef SOCK_PATH
#define SOCK_PATH "cs165_unix_socket"
#endif

#ifndef CS165_DATABASE_PATH
#define CS165_DATABASE_PATH "/cs165/database/"
#endif

#ifndef DB_PATH
#define DB_PATH "/cs165/database/database.metadata"
#endif

#ifndef TABLE_PATH
#define TABLE_PATH "/cs165/database/tables.metadata"
#endif

#ifndef COLUMN_PATH
#define COLUMN_PATH "/cs165/database/columns/"
#endif

#ifndef CONTEXT_PATH
#define CONTEXT_PATH "/cs165/database/context/"
#endif

#if defined(WIN32) || defined(_WIN32)
#define PATH_SEP "\\"
#else
#define PATH_SEP "/"
#endif

#endif  // COMMON_H__
