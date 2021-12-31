# Column Store DB

## Introduction

This repository contains the distribution code for CS165 Fall 2021.
More details about the project: http://daslab.seas.harvard.edu/classes/cs165/project.html

## Key features
### Database organization
The database organizes relational data in the form of columns, one per attribute. The columns store integer data values. Each relation is organized in the form of a table, which contains multiple columns for all the attributes in that relation.
### Storage
For the metadata, a database catalog will be created and persisted to store the metadata of the database, tables, and columns.
For data stored in the database, column files will be created and persisted to store the data in each column. Column files reside in different subdirectories identified by the table name the column belongs to.
### Implementation of db operators
Scan-based operators are implemented to execute on columns, which support queries with selection, projection, aggregation, creating index, joining, etc.
### Client-server communication
The communication between client and server is realized through socket using TCP/IP protocol.
### Fast scan
Batch queries can be sent from the client-side and should be executed in parallel. The system is able to identify when batch queries come and switch its scan mode to batch mode, and switch back properly after batch queries finish.
### Primary and secondary indexing
The project improves the predicate of the data system by adding column indexing and boosting the select operators based on the choice of select types. Therefore, the support for memory-optimized B-tree indices and sorted columns in both clustered and unclustered forms is added to this system. The system is also able to optimize a select operator using the system’s optimizer.
### Efficient joining
The system supports cache-conscious hash joins, grace hash joins, and nested-loop join that utilizes all cores of the underlying hardware.
