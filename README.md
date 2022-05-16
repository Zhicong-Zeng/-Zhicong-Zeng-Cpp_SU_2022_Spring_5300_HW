# Save for backup
CPSC5300/4300 at Seattle U, Spring 2022

## Run Environment
Linux bash.
Need to establish Bekerley DB.
Use Makefile.

Usage (argument is database directory):
<pre>
$ ./sql5300 ~/cpsc5300/data
</pre>

## Tags
- <code>Milestone1</code> is playing around with the AST returned by the HyLine parser and general setup of the command loop.
- <code>Milestone2</code> Implement a rudimentary storage engine. Implemented the basic functions needed for HeapTable with two data types: integer and text.
- <code>Milestone3</code> Implement functions to create tables, drop tables, show tables and show columns.
- <code>Milestone4</code> Implement functions to create, show, and drop indices

## Unit Tests
There are some tests for SlottedPage and HeapTable. They can be invoked from the <code>SQL</code> prompt:
```
SQL> test
```
There are alse some tests for SQLExc.cpp. They can be invoked from the <code>SQL</code> prompt:
```
SQL> test2    or    SQL> test table
```
```
SQL> test3    or    SQL> test index
```
Be aware that failed tests may leave garbage Berkeley DB files lingering in your data directory. If you don't care about any data in there, you are advised to just delete them all after a failed test.
```
$ rm -f data/*
```

## Valgrind (Linux)
To run valgrind (files must be compiled with <code>-ggdb</code>):
```sh
$ valgrind --leak-check=full --suppressions=valgrind.supp ./sql5300 ~/cpsc5300/data
```
Note that we've added suppression for the known issues with the Berkeley DB library <em>vis-à-vis</em> valgrind.

## Memory Leak
There is no memory leak in this project.
Still has many memory leaks in SQLParser.cpp.
