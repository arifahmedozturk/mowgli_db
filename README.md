# MowgliDB

## Table Of Contents

- [Introduction](#introduction)
- [Documentation](#documentation)
  - [Indexing](#indexing)
    - [Trie](#trie)
    - [Optimizations](#optimizations)
  - [Disk Management](#disk-management)
  - [Components](#components)
- [MQL - Mowgli Query Language](#mql---mowgli-query-language)

## Introduction

MowgliDB is a powerful and efficient database engine written in C++. It's designed to handle large amounts of data with ease, using the trie data structure coupled with heavy path decomposition for storing and worst-case logarithmic fetching of indexes.

## Documentation

### Indexing

#### Trie

The primary keys are stored in an altered trie data structure that is designed to be kept on disk and minimize the number of data blocks accessed during insertion and querying. This trie is modified to ensure that the least amount of data blocks are accessed, both when inserting and querying. Using a standard trie structure would lead to a large number of data blocks being accessed, regardless of using the binary or octal numeric system in representing the keys within the trie, which would significantly slow down the engine.

The optimizations used are necessary due to two factors: the maximum length of the primary key should not be too small, thus increasing the number of nodes that need to be accessed to look up a key, and each node carries a significant amount of data due to the number of children of a node. Having to look up many large nodes will result in a large amount of data blocks being accessed.

#### Optimizations

##### Heavy Path Decomposition

Heavy path decomposition is a technique used to optimize operations on trees, particularly for path queries. The algorithm involves breaking a tree into heavy paths, where each path has a heavy node that has a large number of children relative to its siblings. By decomposing the tree in this way, path queries can be performed efficiently by combining the results of queries on each heavy path. This is because between any pair of nodes in a tree with N nodes, at most log(N) heavy paths would be traversed.

In the context of MowgliDB, heavy path decomposition is used for yielding a partition of the trie into chains so that

### Disk Management

Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed quis commodo elit. Phasellus sed orci vel nunc interdum rhoncus vitae eu dui. Proin ac felis felis. Fusce eget mollis ipsum. Duis convallis lacinia purus.

### Components

Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed quis commodo elit. Phasellus sed orci vel nunc interdum rhoncus vitae eu dui. Proin ac felis felis. Fusce eget mollis ipsum. Duis convallis lacinia purus.

## MQL - Mowgli Query Language

Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed quis commodo elit. Phasellus sed orci vel nunc interdum rhoncus vitae eu dui. Proin ac felis felis. Fusce eget mollis ipsum. Duis convallis lacinia purus.
