# MowgliDB

## Table Of Contents

- [Introduction](#introduction)
- [Documentation](#documentation)
  - [Indexing](#indexing)
    - [Heavy Trie](#heavy-trie)
    - [Heavy Path Decomposition](#heavy-path-decomposition)
- [MQL - Mowgli Query Language](#mql---mowgli-query-language)
- [Future plans](#future-plans)

## Introduction

MowgliDB is a powerful and efficient database engine written in C++. It is designed to handle large amounts of data with ease, using the Heavy Trie variation of the trie data structure. This variation has been created solely for the purpose of being able to store a trie on disk and complete trie operations by fetching as few data blocks as possible. It couples the classic trie variation with heavy path decomposition for efficient storage and retrieval of indexes, with a worst-case logarithmic complexity.

## Documentation

### Indexing

#### Heavy Trie

The primary keys are stored in a Heavy Trie, an altered trie data structure that is designed to be kept on disk and minimize the number of data blocks accessed during insertion and querying. This trie is modified to ensure that the least amount of data blocks are accessed, both when inserting and querying. Using a standard trie structure would lead to a large number of data blocks being accessed, regardless of using the binary or octal numeric system in representing the keys within the trie, which would significantly slow down the engine.

The optimizations used are necessary due to two factors: the maximum length of the primary key should not be too small, thus increasing the number of nodes that need to be accessed to look up a key, and each node carries a significant amount of data due to the number of children of a node. Having to look up many large nodes will result in a large amount of data blocks being accessed.

#### Heavy Path Decomposition

Heavy path decomposition is a technique used to optimize operations on trees, particularly for path queries. The algorithm involves breaking a tree into heavy paths, where each path has a heavy node that has a large number of children relative to its siblings. By decomposing the tree in this way, path queries can be performed efficiently by combining the results of queries on each heavy path. This is because between any pair of nodes in a tree with N nodes, at most log(N) heavy paths would be traversed.

In the context of MowgliDB, heavy path decomposition is used for yielding a partition of the trie into chains so that the least amount of chains get traversed from any trie node to the root. The chains are stored on disk and used for storing the nodes in a very small amount of space. This is due to the fact that the title of the file saving the chain will be the prefix within the trie to reach the chain, thus within the file we can actually just save the characters on the trie making up the chain. This allows for you to traverse the trie by fetching chains iteratively, finding the next chain by using the prefix traversed.

Although the algorithm to build the chains takes O(N) for N nodes, rebuilding it after every insert or remove operation would be quite costly. Thus, we employ a different approach in which we keep an adaptive decomposition. This is possible due to heavy path decomposition's favourable property of there being at most a logarithmic amount of chains between any pair of nodes. It can then be concluded that upon inserting or removing a key, a logarithmic amount of chains would change. While the complexity might still seem quite big, taking O(log(N)*MAX_PRIMARY_KEY_LENGTH), in reality it is an amortised O(log(N) + MAX_PRIMARY_KEY_LENGTH) as we'll only traverse from a leaf to the root. Thus, while it may be true that we might have to change a logarithmic amount of chains, the total length of the chains will not exceed MAX_PRIMARY_KEY_LENGTH.

## MQL - Mowgli Query Language

Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed quis commodo elit. Phasellus sed orci vel nunc interdum rhoncus vitae eu dui. Proin ac felis felis. Fusce eget mollis ipsum. Duis convallis lacinia purus.

## Future plans

- Add secondary indexes
- Pack smaller chains together in same file
- Add support for different query operators(<, >, !=)
- Investigate bulk inserts