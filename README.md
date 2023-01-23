# SearchEngine - Multimedia Information Retrieval project 💻

In this project we wrote a program that creates an inverted index structure from a set of text documents and a program that processes queries over such inverted index.

Search Engine based on an inverted index developed by *Stefano Bianchettin*, *Matteo Mugnai* and *Filippo Puccini* for *Multimedia Information Retrieval* course at University of Pisa during academic year 2022/2023. The documentation can be found [here](/Documentation/documentation.pdf). 

## Project structure
The whole project has been developed in *Java*.
It is composed by these main modules:

- **Build structures**
- **Common**
- **Performance tests**
- **Query processing**

### Build structures
This module performs the construction of a document index, an inverted index and a lexicon using the Spimi algorithm and a customized merge sort to combine together different partial blocks. At the end it saves all data structures on disk in a binary format. Only the inverted index is stored using compression strategies.

### Common
This module works as a library: it contains the core data structures and functions needed by all the other modules. It contains the core classes of the project as well and a lot of utility functions.

### PerformanceTest
This module performs tests on Msmarco query collection and writes final results in a format suitable for trec_eval.

### Query processing
This module waits a query from line command. When an user submits a query, it is pre-processed and tokenized, then the program retrieves the posting lists of the query terms and finally applies DAAT algorithm in order to get a ranking of the top most relevant documents for the received query. As scoring function there is the possibility to use TfIdf or BM25. 


## How to compile the modules
### Build structures
This module can be compiled using the following optional flags:

- ***-noss*** : if specified, it disables stopwards removal and stemming for tokens preprocessing.
- ***-debug*** : if specified, it runs code on debug mode (it indexes a sample collection with only 1000 documents).

If no flags are specified, this module will work with normal preprocessing procedure (stopwards and stemming enabled) and it builds the final structures starting from the whole collection (8.8M documents).

### PerformanceTest
This module can be executed by setting an optional flag:
- ***-d*** or ***-c***: it sets disjunctive or conjunctive mode
If no flags are specified, this module will work with disjunctive mode

### Query processing
The Query processing module can be compiled using the following optional flags:

- ***-d*** or ***-c***: it sets disjunctive or conjunctive mode
- ***-tfidf*** or ***-bm25***: it sets TFIDF or BM25 as scoring function
- ***-10*** or ***-20***: it sets k paramter (how many results we want to see in the rank)

If no flags are specified, this module will work with disjunctive mode, TfIdf scoring function and it will show k=20 results.
