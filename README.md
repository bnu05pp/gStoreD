#gStoreD

## gStoreD 1.0

### Overview
gStoreD is a distributed RDF data management system (or what is commonly called a “triple store”) that is based on “partial evaluation” and maintains the graph structure of the original [RDF](http://www.w3.org/TR/rdf11-concepts/) data. Its data model is a labeled, directed multiedge graph, where each vertex corresponds to a subject or an object. We also represent a given [SPARQL](http://www.w3.org/TR/sparql11-overview/) query by a query graph Q. Query processing involves finding subgraph matches of Q over the RDF graph G. 

### Install Steps
System Requirement: 64-bit linux server with GCC, mpich-3.0.4, make, readline installed.
*We have tested on linux server with CentOS 6.2 x86_64 and CentOS 6.6 x86_64. The version of GCC should be 4.4.7 or later.*

You can install gStoreD 1.0 in one command. Just run

`# make` 

to compile the gStore code and build executable "gload", "gquery", "gserver", "gclient".

### Usage
gStoreD 1.0 currently includes four executables and others.

####1. gloadD
gloadD is used to build a new database from a RDF triple format file.

`# mpiexec -f host_file_name -n host_number + 1 ./gloadD db_name rdf_triple_file_name internal_vertices_file_name`

For example, we build a database from dbpedia_example_distgStore.n3 which can be found in example folder.

    [root@master Gstore]# mpiexec -f host.txt -n 5 ./gloadD db_dbpedia_example_distgStore ./example/dbpedia_example_distgStore.n3 ./example/dbpedia_example_distgStore_internal.TXT

####2. gquery
gquery is used to query an exsisting database with SPARQL files.

`./gqueryD db_name query_file_name`  (use `./gquery --help` for detail)

The program shows a command prompt("gsql>"): you can type in a command here, use `help` to see information of all commands.

For `sparql` command, input a file name which can be interpreted as a single SPARQL query. 

When the program finish answering the query, it shows the command prompt again. 

*gStoreD 1.0 only support simple “select” queries now.*

We also take dbpedia_example_distgStore.n3 as an example.

    [root@master Gstore]# mpiexec -f host_file_name -n host_number + 1 ./gquery db_dbpedia_example_distgStore ./example/query.txt
   
Notice: 

All results are output into finalRes.txt.