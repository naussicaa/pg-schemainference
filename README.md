# Property graph inference (PG)

## About the project
This python project infers a schema from a PG stored in Neo4j. This method uses a clustering method, a Bayesian Gaussian Mixture Model, to gather similar nodes together. It is meant to deal with both unlabeled and labeled nodes, properties, multi-labeled nodes, overlapping types.

## Dependencies
python modules : 
- termcolor
- hdbscan==0.8.27
- neo4j==4.3.4

Neo4j :
- Neo4j Desktop 1.4.8
- To import ldbc, mb6 or fib25 with the method below you need your DBMS to be at least Neo4j 3.5.3

To import LDBC, mb6 or fib25, you need to generate the data and then use this method as a reference : https://github.com/connectome-neuprint/neuPrint/blob/master/neo4j_desktop_load.md


## Running the project
1. python3 cluster_script.py
2. Enter Neo4j database info
