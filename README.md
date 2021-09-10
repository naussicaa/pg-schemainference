# Property graph inference (PG)

## About the project
This python project infers a schema from a PG stored in Neo4j. This method uses a clustering method, a Bayesian Gaussian Mixture Model, to gather similar nodes together. It is meant to deal with both unlabeled and labeled nodes, properties, multi-labeled nodes, overlapping types.

## Dependencies
python modules : 
- neo4j==1.7.6
- termcolor
- hdbscan

## Running the project
1. python3 cluster_script.py
2. Enter Neo4j database info
