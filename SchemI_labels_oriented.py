"""Script to infer a Property Graph Schema from an input Property Graph.
This is the Labels-oriented variant. """

##### Imports
import time
import getpass

from neo4j import GraphDatabase

from labelsOriented.preprocessing_labels import *
from labelsOriented.mapreduce_labels import * 
from labelsOriented.find_hierarchies_labels import * 

from format_utils import *


if __name__ == "__main__":
    
    ### Inputs from the user
    print("Labels-oriented PG schema inference\n")
    DBname = input("Name of the database: ")
    PGfilename = "PG_{}.jsonlines".format(DBname) # file to input the MapReduce algorithm
    
    uri = input("Neo4j bolt address: ") 
    user = input("Neo4j username: ")
    passwd = getpass.getpass('Neo4j password: ')
    driver = GraphDatabase.driver(uri, auth=(user, passwd), max_connection_lifetime=3600)
    """
    ### Step 1: serialize PG to JSON and get edge cardinalities and optionalities
    start1 = time.perf_counter()
    edgeTypes, nodeTypes, nodesNoProp = pg_to_json(driver, PGfilename)
    edgesCard = get_edges_card(edgeTypes, nodeTypes)
    stop1 = time.perf_counter()
    
    step1 = stop1 - start1 # time to complete step 1
    print("Step 1: 'Preprocessing queries' completed in ", step1, "s")
    """
    ## Step 2: call the MapReduce algorithm and parse the output
    start2a = time.perf_counter()
    MRfilenameNodes = call_mapreduce(PGfilename.split('.')[0] + "_nodes.jsonlines", nbcores=1)
    MRfilenameEdges = call_mapreduce(PGfilename.split('.')[0] + "_edges.jsonlines", nbcores=1)
    stop2a = time.perf_counter()
    
    step2a = stop2a - start2a # time to complete step 2a
    print("Step 2a: 'call MapReduce algorithm' completed in ", step2a, "s")

    start2b = time.perf_counter()
    
    # parse the MapReduce outputs
    schemaNodes = parse_mapreduce_schema(MRfilenameNodes)
    schemaEdges = parse_mapreduce_schema(MRfilenameEdges)
    schema = merge_nodes_edges(schemaNodes, schemaEdges)
    # merge schema with nodes without properties and edge cardinality and optionality infos
    merge_schema_infos(schema, nodesNoProp, edgesCard)
    stop2b = time.perf_counter()
    
    step2b = stop2b - start2b # time to complete step 2b
    print("Step 2b: 'parse MapReduce output and merge with edge informations' completed in ", step2b, "s")
    
    
    ### Step 3: find hierarchies in the schema
    PGSfilename = PGfilename.split('.')[0] + "_schema.json" # PG schema file name
    
    start3 = time.perf_counter()
    nodes, edges = infer_node_hierarchies(schema, PGSfilename)
    stop3 = time.perf_counter()
    
    step3 = stop3 - start3 # time to complete step 3
    print("Step 3: 'Infer node hierarchies' completed in ", step3, "s")
    
    
    #### Load the schema into Neo4j
    load = input("Do you want to load the schema into Neo4j?\n Please answer yes or no.\n")
    if not bool(load=='no'):
        uri = input("Neo4j bolt address: ") 
        user = input("Neo4j username: ")
        passwd = input('Neo4j password: ')
        allEdges = input('Do you want to remove superfluous edges?\n Please answer yes or no.\n' )
        driver = GraphDatabase.driver(uri, auth=(user, passwd), max_connection_lifetime=3600)
        create_Neo4j_pgschema(nodes, edges, driver, bool(allEdges=='no'))
    