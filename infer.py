""" Script to create a Neo4j graph with """

### Imports
import csv

### Neo4j imports
from neo4j import GraphDatabase,BoltStatementResult

def create_neo4j_graph(driver2, edges=True):
    """ Create a Neo4j graph 

    Parameters
    ----------
    driver2 : GraphDatabase.driver object
        Driver used to access the PG stored in a Neo4j database.
    edges : Boolean.
        If edges is set at True by default.
        When edges is at True, add all edges to the Neo4j graph.
        When edges is at False, only add edges SUBTYPE_OF.

    Returns
    -------
    A Neo4j graph representation of the inferred schema
    """

    uri = input("Neo4j bolt address: ")
    user = input("Neo4j username: ")
    passwd = input('Neo4j password: ')
    file = input('Create the Neo4j graph from which file ? : ')
    driver = GraphDatabase.driver(uri, auth=(user, passwd), encrypted=False)

    with driver.session() as session:
        with open(file) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            line_count = 0

            ind_dict={}
            for row in csv_reader:
                if line_count == 0:
                    line_count += 1
                else:
                    label = row[4]
                    labels = row[1]
                    props = row[2]
                    ind_dict[row[0]] = [label,labels,props]

                    # for base type
                    if row[5] == "yes":

                        # neo4j node creation query
                        query="CREATE (n:"+label+" {labels:'"+labels+"',props:'"+props+"'})"
                        session.run(query)
                    else:
                        parent = row[3]
                        label_parent = ind_dict[parent][0]
                        labels_parent = ind_dict[parent][1]
                        props_parent = ind_dict[parent][2]

                        # neo4j node creation query
                        query = "CREATE (n:"+label+" {labels:'"+labels+"',props:'"+props+"'})"
                        session.run(query)

                        # neo4j subtype_of edge creation query
                        query = "MATCH(n:"+label+"),(m:"+label_parent+")"+"WHERE n.labels='"+labels+"' AND n.props='"+props+"' CREATE (n)-[r:SUBTYPE_OF]->(m)"
                        session.run(query)

    driver.close()

    stop = input("change for localhost")

    with driver2.session() as session:
        query = "MATCH (n)-[r]->(m) \
            RETURN DISTINCT labels(n),keys(n),type(r),labels(m),keys(m)"
        edge_types = session.run(query)
        edge_types = BoltStatementResult.data(edge_types)

    driver2.close()

    stop = input("change for localhost")

    with driver.session() as session:
        for edge_type in edge_types:
            labels_n = ":".join(sorted(edge_types["labels(n)"]))
            keys_n = ":".join(sorted(edge_types["keys(n)"]))
            type_r = ":".join(sorted(edge_types["type(r)"]))
            labels_m = ":".join(sorted(edge_types["labels(m)"]))
            keys_m = ":".join(sorted(edge_types["keys(m)"]))

            query = "MATCH(n),(m) WHERE n.labels='"+labels_n+"' AND n.props='"+keys_n+"' AND m.labels='"+labels_m+"' AND m.props='"+keys_m+"' CREATE (n)-[r:"+type_r+"]-(m)"
            session.run(query)
    
    driver.close()

    

