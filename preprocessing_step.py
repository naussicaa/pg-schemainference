""" Step 1 : Preprocessing data """

##### Imports
from termcolor import colored

### Neo4j imports
from neo4j import BoltStatementResult


def preprocessing(driver):
    """  Queries a property graph using the driver to get all needed labels',properties' and nodes' information

    Parameters
    ----------
    driver : GraphDatabase.driver object
        Driver used to access the PG stored in a Neo4j database.

    Returns
    -------
    amount_dict : Python dict
        A dictionary with node strings as keys and the number of occurrences of the node as a value
        Its format is : {'Label1 Label2 Label3 prop1 prop2 prop3 ...': int, ...}
    list_of_distinct_nodes : Python list
        A list of node strings
        Its format is : ['Label1 Label2 prop1', 'Label1 Label3 prop2', 'prop4 prop5', ...]
    distinct_labels : Python list
        A list of labels
        Its format is : ['Label1', 'Label2', 'Label3', ...]
    labs_sets : Python list of list
        A list of all labels sets
        Its format is : [['Label 1','Label2'],['Label1'],['Label3'],...]
    """
    
    print(colored("Querying neo4j to get all distinct labels:", "yellow"))
    with driver.session() as session:
        all_labels = session.run(
            "MATCH(n) WITH LABELS(n) AS labs \
            UNWIND labs AS lab \
            RETURN DISTINCT lab"
            )
        all_labels = BoltStatementResult.data(all_labels)

        distinct_labels = []
        for labs in all_labels:
            distinct_labels.append(labs["lab"])
    print(colored("Done.", "green"))

    print(colored("Querying neo4j to get all distinct sets of labels:", "yellow"))
    with driver.session() as session:
        labels_sets = session.run(
            "MATCH(n) \
            RETURN DISTINCT LABELS(n)"
            )
        labels_sets = BoltStatementResult.data(labels_sets)

        labs_sets = []
        for labels_set in labels_sets:
            labs_sets.append(labels_set["LABELS(n)"])
    print(colored("Done.", "green"))

    print(colored("Querying neo4j to get all distinct sets of labels and props:", "yellow"))
    with driver.session() as session:
        #get all nodes' labels and properties' names
        distinct_nodes = session.run(
            "MATCH(n) \
            RETURN DISTINCT labels(n), keys(n), COUNT(n)"
            )
        distinct_nodes = BoltStatementResult.data(distinct_nodes)
    print(colored("Done.", "green"))

    # Storing the number of repetitions of the node
    amount_dict = {}

    # transform neo4j dict to python list of string
    list_of_distinct_nodes=[]
    for node in distinct_nodes:
        #get a list of labels
        labels = sorted(node["labels(n)"])

        #get a list of properties
        properties = sorted(node["keys(n)"])

        labels_properties = labels+properties
        labels_properties_str = ' '.join(labels_properties)
        if labels_properties_str in list_of_distinct_nodes:
            amount_dict[labels_properties_str] += node["COUNT(n)"]
        else:
            list_of_distinct_nodes.append(labels_properties_str) 
            amount_dict[labels_properties_str] = node["COUNT(n)"]

    return amount_dict,list_of_distinct_nodes,distinct_labels,labs_sets
