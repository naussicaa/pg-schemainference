""" Functions to query a Property Graph (PG) in order to get nodes, edges and collect statistics.
    They also preprocess the PG and serialize it in JSON format and check edge cardinalities and optionality.
    The PG needs to be already loaded in Neo4j. """

##### Imports
import neo4j
from neo4j import BoltStatementResult
import neotime

import itertools
import more_itertools as mit
import json
import numpy as np

from format_utils import label_format, convert_to_num


def Neo4jNode_to_json(node):
    """ serialize a neo4j.types.graph.Node node into json format (as a record).
    
    Parameters
    ----------
    node : neo4j.types.graph.Node
        The node.
    
    Returns
    -------
    out : dict
        The node serialized in json format.
    
    """
    lab = label_format(node.labels)
    out = {lab:{}} # output = node in json format

    if lab:
        # the node is labeled
        out = {lab:{}} # output = node in json format
    else:
        # the node is unlabeled
        out = {}
    
    # properties
    for prop in node.items():
        # iterate over the properties of the node
        key = prop[0]
        value = prop[1]
        # check whether value is a neo4j cartesian point
        if type(value) == neo4j.types.spatial.CartesianPoint:
            value = {"srid": value.srid, "x": value.x, "y": value.y, "z": value.z, "_neo4j.types.spatial.CartesianPoint":"type"}
        # check whether value is a neo4j DateTime
        elif type(value) == neotime.DateTime:
            #print("datetime",key,value)
            value = { "year": value.year, "month": value.month, "day": value.day, "hour":value.hour, "minute":value.minute,"second":value.second, "_neotime.DateTime":"type"}
        # check whether value is a neo4j Date
        elif type(value) == neotime.Date:
            #print("datetime",key,value)
            value = { "year": value.year, "month": value.month, "day": value.day, "_neotime.Date":"type"}
        # check whether value is a neo4j Time
        elif type(value) == neotime.Time:
            #print("datetime",key,value)
            value = {"hour":value.hour, "minute":value.minute,"second":value.second, "_neotime.Time":"type"}
        # check whether value is a neo4j Duration
        elif type(value) == neotime.Duration:
            #print("datetime",key,value)
            value = { "years": value.years, "months": value.months, "days": value.days, "hours":value.hours, "minutes":value.minutes,"seconds":value.seconds, "_neotime.Duration":"type"}
        
        # check whether value is a string
        elif type(value)==str:
            # check whether value is NaN
            if value.casefold() == "nan" or value == "Infinity" or value == "Inf":
                value = 0.0 #"Null"
            # check whether value is empty string
            elif value == "":
                value = "Null"
            # check whether value is a record

            elif value[0] == "{" and value[-1] == "}" and value.find(':')>0:
                #print("dict",key,value)
                if (value.find("'")>0 or value.find('"')>0):
                    # value is a correctly formated
                    try:
                        value = json.loads(value) 
                    except json.decoder.JSONDecodeError:
                        print(value,"cannot be loaded in JSON format. It will be treated as a string.")
                        value = str(value)
                else:
                    # value is not correctly formated (quotes need to be inserted)
                    value=value
                    propkey, propvalue = value.split(":")
                    propkey = '"'.join([propkey[0], propkey[1:]]) + '"'
                    propvalue = '"' + propvalue[:-1] + '"}'
                    value = json.loads(":".join([propkey, propvalue])) 
            else:
                # check whether value is an int or a float and convert it adequatly
                value = convert_to_num(value)
                
        elif type(value) == float and ( np.isnan(value) or np.isinf(value) ):
            # value is infinity or NaN, thus it is of data type Number
            value = 0.0
        
        if lab:
            # the node is labeled
            out[lab][key] = value 
        else:
            # the node is unlabeled
            out[key] = value 
    
    # json.dumps needed to have double quotes inside the json records
    return json.dumps(out)

def Neo4jEdge_to_json(edge):
    """ serializes an edge into json format (as a record).
    
    Parameters
    ----------
    edge : dict with the following key-value pairs:
        nlabel : array
            Labels of the source node
        elabel : string
            Label of the edge
        mlabel : array
            Labels of the target node
        e : neo4j.types.graph.Relationship object
            The edge 
        
    
    Returns
    -------
    out : dict
        The edge serialized in json format.
        
    """
    # Note: the edge type is (nlabel)-[elabel]->(mlabel)
    
    # labels of the source node (if multi-labels, they are separated by ":")
    nlab = label_format(edge['nlabel'])
    # labels of the target node (if multi-labels, they are separated by ":")
    mlab = label_format(edge['mlabel'])    
    # put all the labels together with the label of the edge
    # Note: does not deal with multi-labeled edges because Neo4j does not
    lab = nlab + "::" + edge['elabel'] + "::" + mlab 
    
    out = {lab:{}} # output = edge in json format
    
    # properties
    for prop in edge['e'].items():
        # iterate over the properties of the node
        key = prop[0]
        value = prop[1]
        # check whether value is a string
        if type(value)==str:
            # check whether value is Nan
            if value.casefold() == "nan":
                value = "Null" 
            # check whether value is empty string
            elif value.casefold() == "":
                value = "Null"
            # check whether value is a record
            elif "{" in value:
                value = json.loads(value)
            else :
                # check whether value is an int or a float and convert it adequatly
                value = convert_to_num(value)
        out[lab][key] = value
    
    # json.dumps needed to have double quotes inside the json records
    return json.dumps(out)
    

def pg_to_json(driver, filename, limitnodes=-1, limitedges=-1):
    """ Serializes to JSON a property graph (PG) available through the driver
        and stores it into the file filename.jsonlines
        Only nodes (edges) with a node (edge) type with properties (other than their labels)
        are written in the JSON file.
        The nodes (edges) that have no properties other than their labels 
        are returned as dictionnaries.
    
    
    Parameters
    ----------
    driver : GraphDatabase.driver object
        Driver used to access the PG stored in a Neo4j database.
    filename : string
        The name of the file where to store the serialized PG. 
    limitnodes : int, optional
        Number of nodes to match. If negative, all nodes will be matched.
    limitedges : int, optional
        Number of edges to match. If negative, all edges will be matched. 
    
    Returns
    -------
    edgeTypes : Python list of dict
        The list of edge types to get the cardinalities and optionality from.
        its format is: [{'nlabel': ['Label1:...:Labeln'], 
                         'elabel': 'Label',
                         'mlabel': ['Label1:...:Labeln'],
                         'nbn': int,
                         'nbm': int,
                         'nbedges': int}, ...]
        with the source node type labels as nlabel, target node type labels as mlabel, 
        edge type label as elabel, # of instances of source nodes (of type n) as nbn,
        # of instances of target nodes (of type m) as nbm, # of instances of this edge type as nbedges.
    nodeTypes : Python list of dict
        List of node types 'nlabel' and their number of instances 'nbn'.
        Its format is: [{'nlabel': ['Label1:...:Labeln'], 'nbn': int}, ...] 
    nodesNoProp : dict
        Contains the node types with no properties (other than their label)
        its format is {'Label1:...:Labeln' : {}, ...}
        
    """
    ### Queries ###
    
    with driver.session() as session:
        
        ### Deal with nodes ###
        
        ## match nodes without labels
        nResults = session.run("MATCH (n) \
            WHERE size(labels(n)) = 0 \
            RETURN DISTINCT n")
        nInGraph = BoltStatementResult.data(nResults)
        unlabeledFile = open(filename.split('.')[0] + "_unlabeled.jsonlines","w")
        for node in nInGraph:
            # add the nodes to the file
            #print(node)
            unlabeledFile.write(str(Neo4jNode_to_json(node['n'])) + "\n")
            #print(Neo4jNode_to_json(node['n']))
        unlabeledFile.close()
        
        ## get node types (and the number of instances of each node type n)
        result = session.run(
            "MATCH (n) \
            WITH labels(n) AS nlabel, size(collect(distinct n)) as nbn  \
            RETURN nlabel, nbn")
        nodeTypes = BoltStatementResult.data(result)     
                
        ## get node properties
        nodeResults = session.run("call db.schema.nodeTypeProperties")
        nodeProperties = BoltStatementResult.data(nodeResults)
        
        ## create a list of node types label(s)
        nodesNoProp = {} # dict of labels corresponding to node types with no properties
        labels = [] # list of labels corresponding to node types with properties 
                    # (the ones that will go through MapReduce)                         
        for prop in nodeProperties:
            # iterate over node properties
            if (None in prop.values()):
                # if node type does not have any properties
                if prop['nodeType'] != "":
                    # node type has a label
                    nlabel = label_format(prop['nodeLabels'])
                    nodesNoProp[nlabel]={}        
            else:
                # node type has properties
                if prop['nodeType'] != "":
                    # node type has a label
                    labels.append(prop['nodeLabels'])
                
        # remove the duplicates from the list of node types labels with properties
        nodeLabels = list(labels for labels,_ in itertools.groupby(labels)) 
        
        # labels to be matched to feed the MapReduce (i.e. node types with properties)
        nodesPropLabels = "False" # Cypher snippet corresponding to the labels to be matched,
                                  # initialized to False 
        print(nodeLabels)
        for ntype in nodeLabels:
            # to deal with multi-labeled node types
            lab = label_format(ntype)
            nodesPropLabels += " OR n:" + lab
            #print(3)
        print(4)
            
        # match nodes with labels query
        if limitnodes<0:
            # no limit on number of nodes to match
            nResults = session.run(
                        "MATCH (n) \
                        WHERE {} \
                        RETURN DISTINCT n".format(nodesPropLabels))
            nInGraph = BoltStatementResult.data(nResults)
        else:
            # limited number of nodes to match
            nResults = session.run(
                        "MATCH (n) \
                        WHERE {lab} \
                        RETURN DISTINCT n LIMIT {limit}".format(lab=nodesPropLabels, limit=limitnodes))
            nInGraph = BoltStatementResult.data(nResults)

        print(5)

        ### Deal with edges ###
        
        ## get edge types 
        # (and for each edge type e, the # of instances of source nodes n, target nodes m and edges)

    with driver.session() as session:

        edgeTypesResults = session.run(
            "MATCH (n)-[e]->(m) \
            WITH labels(n) AS nlabel, size(collect(distinct n)) AS nbn, type(e) AS elabel,\
                labels(m) AS mlabel, size(collect(distinct m)) AS nbm, count(e) AS nbedges \
            RETURN DISTINCT nlabel, elabel, mlabel, nbn, nbm, nbedges")
        edgeTypes = BoltStatementResult.data(edgeTypesResults)    
        
        ## get edges properties
        edgeResults = session.run("call db.schema.relTypeProperties")
        edgeProperties = BoltStatementResult.data(edgeResults)
        
        ## create a list of edge label(s)
        elabels = [] # list of labels corresponding to edge types with properties 
                    # (the ones that will go through MapReduce)
        for prop in edgeProperties:
            # iterate over edge properties
            if not (None in prop.values()):
                # if edge type has some properties
                elabels.append([prop['relType'].strip(':`')])
                
        # remove the duplicates from the list of edge types with properties
        edgeLabels = list(elabels for elabels,_ in itertools.groupby(elabels)) 
        
        # labels to be matched to feed the MapReduce (i.e. edge types with properties)
        edgesPropLabels = "False" # Cypher snippet corresponding to the labels to be matched,
                                  # initialized to False     
        for etype in edgeLabels:
            # Note: there cannot be multi-labeled edges in Neo4j
            edgesPropLabels += " OR type(e)='" + etype[0] + "'"
        
        print(6)

    # match edges query
    if limitedges<0:
        # no limit on number of edges to match
        try:
            with driver.session() as session:
                print(7)
                eResults = session.run(
                            "MATCH (n)-[e]->(m) \
                            WHERE {} \
                            WITH type(e) AS elabel, e, labels(n) AS nlabel, labels(m) AS mlabel \
                            RETURN nlabel, elabel, mlabel, e".format(edgesPropLabels))
                print(11)
                eInGraph = BoltStatementResult.data(eResults)
                test = True
        except Exception as e:
            print(12)
            print(str(e))
            test = False

        if not(test):
            with driver.session() as session:
                    print(7)
                    eResults = session.run(
                                "MATCH (n)-[e]->(m) \
                                WHERE {} \
                                WITH type(e) AS elabel, e, labels(n) AS nlabel, labels(m) AS mlabel \
                                RETURN nlabel, elabel, mlabel, e".format(edgesPropLabels))
                    print(11)
                    eInGraph = BoltStatementResult.data(eResults)

    else:
        # limited number of edges to match
        with driver.session() as session:
            eResults = session.run(
                        "MATCH (n)-[e]->(m) \
                        WHERE {lab} \
                        WITH type(e) AS elabel, e, labels(n) AS nlabel, labels(m) AS mlabel \
                        RETURN nlabel, elabel, mlabel, e LIMIT {limit}".format(lab=edgesPropLabels, limit=limitedges))
            eInGraph = BoltStatementResult.data(eResults)
    print(8)

    ### Serialize to json and write output file ###
    
    ### Nodes
    ## match nodes that have properties (these nodes will be fed the MapReduce)
    nodefile = open(filename.split('.')[0] + "_nodes.jsonlines", "w")  # output file conaining the list of serialized nodes 
    #file.write("{" + json.dumps("Nodes") + ":[") # record containing the nodes
    
    # deal with empty query result
    if nInGraph != []:
        #print(11)
        # the result of the query is not empty
        #for node in nInGraph[:-1]:
        for node in nInGraph:
            # add the nodes to the file
            #print(node)
            try:
                nodefile.write(str(Neo4jNode_to_json(node['n'])) + "\n")
            except Exception as e:
                print(str(e))
                print(node)
            #print(13)
        #file.write(str(Neo4jNode_to_json(nInGraph[-1]['n'])) + "]} \n") # last node
    
    else:
        # the result of the query is empty
        #print(14)
        nodefile.write("{}\n") # close the record
        #print(15)
        print("No nodes with properties in the graph.") 
    #print(10) 
    nodefile.close() # close file
    print("The JSON file {} is written".format(filename.split('.')[0] + "_nodes.jsonlines"))

    print(9)
    ### Edges
    ## match edges that have properties (these edges will go through the MapReduce)
    edgefile = open(filename.split('.')[0] + "_edges.jsonlines", "w")  # output file conaining the list of serialized edges 
    
    # deal with empty query result
    if eInGraph != []:
        # the result of the query is not empty
        for edge in eInGraph:
            # add the edges to the file
            try:
                edgefile.write(str(Neo4jEdge_to_json(edge)) + "\n")
            except Exception as e:
                print(str(e))
                print(edge)
        
    else:
        # the result of the query is empty
        edgefile.write("{}\n") # close the record
        print("No edges with properties in the graph.")

    print(10)
    edgefile.close() # close file
    print("The JSON file {} is written".format(filename.split('.')[0] + "_edges.jsonlines"))

    return edgeTypes, nodeTypes, nodesNoProp

    
def get_edges_card(edgeTypes, nodeTypes):
    """ returns the edge cardinalities and optionality.
    
    Parameters
    ----------
    edgeTypes : Python list of dict
        List of edge types to get the cardinalities and optionality from.
        Its format is: [{'nlabel': ['Label1:...:Labeln'], 
                         'elabel': 'Label',
                         'mlabel': ['Label1:...:Labeln'],
                         'nbn': int,
                         'nbm': int,
                         'nbedges': int}, ...]
        with the source node type labels as nlabel, target node type labels as mlabel, 
        edge type label as elabel, # of instances of source nodes (of type n) as nbn,
        # of instances of target nodes (of type m) as nbm, # of instances of this edge type as nbedges.
    nodeTypes : Python list of dict
        List of node types 'nlabel' and their number of instances 'nbn'.
        Its format is: [{'nlabel': ['Label1:...:Labeln'], 'nbn': int}, ...] 
        
    Returns
    -------
    edgesCard : dict
        Dictionnary containing all edge types 
        and their corresponding cardinality and optionality information.
    
    """
            
    ### Get edge cardinalities and ordinalities ###
            
    edgesCard = {} # store edge cardinalities for each edge type
    for etype in edgeTypes:
        # iterate over edge types
        
        # get the labels
        nlabel = etype['nlabel'] # label(s) of the source node type n
        nlab = label_format(nlabel) # deal with multiple labels
        elabel = etype['elabel'] # label(s) of the edge type e
        mlabel = etype['mlabel'] # label(s) of the target node type m
        mlab = label_format(mlabel) # deal with multiple labels
            
        edge = {}
        
        # get the # of instances of source nodes (of type n)
        nbSource =  etype['nbn']
        # get the # of instances of target nodes (of type m)
        nbTarget =  etype['nbm']
        # get the # of instances of edges
        nbEdges =  etype['nbedges']
        
        # get the # of instances of node type n
        nbnIndex = list(mit.locate(nodeTypes, pred = lambda d: d['nlabel'] == nlabel))
        nbn = nodeTypes[nbnIndex[0]]['nbn']
        # get the # of instances of node type m
        nbnIndex = list(mit.locate(nodeTypes, pred = lambda d: d['nlabel'] == mlabel))
        nbm = nodeTypes[nbnIndex[0]]['nbn']
        
        ## check whether there exist an edge type e that is optional:
        ## does there exist instances of node type n and m s.t. (n)-[e]->(m) does not exist? 
        
        # Note: if # of instances of source nodes of edge type e < # of instances of node type n 
        # or < # of instances of node type m, 
        # then the edge type is optional           
        if nbSource < nbn or nbTarget < nbm:
            # the edge type is optional
            edge['meta_mandatory'] = False
        
        else:
            edge['meta_mandatory'] = True
            
        # Note: if # of instances of source nodes of edge type e < # of instances of node type n,
        # then the edge type is optional for the source node 
        if nbSource < nbn :
            mandatorySource = False
        else:
            mandatorySource = True
            
        # Note: if # of instances of source nodes of edge type e < # of instances of node type m,
        # then the edge type is optional for the target node 
        if nbTarget < nbm:
            mandatoryTarget = False 
        else:
            mandatoryTarget = True
            
        ## check the edge cardinalities
        
        # Note: if # source nodes = # target nodes = # edges,
        # then cardinality is one-to-one
        if nbSource == nbTarget == nbEdges:
            edge['meta_cardinality'] = "1:1"
            if mandatorySource:
                edge['meta_cardinality'] = "1 : "
            else:
                edge['meta_cardinality'] = "0..1 : "
            if mandatoryTarget:
                edge['meta_cardinality'] += "1"
            else:
                edge['meta_cardinality'] += "0..1"
            
            
        # Note: if # source nodes > # target nodes and # edges = # source nodes,
        # then cardinality is many-to-one
        elif nbSource > nbTarget and nbEdges == nbSource:
            edge['meta_cardinality'] = "M:1"
            if mandatorySource:
                edge['meta_cardinality'] = "1..* : "
            else:
                edge['meta_cardinality'] = "0..* : "
            if mandatoryTarget:
                edge['meta_cardinality'] += "1"
            else:
                edge['meta_cardinality'] += "0..1"
            
        # Note: if # source nodes < # target nodes and # edges = # target nodes,
        # then cardinality is one-to-many
        elif nbSource < nbTarget and nbEdges == nbTarget:
            edge['meta_cardinality'] = "1:N"
            if mandatorySource:
                edge['meta_cardinality'] = "1 : "
            else:
                edge['meta_cardinality'] = "0..1 : "
            if mandatoryTarget:
                edge['meta_cardinality'] += "1..*"
            else:
                edge['meta_cardinality'] += "0..*"
            
        # Note: if # source nodes > # target nodes and # edges > # source nodes
        # OR # source nodes < # target nodes and # edges > # target nodes,
        # then cardinality is many-to-many
        else:
            edge['meta_cardinality'] = "M:N"
            if mandatorySource:
                edge['meta_cardinality'] = "1..* : "
            else:
                edge['meta_cardinality'] = "0..* : "
            if mandatoryTarget:
                edge['meta_cardinality'] += "1..*"
            else:
                edge['meta_cardinality'] += "0..*"
            
        edgesCard[nlab + "::" + elabel + "::" + mlab] = edge
        
    return edgesCard
            
        
 
       
