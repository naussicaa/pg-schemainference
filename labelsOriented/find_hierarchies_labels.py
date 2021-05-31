""" find hierarchies """


##### Imports
from itertools import starmap, combinations
import itertools
from operator import itemgetter
from itertools import groupby
import numpy as np
import copy
import json
import ast # to convert string to dict

from neo4j import GraphDatabase
from neo4j import BoltStatementResult


from regraph import Neo4jHierarchy
from regraph import Neo4jGraph
import regraph.attribute_sets as atsets

from format_utils import label_format

def list_intersections(tab):
    """ Get the pairwise intersections of all sets in the list tab 
    and the list of sets that do not intersect any of the other sets.
    
    Parameters
    ----------
    tab : Python list of sets
        The list of sets to intersect.
    
    Returns
    -------
    inters : Python list of sets
        The list of nonempty pairwise intersections.
    intersNo : Python list of sets
        The list of sets that do not intersect with any other set.
    intersPairs : Python list of sets
        The list of pairs that intersect.
        
    """
    
    inters = [] # list of pairwise intersections
    
    for i in range(len(tab)):
        x = tab[i]
        for j in range(len(tab[i:])-1):
            y = tab[j+i+1]
            xintersy = x & y # intersection between x and y
            if xintersy != set() and xintersy!=x:
                # the intersection is not empty
                
                # append the list of intersections
                inters.append(xintersy)
                
    ## remove duplicates  
    # convert to list of lists
    intersLists = list(map(list, inters))  
    # sort
    intersLists.sort() 
    # remove duplicates, convert to list of sets
    inters = list(map(set, list(elem for elem,_ in itertools.groupby(intersLists)))) 
    
    return inters

def prop_intersections(labels, nodes):
    """ returns the intersection of the properties of the labeled nodes listed in labels.
    
    Parameters
    ----------
    labels : Python list of strings
        List of labels whose properties we want to intersect.
        The labels have the following format: 'label1:label2:...:labeln'
    nodes : dict
        Contains labels-properties pairs.
    
    Return
    ------
    propInters : dict
        Intersection of the properties.
    
    """
    # list of property keys grouped by labels
    listPropKeys = list(map(lambda x: set(nodes[x].keys()), labels))
    
    # property keys intersection
    propKeyInters = set.intersection(*listPropKeys)
    
    # property values data types union
    propInters = {}
    for prop in propKeyInters:   
        dataType = set()
        for labPair in list(combinations(labels, 2)):
            lab0, lab1 = labPair[0], labPair[1]
            merged = merge_data_types(str(nodes[lab0][prop]), str(nodes[lab1][prop]))
            dataType.add(merged)
        
        sep = " + " # separator for the union of data types
        propInters[prop] = sep.join(dataType) # union of data types are represented with string
        
    return propInters


def get_list_content(prop):
    """ Get the content of the lists contained in the string prop.
    
    Parameters
    ----------
    prop : str
    
    Returns
    -------
    proplists : Python list
        Content of lists contained in prop
    propelems : Python list
        List of the elements in prop (except list content) formatted adequately
    """
    
    proplists = [] # content of lists contained in prop
    propelems = [] # list of the elements in prop1 formatted adequately
    
    indexBracketl = prop.find("[") # index of "["
    indexBracketr = prop.rfind("]") # index of "]"
    if indexBracketl > -1:
        # there is a list in the string prop
        proplists.append(prop[indexBracketl+1 : indexBracketr].strip("'"))
        propNotlist = prop[: indexBracketl] + prop[indexBracketr+1 : ] # not inside the list
        propelems += list(filter(lambda x: x != "", propNotlist.split(' + ')))
        
    else:
        # there is no lists  in the string
        propelems += prop.split(" + ")
        
    return proplists, propelems

def get_dict_content(prop):
    """ Get the content of the dict contained in the string prop.
    
    Parameters
    ----------
    prop : str
    
    Returns
    -------
    propdicts : Python list
        Content of dicts contained in prop
    propelems : Python list
        List of the elements in prop (except dict content) formatted adequately
    """
    propdicts = [] # content of dicts contained in prop
    propelems = [] # list of the elements in prop1 formatted adequately
    
    indexBracketl = prop.find("{") # index of "{"
    indexBracketr = prop.rfind("}") # index of "}"
    if indexBracketl > -1:
        # there is a dict in the string prop
        propdicts.append(ast.literal_eval(prop[indexBracketl : indexBracketr+1].strip("'")))
        propNotdict = prop[: indexBracketl] + prop[indexBracketr+1 : ] # not inside the list
        propelems += list(filter(lambda x: x != "", propNotdict.split(' + ')))
        
    else:
        # there is no dicts  in the string
        propelems += prop.split(" + ")
        
    return propdicts, propelems
    

def merge_data_types(prop1, prop2):
    """ Merges two data types: prop1 and prop2.
    
    Parameters
    ----------
    prop1 : dict, Python list or string
        A property value data type.
        The dict values can be dicts, lists or strings.
        The list can contain dicts, lists or strings.
    prop2 : dict, Python list or string
        A property value data type.
        The dict values can be dicts, lists or strings.
        The list can contain dicts, lists or strings.
        
    Returns
    -------
    propMerged : dict, Python list or string
        The merged data type.
        
    """
    print("merge_data : prop1", type(prop1), prop1, "prop2", type(prop2), prop2)
    if type(prop1) != type(prop2):
        # then, the merged data type is a union of the two data types
        sep = " + " # separator for the union of data types
        propMerged = sep.join({str(prop1), str(prop2)})
        
    elif type(prop1) == list:
        # then, prop2 is also a list and the merged data type is a list
        propMerged = ""
        
        prop1dicts = [] # list of dictionnaries contained in prop1
        prop2dicts = [] # list of dictionnaries contained in prop2
        
        prop1lists = [] # content of lists contained in prop1
        prop2lists = [] # content of lists contained in prop2
        
        prop1elems = [] # list of the elements in prop1 formatted adequately
        for elem in prop1:
            if type(elem) == dict:
                prop1dicts.append(elem)
                
            elif type(elem) == list:
                prop1lists += elem
            else:
                prop1lst, prop1elems = get_list_content(elem)
                prop1lists += prop1lst
                
        prop2elems = [] # list of the elements in prop2 formatted adequately
        for elem in prop2:
            if type(elem) == dict:
                prop2dicts.append(elem)
                #print(prop2dicts)
            elif type(elem) == list:
                prop2lists += elem
            else:
                prop2lst, prop2elems = get_list_content(elem)
                prop2lists += prop2lst
        
        ## deal with potential lists        
        proplistSet = set()
        if prop1lists and prop2lists:
            # both prop1lists and prop2lists are non-empty (i.e., they contain lists)
            for pair in list(itertools.product(prop1lists, prop2lists)):
                mergedPair = merge_data_types(pair[0], pair[1]) # recursive call
                proplistSet.update(set(mergedPair.split(" + ")))
                
            proplistSet = {str([" + ".join(proplistSet)])}
        else:
            # at least one of the two is empty
            if prop1lists:
                # prop1list is not empty
                prop1elems.append(str(prop1lists)) 
            elif prop2lists:
                # prop2list is not empty
                prop2elems.append(str(prop2lists))
                    
        # deal with the dicts:
        propdictList = []
        if prop1dicts and prop2dicts:
            # none of the two is empty
            for pair in list(itertools.product(prop1dicts, prop2dicts)):
                mergedPair = merge_data_types(pair[0], pair[1]) # recursive call
                propdictList.append(json.dumps(mergedPair))
                  
        else:
            propdictList += list(map(json.dumps, prop1dicts))
            propdictList += list(map(json.dumps, prop2dicts))
        
        propMerged = " + ".join(set(prop1elems) | set(prop2elems) | set(propdictList) | proplistSet)
        
        # wrap mergedPair in a list
        propMerged = str([propMerged])
        
        propMerged = [merge_data_types(prop1[0], prop2[0])]
        
        
    elif type(prop1) == dict:
        # then, prop2 is also a dict and the merged data type is a dict
        propMerged = {}
        # property keys intersection:
        propKeyInters = set(prop1.keys()) & set(prop2.keys()) 
        for key in propKeyInters:
            propMerged[key] = merge_data_types(prop1[key], prop2[key]) # merged value
            
        # prop1 property keys not in the intersection:
        prop1KeyOther = set(prop1.keys()) - propKeyInters 
        for key in prop1KeyOther:
            # Note: this key-value pair is optional
            prop1other = prop1[key]
            if type(prop1other) == str and "?" not in prop1other:
                prop1other += " ?"
            elif type(prop1other) == list and "?" not in prop1other[0]:
                #prop1other.append("?")
                prop1other= [str(prop1other[0]) + " ?"]
            elif type(prop1other) == dict:
                prop1other["meta_mandatory"] = False
            propMerged[key] = prop1other # update output
            
        # prop2 property keys not in the intersection:
        prop2KeyOther = set(prop2.keys()) - propKeyInters 
        for key in prop2KeyOther:
            # Note: this key-value pair is optional
            prop2other = prop2[key]
            if type(prop2other) == str and "?" not in prop2other:
                prop2other += " ?"
            elif type(prop2other) == list and "?" not in prop2other[0]:
                #prop2other.append("?")
                prop2other= [str(prop2other[0]) + " ?"]
            elif type(prop2other) == dict:
                prop2other["meta_mandatory"] = False    
            propMerged[key] = prop2other # update output
            
        #propMerged = json.dumps(propMerged)
        propMerged = propMerged
           
            
    elif type(prop1) == str:
        # prop2 is also a string and the merged data type is a string
        
        # to be able to deal with optional string data types
        optional = False
        if "?" in prop1 or "?" in prop2:
            optional = True
            prop1 = prop1.replace(" ?",'')
            prop2 = prop2.replace(" ?",'')
            if prop1 == prop2:
                propMerged = prop1 + " ?"
                return propMerged
        
        prop1dicts = [] # list of dictionnaries contained in prop1
        prop2dicts = [] # list of dictionnaries contained in prop2
        
        ## deal with potential dicts
        prop1dicts, prop1elems = get_dict_content(prop1)
        prop2dicts, prop2elems = get_dict_content(prop2)
        if prop1dicts and prop2dicts:
            # both prop1lists and prop2lists are non-empty (i.e., they contain lists)
            propdictList = []
            for pair in list(itertools.product(prop1dicts, prop2dicts)):
                mergedPair = merge_data_types(pair[0], pair[1]) # recursive call
                propdictList.append(mergedPair)
            sep = " + " # separator for the union of data types
            propMerged = " + ".join([" + ".join(set(prop1elems) | set(prop2elems)), str([" + ".join(propdictList)])])
            
        else:
            # at least one of the two is empty
            if prop1dicts:
                # prop1list is not empty
                prop1elems.append(str(prop1dicts)) 
            elif prop2dicts:
                # prop2list is not empty
                prop2elems.append(str(prop2dicts))
                    
            propMerged = " + ".join(set(prop1elems) | set(prop2elems))
            

        ## deal with potential lists          
        prop1lists, prop1elems = get_list_content(prop1)
        prop2lists, prop2elems = get_list_content(prop2)   
        print('prop1', prop1, 'prop1lists', prop1lists, 'prop1elems', prop1elems)        
        print('prop2', prop2,'prop2lists', prop2lists, 'prop2elems', prop2elems)

        if prop1lists and prop2lists:
            # both prop1lists and prop2lists are non-empty (i.e., they contain lists)
            proplistSet = set()
            for pair in list(itertools.product(prop1lists, prop2lists)):
                mergedPair = merge_data_types(pair[0], pair[1]) # recursive call
                proplistSet.update(set(mergedPair.split(" + ")))
            sep = " + " # separator for the union of data types
            if not prop1elems and not prop2elems:
                # both are empty
                propMerged = [" + ".join(proplistSet)]
                ## deal with optional string data type
                if optional:
                    propMerged = str([propMerged[0].replace(" ?",'') + " ?"])
            else:
                # one is non-empty
                propMerged = " + ".join([" + ".join(set(prop1elems) | set(prop2elems)), str([" + ".join(proplistSet)])])
            
        else:
            # at least one of the two is empty
            if prop1lists:
                # prop1list is not empty
                prop1elems.append(str(prop1lists)) 
            elif prop2lists:
                # prop2list is not empty
                prop2elems.append(str(prop2lists))
            
            propMerged = " + ".join(set(prop1elems) | set(prop2elems))
            
            ## deal with optional string data type
            if optional:
                propMerged = propMerged.replace(" ?",'') + " ?"
            
            
    elif type(prop1) == bool:
        propMerged = " + ".join({str(prop1), str(prop2)})
            
    else:
        print("{} is not a recognized data type. The merged data type is set as 'Null'.".format(type(prop1)))
        propMerged = "Null"
        
    return propMerged

def supertype_prop(supertype, subtype, nodes):
    """ returns the properties of the supertype node.
    
    Parameters
    ----------
    supertype : Python list
        List of labels of the supertype.
    subtype : Python list
        List of labels of the subtype of supertype.
    nodes : dict
        Contains labels-properties pairs.
    
    Return
    ------
    superProps : dict
        Properties of the supertype node.
    
    """
    ## intersection between supertype and subtype properties
    propInters = prop_intersections([label_format(supertype), label_format(subtype)], nodes)
    superProps = propInters
    
    ## deal with supertype properties that are not a subtype property
    # Note: these properties are considered as optional in the supertype node
    optSuperProps = set(nodes[label_format(supertype)].keys()) - set(propInters.keys()) # optional properties of the supertype
    for opt in optSuperProps:
        optProp = nodes[label_format(supertype)][opt]        
        if type(optProp) == str:
            optProp += ' ?'
        elif type(optProp) == list:
            optProp.append('?')
            optProp = json.dumps(optProp)
        else:
            # record
            optProp['meta_mandatory'] = False
            optProp = json.dumps(optProp)
        #superProps[opt] = json.dumps(optProp)
        superProps[opt] = optProp
        
    #print("\n")
   
    return superProps


def crt_inheritance_edge(elem0, elem1, edges, nodes):
    """ Procedure that creates the inheritance edge between elem0 and elem1, if it applies
        and appends the nodes and edges list accordingly.
    
    Parameters
    ----------
    elem0 : set
        A set of labels.
    elem1 : set
        A set of labels.
    edges : Numpy array
        List of edges stored with the following format:
        (source labels, target labels, {"type": edge labels, "property key": property value, ...})
    nodes : Numpy array
        List of nodes stored with the following format:
        (labels, {"property key":property value, ...})
     
    """
    
    if elem0 != elem1:
        if elem1.issubset(elem0):      
            # elem0 is a subtype of elem1
            
            # to deal with multi-labeled node types:
            # labels of the subtype and supertype, respectively
            sublab, superlab = label_format(elem0), label_format(elem1)
            
            # create the corresponding inheritance edges and append edge list
            edges[sublab + "::SubtypeOf::" + superlab]={}
            
 
            
        elif elem0.issubset(elem1):      
            # elem1 is a subtype of elem0
            
            # to deal with multi-labeled node types:
            # labels of the subtype and supertype, respectively
            sublab, superlab = label_format(elem1), label_format(elem0)
                        
            # create the corresponding inheritance edges and append edge list
            edges[sublab + "::SubtypeOf::" + superlab]={}

            

        

def infer_node_hierarchies(schema, filename):
    """ infers node hierarchies in the provided schema
        and writes it in a JSON file.
    
    Parameters
    ----------
    schema : dict
        Schema from which to infer node hierarchies.
    filename : str
        Name of the output file containing the schema in JSON format.
        
    Return
    ------
    nodes : dict
        Contains the nodes of the schema
    edges : dict
        Contains the edges of the schema
    
    """
    ### Outputs 
    edges = copy.deepcopy(schema['Edges'])
    nodes = copy.deepcopy(schema['Nodes'])

    # node labels    
    nodeLabels = list(map(lambda s: s.lstrip(":").split(":"), nodes.keys()))
    setNLabels = list(map(set, nodeLabels))

    
    ### find supertypes
    supertypes = list_intersections(setNLabels)
    
    ## append node list with supertypes
    for stype in supertypes:
        # to deal with multi-labeled node types
        lab = label_format(stype)
        # append node list with supertypes
        nodes.setdefault(lab, {})
    
    ### find subtypes (including overlapping subtypes) and deal with them
    # new node labels    
    nodeLabels = list(map(lambda s: s.lstrip(":").split(":"), nodes.keys()))
    setNLabels = list(map(set, nodeLabels))

    
    for i in range(len(setNLabels)-1):
        elem0 = setNLabels[i]
                    
        for j in range(len(setNLabels[i:])-1):
            elem1 = setNLabels[j+i+1]
            
            # create inheritance edge if it applies
            crt_inheritance_edge(elem0, elem1, edges, nodes)
                                       
    ### write output file
    out = open(filename,'w')
    #out.write("{nodes:" + str(nodes) + "},\n edges:" + str(edges) +"}") 
    # create the schema dict
    schema = {}
    schema['nodes'] = nodes
    schema['edges'] = edges
    # dump the schema to json and write it in the output file
    out.write(json.dumps(schema))
    out.close()
    return nodes, edges


    
def open_semantic_schema(Nodes, Edges, driver, filename):
    """ Transforms a closed-semantics schema into an open-semantics one and 
        its corresponding constraints (forbidden combinations of types).
        
        Parameters
        ----------
        schema : dict
        driver : GraphDatabase.driver object
            Driver used to access the location where the Neo4j schema will be stored.
        filename : string
            Name of the file where the open-semantics schema will be stored
        
        Returns
        -------
        openSchema : dict
            Open-semantics schema.
        constraints : set of tuples
            Set of forbidden combinations of types.
            
    """
#    ### clean the database
#    with driver.session() as session:
#        session.run("MATCH (n:node) DETACH DELETE n")
    
    nodes = copy.deepcopy(Nodes)
    edges = copy.deepcopy(Edges)  
    
    # Allowed combinations of node labels in the schema
    constraints = set()
    
    ### Add the schema to the Neo4j database
    create_Neo4j_pgschema(nodes, edges, driver, False)
    
    ### Match nodes with 2 or more supertypes (i.e. overlapping types)
    with driver.session() as session:
        result = session.run("MATCH p=(n)<-[e:edge {type:['SubtypeOf']}]-(m)\
                          -[r:edge {type:['SubtypeOf']}]->(o)\
                          RETURN DISTINCT m, n")
    overlaps = result.data()
    
    ### List all inheritance edges
    inheritEdgeList = [e.split("::") for e in edges.keys() if e.split("::")[1]=="SubtypeOf"]
    inheritEdgeList.sort()
    
    ### Inheritance edges stored in a dictionnary {subtype:[supertype1, supertype2, ...], ...}
    inheritEdges={}
    for subtype, etypes in  groupby(inheritEdgeList,key=itemgetter(0)):
        inheritEdges[subtype]=[]
        for subtype,label,supertype in etypes:
            print(inheritEdges[subtype])
            inheritEdges[subtype].append(supertype)
    
    for types in overlaps:
        sublab = types['m']['id'] # labels of the subtype
        superlab = types['n']['id'] # labels of the supertype
        
        # merge the overlappping type property data types with their supertypes'
        for suptype in inheritEdges[sublab]:
            if suptype in nodes and sublab in nodes:
                # to ensure the supertype has not been already deleted
                nodes[suptype] = merge_data_types(nodes[sublab], nodes[suptype])
            edges.pop("::".join([sublab, "SubtypeOf", suptype]), None) 
        
        # add node type constraints:
        # allowed combination of node labels (this specific overlapping type)
        constraints.add(tuple(sublab.split(":")))
        
        # remove the overlapping node type and all inheritance edges starting from it
        nodes.pop(sublab, None) 
                   
    openSchema={}
    openSchema['Nodes'] = nodes
    openSchema['Edges'] = edges
    
    ### write output file
    out = open(filename,'w')
    out.write('{"Nodes":' + str(nodes) + '},\n "Edges":' + str(edges) +'}') 
    out.close()
    
    ### write constraints output file
    outConstraints = open(filename.split('.')[0] + '_constraints.txt','w')
    outConstraints.write(str(constraints)) 
    outConstraints.close()   
        
    return nodes, edges, constraints


###### create Neo4j schema graph
def create_Neo4j_pgschema(Nodes, Edges, driver, all_edges=True):
    """ creates a Neo4j schema graph from the list of nodes and edges.
    
    Parameters
    ----------
    Nodes : dict
        Contains the nodes of the schema
    Edges : dict
        Contains the edges of the schema
    driver : GraphDatabase.driver object
        Driver used to access the location where the Neo4j schema will be stored.
    all_edges : Bool, optional
        True by default. 
        False to remove superfluous edges in cases of multi-layered hierarchies. 
        (the schema will be less cluttered but more difficult to query) 
    
    """
    nodes = copy.deepcopy(Nodes)
    edges = copy.deepcopy(Edges)
    
    ### format the nodes and edges to convert to Neo4j graph (using ReGraph)
    
    ## schema nodes
    # 'id' key renamed to 'ID'                
    for x in nodes.items():
        if "id" in x[1]: 
            x[1]['ID'] = x[1].pop("id")
    # property values converted to string     
    for keyVal, value in nodes.items():
        for key, prop in value.items():
            nodes[keyVal][key] = str(nodes[keyVal][key])
                  
    schemaNodes = list(nodes.items())
    
    ## schema edges
    schemaEdges = [] 
    for keyVal, value in edges.items():
        for key, prop in value.items():
            if type(prop) == dict:
             #   print("dict edge")
                prop = str(json.dumps(prop))
            value[key] = str(prop)
        source, etype, target = keyVal.split("::")
        value['type']=etype
        schemaEdges.append((source, target, value))
    
    schemaNeo4j = Neo4jGraph(driver=driver)
    schemaNeo4j._clear()
    schemaNeo4j.add_nodes_from(schemaNodes)
    schemaNeo4j.add_edges_from(schemaEdges)
    
    ### remove superfluous edges
    if not all_edges:
        with driver.session() as session:
            session.run("MATCH p=(n)-[s:edge{type:['SubtypeOf']}]->(m)\
                        -[r:edge*1.. {type:['SubtypeOf']}]->(o), \
                        q = (n)-[t:edge{type:['SubtypeOf']}]->(o) \
                        DELETE t")
        
