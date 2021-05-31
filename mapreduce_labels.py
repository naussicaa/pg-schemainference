""" Functions to call the MapReduce algorithm [1]_, parse its output,
 merge it with information about edge cardinality and optionality and with other nodes and edges.

References
---------
.. [1] Baazizi, Mohamed-Amine & Colazzo, Dario & Ghelli, Giorgio & Sartiani, Carlo. (2019).
        "Parametric schema inference for massive JSON datasets". 
        The VLDB Journal. 28. 10.1007/s00778-018-0532-7. 
    
"""

##### Imports
import json
import subprocess

def call_mapreduce(pgfilename, equiv="k", nbcores=4):
    """ Calls the MapReduce algorithm from [1]_
    
    Parameters
    ----------
    pgfilename : str
        Name of the JSON file to input the MapReduce algorithm with.
    equiv : str, optional
        Equivalence used in the reduction phase of the MapReduce algorithm.
        Can be either the default value "k" (Kind-equivalence) or "l" (Label-equivalence).
        
    nbcores : int, optional
        Number of cores to use to run the MapReduce algorithm (the default is 4 cores).
        
    Returns
    -------
    MRfilename : str
        Name of the MapReduce output file.
        
    References
    ---------
    .. [1] Baazizi, Mohamed-Amine & Colazzo, Dario & Ghelli, Giorgio & Sartiani, Carlo. (2019).
        "Parametric schema inference for massive JSON datasets". 
        The VLDB Journal. 28. 10.1007/s00778-018-0532-7. 
        
    """
    # raise error if equiv is neither "k" nor "l"
    valid = {"k","l"}
    if equiv not in valid:
        raise ValueError("call_mapreduce: equiv must be either 'k' or 'l'.")

    # command line to run the MapReduce code and format it to be Neo4j compatible
    cmd = "spark-submit --driver-memory 8g --jars 'MapReduce/play-json_2.11-2.7.4.jar' \
     --class 'testing.testRunInference'  --master 'local[{nbcores}]' ./MapReduce/jsonschemainference_2.11-1.1.jar \
     -equiv {equiv} -path {pgfilename}".format(nbcores=nbcores, equiv=equiv, pgfilename=pgfilename) 
     
    cmd_output = subprocess.check_output(cmd, shell=True)
    cmd_output = cmd_output.decode('utf-8')
    MRfilename = cmd_output.split()[-1] # name of the MapReduce output file
    
    return MRfilename


def find_data_type(prop):
    """ find the data type of a json record 
    with the following format: {key: {'__Content':{...}, '__Kind':{...}}}
    it handles optional elements.
    
    Parameters
    ----------
    prop : dict
        JSON record with the following format: {key: {'__Content':{...}, '__Kind':{...}}}
    
    Returns
    -------
    propType
        the data type of prop, 
        either a  string (for basic data types) an array (array type) or a dict (record type)
    
    """
    if not prop:
        # prop is empty
        propType = 'Null'

    else:
        # prop is not empty
        propKind = prop['__Kind']
        
        if propKind == 'ArrayType':
            # data type = array 
            # Note: in Neo4j, arrays must contain homogeneous data types            
            if prop['__Content']['__Kind'] == 'RecordType' or prop['__Content']['__Kind'] == 'union' :
                propType = [find_data_type(prop['__Content'])]
            else:
                propType = [prop['__Content']['__Kind']]
                            
        elif propKind == 'RecordType':
            record = {} # to strore the key-value data type pairs           
            for nprop in prop['__Content'].items():
                key = nprop[0]
                value = nprop[1]
                
                mandatory = True # keep track of optionality
                if '__Optional' in value.keys():
                    # the content is optional
                    value = value['__Optional']
                    mandatory = False
                    
                if value['__Kind'] == 'ArrayType':
                    # array type
                    propType = find_data_type(value) # [ find_data_type(value) ] # recursive call
                    
                elif value['__Kind'] == 'RecordType':
                    # record data type
                    valueContent = value['__Content']
                    propType = valueContent # initialized to deal with the empty content case
                    for subkey in valueContent.keys():
                        if '__Optional' in valueContent[subkey].keys():
                            if valueContent[subkey]['__Optional']['__Kind'] == 'RecordType':
                                # property is optional (to make sure only properties (and not nodes/edges can be marked as optional)
                                valueContent[subkey]['__Optional']['__Content']['meta_mandatory']= {'__Kind':False}
                    propType = find_data_type(value) # recursive call
                    
                elif value['__Kind'] =='union':
                    propList = []
                    for elem in value['__Content']:
                        propList.append(str(find_data_type(elem)))
                    
                    propType = " + "
                    propType = propType.join(propList)
                
                else:
                    # basic data type
                    propType = value['__Kind']
                
                if not mandatory:
                    # property is optional
                    if type(propType) == str:
                        record[key] = propType + " ?"
                    elif type(propType) == list:
                        record[key] = [str(propType[0]) + " ?"]
                    else:
                        # record
                        record[key] = propType
                            
                else:
                    # property is mandatory
                    record[key]=propType
                    
            propType = record # output
        
        elif propKind =='union':
            propList = []
            for elem in prop['__Content']:
                propList.append(str(find_data_type(elem)))
            
            propType = " + "
            propType = propType.join(propList)
        
        else:
            # basic data type (i.e., string or number or boolean or null)
            propType = propKind
            
    return propType


def parse_mapreduce_schema(MRfilename, allLabels=True):
    """ Parses the output schema of the MapReduce algorithm [1]_.
        All node types must be labeled.
    
    Parameters
    ----------
    MRfilename : str
        Name of the MapReduce output file.
        
    Returns
    -------
    schema : dict
        Contains the PG schema inferred with the MapReduce algorithm
        Its format is: {'Nodes':{'label':{properties},...},
                            'Edges':{'label':{properties},...}}
        
    References
    ---------
    .. [1] Baazizi, Mohamed-Amine & Colazzo, Dario & Ghelli, Giorgio & Sartiani, Carlo. (2019).
        "Parametric schema inference for massive JSON datasets". 
        The VLDB Journal. 28. 10.1007/s00778-018-0532-7. 
    
    """
    ## load the output file of the MapReduce algorithm 
    fileMRschema = open(MRfilename,'r')
    MRschema = json.loads(fileMRschema.read()) 
    
	## parse MRschema            
    schema = find_data_type(MRschema) # JSON record of the schema (without hierarchies)
        
    return schema

def merge_nodes_edges(schemaNodes, schemaEdges):
    """ Merges nodes and edges dicts to form the schema dict."""
    schema = {}
    schema['Nodes'] = schemaNodes
    schema['Edges'] = schemaEdges
    return schema
    
def parse_mapreduce_unlabeled(MRfilename):
    """Parses the output unlabeled node list of the MapReduce algorithm [1]_.
    
    Parameters
    ----------
    MRfilename : str
        Name of the MapReduce output file.
        
    Returns
    -------
    unlabNodes : Python list
        List of the unlabeled node types inferred with the MapReduce algorithm.
    
    References
    ---------
    .. [1] Baazizi, Mohamed-Amine & Colazzo, Dario & Ghelli, Giorgio & Sartiani, Carlo. (2019).
        "Parametric schema inference for massive JSON datasets". 
        The VLDB Journal. 28. 10.1007/s00778-018-0532-7. 
    
    """
    ## load the output file of the MapReduce algorithm 
    fileMRoutput = open(MRfilename,'r')
    MRoutput = json.loads(fileMRoutput.read()) 
    
    unlabNodes = [] # list of the unlabeled node types
    for elem in MRoutput['__Content']:
        unlabNodes.append(find_data_type(elem))
        
    return unlabNodes


def merge_schema_infos(schema, nodesNoProp, edgesCard):
    """ Procedure to merge the schema with the nodes and edges with no properties 
        and the information about edge cardinalities and optionality.
        
    Parameters
    ----------
    schema : dict
        Contains a PG schema to be completed
        Its format is: {'Nodes':{'label':{properties},...},
                            'Edges':{'label':{properties},...}}
        
    nodesNoProp : dict
        Contains the nodes with no properties
        Its format is : {'label':{properties},...}
            
    edgesCard : dict
        Contains all the edges of the complete PG schema
        Its format is: {'label':{cardinalty and optionality infos},...}
            
    """
    ## add the node types with no properties
    schema['Nodes'].update(nodesNoProp)

    ## merge cardinality info
    for key in schema['Edges'].keys():
        edgesCard[key].update(schema['Edges'][key])
    schema['Edges'] = edgesCard
    
    
def merge_unlabeled_nodes(schema, unlabNodes):
    """ Procedure to merge the schema with the unlabeled nodes.
        Node labels are converted as properties of data type 'Void'.
        The value associated with the 'Nodes' key in the schema is converted 
        into a list of dictionnaries (encoding the node types).
    
    Parameters
    ----------
    schema : dict
        Contains a PG schema to be completed
        Its format is: {'Nodes':{'label':{properties},...},
                            'Edges':{'label':{properties},...}}    
    unlabNodes : Python list
        List of the unlabeled node types inferred with the MapReduce algorithm.
    
    """
    i = 0 # counter to define node ids
    
    ## encode labels as properties of data type 'Void'
    labNodes = [] # list of all node types (both labeled and unlabeled)
    for elem in list(schema['Nodes'].items()):
        for label in elem[0].split(":"):
            # deal with multiple labels
            elem[1][label]="Void"
        elem[1]['meta_id'] = i
        i += 1
        labNodes.append(elem[1])
        
    for elem in unlabNodes:
        elem['meta_id'] = i
        i += 1
            
    schema['Nodes'] = labNodes + unlabNodes
            
