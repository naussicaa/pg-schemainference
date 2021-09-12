""" Script used to make the sampling of data """

##### Imports
import random
from termcolor import colored

def sampling(amount_dict,list_of_distinct_nodes, training_percentage):
	""" Separates the data in three sets

	Parameters
	----------
	amount_dict : Python dict
        A dictionary with node strings as keys and the number of occurrences of the node as a value
        Its format is : {'Label1 Label2 Label3 prop1 prop2 prop3 ...': int, ...}
	list_of_distinct_nodes : Python list
        A list of node strings
        Its format is : ['Label1 Label2 prop1', 'Label1 Label3 prop2', 'prop4 prop5', ...]
	training_percentage : Int
		An integer to represent the percentage of data used for the training set
		It should be 80, 70 or 50

	Returns
	-------
	amount_dict : Python dict (training set)
        A dictionary with node strings as keys and the number of occurrences of the node as a value
        Its format is : {'Label1 Label2 Label3 prop1 prop2 prop3 ...': int, ...}
	list_of_distinct_nodes : Python list (training set)
        A list of node strings
        Its format is : ['Label1 Label2 prop1', 'Label1 Label3 prop2', 'prop4 prop5', ...]
	validate : Python list (validation set)
        A list of node strings
        Its format is : ['Label1 Label2 prop1', 'Label1 Label3 prop2', 'prop4 prop5', ...]
	test : Python list (test set)
        A list of node strings
        Its format is : ['Label1 Label2 prop1', 'Label1 Label3 prop2', 'prop4 prop5', ...]

	"""

	# get the number of occurrences of each node in a variable
	data = []
	for node in list_of_distinct_nodes:
	    amount = amount_dict[node]
	    for i in range(amount):
	            data.append(node)

	data = random.sample(data,len(data))

	if training_percentage == 80:
		train = data[:int(len(data)*0.8)]
		validate = data[int(len(data)*0.8):int(len(data)*0.9)]
		test = data[int(len(data)*0.9):]
	elif training_percentage == 70:
		train = data[:int(len(data)*0.7)]
		validate = data[int(len(data)*0.7):int(len(data)*0.85)]
		test = data[int(len(data)*0.85):]
	elif training_percentage == 50:
		train = data[:int(len(data)*0.5)]
		validate = data[int(len(data)*0.5):int(len(data)*0.75)]
		test = data[int(len(data)*0.75):]
	else:
		print(colored("Unvalid training percentage, should be 80, 70 or 50.", "red"))

	# training set with unique nodes
	list_of_distinct_nodes = list(set(train))

	# number of occurrences of the nodes in the training set
	for node in list_of_distinct_nodes:
	    amount_dict[node] = train.count(node)

	return amount_dict,list_of_distinct_nodes,validate,test