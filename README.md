# Property graph inference (PG)

## About the project
This python project infers a schema from a PG stored in Neo4j. This method uses a clustering method, a Bayesian Gaussian Mixture Model, to gather similar nodes together. It is meant to deal with both unlabeled and labeled nodes, properties, multi-labeled nodes, overlapping types.

## Dependencies
Use pip to install these 3 python modules : 
- termcolor
- hdbscan==0.8.27
- neo4j==4.3.4

Neo4j :
- Neo4j Desktop 1.4.8
- To import ldbc, mb6 or fib25 with the method below you need your DBMS to be at least Neo4j 3.5.3

To import LDBC, mb6 or fib25, you need to generate the data and then use this method as a reference : https://github.com/connectome-neuprint/neuPrint/blob/master/neo4j_desktop_load.md

At step 10 of the previous page for LDBC and fib25 you will need a command to load the data.
I suggest you to put all your csv files in Settings => import and use:

For LDBC :
```
./bin/neo4j-admin import --database=ldbc.db --delimiter='|' --nodes=Comment=import/comment_0_0.csv --nodes=Forum=import/forum_0_0.csv --nodes=Person=import/person_0_0.csv --nodes=Post=import/post_0_0.csv --nodes=Place=import/place_0_0.csv --nodes=Organisation=import/organisation_0_0.csv --nodes=TagClass=import/tagclass_0_0.csv --nodes=Tag=import/tag_0_0.csv --relationships=HAS_CREATOR=import/comment_hasCreator_person_0_0.csv --relationships=HAS_TAG=import/comment_hasTag_tag_0_0.csv --relationships=IS_LOCATED_IN=import/comment_isLocatedIn_place_0_0.csv --relationships=REPLY_OF=import/comment_replyOf_comment_0_0.csv --relationships=REPLY_OF=import/comment_replyOf_post_0_0.csv --relationships=CONTAINER_OF=import/forum_containerOf_post_0_0.csv --relationships=HAS_MEMBER=import/forum_hasMember_person_0_0.csv --relationships=HAS_MODERATOR=import/forum_hasModerator_person_0_0.csv --relationships=HAS_TAG=import/forum_hasTag_tag_0_0.csv --relationships=HAS_INTEREST=import/person_hasInterest_tag_0_0.csv --relationships=IS_LOCATED_IN=import/person_isLocatedIn_place_0_0.csv --relationships=KNOWS=import/person_knows_person_0_0.csv --relationships=LIKES=import/person_likes_comment_0_0.csv --relationships=LIKES=import/person_likes_post_0_0.csv --relationships=STUDIES_AT=import/person_studyAt_organisation_0_0.csv --relationships=WORKS_AT=import/person_workAt_organisation_0_0.csv --relationships=HAS_CREATOR=import/post_hasCreator_person_0_0.csv --relationships=HAS_TAG=import/post_hasTag_tag_0_0.csv --relationships=IS_LOCATED_IN=import/post_isLocatedIn_place_0_0.csv --relationships=IS_LOCATED_IN=import/organisation_isLocatedIn_place_0_0.csv --relationships=IS_PART_OF=import/place_isPartOf_place_0_0.csv --relationships=HAS_TYPE=import/tag_hasType_tagclass_0_0.csv --relationships=IS_SUBCLASS_OF=import/tagclass_isSubclassOf_tagclass_0_0.csv
```

For fib25 : 
```
/bin/neo4j-admin import --database=fib25.db --nodes=import/Neuprint_Meta_fib25.csv --nodes=import/Neuprint_Neurons_fib25.csv --relationships=ConnectsTo=import/Neuprint_Neuron_Connections_fib25.csv --nodes=import/Neuprint_SynapseSet_fib25.csv --relationships=ConnectsTo=import/Neuprint_SynapseSet_to_SynapseSet_fib25.csv --relationships=Contains=import/Neuprint_Neuron_to_SynapseSet_fib25.csv --nodes=import/Neuprint_Synapses_fib25.csv --relationships=SynapsesTo=import/Neuprint_Synapse_Connections_fib25.csv --relationships=Contains=import/Neuprint_SynapseSet_to_Synapses_fib25.csv
```

## Running the project
1. python3 cluster_script.py
2. Enter Neo4j database info
