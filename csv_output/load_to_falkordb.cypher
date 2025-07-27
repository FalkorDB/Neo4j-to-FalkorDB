// FalkorDB Load Script - Generated from Neo4j Export
// Run this script in FalkorDB to import the data

// Load Nodes

LOAD CSV WITH HEADERS FROM 'file:///nodes_movie.csv' AS row
MERGE (n:Movie {id: toInteger(row.id)})
SET n += row
REMOVE n.id, n.labels;


LOAD CSV WITH HEADERS FROM 'file:///nodes_person.csv' AS row
MERGE (n:Person {id: toInteger(row.id)})
SET n += row
REMOVE n.id, n.labels;

// Load Relationships

LOAD CSV WITH HEADERS FROM 'file:///edges_acted_in.csv' AS row
MATCH (a {id: toInteger(row.source)})
MATCH (b {id: toInteger(row.target)})
MERGE (a)-[r:ACTED_IN]->(b)
SET r += row
REMOVE r.source, r.target, r.type;


LOAD CSV WITH HEADERS FROM 'file:///edges_directed.csv' AS row
MATCH (a {id: toInteger(row.source)})
MATCH (b {id: toInteger(row.target)})
MERGE (a)-[r:DIRECTED]->(b)
SET r += row
REMOVE r.source, r.target, r.type;


LOAD CSV WITH HEADERS FROM 'file:///edges_produced.csv' AS row
MATCH (a {id: toInteger(row.source)})
MATCH (b {id: toInteger(row.target)})
MERGE (a)-[r:PRODUCED]->(b)
SET r += row
REMOVE r.source, r.target, r.type;


LOAD CSV WITH HEADERS FROM 'file:///edges_wrote.csv' AS row
MATCH (a {id: toInteger(row.source)})
MATCH (b {id: toInteger(row.target)})
MERGE (a)-[r:WROTE]->(b)
SET r += row
REMOVE r.source, r.target, r.type;


LOAD CSV WITH HEADERS FROM 'file:///edges_follows.csv' AS row
MATCH (a {id: toInteger(row.source)})
MATCH (b {id: toInteger(row.target)})
MERGE (a)-[r:FOLLOWS]->(b)
SET r += row
REMOVE r.source, r.target, r.type;


LOAD CSV WITH HEADERS FROM 'file:///edges_reviewed.csv' AS row
MATCH (a {id: toInteger(row.source)})
MATCH (b {id: toInteger(row.target)})
MERGE (a)-[r:REVIEWED]->(b)
SET r += row
REMOVE r.source, r.target, r.type;

