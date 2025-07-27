# Neo4j to FalkorDB migration (without APOC)

Migrating graph database contents from one property graph solution to another is a common task.\
In this example, we use stardard cypher queries to extract data from a sample graph example availble in neo4j namely "Movies",\
allow transformation of labels and properties, and as a next step load the csv files one by one into falkor db.

## Step 1: Setting up neo4j 
Follow neo4j documentation to setup a locally run neo4j database: 
https://neo4j.com/docs/operations-manual/current/installation/

## Step 2: Loading the movies sample graph data to neo4j
You can load the movies dataset by following the ":guide movies" command in the neo4j browser.
More details are available here: https://neo4j.com/docs/getting-started/appendix/example-data/

## Step 3: Reviewing and updating mapping configuration file
The configuration files migrate_config.json allows you to modify labels and properties representation in FalkorDB.
In order to extract the ontology (for a dataset which is not the movies sample dataset),\
you may generate a template config file using this command:
```bash
python3 neo4j_to_csv_extractor.py --password <your-password> --generate-template <your-template>.json --analyze-only
```

## Step 4: Extracting data from neo4j and generating csv files
To extract data from neo4j you can activate the relevant python script:
```bash
python3 neo4j_to_csv_extractor.py --password <your-password> --config migrate_config.json
```
The script would read data from neo4j and create in csv_output subfolder all nodes and edges csv files\ 
with headers and content adapted based on guidelines in the migrate_config.json file

--------
Execution output example
--------
Connecting to Neo4j at bolt://localhost:7687 with username 'neo4j'...\
✅ Loaded configuration from migrate_config.json\
✅ Successfully connected to Neo4j!\
Starting Neo4j to CSV extraction...\
Found 2 node labels: ['Movie', 'Person']\
Extracting nodes with label: Movie\
  🔄 Transformed Movie.released: '1999' -> '1999'\
  🔄 Transformed Movie.released: '2003' -> '2003'\
  🔄 Transformed Movie.released: '2003' -> '2003'\
  🔄 Transformed Movie.released: '1997' -> '1997'\
  🔄 Transformed Movie.released: '1992' -> '1992'\
  🔄 Transformed Movie.released: '1986' -> '1986'\
  🔄 Transformed Movie.released: '2000' -> '2000'\
  🔄 Transformed Movie.released: '1986' -> '1986'\
  🔄 Transformed Movie.released: '1997' -> '1997'\
  🔄 Transformed Movie.released: '1998' -> '1998'\
  🔄 Transformed Movie.released: '1999' -> '1999'\
  🔄 Transformed Movie.released: '1998' -> '1998'\
  🔄 Transformed Movie.released: '1993' -> '1993'\
  🔄 Transformed Movie.released: '1990' -> '1990'\
  🔄 Transformed Movie.released: '1998' -> '1998'\
  🔄 Transformed Movie.released: '1996' -> '1996'\
  🔄 Transformed Movie.released: '2000' -> '2000'\
  🔄 Transformed Movie.released: '2006' -> '2006'\
  🔄 Transformed Movie.released: '1996' -> '1996'\
  🔄 Transformed Movie.released: '1992' -> '1992'\
  🔄 Transformed Movie.released: '1995' -> '1995'\
  🔄 Transformed Movie.released: '2012' -> '2012'\
  🔄 Transformed Movie.released: '2006' -> '2006'\
  🔄 Transformed Movie.released: '2006' -> '2006'\
  🔄 Transformed Movie.released: '2008' -> '2008'\
  🔄 Transformed Movie.released: '2009' -> '2009'\
  🔄 Transformed Movie.released: '1999' -> '1999'\
  🔄 Transformed Movie.released: '2008' -> '2008'\
  🔄 Transformed Movie.released: '1992' -> '1992'\
  🔄 Transformed Movie.released: '1995' -> '1995'\
  🔄 Transformed Movie.released: '1996' -> '1996'\
  🔄 Transformed Movie.released: '2000' -> '2000'\
  🔄 Transformed Movie.released: '1975' -> '1975'\
  🔄 Transformed Movie.released: '2003' -> '2003'\
  🔄 Transformed Movie.released: '1999' -> '1999'\
  🔄 Transformed Movie.released: '2007' -> '2007'\
  🔄 Transformed Movie.released: '2004' -> '2004'\
  🔄 Transformed Movie.released: '1992' -> '1992'\
  Processed 38 nodes...\
  Exported 38 nodes to csv_output/nodes_movie.csv\
Extracting nodes with label: Person\
  🔄 Transformed Person.born: '1964' -> '1964'\
  🔄 Transformed Person.born: '1967' -> '1967'\
  🔄 Transformed Person.born: '1961' -> '1961'\
  🔄 Transformed Person.born: '1960' -> '1960'\
  🔄 Transformed Person.born: '1967' -> '1967'\
  🔄 Transformed Person.born: '1965' -> '1965'\
  🔄 Transformed Person.born: '1952' -> '1952'\
  🔄 Transformed Person.born: '1978' -> '1978'\
  🔄 Transformed Person.born: '1975' -> '1975'\
  🔄 Transformed Person.born: '1940' -> '1940'\
  🔄 Transformed Person.born: '1944' -> '1944'\
  🔄 Transformed Person.born: '1962' -> '1962'\
  🔄 Transformed Person.born: '1937' -> '1937'\
  🔄 Transformed Person.born: '1962' -> '1962'\
  🔄 Transformed Person.born: '1958' -> '1958'\
  🔄 Transformed Person.born: '1966' -> '1966'\
  🔄 Transformed Person.born: '1971' -> '1971'\
  🔄 Transformed Person.born: '1968' -> '1968'\
  🔄 Transformed Person.born: '1957' -> '1957'\
  🔄 Transformed Person.born: '1943' -> '1943'\
  🔄 Transformed Person.born: '1967' -> '1967'\
  🔄 Transformed Person.born: '1948' -> '1948'\
  🔄 Transformed Person.born: '1947' -> '1947'\
  🔄 Transformed Person.born: '1961' -> '1961'\
  🔄 Transformed Person.born: '1957' -> '1957'\
  🔄 Transformed Person.born: '1959' -> '1959'\
  🔄 Transformed Person.born: '1962' -> '1962'\
  🔄 Transformed Person.born: '1933' -> '1933'\
  🔄 Transformed Person.born: '1961' -> '1961'\
  🔄 Transformed Person.born: '1944' -> '1944'\
  🔄 Transformed Person.born: '1941' -> '1941'\
  🔄 Transformed Person.born: '1969' -> '1969'\
  🔄 Transformed Person.born: '1962' -> '1962'\
  🔄 Transformed Person.born: '1974' -> '1974'\
  🔄 Transformed Person.born: '1970' -> '1970'\
  🔄 Transformed Person.born: '1961' -> '1961'\
  🔄 Transformed Person.born: '1971' -> '1971'\
  🔄 Transformed Person.born: '1996' -> '1996'\
  🔄 Transformed Person.born: '1957' -> '1957'\
  🔄 Transformed Person.born: '1970' -> '1970'\
  🔄 Transformed Person.born: '1971' -> '1971'\
  🔄 Transformed Person.born: '1972' -> '1972'\
  🔄 Transformed Person.born: '1966' -> '1966'\
  🔄 Transformed Person.born: '1942' -> '1942'\
  🔄 Transformed Person.born: '1963' -> '1963'\
  🔄 Transformed Person.born: '1963' -> '1963'\
  🔄 Transformed Person.born: '1940' -> '1940'\
  🔄 Transformed Person.born: '1960' -> '1960'\
  🔄 Transformed Person.born: '1929' -> '1929'\
  🔄 Transformed Person.born: '1942' -> '1942'\
  🔄 Transformed Person.born: '1951' -> '1951'\
  🔄 Transformed Person.born: '1956' -> '1956'\
  🔄 Transformed Person.born: '1970' -> '1970'\
  🔄 Transformed Person.born: '1971' -> '1971'\
  🔄 Transformed Person.born: '1940' -> '1940'\
  🔄 Transformed Person.born: '1953' -> '1953'\
  🔄 Transformed Person.born: '1956' -> '1956'\
  🔄 Transformed Person.born: '1968' -> '1968'\
  🔄 Transformed Person.born: '1973' -> '1973'\
  🔄 Transformed Person.born: '1967' -> '1967'\
  🔄 Transformed Person.born: '1941' -> '1941'\
  🔄 Transformed Person.born: '1956' -> '1956'\
  🔄 Transformed Person.born: '1953' -> '1953'\
  🔄 Transformed Person.born: '1949' -> '1949'\
  🔄 Transformed Person.born: '1962' -> '1962'\
  🔄 Transformed Person.born: '1950' -> '1950'\
  🔄 Transformed Person.born: '1956' -> '1956'\
  🔄 Transformed Person.born: '1948' -> '1948'\
  🔄 Transformed Person.born: '1956' -> '1956'\
  🔄 Transformed Person.born: '1949' -> '1949'\
  🔄 Transformed Person.born: '1977' -> '1977'\
  🔄 Transformed Person.born: '1970' -> '1970'\
  🔄 Transformed Person.born: '1930' -> '1930'\
  🔄 Transformed Person.born: '1968' -> '1968'\
  🔄 Transformed Person.born: '1950' -> '1950'\
  🔄 Transformed Person.born: '1974' -> '1974'\
  🔄 Transformed Person.born: '1954' -> '1954'\
  🔄 Transformed Person.born: '1931' -> '1931'\
  🔄 Transformed Person.born: '1930' -> '1930'\
  🔄 Transformed Person.born: '1930' -> '1930'\
  🔄 Transformed Person.born: '1947' -> '1947'\
  🔄 Transformed Person.born: '1968' -> '1968'\
  🔄 Transformed Person.born: '1958' -> '1958'\
  🔄 Transformed Person.born: '1953' -> '1953'\
  🔄 Transformed Person.born: '1966' -> '1966'\
  🔄 Transformed Person.born: '1949' -> '1949'\
  🔄 Transformed Person.born: '1965' -> '1965'\
  🔄 Transformed Person.born: '1969' -> '1969'\
  🔄 Transformed Person.born: '1961' -> '1961'\
  🔄 Transformed Person.born: '1939' -> '1939'\
  🔄 Transformed Person.born: '1976' -> '1976'\
  🔄 Transformed Person.born: '1971' -> '1971'\
  🔄 Transformed Person.born: '1954' -> '1954'\
  🔄 Transformed Person.born: '1981' -> '1981'\
  🔄 Transformed Person.born: '1946' -> '1946'\
  🔄 Transformed Person.born: '1940' -> '1940'\
  🔄 Transformed Person.born: '1967' -> '1967'\
  🔄 Transformed Person.born: '1985' -> '1985'\
  🔄 Transformed Person.born: '1960' -> '1960'\
  🔄 Transformed Person.born: '1946' -> '1946'\
  🔄 Transformed Person.born: '1966' -> '1966'\
  🔄 Transformed Person.born: '1980' -> '1980'\
  🔄 Transformed Person.born: '1982' -> '1982'\
  🔄 Transformed Person.born: '1957' -> '1957'\
  🔄 Transformed Person.born: '1953' -> '1953'\
  🔄 Transformed Person.born: '1968' -> '1968'\
  🔄 Transformed Person.born: '1955' -> '1955'\
  🔄 Transformed Person.born: '1959' -> '1959'\
  🔄 Transformed Person.born: '1959' -> '1959'\
  🔄 Transformed Person.born: '1938' -> '1938'\
  🔄 Transformed Person.born: '1969' -> '1969'\
  🔄 Transformed Person.born: '1960' -> '1960'\
  🔄 Transformed Person.born: '1944' -> '1944'\
  🔄 Transformed Person.born: '1965' -> '1965'\
  🔄 Transformed Person.born: '1950' -> '1950'\
  🔄 Transformed Person.born: '1955' -> '1955'\
  🔄 Transformed Person.born: '1967' -> '1967'\
  🔄 Transformed Person.born: '1943' -> '1943'\
  🔄 Transformed Person.born: '1951' -> '1951'\
  🔄 Transformed Person.born: '1932' -> '1932'\
  🔄 Transformed Person.born: '1946' -> '1946'\
  🔄 Transformed Person.born: '1949' -> '1949'\
  🔄 Transformed Person.born: '1958' -> '1958'\
  🔄 Transformed Person.born: '1967' -> '1967'\
  🔄 Transformed Person.born: '1954' -> '1954'\
  🔄 Transformed Person.born: '1956' -> '1956'\
  🔄 Transformed Person.born: '1963' -> '1963'\
  🔄 Transformed Person.born: '1943' -> '1943'\
  Processed 133 nodes...\
  Exported 133 nodes to csv_output/nodes_person.csv\
Found 6 relationship types: ['ACTED_IN', 'DIRECTED', 'PRODUCED', 'WROTE', 'FOLLOWS', 'REVIEWED']\
Extracting relationships of type: ACTED_IN\
  Processed 172 relationships...\
  Exported 172 relationships to csv_output/edges_acted_in.csv\
Extracting relationships of type: DIRECTED\
  Processed 44 relationships...\
  Exported 44 relationships to csv_output/edges_directed.csv\
Extracting relationships of type: PRODUCED\
  Processed 15 relationships...\
  Exported 15 relationships to csv_output/edges_produced.csv\
Extracting relationships of type: WROTE\
  Processed 10 relationships...\
  Exported 10 relationships to csv_output/edges_wrote.csv\
Extracting relationships of type: FOLLOWS\
  Processed 3 relationships...\
  Exported 3 relationships to csv_output/edges_follows.csv\
Extracting relationships of type: REVIEWED\
  Processed 9 relationships...\
  Exported 9 relationships to csv_output/edges_reviewed.csv\
Generated FalkorDB load script: csv_output/load_to_falkordb.cypher\

Extraction complete!\
Node files: 2\
Edge files: 6\
Load script: csv_output/load_to_falkordb.cypher\
--------


## Step 5: Loading csv data into falkordb
Setup falkordb on your local machine (you can either select the "server" option only, \
or use standard deployment that includes the browser component:
https://docs.falkordb.com/getting_started.html
After setup you will have falkordb data conntection at port 6379 and web browser at port 3000
```bash
python3 falkordb_csv_loader_fixed.py MOVIES --port 6379 --stats
```
---------
Execution output example
---------
Connecting to FalkorDB at localhost:6379...\
✅ Connected to FalkorDB graph 'MOVIES'\
Found 2 node files and 6 edge files\

📥 Loading nodes...\
Loading nodes from csv_output/nodes_movie.csv...\
  Read 38 rows from csv_output/nodes_movie.csv\
  CSV headers: ['id', 'labels', 'release_year', 'tagline', 'name']\
    Record 1: properties = {'release_year': 1999, 'tagline': 'Welcome to the Real World', 'name': 'The Matrix'}\
    Generated query: CREATE (:Movie {id: 0, release_year: 1999, tagline: 'Welcome to the Real World', name: 'The Matrix'})\
    Record 2: properties = {'release_year': 2003, 'tagline': 'Free your mind', 'name': 'The Matrix Reloaded'}\
    Generated query: CREATE (:Movie {id: 9, release_year: 2003, tagline: 'Free your mind', name: 'The Matrix Reloaded'})\
    Record 3: properties = {'release_year': 2003, 'tagline': 'Everything that has a beginning has an end', 'name': 'The Matrix Revolutions'}\
    Generated query: CREATE (:Movie {id: 10, release_year: 2003, tagline: 'Everything that has a beginning has an end', name: 'The Matrix Revolutions'})\
  Loaded 38/38 nodes...\
✅ Loaded 38 Movie nodes\
Loading nodes from csv_output/nodes_person.csv...\
  Read 133 rows from csv_output/nodes_person.csv\
  CSV headers: ['id', 'labels', 'birth_year', 'name']\
    Record 1: properties = {'birth_year': 1964, 'name': 'Keanu Reeves'}\
    Generated query: CREATE (:Person {id: 1, birth_year: 1964, name: 'Keanu Reeves'})\
    Record 2: properties = {'birth_year': 1967, 'name': 'Carrie-Anne Moss'}\
    Generated query: CREATE (:Person {id: 2, birth_year: 1967, name: 'Carrie-Anne Moss'})\
    Record 3: properties = {'birth_year': 1961, 'name': 'Laurence Fishburne'}\
    Generated query: CREATE (:Person {id: 3, birth_year: 1961, name: 'Laurence Fishburne'})\
  Loaded 100/133 nodes...\
  Loaded 133/133 nodes...\
✅ Loaded 133 Person nodes\

🔗 Loading edges...\
Loading edges from csv_output/edges_produced.csv...\
  Read 15 rows from csv_output/edges_produced.csv\
  Loaded 15/15 edges...\
✅ Loaded 15 PRODUCED relationships\
Loading edges from csv_output/edges_follows.csv...\
  Read 3 rows from csv_output/edges_follows.csv\
  Loaded 3/3 edges...\
✅ Loaded 3 FOLLOWS relationships\
Loading edges from csv_output/edges_acted_in.csv...\
  Read 172 rows from csv_output/edges_acted_in.csv\
  Loaded 100/172 edges...\
  Loaded 172/172 edges...\
✅ Loaded 172 ACTED_IN relationships\
Loading edges from csv_output/edges_reviewed.csv...\
  Read 9 rows from csv_output/edges_reviewed.csv\
  Loaded 9/9 edges...\
✅ Loaded 9 REVIEWED relationships\
Loading edges from csv_output/edges_wrote.csv...\
  Read 10 rows from csv_output/edges_wrote.csv\
  Loaded 10/10 edges...\
✅ Loaded 10 WROTE relationships\
Loading edges from csv_output/edges_directed.csv...\
  Read 44 rows from csv_output/edges_directed.csv\
  Loaded 44/44 edges...\
✅ Loaded 44 DIRECTED relationships\

✅ Successfully loaded data into graph 'MOVIES'\

📊 Graph Statistics:\
Nodes:\
  ['Movie']: 38\
  ['Person']: 133\
Relationships:\
  PRODUCED: 15\
  REVIEWED: 9\
  DIRECTED: 44\
  WROTE: 10\
  ACTED_IN: 172\
  FOLLOWS: 3\

---------

## Step 6: Validating content of data in FalkorDB

Here is how the same cypher query is visualized in both Neo4j and FalkorDB broswers:
```cypher
MATCH p=()-[:REVIEWED]->() RETURN p LIMIT 25;
```

Neo4j view
<img width="742" height="695" alt="neo4j-movies-reviews" src="https://github.com/user-attachments/assets/c8e8b024-a8c8-4f7e-a995-82175d434846" />

FalkorDB view
<img width="1203" height="733" alt="falkordb-movies-reviewed" src="https://github.com/user-attachments/assets/8ba6c36a-d233-4336-81bf-de74d219c947" />

Good Luck!
