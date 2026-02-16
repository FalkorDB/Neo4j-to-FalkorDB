# Neo4j to FalkorDB migration (without APOC)

Migrating graph database contents from one property graph solution to another is a common task.\
In this example, we use stardard cypher queries to extract data from a sample graph example availble in neo4j namely "Movies",\
allow transformation of labels and properties, and as a next step load the csv files one by one into falkor db.\
Please note that you can use any source neo4j dataset, and the process migrates nodes, edges, constraints and indexes.

NOTE: there are 2 options to load exported dataset to FalkorDB. The python script in this repo and a Rust based implementation\
that can be found in here https://github.com/FalkorDB/FalkorDB-Loader-RS

## Step 1: Setting up neo4j 
Follow neo4j documentation to setup a locally run neo4j database: 
https://neo4j.com/docs/operations-manual/current/installation/

## Step 2: Loading the movies sample graph data to neo4j
You can load the movies dataset by following the ":guide movies" command in the neo4j browser.
More details are available here: https://neo4j.com/docs/getting-started/appendix/example-data/

## Step 3: Reviewing and updating mapping configuration file
The configuration file migrate_config.json allows you to modify how the labels and properties are represented in FalkorDB.
In order to extract the ontology (for a dataset which is not the movies sample dataset),\
you may generate a template config file using this command:
```bash
python3 neo4j_to_csv_extractor.py --password <your-neo4j-password> --generate-template <your-template>.json --analyze-only
```
Usage of neo4j_to_csv_extrator.py is shown below:
```bash
usage: neo4j_to_csv_extractor.py [-h] [--uri URI] [--username USERNAME] --password PASSWORD [--database DATABASE] [--batch-size BATCH_SIZE]
                                 [--nodes-only] [--edges-only] [--indexes-only] [--config CONFIG] [--generate-template GENERATE_TEMPLATE]
                                 [--analyze-only] [--tenant-mode {label,property}] [--tenant-filter TENANT_FILTER]

Extract Neo4j data to CSV for FalkorDB migration

options:
  -h, --help            show this help message and exit
  --uri URI             Neo4j URI
  --username USERNAME   Neo4j username
  --password PASSWORD   Neo4j password
  --database DATABASE   Neo4j database name
  --batch-size BATCH_SIZE
                        Batch size for extraction
  --nodes-only          Extract only nodes
  --edges-only          Extract only relationships
  --indexes-only        Extract only indexes and constraints
  --config CONFIG       Path to migration configuration JSON file
  --generate-template GENERATE_TEMPLATE
                        Generate template migration config file from Neo4j topology (specify output filename)
  --analyze-only        Only analyze topology and generate template config, do not extract data
  --tenant-mode {label,property}
                        Enable multi-tenant mode: "label" (filter by node label) or "property" (filter by property value)
  --tenant-filter TENANT_FILTER
                        The label name or property name to use for tenant segregation (required with --tenant-mode)
```

Sample output of the ontology extraction phase is shown below as executed against the movies dataset
--------
```bash
Connecting to Neo4j at bolt://localhost:7687 with username 'neo4j'...
✅ Loaded configuration from migrate_config.json
✅ Successfully connected to Neo4j!
🔍 Analyzing Neo4j database topology...
  Found 2 node labels: ['Movie', 'Person']
  Found 6 relationship types: ['ACTED_IN', 'DIRECTED', 'PRODUCED', 'WROTE', 'FOLLOWS', 'REVIEWED']

📊 Analyzing node properties...
  Analyzing Movie properties...
    Found 3 properties: ['released', 'tagline', 'title']
  Analyzing Person properties...
    Found 2 properties: ['born', 'name']

🔗 Analyzing relationship properties...
  Analyzing ACTED_IN properties...
    Found 1 properties: ['roles']
  Analyzing DIRECTED properties...
    No properties found for DIRECTED
  Analyzing FOLLOWS properties...
    No properties found for FOLLOWS
  Analyzing PRODUCED properties...
    No properties found for PRODUCED
  Analyzing REVIEWED properties...
    Found 2 properties: ['rating', 'summary']
  Analyzing WROTE properties...
    No properties found for WROTE

📈 Gathering database statistics...
  Movie: 38 nodes
  Person: 133 nodes
  ACTED_IN: 172 relationships
  DIRECTED: 44 relationships
  PRODUCED: 15 relationships
  WROTE: 10 relationships
  FOLLOWS: 3 relationships
  REVIEWED: 9 relationships

✅ Generated template migration config: shahar_template.json
📝 Total elements analyzed:
   - 2 node labels
   - 6 relationship types
   - 5 node properties
   - 3 relationship properties

📋 Analysis complete! Use the generated template to customize your migration.
```
--------
## Step 4: Extracting data from neo4j and generating csv files
To extract data from neo4j you can activate the relevant python script:
```bash
python3 neo4j_to_csv_extractor.py --password <your-neo4j-password> --config migrate_config.json
```
The script would read data from neo4j and create in csv_output subfolder all nodes and edges csv files
with headers and content adapted based on guidelines in the migrate_config.json file

### Multi-Tenant Data Extraction

If your Neo4j database contains multi-tenant data, you can extract each tenant's data into separate subdirectories:

**Using property-based tenant segregation:**
```bash
python3 neo4j_to_csv_extractor.py --password <your-neo4j-password> \
  --tenant-mode property --tenant-filter tenantId
```

This will:
- Discover all distinct values of the `tenantId` property
- Create a subdirectory for each tenant (e.g., `csv_output/tenant_shopfast/`, `csv_output/tenant_cloudserve/`)
- Export each tenant's nodes and relationships to their respective subdirectories
- Automatically omit the `tenantId` property from CSV exports (since it's encoded in the folder name)

**Using label-based tenant segregation:**
```bash
python3 neo4j_to_csv_extractor.py --password <your-neo4j-password> \
  --tenant-mode label --tenant-filter TenantLabel
```

This filters nodes that have the specified label.

--------
Execution output example
--------
```bash

Connecting to Neo4j at bolt://localhost:7687 with username 'neo4j'...
✅ Loaded configuration from migrate_config.json
✅ Successfully connected to Neo4j!
Starting Neo4j to CSV extraction...
Found 2 node labels: ['Movie', 'Person']
Extracting nodes with label: Movie
  🔄 Transformed Movie.released: '1999' -> '1999'
  🔄 Transformed Movie.released: '2003' -> '2003'
  🔄 Transformed Movie.released: '2003' -> '2003'
  🔄 Transformed Movie.released: '1997' -> '1997'
  🔄 Transformed Movie.released: '1992' -> '1992'
  🔄 Transformed Movie.released: '1986' -> '1986'
  🔄 Transformed Movie.released: '2000' -> '2000'
  🔄 Transformed Movie.released: '1986' -> '1986'
  🔄 Transformed Movie.released: '1997' -> '1997'
  🔄 Transformed Movie.released: '1998' -> '1998'
  🔄 Transformed Movie.released: '1999' -> '1999'
  🔄 Transformed Movie.released: '1998' -> '1998'
  🔄 Transformed Movie.released: '1993' -> '1993'
  🔄 Transformed Movie.released: '1990' -> '1990'
  🔄 Transformed Movie.released: '1998' -> '1998'
  🔄 Transformed Movie.released: '1996' -> '1996'
  🔄 Transformed Movie.released: '2000' -> '2000'
  🔄 Transformed Movie.released: '2006' -> '2006'
  🔄 Transformed Movie.released: '1996' -> '1996'
  🔄 Transformed Movie.released: '1992' -> '1992'
  🔄 Transformed Movie.released: '1995' -> '1995'
  🔄 Transformed Movie.released: '2012' -> '2012'
  🔄 Transformed Movie.released: '2006' -> '2006'
  🔄 Transformed Movie.released: '2006' -> '2006'
  🔄 Transformed Movie.released: '2008' -> '2008'
  🔄 Transformed Movie.released: '2009' -> '2009'
  🔄 Transformed Movie.released: '1999' -> '1999'
  🔄 Transformed Movie.released: '2008' -> '2008'
  🔄 Transformed Movie.released: '1992' -> '1992'
  🔄 Transformed Movie.released: '1995' -> '1995'
  🔄 Transformed Movie.released: '1996' -> '1996'
  🔄 Transformed Movie.released: '2000' -> '2000'
  🔄 Transformed Movie.released: '1975' -> '1975'
  🔄 Transformed Movie.released: '2003' -> '2003'
  🔄 Transformed Movie.released: '1999' -> '1999'
  🔄 Transformed Movie.released: '2007' -> '2007'
  🔄 Transformed Movie.released: '2004' -> '2004'
  🔄 Transformed Movie.released: '1992' -> '1992'
  Processed 38 nodes...
  Exported 38 nodes to csv_output/nodes_movie.csv
Extracting nodes with label: Person
  Processed 133 nodes...
  Exported 133 nodes to csv_output/nodes_person.csv
Found 6 relationship types: ['ACTED_IN', 'DIRECTED', 'PRODUCED', 'WROTE', 'FOLLOWS', 'REVIEWED']
Extracting relationships of type: ACTED_IN
  Processed 172 relationships...
  Exported 172 relationships to csv_output/edges_acted_in.csv
Extracting relationships of type: DIRECTED
  Processed 44 relationships...
  Exported 44 relationships to csv_output/edges_directed.csv
Extracting relationships of type: PRODUCED
  Processed 15 relationships...
  Exported 15 relationships to csv_output/edges_produced.csv
Extracting relationships of type: WROTE
  Processed 10 relationships...
  Exported 10 relationships to csv_output/edges_wrote.csv
Extracting relationships of type: FOLLOWS
  Processed 3 relationships...
  Exported 3 relationships to csv_output/edges_follows.csv
Extracting relationships of type: REVIEWED
  Processed 9 relationships...
  Exported 9 relationships to csv_output/edges_reviewed.csv
Extracting database indexes...
  Exported 4 indexes to csv_output/indexes.csv
Extracting database constraints...
  Exported 2 constraints to csv_output/constraints.csv
Generated FalkorDB load script: csv_output/load_to_falkordb.cypher
Generated FalkorDB index creation script: csv_output/create_indexes_falkordb.cypher

Extraction complete!
Node files: 2
Edge files: 6
Index file: csv_output/indexes.csv
Constraint file: csv_output/constraints.csv
Data load script: csv_output/load_to_falkordb.cypher
Index creation script: csv_output/create_indexes_falkordb.cypher

```
--------


## Step 5: Loading csv data into falkordb
Setup falkordb on your local machine (you can either select the "server" option only, \
or use standard deployment that includes the browser component:
https://docs.falkordb.com/getting_started.html
After setup you will have falkordb data conntection at port 6379 and web browser at port 3000
```bash
python3 falkordb_csv_loader.py MOVIES --port 6379 --stats
```

### Loader connection auth (connect-as)
In case your FalkorDB connection is already secured with username and password, you can pass `--username/--password` so the loader can connect.

### Provision auth for the migrated graph(s) (create/update ACL user)
If you want the migrated graph(s) to be accessible via a specific username/password after the migration, you can ask the loader to **create/update** a Redis ACL user:

```bash
python3 falkordb_csv_loader.py MOVIES \
  --provision-username movies_app \
  --provision-password '<password>'
```

By default this provisions permissions in `graph-only` mode and **overwrites** any existing definition of that ACL user (useful when re-running the migration).

Important: creating an ACL user alone does **not** prevent unauthenticated access if Redis `default` user is still enabled without a password.

To require authentication for new connections, you can disable the `default` user after provisioning:

```bash
python3 falkordb_csv_loader.py MOVIES \
  --provision-username movies_app \
  --provision-password '<password>' \
  --lockdown-default-user
```

⚠️ Use `--lockdown-default-user` with care: if you lose the provisioned credentials you can lock yourself out.

Note: In some deployments, running ACL commands requires an admin user. If needed, pass `--admin-username/--admin-password` so provisioning runs with admin privileges.

You may also control the batch size used per loaded CSV file.
```bash
usage: falkordb_csv_loader.py [-h] [--host HOST] [--port PORT] [--username USERNAME] [--password PASSWORD]
                              [--provision-username PROVISION_USERNAME] [--provision-password PROVISION_PASSWORD]
                              [--provision-key-pattern PROVISION_KEY_PATTERN] [--provision-commands {graph-only,all}]
                              [--admin-username ADMIN_USERNAME] [--admin-password ADMIN_PASSWORD] [--lockdown-default-user]
                              [--batch-size BATCH_SIZE] [--stats] [--csv-dir CSV_DIR] [--merge-mode] [--multi-graph]
                              graph_name

Load CSV files into FalkorDB

Run `python3 falkordb_csv_loader.py --help` for the full option list.
```

### Multi-Tenant Data Loading

If you extracted multi-tenant data (with `--tenant-mode` option), you can load each tenant into a separate FalkorDB graph:

```bash
python3 falkordb_csv_loader.py MYAPP --multi-graph --port 6379
```

If you also want to provision a **shared** ACL user that can access all tenant graphs created by this run (default key pattern: `MYAPP_*`):

```bash
python3 falkordb_csv_loader.py MYAPP --multi-graph --port 6379 \
  --provision-username myapp_app \
  --provision-password '<password>'
```

This will:
- Scan for `tenant_*` subdirectories in `csv_output/`
- Create a separate graph for each tenant (e.g., `MYAPP_shopfast`, `MYAPP_cloudserve`, `MYAPP_learnhub`)
- Load each tenant's data into its own isolated graph
- Provide detailed progress reporting per tenant

Example output:
```
🗂️  Found 3 tenant directories: ['tenant_cloudserve', 'tenant_learnhub', 'tenant_shopfast']
   Each will be loaded into a separate graph

================================================================================
📊 Processing tenant: cloudserve
   Target graph: MYAPP_cloudserve
   Source directory: csv_output/tenant_cloudserve
================================================================================
...
✅ Completed loading tenant 'cloudserve' in 0:00:02.345

================================================================================
✅ Multi-graph loading complete!
   Loaded 3 tenants into separate graphs
   Total time: 0:00:07.891
================================================================================
```
Note: In addition to the python loader indicated above, you may use a Rust based data loader, which can improve loading speed.
You can find the loader in FalkorDB/FalkorDB-Loader-RS repo.

---------
Execution output example
---------
```bash
Connecting to FalkorDB at localhost:6379...
✅ Connected to FalkorDB graph 'MOVIES'
Found 2 node files and 6 edge files

🗼️ Setting up database schema...
🔧 Creating ID indexes for all node labels...
  Creating ID index: CREATE INDEX ON :Movie(id)
  Creating ID index: CREATE INDEX ON :Person(id)
✅ Created 2 ID indexes
🔧 Creating indexes from CSV...
  Read 4 rows from csv_output/indexes.csv
  Creating: CREATE INDEX ON :Movie(name)
  Creating: CREATE INDEX ON :Person(name)
✅ Created 2 indexes from CSV, skipped 2
🔧 Creating supporting indexes for constraints...
  Read 2 rows from csv_output/constraints.csv
  Creating supporting index: CREATE INDEX FOR (n:Movie) ON (n.name)
  Creating supporting index: CREATE INDEX FOR (n:Person) ON (n.name)
🔒 Creating constraints...
  Read 2 rows from csv_output/constraints.csv
  ✅ Successfully created UNIQUE constraint on Movie(name), status: PENDING
  ✅ Successfully created UNIQUE constraint on Person(name), status: PENDING
✅ Created 2 constraints

[2025-08-03 14:04:06] 📥 Loading nodes...
[2025-08-03 14:04:06] Loading nodes from csv_output/nodes_movie.csv...
  Read 38 rows from csv_output/nodes_movie.csv
  CSV headers: ['id', 'labels', 'release_year', 'tagline', 'name']
    Record 1: properties = {'release_year': 1999, 'tagline': 'Welcome to the Real World', 'name': 'The Matrix'}
    Generated query: CREATE (:Movie {id: 0, release_year: 1999, tagline: 'Welcome to the Real World', name: 'The Matrix'})
    Record 2: properties = {'release_year': 2003, 'tagline': 'Free your mind', 'name': 'The Matrix Reloaded'}
    Generated query: CREATE (:Movie {id: 9, release_year: 2003, tagline: 'Free your mind', name: 'The Matrix Reloaded'})
    Record 3: properties = {'release_year': 2003, 'tagline': 'Everything that has a beginning has an end', 'name': 'The Matrix Revolutions'}
    Generated query: CREATE (:Movie {id: 10, release_year: 2003, tagline: 'Everything that has a beginning has an end', name: 'The Matrix Revolutions'})
[2025-08-03 14:04:06] Batch complete: Loaded 38 nodes (Duration: 0:00:00.012957)
[2025-08-03 14:04:06] ✅ Loaded 38 Movie nodes (Duration: 0:00:00.013085)
[2025-08-03 14:04:06] Loading nodes from csv_output/nodes_person.csv...
  Read 133 rows from csv_output/nodes_person.csv
  CSV headers: ['id', 'labels', 'birth_year', 'name']
    Record 1: properties = {'birth_year': 1964, 'name': 'Keanu Reeves'}
    Generated query: CREATE (:Person {id: 1, birth_year: 1964, name: 'Keanu Reeves'})
    Record 2: properties = {'birth_year': 1967, 'name': 'Carrie-Anne Moss'}
    Generated query: CREATE (:Person {id: 2, birth_year: 1967, name: 'Carrie-Anne Moss'})
    Record 3: properties = {'birth_year': 1961, 'name': 'Laurence Fishburne'}
    Generated query: CREATE (:Person {id: 3, birth_year: 1961, name: 'Laurence Fishburne'})
[2025-08-03 14:04:06] Batch complete: Loaded 133 nodes (Duration: 0:00:00.035237)
[2025-08-03 14:04:06] ✅ Loaded 133 Person nodes (Duration: 0:00:00.035437)
[2025-08-03 14:04:06] ✅ All nodes loaded (Total duration: 0:00:00.048588)

[2025-08-03 14:04:06] 🔗 Loading edges...
[2025-08-03 14:04:06] Loading edges from csv_output/edges_produced.csv...
  Read 15 rows from csv_output/edges_produced.csv
    Record 1: source_label=, target_label=
    Using CREATE mode for relationships
    Record 2: source_label=, target_label=
    Using CREATE mode for relationships
    Record 3: source_label=, target_label=
    Using CREATE mode for relationships
[2025-08-03 14:04:06] Batch complete: Loaded 15 edges (Duration: 0:00:00.005070)
[2025-08-03 14:04:06] ✅ Loaded 15 PRODUCED relationships (Duration: 0:00:00.005181)
[2025-08-03 14:04:06] Loading edges from csv_output/edges_follows.csv...
  Read 3 rows from csv_output/edges_follows.csv
    Record 1: source_label=, target_label=
    Using CREATE mode for relationships
    Record 2: source_label=, target_label=
    Using CREATE mode for relationships
    Record 3: source_label=, target_label=
    Using CREATE mode for relationships
[2025-08-03 14:04:06] Batch complete: Loaded 3 edges (Duration: 0:00:00.000969)
[2025-08-03 14:04:06] ✅ Loaded 3 FOLLOWS relationships (Duration: 0:00:00.001039)
[2025-08-03 14:04:06] Loading edges from csv_output/edges_acted_in.csv...
  Read 172 rows from csv_output/edges_acted_in.csv
    Record 1: source_label=, target_label=
    Using CREATE mode for relationships
    Record 2: source_label=, target_label=
    Using CREATE mode for relationships
    Record 3: source_label=, target_label=
    Using CREATE mode for relationships
[2025-08-03 14:04:06] Batch complete: Loaded 172 edges (Duration: 0:00:00.066439)
[2025-08-03 14:04:06] ✅ Loaded 172 ACTED_IN relationships (Duration: 0:00:00.068332)
[2025-08-03 14:04:06] Loading edges from csv_output/edges_reviewed.csv...
  Read 9 rows from csv_output/edges_reviewed.csv
    Record 1: source_label=, target_label=
    Using CREATE mode for relationships
    Record 2: source_label=, target_label=
    Using CREATE mode for relationships
    Record 3: source_label=, target_label=
    Using CREATE mode for relationships
[2025-08-03 14:04:06] Batch complete: Loaded 9 edges (Duration: 0:00:00.003290)
[2025-08-03 14:04:06] ✅ Loaded 9 REVIEWED relationships (Duration: 0:00:00.003417)
[2025-08-03 14:04:06] Loading edges from csv_output/edges_wrote.csv...
  Read 10 rows from csv_output/edges_wrote.csv
    Record 1: source_label=, target_label=
    Using CREATE mode for relationships
    Record 2: source_label=, target_label=
    Using CREATE mode for relationships
    Record 3: source_label=, target_label=
    Using CREATE mode for relationships
[2025-08-03 14:04:06] Batch complete: Loaded 10 edges (Duration: 0:00:00.003456)
[2025-08-03 14:04:06] ✅ Loaded 10 WROTE relationships (Duration: 0:00:00.003566)
[2025-08-03 14:04:06] Loading edges from csv_output/edges_directed.csv...
  Read 44 rows from csv_output/edges_directed.csv
    Record 1: source_label=, target_label=
    Using CREATE mode for relationships
    Record 2: source_label=, target_label=
    Using CREATE mode for relationships
    Record 3: source_label=, target_label=
    Using CREATE mode for relationships
[2025-08-03 14:04:06] Batch complete: Loaded 44 edges (Duration: 0:00:00.014205)
[2025-08-03 14:04:06] ✅ Loaded 44 DIRECTED relationships (Duration: 0:00:00.014337)
[2025-08-03 14:04:06] ✅ All edges loaded (Total duration: 0:00:00.095961)

[2025-08-03 14:04:06] ✅ Successfully loaded data into graph 'MOVIES' (Total loading time: 0:00:00.144561)

📊 Graph Statistics:
Nodes:
  ['Movie']: 38
  ['Person']: 133
Relationships:
  PRODUCED: 15
  REVIEWED: 9
  DIRECTED: 44
  WROTE: 10
  ACTED_IN: 172
  FOLLOWS: 3

🔍 Sample Person nodes with their attributes:
  Node 1: (:Person{birth_year:1964,id:1,name:"Keanu Reeves"})
  Node 2: (:Person{birth_year:1967,id:2,name:"Carrie-Anne Moss"})
  Node 3: (:Person{birth_year:1961,id:3,name:"Laurence Fishburne"})

```
---------

## Step 6: Validating content of data in FalkorDB

Here is how the same cypher query is visualized in both Neo4j and FalkorDB broswers:
```cypher
MATCH p=()-[:REVIEWED]->() RETURN p LIMIT 25;
```

Neo4j view

<img width="742" height="695" alt="neo4j-movies-reviews" src="https://github.com/user-attachments/assets/c5536967-135b-467e-9bbe-ff6c7182f79f" />


FalkorDB view

<img width="1203" height="733" alt="falkordb-movies-reviewed" src="https://github.com/user-attachments/assets/8173d706-f989-4611-ac00-d4752e8f8ae8" />



Good Luck!
