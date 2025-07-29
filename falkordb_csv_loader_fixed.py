#!/usr/bin/env python3
"""
FalkorDB CSV Loader

Loads nodes and edges from CSV files in the 'csv_output' folder into FalkorDB.
Uses the falkordb-py library with batch processing and proper error handling.
"""

import os
import csv
import argparse
import sys
from typing import Dict, List, Any
from falkordb import FalkorDB


class FalkorDBCSVLoader:
    def __init__(self, host: str = "localhost", port: int = 6379, graph_name: str = "graph", username: str = None, password: str = None):
        """
        Initialize FalkorDB connection
        
        :param host: FalkorDB host
        :param port: FalkorDB port  
        :param graph_name: Target graph name
        :param username: FalkorDB username (optional)
        :param password: FalkorDB password (optional)
        """
        self.host = host
        self.port = port
        self.graph_name = graph_name
        self.csv_dir = "csv_output"
        
        try:
            print(f"Connecting to FalkorDB at {host}:{port}...")
            self.db = FalkorDB(host=host, port=port, username=username, password=password)
            self.graph = self.db.select_graph(graph_name)
            print(f"✅ Connected to FalkorDB graph '{graph_name}'")
        except Exception as e:
            print(f"❌ Failed to connect to FalkorDB: {e}")
            sys.exit(1)
    
    def read_csv_file(self, file_path: str) -> List[Dict[str, Any]]:
        """Read CSV file and return list of dictionaries"""
        rows = []
        try:
            with open(file_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    rows.append(row)
            print(f"  Read {len(rows)} rows from {file_path}")
            return rows
        except Exception as e:
            print(f"❌ Error reading {file_path}: {e}")
            return []
    
    def create_indexes_from_csv(self):
        """Create indexes from indexes.csv file"""
        indexes_file = os.path.join(self.csv_dir, 'indexes.csv')
        if not os.path.exists(indexes_file):
            print("⚠️ No indexes.csv file found, skipping index creation")
            return
        
        print("🔧 Creating indexes...")
        indexes = self.read_csv_file(indexes_file)
        
        created_count = 0
        skipped_count = 0
        
        for index in indexes:
            labels = index.get('labels', '').strip()
            properties = index.get('properties', '').strip()
            uniqueness = index.get('uniqueness', 'NON_UNIQUE')
            index_type = index.get('type', '').upper()
            
            # Skip system indexes and indexes without labels/properties
            if not labels or not properties or index_type == 'LOOKUP':
                skipped_count += 1
                continue
            
            # Split labels and properties (in case there are multiple)
            label_list = [l.strip() for l in labels.split(';') if l.strip()]
            prop_list = [p.strip() for p in properties.split(';') if p.strip()]
            
            # Create index for each label-property combination
            for label in label_list:
                for prop in prop_list:
                    try:
                        if uniqueness == 'UNIQUE':
                            # Create unique constraint
                            query = f"CREATE CONSTRAINT ON (n:{label}) ASSERT n.{prop} IS UNIQUE"
                        else:
                            # Create regular index
                            query = f"CREATE INDEX ON :{label}({prop})"
                        
                        print(f"  Creating: {query}")
                        result = self.graph.query(query)
                        created_count += 1
                        
                    except Exception as e:
                        error_msg = str(e).lower()
                        if 'already exists' in error_msg or 'equivalent' in error_msg:
                            print(f"  ⚠️ Index on {label}.{prop} already exists, skipping")
                        else:
                            print(f"  ❌ Error creating index on {label}.{prop}: {e}")
        
        print(f"✅ Created {created_count} indexes, skipped {skipped_count}")
    
    def create_constraints_from_csv(self):
        """Create constraints from constraints.csv file"""
        constraints_file = os.path.join(self.csv_dir, 'constraints.csv')
        if not os.path.exists(constraints_file):
            print("⚠️ No constraints.csv file found, skipping constraint creation")
            return
        
        print("🔒 Creating constraints...")
        constraints = self.read_csv_file(constraints_file)
        
        if not constraints:
            print("  No constraints to create")
            return
        
        created_count = 0
        skipped_count = 0
        
        for constraint in constraints:
            labels = constraint.get('labels', '').strip()
            properties = constraint.get('properties', '').strip()
            constraint_type = constraint.get('type', '').upper()
            
            # Skip constraints without labels/properties
            if not labels or not properties:
                skipped_count += 1
                continue
            
            # Split labels and properties (in case there are multiple)
            label_list = [l.strip() for l in labels.split(';') if l.strip()]
            prop_list = [p.strip() for p in properties.split(';') if p.strip()]
            
            # Create constraint for each label-property combination
            for label in label_list:
                for prop in prop_list:
                    if 'UNIQUE' in constraint_type:
                        # For UNIQUE constraints, try different syntax approaches
                        success = False
                        
                        # Try modern Cypher syntax first
                        try:
                            query = f"CREATE CONSTRAINT FOR (n:{label}) REQUIRE n.{prop} IS UNIQUE"
                            print(f"  Trying: {query}")
                            result = self.graph.query(query)
                            created_count += 1
                            success = True
                        except Exception as e1:
                            # Try Neo4j-style syntax  
                            try:
                                query = f"CREATE CONSTRAINT ON (n:{label}) ASSERT n.{prop} IS UNIQUE"
                                print(f"  Trying: {query}")
                                result = self.graph.query(query)
                                created_count += 1
                                success = True
                            except Exception as e2:
                                # Try FalkorDB Redis command style
                                try:
                                    result = self.db.execute_command('GRAPH.CONSTRAINT', self.graph_name, 'CREATE', 'UNIQUE', label, prop)
                                    print(f"  Created constraint via Redis command: {label}.{prop}")
                                    created_count += 1
                                    success = True
                                except Exception as e3:
                                    print(f"  ⚠️ FalkorDB may not support unique constraints yet. Skipping {label}.{prop}")
                                    print(f"    Tried multiple approaches: {str(e1)[:100]}...")
                                    skipped_count += 1
                        
                        if success:
                            print(f"  ✅ Created unique constraint on {label}.{prop}")
                    else:
                        # Non-unique constraint types are not commonly supported
                        print(f"  ⚠️ Constraint type '{constraint_type}' not supported, skipping {label}.{prop}")
                        skipped_count += 1
        
        if created_count > 0:
            print(f"✅ Created {created_count} constraints")
        if skipped_count > 0:
            print(f"⚠️ Skipped {skipped_count} constraints (not supported in current FalkorDB version)")
    
    def load_nodes_batch(self, file_path: str, batch_size: int = 1000):
        """Load nodes from CSV file in batches"""
        print(f"Loading nodes from {file_path}...")
        
        # Extract label from filename (e.g., nodes_person.csv -> Person)
        filename = os.path.basename(file_path)
        label = filename.replace('nodes_', '').replace('.csv', '').title()
        
        rows = self.read_csv_file(file_path)
        if not rows:
            return
        
        # Debug: show CSV headers
        if rows:
            print(f"  CSV headers: {list(rows[0].keys())}")
        
        total_loaded = 0
        
        # Process in batches
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            
            # Build Cypher query for batch
            query_parts = []
            for j, row in enumerate(batch):
                node_id = row.get('id', '')
                properties = {}
                
                # Add all properties except id and labels
                for key, value in row.items():
                    if key not in ['id', 'labels']:
                        # Handle empty values gracefully
                        if value:
                            # Try to convert to appropriate type
                            if value.isdigit():
                                properties[key] = int(value)
                            elif value.replace('.', '', 1).lstrip('-').isdigit():
                                properties[key] = float(value)
                            else:
                                properties[key] = value
                        else:
                            properties[key] = None
                
                # Build property string - handle None values properly
                prop_parts = []
                for k, v in properties.items():
                    if v is None:
                        # Skip None values in FalkorDB queries
                        continue
                    else:
                        prop_parts.append(f"{k}: {repr(v)}")
                prop_str = ', '.join(prop_parts)
                
                # Smart ID handling: quote if not a pure number
                if node_id.isdigit():
                    id_str = node_id  # Numeric ID, no quotes needed
                else:
                    id_str = f"'{node_id}'"  # String ID, needs quotes
                
                # Debug: show properties for first few records
                if i == 0 and j < 3:
                    print(f"    Record {j+1}: properties = {properties}")
                    print(f"    Generated query: CREATE (:{label} {{id: {id_str}{', ' + prop_str if prop_str else ''}}})")
                
                query_parts.append(f"CREATE (:{label} {{id: {id_str}{', ' + prop_str if prop_str else ''}}})")            
            # Execute each node query individually
            for query in query_parts:
                try:
                    self.graph.query(query)
                    total_loaded += 1
                except Exception as e:
                    print(f"❌ Error loading node: {e}")
                    print(f"Query: {query}")
            
            if len(batch) > 0:
                print(f"  Loaded {total_loaded}/{len(rows)} nodes...")
        
        print(f"✅ Loaded {total_loaded} {label} nodes")
    
    def load_edges_batch(self, file_path: str, batch_size: int = 1000):
        """Load edges from CSV file in batches"""
        print(f"Loading edges from {file_path}...")
        
        # Extract relationship type from filename (e.g., edges_acted_in.csv -> ACTED_IN)
        filename = os.path.basename(file_path)
        rel_type = filename.replace('edges_', '').replace('.csv', '').upper()
        
        rows = self.read_csv_file(file_path)
        if not rows:
            return
        
        total_loaded = 0
        
        # Process in batches
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            
            # Build Cypher query for batch
            query_parts = []
            for j, row in enumerate(batch):
                source_id = row.get('source', '')
                target_id = row.get('target', '')
                
                if not source_id or not target_id:
                    continue
                
                properties = {}
                
                # Add all properties except source, target, type
                for key, value in row.items():
                    if key not in ['source', 'target', 'type'] and value:
                        # Try to convert to appropriate type
                        if value.isdigit():
                            properties[key] = int(value)
                        elif value.replace('.', '', 1).isdigit():
                            properties[key] = float(value)
                        else:
                            properties[key] = value
                
                # Build property string
                prop_str = ', '.join([f"{k}: {repr(v)}" for k, v in properties.items()])
                
                # Smart ID handling for both source and target
                source_id_str = source_id if source_id.isdigit() else f"'{source_id}'"
                target_id_str = target_id if target_id.isdigit() else f"'{target_id}'"
                
                query_parts.append(
                    f"MATCH (a {{id: {source_id_str}}}), (b {{id: {target_id_str}}}) "
                    f"CREATE (a)-[:{rel_type}{' {' + prop_str + '}' if prop_str else ''}]-\u003e(b)"
                )
            
            # Execute each relationship query individually to avoid WITH clause issues
            for query in query_parts:
                try:
                    self.graph.query(query)
                    total_loaded += 1
                except Exception as e:
                    print(f"❌ Error loading edge: {e}")
                    print(f"Query: {query}")
            
            if len(batch) > 0:
                print(f"  Loaded {total_loaded}/{len(rows)} edges...")
        
        print(f"✅ Loaded {total_loaded} {rel_type} relationships")
    
    def load_all_csvs(self, batch_size: int = 1000):
        """Load all CSV files from the csv_output directory"""
        if not os.path.exists(self.csv_dir):
            print(f"❌ Directory {self.csv_dir} does not exist")
            return
        
        csv_files = os.listdir(self.csv_dir)
        node_files = [f for f in csv_files if f.startswith('nodes_') and f.endswith('.csv')]
        edge_files = [f for f in csv_files if f.startswith('edges_') and f.endswith('.csv')]
        
        print(f"Found {len(node_files)} node files and {len(edge_files)} edge files")
        
        # Create indexes and constraints first (for better performance)
        print("\n🗂️ Setting up database schema...")
        self.create_indexes_from_csv()
        self.create_constraints_from_csv()
        
        # Load nodes first
        print("\n📥 Loading nodes...")
        for node_file in node_files:
            file_path = os.path.join(self.csv_dir, node_file)
            self.load_nodes_batch(file_path, batch_size)
        
        # Then load edges
        print("\n🔗 Loading edges...")
        for edge_file in edge_files:
            file_path = os.path.join(self.csv_dir, edge_file)
            self.load_edges_batch(file_path, batch_size)
        
        print(f"\n✅ Successfully loaded data into graph '{self.graph_name}'")
    
    def load_data_only(self, batch_size: int = 1000):
        """Load only data from CSV files, skip index creation"""
        if not os.path.exists(self.csv_dir):
            print(f"❌ Directory {self.csv_dir} does not exist")
            return
        
        csv_files = os.listdir(self.csv_dir)
        node_files = [f for f in csv_files if f.startswith('nodes_') and f.endswith('.csv')]
        edge_files = [f for f in csv_files if f.startswith('edges_') and f.endswith('.csv')]
        
        print(f"Found {len(node_files)} node files and {len(edge_files)} edge files")
        print("⚠️ Skipping index creation as requested")
        
        # Load nodes first
        print("\n📥 Loading nodes...")
        for node_file in node_files:
            file_path = os.path.join(self.csv_dir, node_file)
            self.load_nodes_batch(file_path, batch_size)
        
        # Then load edges
        print("\n🔗 Loading edges...")
        for edge_file in edge_files:
            file_path = os.path.join(self.csv_dir, edge_file)
            self.load_edges_batch(file_path, batch_size)
        
        print(f"\n✅ Successfully loaded data into graph '{self.graph_name}' (without indexes)")
    
    def verify_node_attributes(self, label: str = "Person", limit: int = 5):
        """Verify what attributes were loaded for a specific node type"""
        try:
            query = f"MATCH (n:{label}) RETURN n LIMIT {limit}"
            result = self.graph.query(query)
            print(f"\n🔍 Sample {label} nodes with their attributes:")
            for i, record in enumerate(result.result_set):
                node = record[0]
                print(f"  Node {i+1}: {node}")
                
        except Exception as e:
            print(f"❌ Error verifying node attributes: {e}")
    
    def get_graph_stats(self):
        """Get statistics about the loaded graph"""
        try:
            # Count nodes by label
            node_result = self.graph.query("MATCH (n) RETURN labels(n) as labels, count(n) as count")
            print("\n📊 Graph Statistics:")
            print("Nodes:")
            for record in node_result.result_set:
                labels = record[0] if record[0] else ['Unknown']
                count = record[1]
                print(f"  {labels}: {count}")
            
            # Count relationships by type
            rel_result = self.graph.query("MATCH ()-[r]->() RETURN type(r) as type, count(r) as count")
            print("Relationships:")
            for record in rel_result.result_set:
                rel_type = record[0]
                count = record[1]
                print(f"  {rel_type}: {count}")
                
        except Exception as e:
            print(f"❌ Error getting graph statistics: {e}")
    
    def close(self):
        """Close the database connection"""
        # FalkorDB doesn't require explicit closing
        pass


def main():
    parser = argparse.ArgumentParser(description='Load CSV files into FalkorDB')
    parser.add_argument('graph_name', help='Target graph name in FalkorDB')
    parser.add_argument('--host', default='localhost', help='FalkorDB host')
    parser.add_argument('--port', type=int, default=6379, help='FalkorDB port')
    parser.add_argument('--username', help='FalkorDB username (optional)')
    parser.add_argument('--password', help='FalkorDB password (optional)')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for loading')
    parser.add_argument('--stats', action='store_true', help='Show graph statistics after loading')
    parser.add_argument('--skip-indexes', action='store_true', help='Skip index and constraint creation')
    parser.add_argument('--indexes-only', action='store_true', help='Only create indexes and constraints, skip data loading')
    
    args = parser.parse_args()
    
    loader = FalkorDBCSVLoader(
        host=args.host,
        port=args.port,
        graph_name=args.graph_name,
        username=args.username,
        password=args.password
    )
    
    try:
        if args.indexes_only:
            # Only create indexes and constraints
            print("🗂️ Creating indexes and constraints only...")
            loader.create_indexes_from_csv()
            loader.create_constraints_from_csv()
        elif args.skip_indexes:
            # Load data without creating indexes
            loader.load_data_only(args.batch_size)
        else:
            # Default: load everything
            loader.load_all_csvs(args.batch_size)
        
        if args.stats and not args.indexes_only:
            loader.get_graph_stats()
            loader.verify_node_attributes("Person", 3)
            
    except KeyboardInterrupt:
        print("\n❌ Loading interrupted by user")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    finally:
        loader.close()


if __name__ == "__main__":
    main()
