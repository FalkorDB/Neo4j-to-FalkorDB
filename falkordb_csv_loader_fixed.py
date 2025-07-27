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
    def __init__(self, host: str = "localhost", port: int = 6379, graph_name: str = "graph"):
        """
        Initialize FalkorDB connection
        
        :param host: FalkorDB host
        :param port: FalkorDB port  
        :param graph_name: Target graph name
        """
        self.host = host
        self.port = port
        self.graph_name = graph_name
        self.csv_dir = "csv_output"
        
        try:
            print(f"Connecting to FalkorDB at {host}:{port}...")
            self.db = FalkorDB(host=host, port=port)
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
                
                # Debug: show properties for first few records
                if i == 0 and j < 3:
                    print(f"    Record {j+1}: properties = {properties}")
                    print(f"    Generated query: CREATE (:{label} {{id: {node_id}{', ' + prop_str if prop_str else ''}}})")
                
                query_parts.append(f"CREATE (:{label} {{id: {node_id}{', ' + prop_str if prop_str else ''}}})")            
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
                
                query_parts.append(
                    f"MATCH (a {{id: {source_id}}}), (b {{id: {target_id}}}) "
                    f"CREATE (a)-[:{rel_type}{' {' + prop_str + '}' if prop_str else ''}]->(b)"
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
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for loading')
    parser.add_argument('--stats', action='store_true', help='Show graph statistics after loading')
    
    args = parser.parse_args()
    
    loader = FalkorDBCSVLoader(
        host=args.host,
        port=args.port,
        graph_name=args.graph_name
    )
    
    try:
        loader.load_all_csvs(args.batch_size)
        
        if args.stats:
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
