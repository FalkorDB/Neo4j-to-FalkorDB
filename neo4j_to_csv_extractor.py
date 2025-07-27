#!/usr/bin/env python3
"""
Neo4j to CSV Extractor
Extract nodes and edges from Neo4j database to CSV files for FalkorDB import
Does not require APOC library - uses pure Cypher queries
"""

import csv
import json
from typing import Dict, List, Any, Set, Optional
from neo4j import GraphDatabase
from neo4j.exceptions import AuthError, ServiceUnavailable
import argparse
import os
import sys
from collections import defaultdict


class Neo4jToCSVExtractor:
    def __init__(self, uri: str, username: str, password: str, database: str = "neo4j", config_file: Optional[str] = None):
        """Initialize connection to Neo4j database"""
        print(f"Connecting to Neo4j at {uri} with username '{username}'...")
        
        # Load configuration
        self.config = self._load_config(config_file)
        
        try:
            self.driver = GraphDatabase.driver(uri, auth=(username, password))
            self.database = database
            self.output_dir = "csv_output"
            
            # Test the connection
            self._test_connection()
            
            # Ensure output directory exists
            os.makedirs(self.output_dir, exist_ok=True)
            print("✅ Successfully connected to Neo4j!")
            
        except AuthError as e:
            print(f"❌ Authentication failed: {e}")
            print("\n🔧 Troubleshooting tips:")
            print("1. Check your username and password")
            print("2. Make sure Neo4j is running")
            print("3. Try connecting with Neo4j Browser first")
            print("4. Check if you need to change the initial password")
            print("\n💡 Common solutions:")
            print("   - Default password might be 'neo4j' (change required on first login)")
            print("   - Try connecting to Neo4j Browser at http://localhost:7474")
            print("   - Reset password if needed: neo4j-admin set-initial-password <newpassword>")
            sys.exit(1)
        except ServiceUnavailable as e:
            print(f"❌ Cannot connect to Neo4j service: {e}")
            print("\n🔧 Troubleshooting tips:")
            print("1. Make sure Neo4j is running")
            print("2. Check the URI (default: bolt://localhost:7687)")
            print("3. Verify firewall settings")
            print("4. Check Neo4j logs for errors")
            sys.exit(1)
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            sys.exit(1)
    
    def _load_config(self, config_file: Optional[str]) -> Dict[str, Any]:
        """Load migration configuration from JSON file"""
        default_config = {
            "label_mappings": {},
            "property_mappings": {},
            "relationship_mappings": {},
            "relationship_property_mappings": {},
            "excluded_labels": [],
            "excluded_relationships": [],
            "excluded_properties": {},
            "data_transformations": {}
        }
        
        if not config_file:
            config_file = "migrate_config.json"
        
        if os.path.exists(config_file):
            try:
                with open(config_file, 'r') as f:
                    config = json.load(f)
                    print(f"✅ Loaded configuration from {config_file}")
                    # Merge with defaults
                    for key in default_config:
                        if key not in config:
                            config[key] = default_config[key]
                    return config
            except Exception as e:
                print(f"⚠️ Error loading config file {config_file}: {e}")
                print("Using default configuration")
        else:
            print(f"📄 Config file {config_file} not found, using default configuration")
        
        return default_config
    
    def _test_connection(self):
        """Test the database connection"""
        try:
            with self.driver.session(database=self.database) as session:
                # Simple query to test connection
                result = session.run("RETURN 1 as test")
                result.single()
        except Exception as e:
            print(f"❌ Connection test failed: {e}")
            raise
    
    def close(self):
        """Close the database connection"""
        if self.driver:
            self.driver.close()
    
    def get_node_labels(self) -> List[str]:
        """Get all node labels in the database"""
        with self.driver.session(database=self.database) as session:
            result = session.run("CALL db.labels()")
            labels = [record["label"] for record in result]
            # Filter out excluded labels
            excluded = self.config.get("excluded_labels", [])
            return [label for label in labels if label not in excluded]
    
    def get_relationship_types(self) -> List[str]:
        """Get all relationship types in the database"""
        with self.driver.session(database=self.database) as session:
            result = session.run("CALL db.relationshipTypes()")
            rel_types = [record["relationshipType"] for record in result]
            # Filter out excluded relationships
            excluded = self.config.get("excluded_relationships", [])
            return [rel_type for rel_type in rel_types if rel_type not in excluded]
    
    def get_node_properties(self, label: str) -> Set[str]:
        """Get all properties for nodes with a specific label"""
        with self.driver.session(database=self.database) as session:
            # Get a sample of nodes to determine properties
            query = f"""
            MATCH (n:{label})
            WITH n LIMIT 1000
            UNWIND keys(n) AS key
            RETURN DISTINCT key
            """
            result = session.run(query)
            properties = {record["key"] for record in result}
            # Filter out excluded properties for this label
            excluded_props = self.config.get("excluded_properties", {}).get(label, [])
            return {prop for prop in properties if prop not in excluded_props}
    
    def get_relationship_properties(self, rel_type: str) -> Set[str]:
        """Get all properties for relationships of a specific type"""
        with self.driver.session(database=self.database) as session:
            query = f"""
            MATCH ()-[r:{rel_type}]->()
            WITH r LIMIT 1000
            UNWIND keys(r) AS key
            RETURN DISTINCT key
            """
            result = session.run(query)
            return {record["key"] for record in result}
    
    def extract_nodes_by_label(self, label: str, batch_size: int = 1000) -> str:
        """Extract all nodes with a specific label to CSV"""
        print(f"Extracting nodes with label: {label}")
        
        # Get properties for this label
        properties = sorted(list(self.get_node_properties(label)))
        
        # Create CSV file
        csv_filename = os.path.join(self.output_dir, f"nodes_{label.lower()}.csv")
        
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            # Map property names for headers
            mapped_properties = []
            for prop in properties:
                mapped_prop = self.config['property_mappings'].get(label, {}).get(prop, prop)
                mapped_properties.append(mapped_prop)
            
            # CSV headers: id, labels, and all mapped properties
            headers = ['id', 'labels'] + mapped_properties
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            
            # Extract nodes in batches
            skip = 0
            total_count = 0
            
            with self.driver.session(database=self.database) as session:
                while True:
                    query = f"""
                    MATCH (n:{label})
                    RETURN id(n) as node_id, labels(n) as labels, n
                    SKIP {skip} LIMIT {batch_size}
                    """
                    
                    result = session.run(query)
                    records = list(result)
                    
                    if not records:
                        break
                    
                    for record in records:
                        node_data = dict(record['n'])
                        row = {
                            'id': record['node_id'],
                            'labels': ':'.join(record['labels'])
                        }
                        
                        # Add properties
                        for prop in properties:
                            value = node_data.get(prop, '')
                            # Handle list/array properties
                            if isinstance(value, list):
                                value = ';'.join(str(v) for v in value)
                            
                            # Map properties using the config
                            mapped_prop = self.config['property_mappings'].get(label, {}).get(prop, prop)
                            
                            # Transform data if necessary
                            transformation = self.config['data_transformations'].get(label, {}).get(prop)
                            if transformation:
                                original_value = value
                                value = self._apply_transformation(value, transformation)
                                if original_value != value:
                                    print(f"  🔄 Transformed {label}.{prop}: '{original_value}' -> '{value}'")
                            
                            row[mapped_prop] = str(value) if value is not None else ''
                        
                        writer.writerow(row)
                        total_count += 1
                    
                    skip += batch_size
                    print(f"  Processed {total_count} nodes...")
        
        print(f"  Exported {total_count} nodes to {csv_filename}")
        return csv_filename
    
    def extract_all_nodes(self, batch_size: int = 1000) -> List[str]:
        """Extract all nodes grouped by label"""
        labels = self.get_node_labels()
        csv_files = []
        
        print(f"Found {len(labels)} node labels: {labels}")
        
        for label in labels:
            csv_file = self.extract_nodes_by_label(label, batch_size)
            csv_files.append(csv_file)
        
        return csv_files
    
    def _apply_transformation(self, value: Any, transformation: Dict[str, Any]) -> Any:
        """Apply data transformation based on the configuration"""
        if transformation['type'] == 'date_format':
            return self._transform_date_format(value, transformation)
        elif transformation['type'] == 'integer_to_string':
            return str(value)
        elif transformation['type'] == 'string_to_integer':
            try:
                return int(value) if value else 0
            except (ValueError, TypeError):
                return 0
        elif transformation['type'] == 'uppercase':
            return str(value).upper() if value else ''
        elif transformation['type'] == 'lowercase':
            return str(value).lower() if value else ''
        return value
    
    def _transform_date_format(self, value: Any, transformation: Dict[str, Any]) -> str:
        """Transform date format based on configuration"""
        if not value:
            return ''
        
        from_format = transformation.get('from_format', '')
        to_format = transformation.get('to_format', '')
        
        # Handle common date format transformations
        value_str = str(value)
        
        # Year only formats
        if from_format == 'yyyy' and to_format == 'yyyy-mm-dd':
            # Transform year to full date (e.g., 1964 -> 1964-01-01)
            return f"{value_str}-01-01"
        elif from_format == 'yyyy' and to_format == 'yyyy-mm':
            # Transform year to year-month (e.g., 1964 -> 1964-01)
            return f"{value_str}-01"
        elif from_format == 'yyyy-mm-dd' and to_format == 'yyyy':
            # Extract year from full date (e.g., 1964-03-15 -> 1964)
            return value_str.split('-')[0] if '-' in value_str else value_str
        elif from_format == 'yyyy-mm-dd' and to_format == 'dd/mm/yyyy':
            # Transform ISO date to DD/MM/YYYY format
            if '-' in value_str and len(value_str.split('-')) == 3:
                year, month, day = value_str.split('-')
                return f"{day}/{month}/{year}"
        elif from_format == 'dd/mm/yyyy' and to_format == 'yyyy-mm-dd':
            # Transform DD/MM/YYYY to ISO date format
            if '/' in value_str and len(value_str.split('/')) == 3:
                day, month, year = value_str.split('/')
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        
        # If no specific transformation matches, try generic formatting
        if to_format and from_format != to_format:
            print(f"⚠️  Unsupported date transformation from '{from_format}' to '{to_format}' for value '{value}'. Using original value.")
        
        return value_str

    def extract_relationships_by_type(self, rel_type: str, batch_size: int = 1000) -> str:
        """Extract all relationships of a specific type to CSV"""
        # Map relationship type name
        mapped_rel_type = self.config['relationship_mappings'].get(rel_type, rel_type)
        print(f"Extracting relationships of type: {mapped_rel_type}")
        
        # Get properties for this relationship type
        properties = sorted(list(self.get_relationship_properties(rel_type)))
        
        # Create CSV file
        csv_filename = os.path.join(self.output_dir, f"edges_{rel_type.lower()}.csv")
        
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            # Map property names for headers
            mapped_properties = []
            for prop in properties:
                mapped_prop = self.config['relationship_property_mappings'].get(rel_type, {}).get(prop, prop)
                mapped_properties.append(mapped_prop)
            
            # CSV headers: source, target, type, and all mapped properties
            headers = ['source', 'target', 'type'] + mapped_properties
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            
            # Extract relationships in batches
            skip = 0
            total_count = 0
            
            with self.driver.session(database=self.database) as session:
                while True:
                    query = f"""
                    MATCH (a)-[r:{rel_type}]->(b)
                    RETURN id(a) as source_id, id(b) as target_id, type(r) as rel_type, r
                    SKIP {skip} LIMIT {batch_size}
                    """
                    
                    result = session.run(query)
                    records = list(result)
                    
                    if not records:
                        break
                    
                    for record in records:
                        rel_data = dict(record['r'])
                        row = {
                            'source': record['source_id'],
                            'target': record['target_id'],
                            'type': record['rel_type']
                        }
                        
                        # Add properties
                        for prop in properties:
                            value = rel_data.get(prop, '')
                            # Handle list/array properties
                            if isinstance(value, list):
                                value = ';'.join(str(v) for v in value)
                            
                            # Map relationship properties using the config
                            mapped_prop = self.config['relationship_property_mappings'].get(rel_type, {}).get(prop, prop)
                            
                            # Transform data if necessary (could add transformation for relationships too)
                            row[mapped_prop] = str(value) if value is not None else ''
                        
                        writer.writerow(row)
                        total_count += 1
                    
                    skip += batch_size
                    print(f"  Processed {total_count} relationships...")
        
        print(f"  Exported {total_count} relationships to {csv_filename}")
        return csv_filename
    
    def extract_all_relationships(self, batch_size: int = 1000) -> List[str]:
        """Extract all relationships grouped by type"""
        rel_types = self.get_relationship_types()
        csv_files = []
        
        print(f"Found {len(rel_types)} relationship types: {rel_types}")
        
        for rel_type in rel_types:
            csv_file = self.extract_relationships_by_type(rel_type, batch_size)
            csv_files.append(csv_file)
        
        return csv_files
    
    def generate_falkordb_load_script(self, node_files: List[str], edge_files: List[str]) -> str:
        """Generate FalkorDB LOAD CSV commands"""
        script_filename = os.path.join(self.output_dir, "load_to_falkordb.cypher")
        
        with open(script_filename, 'w') as f:
            f.write("// FalkorDB Load Script - Generated from Neo4j Export\n")
            f.write("// Run this script in FalkorDB to import the data\n\n")
            
            # Load nodes
            f.write("// Load Nodes\n")
            for node_file in node_files:
                label = os.path.basename(node_file).replace('nodes_', '').replace('.csv', '').title()
                f.write(f"""
LOAD CSV WITH HEADERS FROM 'file:///{os.path.basename(node_file)}' AS row
MERGE (n:{label} {{id: toInteger(row.id)}})
SET n += row
REMOVE n.id, n.labels;

""")
            
            # Load relationships
            f.write("// Load Relationships\n")
            for edge_file in edge_files:
                rel_type = os.path.basename(edge_file).replace('edges_', '').replace('.csv', '').upper()
                f.write(f"""
LOAD CSV WITH HEADERS FROM 'file:///{os.path.basename(edge_file)}' AS row
MATCH (a {{id: toInteger(row.source)}})
MATCH (b {{id: toInteger(row.target)}})
MERGE (a)-[r:{rel_type}]->(b)
SET r += row
REMOVE r.source, r.target, r.type;

""")
        
        print(f"Generated FalkorDB load script: {script_filename}")
        return script_filename
    
    def extract_all(self, batch_size: int = 1000) -> Dict[str, List[str]]:
        """Extract all nodes and relationships"""
        print("Starting Neo4j to CSV extraction...")
        
        node_files = self.extract_all_nodes(batch_size)
        edge_files = self.extract_all_relationships(batch_size)
        
        script_file = self.generate_falkordb_load_script(node_files, edge_files)
        
        return {
            'nodes': node_files,
            'edges': edge_files,
            'script': script_file
        }


def main():
    parser = argparse.ArgumentParser(description='Extract Neo4j data to CSV for FalkorDB')
    parser.add_argument('--uri', default='bolt://localhost:7687', help='Neo4j URI')
    parser.add_argument('--username', default='neo4j', help='Neo4j username')
    parser.add_argument('--password', required=True, help='Neo4j password')
    parser.add_argument('--database', default='neo4j', help='Neo4j database name')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for extraction')
    parser.add_argument('--nodes-only', action='store_true', help='Extract only nodes')
    parser.add_argument('--edges-only', action='store_true', help='Extract only relationships')
    parser.add_argument('--config', help='Path to migration configuration JSON file')
    
    args = parser.parse_args()
    
    extractor = Neo4jToCSVExtractor(args.uri, args.username, args.password, args.database, args.config)
    
    try:
        if args.nodes_only:
            extractor.extract_all_nodes(args.batch_size)
        elif args.edges_only:
            extractor.extract_all_relationships(args.batch_size)
        else:
            result = extractor.extract_all(args.batch_size)
            print("\nExtraction complete!")
            print(f"Node files: {len(result['nodes'])}")
            print(f"Edge files: {len(result['edges'])}")
            print(f"Load script: {result['script']}")
    
    finally:
        extractor.close()


if __name__ == "__main__":
    main()
