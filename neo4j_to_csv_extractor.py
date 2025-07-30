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
    
    def get_indexes(self) -> List[Dict[str, Any]]:
        """Get all indexes in the database"""
        indexes = []
        with self.driver.session(database=self.database) as session:
            try:
                # Try the modern SHOW INDEXES command (Neo4j 4.0+)
                result = session.run("SHOW INDEXES")
                for record in result:
                    index_info = {
                        'name': record.get('name', 'unknown'),
                        'type': record.get('type', 'unknown'),
                        'state': record.get('state', 'unknown'),
                        'population_percent': record.get('populationPercent', 0),
                        'uniqueness': record.get('uniqueness', 'NON_UNIQUE'),
                        'entity_type': record.get('entityType', 'NODE'),
                        'labels': record.get('labelsOrTypes', []),
                        'properties': record.get('properties', []),
                        'provider': record.get('provider', {})
                    }
                    indexes.append(index_info)
            except Exception as e:
                print(f"SHOW INDEXES failed, trying legacy command: {e}")
                try:
                    # Fallback to legacy db.indexes() for older Neo4j versions
                    result = session.run("CALL db.indexes()")
                    for record in result:
                        index_info = {
                            'name': record.get('indexName', record.get('name', 'unknown')),
                            'type': record.get('type', 'unknown'),
                            'state': record.get('state', 'unknown'),
                            'population_percent': record.get('populationPercent', 0),
                            'uniqueness': 'UNIQUE' if record.get('uniqueness', False) else 'NON_UNIQUE',
                            'entity_type': 'NODE',  # Legacy indexes are typically node indexes
                            'labels': record.get('tokenNames', []),
                            'properties': record.get('properties', []),
                            'provider': record.get('provider', {})
                        }
                        indexes.append(index_info)
                except Exception as e2:
                    print(f"Legacy db.indexes() also failed: {e2}")
                    # Try even older approach
                    try:
                        result = session.run("CALL db.schema.nodeTypeProperties()")
                        schema_info = list(result)
                        if schema_info:
                            print("Found schema information, but no direct index access available")
                    except Exception as e3:
                        print(f"No index information could be retrieved: {e3}")
        
        return indexes
    
    def get_constraints(self) -> List[Dict[str, Any]]:
        """Get all constraints in the database"""
        constraints = []
        with self.driver.session(database=self.database) as session:
            try:
                # Try the modern SHOW CONSTRAINTS command (Neo4j 4.0+)
                result = session.run("SHOW CONSTRAINTS")
                for record in result:
                    constraint_info = {
                        'name': record.get('name', 'unknown'),
                        'type': record.get('type', 'unknown'),
                        'entity_type': record.get('entityType', 'NODE'),
                        'labels': record.get('labelsOrTypes', []),
                        'properties': record.get('properties', []),
                        'options': record.get('options', {})
                    }
                    constraints.append(constraint_info)
            except Exception as e:
                print(f"SHOW CONSTRAINTS failed, trying legacy command: {e}")
                try:
                    # Fallback to legacy db.constraints() for older Neo4j versions
                    result = session.run("CALL db.constraints()")
                    for record in result:
                        constraint_info = {
                            'name': record.get('name', 'unknown'),
                            'type': record.get('type', 'unknown'),
                            'entity_type': 'NODE',  # Legacy constraints are typically node constraints
                            'labels': record.get('label', '').split(':') if record.get('label') else [],
                            'properties': record.get('properties', []),
                            'options': {}
                        }
                        constraints.append(constraint_info)
                except Exception as e2:
                    print(f"Legacy db.constraints() also failed: {e2}")
        
        return constraints
    
    def extract_indexes_to_csv(self) -> str:
        """Extract all indexes to CSV file"""
        print("Extracting database indexes...")
        
        indexes = self.get_indexes()
        # Apply property mappings to indexes
        indexes = self._translate_index_properties(indexes)
        csv_filename = os.path.join(self.output_dir, "indexes.csv")
        
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            headers = ['name', 'type', 'state', 'population_percent', 'uniqueness', 
                      'entity_type', 'labels', 'properties', 'provider']
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            
            for index in indexes:
                # Safely handle labels field
                labels = index.get('labels', [])
                if not isinstance(labels, list):
                    labels = [str(labels)] if labels else []
                
                # Safely handle properties field
                properties = index.get('properties', [])
                if not isinstance(properties, list):
                    properties = [str(properties)] if properties else []
                
                row = {
                    'name': index.get('name', ''),
                    'type': index.get('type', ''),
                    'state': index.get('state', ''),
                    'population_percent': index.get('population_percent', 0),
                    'uniqueness': index.get('uniqueness', ''),
                    'entity_type': index.get('entity_type', ''),
                    'labels': ';'.join(labels),
                    'properties': ';'.join(properties),
                    'provider': json.dumps(index.get('provider', {}))
                }
                writer.writerow(row)
        
        print(f"  Exported {len(indexes)} indexes to {csv_filename}")
        return csv_filename
    
    def extract_constraints_to_csv(self) -> str:
        """Extract all constraints to CSV file"""
        print("Extracting database constraints...")
        
        constraints = self.get_constraints()
        # Apply property mappings to constraints
        constraints = self._translate_constraint_properties(constraints)
        csv_filename = os.path.join(self.output_dir, "constraints.csv")
        
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            headers = ['name', 'type', 'entity_type', 'labels', 'properties', 'options']
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            
            for constraint in constraints:
                # Safely handle labels field
                labels = constraint.get('labels', [])
                if not isinstance(labels, list):
                    labels = [str(labels)] if labels else []
                
                # Safely handle properties field
                properties = constraint.get('properties', [])
                if not isinstance(properties, list):
                    properties = [str(properties)] if properties else []
                
                row = {
                    'name': constraint.get('name', ''),
                    'type': constraint.get('type', ''),
                    'entity_type': constraint.get('entity_type', ''),
                    'labels': ';'.join(labels),
                    'properties': ';'.join(properties),
                    'options': json.dumps(constraint.get('options', {}))
                }
                writer.writerow(row)
        
        print(f"  Exported {len(constraints)} constraints to {csv_filename}")
        return csv_filename

    def _translate_constraint_properties(self, constraints: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Translate constraint properties based on the configuration."""
        
        translated_constraints = []
        
        for constraint in constraints:
            translated_constraint = constraint.copy()
            labels = translated_constraint.get('labels', [])
            properties = translated_constraint.get('properties', [])
            
            if not labels or not properties:
                translated_constraints.append(translated_constraint)
                continue

            # Assuming single label for simplicity; adjust if constraints can have multiple labels
            label = labels[0]
            
            # Get property mappings for this label
            property_mappings = self.config.get('property_mappings', {}).get(label, {})
            
            if not property_mappings:
                translated_constraints.append(translated_constraint)
                continue

            # Translate properties
            translated_properties = [property_mappings.get(prop, prop) for prop in properties]
            translated_constraint['properties'] = translated_properties
            translated_constraints.append(translated_constraint)
            
        return translated_constraints
    

    def _translate_index_properties(self, indexes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Translate index properties based on the configuration."""
        
        translated_indexes = []
        
        for index in indexes:
            translated_index = index.copy()
            labels = translated_index.get('labels', [])
            properties = translated_index.get('properties', [])
            
            if not labels or not properties:
                translated_indexes.append(translated_index)
                continue

            # Assuming single label for simplicity; adjust if indexes can have multiple labels
            label = labels[0]
            
            # Get property mappings for this label
            property_mappings = self.config.get('property_mappings', {}).get(label, {})
            
            if not property_mappings:
                translated_indexes.append(translated_index)
                continue

            # Translate properties
            translated_properties = [property_mappings.get(prop, prop) for prop in properties]
            translated_index['properties'] = translated_properties
            translated_indexes.append(translated_index)
            
        return translated_indexes
        
    def generate_falkordb_index_script(self, index_file: str, constraint_file: str) -> str:
        """Generate FalkorDB index creation script"""
        script_filename = os.path.join(self.output_dir, "create_indexes_falkordb.cypher")
        
        with open(script_filename, 'w') as f:
            f.write("// FalkorDB Index Creation Script - Generated from Neo4j Export\n")
            f.write("// Run this script in FalkorDB to recreate indexes and constraints\n\n")
            
            # Read and process indexes
            f.write("// Create Indexes\n")
            try:
                with open(index_file, 'r', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        if row['labels'] and row['properties']:
                            labels = row['labels'].split(';')
                            properties = row['properties'].split(';')
                            
                            for label in labels:
                                for prop in properties:
                                    # Convert Neo4j index to FalkorDB syntax
                                    if row['uniqueness'] == 'UNIQUE':
                                        f.write(f"CREATE CONSTRAINT ON (n:{label}) ASSERT n.{prop} IS UNIQUE;\n")
                                    else:
                                        f.write(f"CREATE INDEX ON :{label}({prop});\n")
                f.write("\n")
            except Exception as e:
                f.write(f"// Error reading index file: {e}\n\n")
            
            # Read and process constraints
            f.write("// Create Constraints\n")
            try:
                with open(constraint_file, 'r', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        if row['labels'] and row['properties']:
                            labels = row['labels'].split(';')
                            properties = row['properties'].split(';')
                            
                            for label in labels:
                                for prop in properties:
                                    constraint_type = row['type'].upper()
                                    
                                    if 'UNIQUE' in constraint_type:
                                        f.write(f"CREATE CONSTRAINT ON (n:{label}) ASSERT n.{prop} IS UNIQUE;\n")
                                    elif 'EXIST' in constraint_type or 'NOT_NULL' in constraint_type:
                                        f.write(f"// Note: FalkorDB may not support existence constraints - verify manually\n")
                                        f.write(f"// CREATE CONSTRAINT ON (n:{label}) ASSERT exists(n.{prop});\n")
                f.write("\n")
            except Exception as e:
                f.write(f"// Error reading constraint file: {e}\n\n")
        
        print(f"Generated FalkorDB index creation script: {script_filename}")
        return script_filename
    
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
            headers = ['source', 'source_label', 'target', 'target_label', 'type'] + mapped_properties
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            
            # Extract relationships in batches
            skip = 0
            total_count = 0
            
            with self.driver.session(database=self.database) as session:
                while True:
                    query = f"""
                    MATCH (a)-[r:{rel_type}]->(b)
                    RETURN id(a) as source_id, labels(a) as source_label, id(b) as target_id, labels(b) as target_label, type(r) as rel_type, r
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
                            'source_label': ':'.join(record['source_label']),
                            'target': record['target_id'],
                            'target_label': ':'.join(record['target_label']),
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
                
                # Read the first row of the CSV to get label information
                try:
                    with open(edge_file, 'r', encoding='utf-8') as csvfile:
                        reader = csv.DictReader(csvfile)
                        first_row = next(reader, None)
                        if first_row and 'source_label' in first_row and 'target_label' in first_row:
                            source_label = first_row['source_label'].split(':')[0]  # Take first label if multiple
                            target_label = first_row['target_label'].split(':')[0]  # Take first label if multiple
                            
                            f.write(f"""
LOAD CSV WITH HEADERS FROM 'file:///{os.path.basename(edge_file)}' AS row
MATCH (a:{source_label} {{id: toInteger(row.source)}})
MATCH (b:{target_label} {{id: toInteger(row.target)}})
MERGE (a)-[r:{rel_type}]->(b)
SET r += row
REMOVE r.source, r.source_label, r.target, r.target_label, r.type;

""")
                        else:
                            # Fallback to generic approach without labels
                            f.write(f"""
LOAD CSV WITH HEADERS FROM 'file:///{os.path.basename(edge_file)}' AS row
MATCH (a {{id: toInteger(row.source)}})
MATCH (b {{id: toInteger(row.target)}})
MERGE (a)-[r:{rel_type}]->(b)
SET r += row
REMOVE r.source, r.target, r.type;

""")
                except Exception as e:
                    print(f"Warning: Could not read labels from {edge_file}: {e}")
                    # Fallback to generic approach without labels
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
    
    def generate_template_config(self, output_file: str = "template_migrate_config.json") -> str:
        """Generate a template migration config file based on Neo4j topology"""
        print("🔍 Analyzing Neo4j database topology...")
        
        # Get all labels and relationship types without filtering
        with self.driver.session(database=self.database) as session:
            # Get all node labels
            result = session.run("CALL db.labels()")
            all_labels = [record["label"] for record in result]
            
            # Get all relationship types
            result = session.run("CALL db.relationshipTypes()")
            all_rel_types = [record["relationshipType"] for record in result]
        
        print(f"  Found {len(all_labels)} node labels: {all_labels}")
        print(f"  Found {len(all_rel_types)} relationship types: {all_rel_types}")
        
        # Build template configuration
        template_config = {
            "label_mappings": {label: label for label in sorted(all_labels)},
            "property_mappings": {},
            "relationship_mappings": {rel_type: rel_type for rel_type in sorted(all_rel_types)},
            "relationship_property_mappings": {},
            "excluded_labels": [],
            "excluded_relationships": [],
            "excluded_properties": {},
            "data_transformations": {}
        }
        
        # Analyze properties for each label
        print("\n📊 Analyzing node properties...")
        with self.driver.session(database=self.database) as session:
            for label in sorted(all_labels):
                print(f"  Analyzing {label} properties...")
                
                # Get properties for this label
                query = f"""
                MATCH (n:{label})
                WITH n LIMIT 1000
                UNWIND keys(n) AS key
                RETURN DISTINCT key
                ORDER BY key
                """
                result = session.run(query)
                properties = [record["key"] for record in result]
                
                if properties:
                    template_config["property_mappings"][label] = {
                        prop: prop for prop in properties
                    }
                    template_config["excluded_properties"][label] = []
                    template_config["data_transformations"][label] = {}
                    
                    # Add example transformations for common property patterns
                    for prop in properties:
                        if prop.lower() in ['born', 'birth_year', 'birthdate']:
                            template_config["data_transformations"][label][prop] = {
                                "type": "date_format",
                                "from_format": "yyyy",
                                "to_format": "yyyy-mm-dd",
                                "description": "Transform birth year to full date format"
                            }
                        elif prop.lower() in ['released', 'release_year', 'year']:
                            template_config["data_transformations"][label][prop] = {
                                "type": "date_format",
                                "from_format": "yyyy",
                                "to_format": "yyyy-mm-dd",
                                "description": "Transform release year to full date format"
                            }
                    
                    print(f"    Found {len(properties)} properties: {properties}")
                else:
                    print(f"    No properties found for {label}")
        
        # Analyze relationship properties
        print("\n🔗 Analyzing relationship properties...")
        with self.driver.session(database=self.database) as session:
            for rel_type in sorted(all_rel_types):
                print(f"  Analyzing {rel_type} properties...")
                
                # Get properties for this relationship type
                query = f"""
                MATCH ()-[r:{rel_type}]->()
                WITH r LIMIT 1000
                UNWIND keys(r) AS key
                RETURN DISTINCT key
                ORDER BY key
                """
                result = session.run(query)
                properties = [record["key"] for record in result]
                
                if properties:
                    template_config["relationship_property_mappings"][rel_type] = {
                        prop: prop for prop in properties
                    }
                    print(f"    Found {len(properties)} properties: {properties}")
                else:
                    print(f"    No properties found for {rel_type}")
        
        # Add database statistics
        print("\n📈 Gathering database statistics...")
        with self.driver.session(database=self.database) as session:
            # Count nodes by label
            node_counts = {}
            for label in all_labels:
                result = session.run(f"MATCH (n:{label}) RETURN count(n) as count")
                count = result.single()["count"]
                node_counts[label] = count
                print(f"  {label}: {count} nodes")
            
            # Count relationships by type
            rel_counts = {}
            for rel_type in all_rel_types:
                result = session.run(f"MATCH ()-[r:{rel_type}]->() RETURN count(r) as count")
                count = result.single()["count"]
                rel_counts[rel_type] = count
                print(f"  {rel_type}: {count} relationships")
        
        # Add metadata to the config
        template_config["_metadata"] = {
            "generated_from": "Neo4j topology analysis",
            "database_stats": {
                "node_counts": node_counts,
                "relationship_counts": rel_counts,
                "total_nodes": sum(node_counts.values()),
                "total_relationships": sum(rel_counts.values())
            },
            "instructions": [
                "This is a template configuration generated from your Neo4j database topology.",
                "Customize the mappings, exclusions, and transformations as needed.",
                "Remove the '_metadata' section before using this config for migration.",
                "See MIGRATION_CONFIG_README.md for detailed configuration options."
            ]
        }
        
        # Save template to file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(template_config, f, indent=2, ensure_ascii=False)
        
        print(f"\n✅ Generated template migration config: {output_file}")
        print(f"📝 Total elements analyzed:")
        print(f"   - {len(all_labels)} node labels")
        print(f"   - {len(all_rel_types)} relationship types")
        print(f"   - {sum(len(props) for props in template_config['property_mappings'].values())} node properties")
        print(f"   - {sum(len(props) for props in template_config['relationship_property_mappings'].values())} relationship properties")
        
        return output_file
    
    def extract_all(self, batch_size: int = 1000) -> Dict[str, Any]:
        """Extract all nodes, relationships, indexes, and constraints"""
        print("Starting Neo4j to CSV extraction...")
        
        node_files = self.extract_all_nodes(batch_size)
        edge_files = self.extract_all_relationships(batch_size)
        
        # Extract indexes and constraints
        index_file = self.extract_indexes_to_csv()
        constraint_file = self.extract_constraints_to_csv()
        
        # Generate scripts
        data_script_file = self.generate_falkordb_load_script(node_files, edge_files)
        index_script_file = self.generate_falkordb_index_script(index_file, constraint_file)
        
        return {
            'nodes': node_files,
            'edges': edge_files,
            'indexes': index_file,
            'constraints': constraint_file,
            'data_script': data_script_file,
            'index_script': index_script_file
        }


def main():
    parser = argparse.ArgumentParser(
        description='Extract Neo4j data to CSV for FalkorDB migration',
        epilog='''
Examples:
  # Analyze topology and generate template config
  python3 neo4j_to_csv_extractor.py --password mypass --analyze-only
  
  # Generate custom template config file
  python3 neo4j_to_csv_extractor.py --password mypass --generate-template my_template.json
  
  # Extract data using existing config
  python3 neo4j_to_csv_extractor.py --password mypass --config migrate_config.json
  
  # Extract only nodes
  python3 neo4j_to_csv_extractor.py --password mypass --nodes-only
''',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--uri', default='bolt://localhost:7687', help='Neo4j URI')
    parser.add_argument('--username', default='neo4j', help='Neo4j username')
    parser.add_argument('--password', required=True, help='Neo4j password')
    parser.add_argument('--database', default='neo4j', help='Neo4j database name')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for extraction')
    parser.add_argument('--nodes-only', action='store_true', help='Extract only nodes')
    parser.add_argument('--edges-only', action='store_true', help='Extract only relationships')
    parser.add_argument('--indexes-only', action='store_true', help='Extract only indexes and constraints')
    parser.add_argument('--config', help='Path to migration configuration JSON file')
    parser.add_argument('--generate-template', help='Generate template migration config file from Neo4j topology (specify output filename)')
    parser.add_argument('--analyze-only', action='store_true', help='Only analyze topology and generate template config, do not extract data')
    
    args = parser.parse_args()
    
    extractor = Neo4jToCSVExtractor(args.uri, args.username, args.password, args.database, args.config)
    
    try:
        # Handle template generation
        if args.generate_template or args.analyze_only:
            template_file = args.generate_template if args.generate_template else "template_migrate_config.json"
            extractor.generate_template_config(template_file)
            
            if args.analyze_only:
                print("\n📋 Analysis complete! Use the generated template to customize your migration.")
                return
        
        # Handle data extraction
        if args.nodes_only:
            extractor.extract_all_nodes(args.batch_size)
        elif args.edges_only:
            extractor.extract_all_relationships(args.batch_size)
        elif args.indexes_only:
            index_file = extractor.extract_indexes_to_csv()
            constraint_file = extractor.extract_constraints_to_csv()
            index_script_file = extractor.generate_falkordb_index_script(index_file, constraint_file)
            print("\nIndex and constraint extraction complete!")
            print(f"Index file: {index_file}")
            print(f"Constraint file: {constraint_file}")
            print(f"Index creation script: {index_script_file}")
        else:
            result = extractor.extract_all(args.batch_size)
            print("\nExtraction complete!")
            print(f"Node files: {len(result['nodes'])}")
            print(f"Edge files: {len(result['edges'])}")
            print(f"Index file: {result['indexes']}")
            print(f"Constraint file: {result['constraints']}")
            print(f"Data load script: {result['data_script']}")
            print(f"Index creation script: {result['index_script']}")
    
    finally:
        extractor.close()


if __name__ == "__main__":
    main()
