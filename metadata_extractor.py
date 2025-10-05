import psycopg2
from psycopg2.extras import RealDictCursor
import json
from typing import Dict, List, Any, Optional
import os
from datetime import datetime, timedelta
from collections import defaultdict
import pickle

class DatabaseMetadataExtractor:
    """
    Extract comprehensive database metadata for AI/LLM consumption.
    Optimized for DigitalOcean AI and other natural language query systems.
    Includes caching, views, statistics, and relationship mapping.
    """
    
    def __init__(self, connection_params: Dict[str, str], cache_file: str = '.db_metadata_cache.pkl'):
        """
        Initialize with database connection parameters.
        
        Args:
            connection_params: Dictionary with keys: host, database, user, password, port
            cache_file: Path to cache file for metadata
        """
        self.connection_params = connection_params
        self.metadata = {}
        self.cache_file = cache_file
        self.cache_ttl = timedelta(hours=1)  # Cache expires after 1 hour
    
    def connect(self):
        """Create database connection"""
        try:
            self.conn = psycopg2.connect(**self.connection_params)
            self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            return True
        except Exception as e:
            print(f"Connection failed: {e}")
            return False
    
    def close(self):
        """Close database connection"""
        if hasattr(self, 'cursor'):
            self.cursor.close()
        if hasattr(self, 'conn'):
            self.conn.close()
    
    def get_tables_info(self) -> List[Dict[str, Any]]:
        """Get all tables with their basic information"""
        query = """
        SELECT 
            t.table_name,
            t.table_type,
            pg_catalog.obj_description(pgc.oid, 'pg_class') as table_comment,
            (SELECT COUNT(*) 
             FROM information_schema.columns c 
             WHERE c.table_name = t.table_name 
             AND c.table_schema = 'public') as column_count
        FROM information_schema.tables t
        LEFT JOIN pg_catalog.pg_class pgc ON pgc.relname = t.table_name
        LEFT JOIN pg_catalog.pg_namespace pgn ON pgn.oid = pgc.relnamespace 
            AND pgn.nspname = 'public'
        WHERE t.table_schema = 'public'
        AND t.table_type = 'BASE TABLE'
        ORDER BY t.table_name;
        """
        
        self.cursor.execute(query)
        return [dict(row) for row in self.cursor.fetchall()]
    
    def get_views_info(self) -> List[Dict[str, Any]]:
        """Get all views and materialized views"""
        query = """
        SELECT 
            t.table_name as view_name,
            t.table_type,
            pg_catalog.obj_description(pgc.oid, 'pg_class') as view_comment,
            pg_get_viewdef(pgc.oid, true) as view_definition
        FROM information_schema.tables t
        LEFT JOIN pg_catalog.pg_class pgc ON pgc.relname = t.table_name
        LEFT JOIN pg_catalog.pg_namespace pgn ON pgn.oid = pgc.relnamespace 
            AND pgn.nspname = 'public'
        WHERE t.table_schema = 'public'
        AND t.table_type IN ('VIEW', 'MATERIALIZED VIEW')
        ORDER BY t.table_name;
        """
        
        self.cursor.execute(query)
        return [dict(row) for row in self.cursor.fetchall()]
    
    def get_columns_info(self, table_name: str) -> List[Dict[str, Any]]:
        """Get detailed column information for a specific table"""
        query = """
        SELECT 
            c.column_name,
            c.data_type,
            c.character_maximum_length,
            c.numeric_precision,
            c.numeric_scale,
            c.is_nullable,
            c.column_default,
            pg_catalog.col_description(
                (SELECT oid FROM pg_catalog.pg_class WHERE relname = c.table_name),
                c.ordinal_position
            ) as column_comment,
            c.ordinal_position
        FROM information_schema.columns c
        WHERE c.table_schema = 'public'
        AND c.table_name = %s
        ORDER BY c.ordinal_position;
        """
        
        self.cursor.execute(query, (table_name,))
        return [dict(row) for row in self.cursor.fetchall()]
    
    def get_primary_keys(self, table_name: str) -> List[str]:
        """Get primary key columns for a table"""
        query = """
        SELECT a.attname as column_name
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid 
            AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = %s::regclass
        AND i.indisprimary
        ORDER BY a.attnum;
        """
        
        try:
            self.cursor.execute(query, (table_name,))
            return [row['column_name'] for row in self.cursor.fetchall()]
        except:
            return []
    
    def get_foreign_keys(self, table_name: str) -> List[Dict[str, Any]]:
        """Get foreign key relationships for a table"""
        query = """
        SELECT
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name,
            rc.constraint_name,
            rc.update_rule,
            rc.delete_rule
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        JOIN information_schema.referential_constraints rc
            ON tc.constraint_name = rc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_name = %s
        AND tc.table_schema = 'public';
        """
        
        self.cursor.execute(query, (table_name,))
        return [dict(row) for row in self.cursor.fetchall()]
    
    def get_indexes(self, table_name: str) -> List[Dict[str, Any]]:
        """Get indexes for a table"""
        query = """
        SELECT
            i.relname as index_name,
            a.attname as column_name,
            ix.indisunique as is_unique,
            ix.indisprimary as is_primary,
            am.amname as index_type
        FROM pg_class t
        JOIN pg_index ix ON t.oid = ix.indrelid
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_am am ON i.relam = am.oid
        JOIN pg_attribute a ON a.attrelid = t.oid 
            AND a.attnum = ANY(ix.indkey)
        WHERE t.relkind = 'r'
        AND t.relname = %s
        ORDER BY i.relname, a.attnum;
        """
        
        try:
            self.cursor.execute(query, (table_name,))
            
            # Group by index name
            indexes = {}
            for row in self.cursor.fetchall():
                idx_name = row['index_name']
                if idx_name not in indexes:
                    indexes[idx_name] = {
                        'index_name': idx_name,
                        'columns': [],
                        'is_unique': row['is_unique'],
                        'is_primary': row['is_primary'],
                        'index_type': row['index_type']
                    }
                indexes[idx_name]['columns'].append(row['column_name'])
            
            return list(indexes.values())
        except:
            return []
    
    def get_sample_data(self, table_name: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Get sample rows from a table"""
        query = f'SELECT * FROM "{table_name}" LIMIT %s;'
        
        try:
            self.cursor.execute(query, (limit,))
            return [dict(row) for row in self.cursor.fetchall()]
        except Exception as e:
            print(f"Could not fetch sample data from {table_name}: {e}")
            return []
    
    def get_row_count(self, table_name: str) -> int:
        """Get approximate row count for a table"""
        query = """
        SELECT reltuples::bigint AS estimate
        FROM pg_class
        WHERE relname = %s;
        """
        
        try:
            self.cursor.execute(query, (table_name,))
            result = self.cursor.fetchone()
            return int(result['estimate']) if result else 0
        except:
            return 0
    
    def get_table_size(self, table_name: str) -> str:
        """Get table size in human-readable format"""
        query = """
        SELECT pg_size_pretty(pg_total_relation_size(%s)) as size;
        """
        
        try:
            self.cursor.execute(query, (table_name,))
            result = self.cursor.fetchone()
            return result['size'] if result else 'Unknown'
        except:
            return 'Unknown'
    
    def get_column_statistics(self, table_name: str) -> Dict[str, Any]:
        """Get statistics about column usage and null percentages"""
        columns = self.get_columns_info(table_name)
        stats = {}
        
        for col in columns:
            col_name = col['column_name']
            try:
                # Get null count and distinct values
                query = f'''
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT("{col_name}") as non_null_count,
                    COUNT(DISTINCT "{col_name}") as distinct_count
                FROM "{table_name}";
                '''
                
                self.cursor.execute(query)
                result = self.cursor.fetchone()
                
                total = result['total_rows']
                non_null = result['non_null_count']
                null_count = total - non_null
                
                stats[col_name] = {
                    'null_count': null_count,
                    'null_percentage': round((null_count / total * 100) if total > 0 else 0, 2),
                    'distinct_count': result['distinct_count'],
                    'distinct_percentage': round((result['distinct_count'] / total * 100) if total > 0 else 0, 2)
                }
            except Exception as e:
                stats[col_name] = {'error': str(e)}
        
        return stats
    
    def get_table_relationships(self) -> Dict[str, List[Dict[str, str]]]:
        """Build a map of all table relationships in the database"""
        query = """
        SELECT
            tc.table_name as from_table,
            kcu.column_name as from_column,
            ccu.table_name AS to_table,
            ccu.column_name AS to_column
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_schema = 'public'
        ORDER BY tc.table_name;
        """
        
        self.cursor.execute(query)
        relationships = defaultdict(list)
        
        for row in self.cursor.fetchall():
            relationships[row['from_table']].append({
                'from_column': row['from_column'],
                'to_table': row['to_table'],
                'to_column': row['to_column']
            })
        
        return dict(relationships)
    
    def generate_relationship_diagram(self) -> str:
        """Generate a text-based relationship diagram"""
        relationships = self.get_table_relationships()
        
        diagram = ["\n" + "="*80]
        diagram.append("DATABASE RELATIONSHIP DIAGRAM")
        diagram.append("="*80 + "\n")
        
        if not relationships:
            diagram.append("No foreign key relationships found in the database.\n")
            return "\n".join(diagram)
        
        for from_table, rels in relationships.items():
            diagram.append(f"\n📊 {from_table.upper()}")
            for rel in rels:
                diagram.append(f"   └─→ {rel['from_column']} references {rel['to_table']}.{rel['to_column']}")
        
        diagram.append("\n" + "="*80 + "\n")
        return "\n".join(diagram)
    
    def extract_all_metadata(self, include_samples: bool = True, sample_rows: int = 3, 
                           include_statistics: bool = True, use_cache: bool = True) -> Dict[str, Any]:
        """
        Extract all database metadata in a comprehensive format.
        
        Args:
            include_samples: Whether to include sample data rows
            sample_rows: Number of sample rows to include per table
            include_statistics: Whether to calculate column statistics
            use_cache: Whether to use cached metadata if available
            
        Returns:
            Dictionary containing all metadata
        """
        # Check cache first
        if use_cache and self.load_from_cache():
            print("Using cached metadata...")
            return self.metadata
        
        if not self.connect():
            return {}
        
        try:
            # Get tables and views
            tables = self.get_tables_info()
            views = self.get_views_info()
            relationships = self.get_table_relationships()
            
            metadata = {
                'database_name': self.connection_params.get('database'),
                'extracted_at': datetime.now().isoformat(),
                'total_tables': len(tables),
                'total_views': len(views),
                'tables': {},
                'views': {},
                'relationships': relationships
            }
            
            # Process tables
            for table_info in tables:
                table_name = table_info['table_name']
                print(f"Processing table: {table_name}...")
                
                columns = self.get_columns_info(table_name)
                primary_keys = self.get_primary_keys(table_name)
                foreign_keys = self.get_foreign_keys(table_name)
                indexes = self.get_indexes(table_name)
                row_count = self.get_row_count(table_name)
                table_size = self.get_table_size(table_name)
                
                table_metadata = {
                    'table_type': table_info['table_type'],
                    'comment': table_info['table_comment'],
                    'row_count': row_count,
                    'table_size': table_size,
                    'columns': columns,
                    'primary_keys': primary_keys,
                    'foreign_keys': foreign_keys,
                    'indexes': indexes
                }
                
                if include_statistics and row_count > 0:
                    print(f"  Calculating statistics for {table_name}...")
                    table_metadata['column_statistics'] = self.get_column_statistics(table_name)
                
                if include_samples and row_count > 0:
                    table_metadata['sample_data'] = self.get_sample_data(table_name, sample_rows)
                
                metadata['tables'][table_name] = table_metadata
            
            # Process views
            for view_info in views:
                view_name = view_info['view_name']
                print(f"Processing view: {view_name}...")
                
                columns = self.get_columns_info(view_name)
                
                view_metadata = {
                    'view_type': view_info['table_type'],
                    'comment': view_info['view_comment'],
                    'definition': view_info['view_definition'],
                    'columns': columns
                }
                
                if include_samples:
                    view_metadata['sample_data'] = self.get_sample_data(view_name, sample_rows)
                
                metadata['views'][view_name] = view_metadata
            
            self.metadata = metadata
            
            # Save to cache
            if use_cache:
                self.save_to_cache()
            
            return metadata
            
        finally:
            self.close()
    
    def format_for_ai(self) -> str:
        """
        Format metadata as a detailed text description optimized for AI consumption.
        This creates a natural language description of the database schema.
        """
        if not self.metadata:
            return "No metadata available. Run extract_all_metadata() first."
        
        output = []
        output.append(f"DATABASE: {self.metadata['database_name']}")
        output.append(f"Extracted: {self.metadata['extracted_at']}")
        output.append(f"Total Tables: {self.metadata['total_tables']}")
        output.append(f"Total Views: {self.metadata['total_views']}\n")
        
        # Add relationship diagram
        output.append(self.generate_relationship_diagram())
        
        output.append("="*80)
        output.append("DETAILED SCHEMA INFORMATION")
        output.append("="*80)
        
        # Tables
        for table_name, table_data in self.metadata['tables'].items():
            output.append(f"\n{'='*80}")
            output.append(f"TABLE: {table_name}")
            output.append(f"{'='*80}")
            output.append(f"Type: {table_data['table_type']}")
            output.append(f"Row Count: ~{table_data['row_count']:,}")
            output.append(f"Size: {table_data['table_size']}")
            
            if table_data.get('comment'):
                output.append(f"Description: {table_data['comment']}")
            
            # Primary Keys
            if table_data['primary_keys']:
                output.append(f"\nPrimary Key(s): {', '.join(table_data['primary_keys'])}")
            
            # Columns
            output.append(f"\nCOLUMNS ({len(table_data['columns'])}):")
            for col in table_data['columns']:
                col_desc = f"  • {col['column_name']}: {col['data_type']}"
                
                if col['character_maximum_length']:
                    col_desc += f"({col['character_maximum_length']})"
                elif col['numeric_precision']:
                    col_desc += f"({col['numeric_precision']}"
                    if col['numeric_scale']:
                        col_desc += f",{col['numeric_scale']}"
                    col_desc += ")"
                
                nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
                col_desc += f" {nullable}"
                
                if col['column_default']:
                    col_desc += f" DEFAULT {col['column_default']}"
                
                # Add statistics if available
                if 'column_statistics' in table_data:
                    stats = table_data['column_statistics'].get(col['column_name'], {})
                    if 'null_percentage' in stats:
                        col_desc += f" [Nulls: {stats['null_percentage']}%, Distinct: {stats['distinct_count']}]"
                
                if col.get('column_comment'):
                    col_desc += f"\n    Comment: {col['column_comment']}"
                
                output.append(col_desc)
            
            # Foreign Keys
            if table_data['foreign_keys']:
                output.append("\nFOREIGN KEYS:")
                for fk in table_data['foreign_keys']:
                    fk_desc = f"  • {fk['column_name']} → {fk['foreign_table_name']}.{fk['foreign_column_name']}"
                    fk_desc += f"\n    ON UPDATE: {fk['update_rule']}, ON DELETE: {fk['delete_rule']}"
                    output.append(fk_desc)
            
            # Indexes
            if table_data['indexes']:
                output.append("\nINDEXES:")
                for idx in table_data['indexes']:
                    idx_type = "UNIQUE" if idx['is_unique'] else "INDEX"
                    if idx['is_primary']:
                        idx_type = "PRIMARY KEY"
                    output.append(f"  • {idx['index_name']} ({idx_type}, {idx.get('index_type', 'btree')}) on [{', '.join(idx['columns'])}]")
            
            # Sample Data
            if table_data.get('sample_data') and len(table_data['sample_data']) > 0:
                output.append(f"\nSAMPLE DATA (first {len(table_data['sample_data'])} rows):")
                for i, row in enumerate(table_data['sample_data'], 1):
                    output.append(f"  Row {i}: {json.dumps(row, default=str)}")
        
        # Views
        if self.metadata['views']:
            output.append(f"\n\n{'='*80}")
            output.append("VIEWS AND MATERIALIZED VIEWS")
            output.append(f"{'='*80}")
            
            for view_name, view_data in self.metadata['views'].items():
                output.append(f"\n{'-'*80}")
                output.append(f"VIEW: {view_name}")
                output.append(f"{'-'*80}")
                output.append(f"Type: {view_data['view_type']}")
                
                if view_data.get('comment'):
                    output.append(f"Description: {view_data['comment']}")
                
                output.append(f"\nDefinition:\n{view_data['definition']}")
                
                output.append(f"\nCOLUMNS ({len(view_data['columns'])}):")
                for col in view_data['columns']:
                    output.append(f"  • {col['column_name']}: {col['data_type']}")
                
                if view_data.get('sample_data'):
                    output.append(f"\nSAMPLE DATA:")
                    for i, row in enumerate(view_data['sample_data'], 1):
                        output.append(f"  Row {i}: {json.dumps(row, default=str)}")
        
        return "\n".join(output)
    
    def save_to_json(self, filename: str = 'database_metadata.json'):
        """Save metadata to JSON file"""
        with open(filename, 'w') as f:
            json.dump(self.metadata, f, indent=2, default=str)
        print(f"Metadata saved to {filename}")
    
    def save_to_text(self, filename: str = 'database_schema.txt'):
        """Save AI-formatted metadata to text file"""
        with open(filename, 'w') as f:
            f.write(self.format_for_ai())
        print(f"Schema description saved to {filename}")
    
    def save_to_cache(self):
        """Save metadata to cache file"""
        cache_data = {
            'metadata': self.metadata,
            'cached_at': datetime.now()
        }
        try:
            with open(self.cache_file, 'wb') as f:
                pickle.dump(cache_data, f)
            print(f"Metadata cached to {self.cache_file}")
        except Exception as e:
            print(f"Failed to save cache: {e}")
    
    def load_from_cache(self) -> bool:
        """Load metadata from cache if valid"""
        if not os.path.exists(self.cache_file):
            return False
        
        try:
            with open(self.cache_file, 'rb') as f:
                cache_data = pickle.load(f)
            
            cached_at = cache_data['cached_at']
            if datetime.now() - cached_at > self.cache_ttl:
                print("Cache expired")
                return False
            
            self.metadata = cache_data['metadata']
            print(f"Loaded metadata from cache (cached at {cached_at})")
            return True
        except Exception as e:
            print(f"Failed to load cache: {e}")
            return False
    
    def clear_cache(self):
        """Clear the metadata cache"""
        if os.path.exists(self.cache_file):
            os.remove(self.cache_file)
            print("Cache cleared")


# Example usage
if __name__ == "__main__":
    # Database connection parameters
    connection_params = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'database': os.getenv('DB_NAME', 'your_database'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', ''),
        'port': os.getenv('DB_PORT', '5432')
    }
    
    # Create extractor
    extractor = DatabaseMetadataExtractor(connection_params)
    
    # Extract all metadata (uses cache if available)
    print("Extracting database metadata...")
    metadata = extractor.extract_all_metadata(
        include_samples=True,
        sample_rows=3,
        include_statistics=True,
        use_cache=True
    )
    
    # Save as JSON (for programmatic use)
    extractor.save_to_json('database_metadata.json')
    
    # Save as formatted text (for AI/LLM context)
    extractor.save_to_text('database_schema.txt')
    
    # Print formatted version
    print("\n" + "="*80)
    print("DATABASE SCHEMA FOR AI:")
    print("="*80)
    print(extractor.format_for_ai())
    
    # Example: Get just the schema as a string for AI prompt
    schema_for_ai = extractor.format_for_ai()
    print(f"\nSchema text length: {len(schema_for_ai)} characters")
    print("This can be included in your AI prompts for natural language queries!")