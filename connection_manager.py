import json
import os
from typing import Dict, List, Optional
from datetime import datetime
from cryptography.fernet import Fernet
import base64
from pathlib import Path

class ConnectionManager:
    """
    Manages multiple database connections with encrypted storage.
    Allows users to save, retrieve, and switch between database connections.
    """
    
    def __init__(self, storage_file: str = 'connections.enc'):
        """
        Initialize connection manager.
        
        Args:
            storage_file: Path to encrypted connections storage file
        """
        self.storage_file = storage_file
        self.encryption_key = self._get_or_create_key()
        self.cipher = Fernet(self.encryption_key)
        self.connections = self._load_connections()
    
    def _get_or_create_key(self) -> bytes:
        """Get or create encryption key for connection credentials"""
        key_file = '.connection_key'
        
        if os.path.exists(key_file):
            with open(key_file, 'rb') as f:
                return f.read()
        else:
            # Generate new key
            key = Fernet.generate_key()
            with open(key_file, 'wb') as f:
                f.write(key)
            return key
    
    def _load_connections(self) -> Dict[str, Dict]:
        """Load connections from encrypted storage"""
        if not os.path.exists(self.storage_file):
            return {}
        
        try:
            with open(self.storage_file, 'rb') as f:
                encrypted_data = f.read()
            
            decrypted_data = self.cipher.decrypt(encrypted_data)
            return json.loads(decrypted_data.decode('utf-8'))
        except Exception as e:
            print(f"Error loading connections: {e}")
            return {}
    
    def _save_connections(self):
        """Save connections to encrypted storage"""
        try:
            json_data = json.dumps(self.connections, indent=2)
            encrypted_data = self.cipher.encrypt(json_data.encode('utf-8'))
            
            with open(self.storage_file, 'wb') as f:
                f.write(encrypted_data)
        except Exception as e:
            print(f"Error saving connections: {e}")
            raise
    
    def add_connection(self, name: str, host: str, database: str, 
                      user: str, password: str, port: int = 5432,
                      description: str = "") -> Dict:
        """
        Add or update a database connection.
        
        Args:
            name: Unique name for this connection
            host: Database host
            database: Database name
            user: Database user
            password: Database password
            port: Database port (default: 5432)
            description: Optional description
            
        Returns:
            Dictionary with connection details (password excluded)
        """
        connection = {
            'host': host,
            'database': database,
            'user': user,
            'password': password,
            'port': port,
            'description': description,
            'created_at': datetime.now().isoformat(),
            'last_used': None
        }
        
        # Update if exists, otherwise create
        if name in self.connections:
            connection['created_at'] = self.connections[name]['created_at']
        
        self.connections[name] = connection
        self._save_connections()
        
        # Return safe version without password
        return self._safe_connection_info(name, connection)
    
    def get_connection(self, name: str) -> Optional[Dict]:
        """
        Get connection details by name.
        
        Args:
            name: Connection name
            
        Returns:
            Connection dictionary with credentials or None if not found
        """
        if name not in self.connections:
            return None
        
        # Update last used timestamp
        self.connections[name]['last_used'] = datetime.now().isoformat()
        self._save_connections()
        
        return self.connections[name].copy()
    
    def list_connections(self) -> List[Dict]:
        """
        List all saved connections (without passwords).
        
        Returns:
            List of connection info dictionaries
        """
        return [
            self._safe_connection_info(name, conn)
            for name, conn in self.connections.items()
        ]
    
    def delete_connection(self, name: str) -> bool:
        """
        Delete a connection.
        
        Args:
            name: Connection name to delete
            
        Returns:
            True if deleted, False if not found
        """
        if name in self.connections:
            del self.connections[name]
            self._save_connections()
            return True
        return False
    
    def test_connection(self, name: str) -> Dict:
        """
        Test if a connection works.
        
        Args:
            name: Connection name
            
        Returns:
            Dictionary with test results
        """
        import psycopg2
        
        conn_info = self.get_connection(name)
        if not conn_info:
            return {'success': False, 'error': 'Connection not found'}
        
        try:
            conn = psycopg2.connect(
                host=conn_info['host'],
                database=conn_info['database'],
                user=conn_info['user'],
                password=conn_info['password'],
                port=conn_info['port'],
                connect_timeout=5
            )
            
            cursor = conn.cursor()
            cursor.execute('SELECT version();')
            version = cursor.fetchone()[0]
            
            cursor.close()
            conn.close()
            
            return {
                'success': True,
                'message': 'Connection successful',
                'version': version
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def _safe_connection_info(self, name: str, connection: Dict) -> Dict:
        """Return connection info without password"""
        safe_conn = connection.copy()
        safe_conn.pop('password', None)
        safe_conn['name'] = name
        return safe_conn
    
    def get_connection_config(self, name: str) -> Optional[Dict]:
        """
        Get connection config in format ready for psycopg2.
        
        Args:
            name: Connection name
            
        Returns:
            Dictionary with host, database, user, password, port
        """
        conn = self.get_connection(name)
        if not conn:
            return None
        
        return {
            'host': conn['host'],
            'database': conn['database'],
            'user': conn['user'],
            'password': conn['password'],
            'port': conn['port']
        }
    
    def export_connection(self, name: str, include_password: bool = False) -> Optional[Dict]:
        """
        Export connection for sharing (optionally without password).
        
        Args:
            name: Connection name
            include_password: Whether to include password in export
            
        Returns:
            Connection dictionary or None
        """
        conn = self.get_connection(name)
        if not conn:
            return None
        
        export = {
            'name': name,
            'host': conn['host'],
            'database': conn['database'],
            'user': conn['user'],
            'port': conn['port'],
            'description': conn['description']
        }
        
        if include_password:
            export['password'] = conn['password']
        
        return export
    
    def import_connection(self, connection_data: Dict) -> str:
        """
        Import a connection from exported data.
        
        Args:
            connection_data: Dictionary with connection details
            
        Returns:
            Name of imported connection
        """
        name = connection_data.get('name', f"imported_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        
        self.add_connection(
            name=name,
            host=connection_data['host'],
            database=connection_data['database'],
            user=connection_data['user'],
            password=connection_data.get('password', ''),
            port=connection_data.get('port', 5432),
            description=connection_data.get('description', 'Imported connection')
        )
        
        return name