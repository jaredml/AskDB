from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
import anthropic
import os
from typing import Optional, List, Dict, Any
from metadata_extractor import DatabaseMetadataExtractor
from connection_manager import ConnectionManager

app = FastAPI(
    title="QueryMind - Natural Language Query Engine",
    description="Ask questions about your database in plain English",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Global variables
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
connection_manager = ConnectionManager()
active_connection = None
metadata_extractor = None

# Pydantic models
class QueryRequest(BaseModel):
    question: str

class ConnectionCreate(BaseModel):
    name: str
    host: str
    database: str
    user: str
    password: str
    port: int = 5432
    description: str = ""

class ConnectionTest(BaseModel):
    host: str
    database: str
    user: str
    password: str
    port: int = 5432

class QueryResponse(BaseModel):
    sql: str
    results: List[Dict[str, Any]]
    row_count: int

# Helper functions
def get_db_connection():
    """Get database connection using active connection"""
    global active_connection
    
    if not active_connection:
        raise HTTPException(status_code=400, detail="No active database connection")
    
    conn_config = connection_manager.get_connection_config(active_connection)
    if not conn_config:
        raise HTTPException(status_code=404, detail="Active connection not found")
    
    try:
        conn = psycopg2.connect(**conn_config)
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

def get_metadata_extractor():
    """Get or create metadata extractor for active connection"""
    global metadata_extractor, active_connection
    
    if not active_connection:
        raise HTTPException(status_code=400, detail="No active database connection")
    
    conn_config = connection_manager.get_connection_config(active_connection)
    if not conn_config:
        raise HTTPException(status_code=404, detail="Connection configuration not found")
    
    # Create new extractor with current connection
    metadata_extractor = DatabaseMetadataExtractor(conn_config)
    return metadata_extractor

def convert_nl_to_sql(question: str, schema: str) -> str:
    """Use Claude to convert natural language to SQL"""
    if not ANTHROPIC_API_KEY:
        raise HTTPException(status_code=500, detail="Anthropic API key not configured")
    
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    
    prompt = f"""You are a SQL expert. Convert the following natural language question into a PostgreSQL query.

{schema}

User Question: {question}

Requirements:
1. Generate ONLY the SQL query, no explanations
2. Use proper PostgreSQL syntax
3. Make the query safe (SELECT only, no modifications)
4. Use appropriate JOINs if multiple tables are needed
5. Add LIMIT clauses where appropriate to prevent overwhelming results
6. Return only valid, executable SQL

SQL Query:"""

    try:
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}]
        )
        
        sql = message.content[0].text.strip()
        
        # Clean markdown
        if sql.startswith("```sql"):
            sql = sql[6:]
        if sql.startswith("```"):
            sql = sql[3:]
        if sql.endswith("```"):
            sql = sql[:-3]
        sql = sql.strip()
        
        # Safety check
        sql_upper = sql.upper()
        forbidden = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE']
        if any(word in sql_upper for word in forbidden):
            raise HTTPException(status_code=400, detail="Query contains forbidden operations")
        
        return sql
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate SQL: {str(e)}")

def execute_query(sql: str) -> List[Dict[str, Any]]:
    """Execute SQL query and return results"""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Query execution failed: {str(e)}")
    finally:
        cursor.close()
        conn.close()

# API Endpoints

@app.get("/")
def root():
    """Health check endpoint"""
    return {
        "status": "QueryMind is running",
        "version": "1.0.0",
        "active_connection": active_connection,
        "endpoints": {
            "ui": "/ui",
            "connections": "/connections",
            "schema": "/schema",
            "query": "/query",
            "metadata": "/metadata"
        }
    }

@app.get("/ui")
async def serve_ui():
    """Serve the web interface"""
    return FileResponse('static/index.html')

# Connection Management Endpoints

@app.get("/connections")
def list_connections():
    """Get list of all saved connections"""
    return connection_manager.list_connections()

@app.get("/connections/{name}")
def get_connection(name: str):
    """Get connection details by name (without password)"""
    conn = connection_manager.get_connection(name)
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    # Return without password
    safe_conn = conn.copy()
    safe_conn.pop('password', None)
    safe_conn['name'] = name
    return safe_conn

@app.post("/connections")
def create_connection(connection: ConnectionCreate):
    """Create or update a database connection"""
    try:
        result = connection_manager.add_connection(
            name=connection.name,
            host=connection.host,
            database=connection.database,
            user=connection.user,
            password=connection.password,
            port=connection.port,
            description=connection.description
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/connections/{name}")
def delete_connection(name: str):
    """Delete a connection"""
    global active_connection, metadata_extractor
    
    if connection_manager.delete_connection(name):
        # Clear active connection if it was deleted
        if active_connection == name:
            active_connection = None
            metadata_extractor = None
        return {"status": "success", "message": f"Connection '{name}' deleted"}
    else:
        raise HTTPException(status_code=404, detail="Connection not found")

@app.post("/connections/test")
def test_connection(connection: ConnectionTest):
    """Test a database connection"""
    try:
        conn = psycopg2.connect(
            host=connection.host,
            database=connection.database,
            user=connection.user,
            password=connection.password,
            port=connection.port,
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

@app.post("/connections/{name}/activate")
def activate_connection(name: str):
    """Set a connection as the active connection"""
    global active_connection, metadata_extractor
    
    conn = connection_manager.get_connection(name)
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    # Test the connection
    try:
        conn_config = connection_manager.get_connection_config(name)
        test_conn = psycopg2.connect(**conn_config, connect_timeout=5)
        test_conn.close()
        
        active_connection = name
        metadata_extractor = None  # Will be recreated when needed
        
        return {
            "status": "success",
            "message": f"Connection '{name}' activated",
            "active_connection": name
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Connection test failed: {str(e)}")

# Query Endpoints

@app.get("/schema")
def get_schema():
    """Get database schema for active connection"""
    try:
        extractor = get_metadata_extractor()
        metadata = extractor.extract_all_metadata(
            include_samples=False,
            include_statistics=False,
            use_cache=True
        )
        return {"schema": extractor.format_for_ai()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/metadata")
def get_full_metadata():
    """Get complete database metadata with statistics"""
    try:
        extractor = get_metadata_extractor()
        metadata = extractor.extract_all_metadata(
            include_samples=True,
            sample_rows=3,
            include_statistics=True,
            use_cache=True
        )
        return metadata
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/metadata/refresh")
def refresh_metadata():
    """Force refresh of metadata cache"""
    try:
        extractor = get_metadata_extractor()
        extractor.clear_cache()
        metadata = extractor.extract_all_metadata(
            include_samples=True,
            sample_rows=3,
            include_statistics=True,
            use_cache=False
        )
        return {"status": "success", "message": "Metadata refreshed"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/query", response_model=QueryResponse)
def natural_language_query(request: QueryRequest):
    """Convert natural language question to SQL and execute it"""
    try:
        # Get schema
        extractor = get_metadata_extractor()
        metadata = extractor.extract_all_metadata(
            include_samples=False,
            include_statistics=False,
            use_cache=True
        )
        schema = extractor.format_for_ai()
        
        # Convert to SQL
        sql = convert_nl_to_sql(request.question, schema)
        
        # Execute query
        results = execute_query(sql)
        
        return QueryResponse(
            sql=sql,
            results=results,
            row_count=len(results)
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)