import sqlite3
import json
from datetime import datetime


"""
Utility functions for extracting and displaying agent responses.
"""
from langchain_core.tools import tool
from IPython.display import display, Markdown

def extract_agent_response(result):
    """
    Extract the AI's response from an agent invocation result.
    
    Args:
        result: The result object returned from agent.invoke()
        
    Returns:
        str: The AI's response content, or an error message if extraction fails
    """
    # Check if result is a dictionary with messages (typical LangGraph format)
    if isinstance(result, dict) and "messages" in result:
        messages = result["messages"]
        # Get the last message (usually the AI's response)
        if messages:
            last_message = messages[-1]
            return last_message.content
        else:
            return "No messages found in result"
    else:
        # Try alternative access methods
        try:
            content = result.get() if hasattr(result, 'get') else result
            return str(content)
        except Exception as e:
            return f"Error accessing result: {e}\nResult type: {type(result)}\nResult: {result}"


def print_agent_response(result):
    """
    Print the AI's response from an agent invocation result in a user-friendly format.
    
    Args:
        result: The result object returned from agent.invoke()
    """
    
    response = extract_agent_response(result)
    display(Markdown(response))




def create_logs_database():
    # Create in-memory SQLite database
    conn = sqlite3.connect(':memory:', check_same_thread=False)
    cursor = conn.cursor()
    
    # Create logs table
    cursor.execute('''
        CREATE TABLE logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            spark_app_id TEXT,
            log_message TEXT,
            log_level TEXT,
            time TIMESTAMP
        )
    ''')
    
    # Load and insert data from multiple JSON files
    json_files = ['oom.json', 'disk.json', 'throttle.json']
    total_entries = 0
    
    for json_file in json_files:
        try:
            with open(json_file, 'r') as f:
                log_entries = json.load(f)
            
            for entry in log_entries:
                # Convert time string to datetime
                time_obj = datetime.strptime(entry['time'], '%y/%m/%d %H:%M:%S')
                
                cursor.execute('''
                    INSERT INTO logs (spark_app_id, log_message, log_level, time)
                    VALUES (?, ?, ?, ?)
                ''', (entry['application_id'], entry['message'], entry['level'], time_obj))
            
            total_entries += len(log_entries)
            print(f"Loaded {len(log_entries)} entries from {json_file}")
            
        except FileNotFoundError:
            print(f"Warning: {json_file} not found, skipping...")
    
    conn.commit()
    print(f"Total: {total_entries} log entries loaded into in-memory database")
    return conn

global conn
conn = create_logs_database()
cursor = conn.cursor()

# Example queries:
# cursor.execute("SELECT COUNT(*) FROM logs").fetchone()
# cursor.execute("SELECT * FROM logs WHERE level = 'ERROR'").fetchall()
# cursor.execute("SELECT level, COUNT(*) FROM logs GROUP BY level").fetchall()
# cursor.execute("SELECT * FROM logs ORDER BY time DESC LIMIT 5").fetchall()


@tool
def execute_query(query):
    """
    Args:
        query (str): SQL SELECT query to execute
        
    Returns:
        list: List of tuples containing query results. Each tuple represents one row.
              Empty list if no results found.
    """
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor.fetchall()
