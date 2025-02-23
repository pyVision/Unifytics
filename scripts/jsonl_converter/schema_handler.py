"""
Schema Handler for JSONL to Parquet Conversion

This module provides functionality for handling custom schemas and data types
when converting JSONL files to Parquet format.
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Optional, Dict, Any

def create_custom_schema(schema_definition: Dict[str, Any]) -> pa.Schema:
    """
    Create a PyArrow schema from a dictionary definition.
    
    Args:
        schema_definition (Dict[str, Any]): Dictionary mapping column names to their data types
    
    Returns:
        pa.Schema: PyArrow schema object
    """
    fields = []
    for name, dtype in schema_definition.items():
        if isinstance(dtype, str):
            pa_type = getattr(pa, dtype)()
        else:
            pa_type = dtype
        fields.append((name, pa_type))
    return pa.schema(fields)

def convert_with_custom_schema(
    input_path: str,
    output_path: str,
    schema_definition: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Convert JSONL file to Parquet using a custom schema.
    
    Args:
        input_path (str): Path to input JSONL file
        output_path (str): Path for output Parquet file
        schema_definition (Dict[str, Any], optional): Custom schema definition.
            If None, uses default schema:
            {
                'id': 'int64',
                'timestamp': pa.timestamp('ns'),
                'value': 'float64',
                'category': 'string'
            }
    
    Returns:
        bool: True if conversion successful, False otherwise
    """
    try:
        if schema_definition is None:
            schema_definition = {
                'id': 'int64',
                'timestamp': pa.timestamp('ns'),
                'value': 'float64',
                'category': 'string'
            }
        
        schema = create_custom_schema(schema_definition)
        df = pd.read_json(input_path, lines=True)
        table = pa.Table.from_pandas(df, schema=schema)
        pq.write_table(table, output_path)
        return True
        
    except Exception as e:
        print(f"Error during schema-based conversion: {str(e)}")
        return False