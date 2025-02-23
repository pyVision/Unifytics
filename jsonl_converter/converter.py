"""
JSONL to Parquet Converter

This module provides functionality to convert JSONL (JSON Lines) files to Apache Parquet format
with support for nested JSON structures, chunked processing, and detailed performance metrics.

Features:
- Chunked processing for memory efficiency
- Nested JSON structure handling
- Performance metrics tracking
- Compression options
- Error handling and logging

Usage Examples:
    # Basic conversion with default settings
    python converter.py input.jsonl output.parquet

    # Specify compression algorithm and chunk size
    python converter.py input.jsonl output.parquet --compression gzip --chunk-size 50000

    # Use no compression
    python converter.py input.jsonl output.parquet --compression none

Compression options:
    - snappy (default, fast compression)
    - gzip (better compression ratio)
    - brotli (best compression ratio)
    - none (no compression)
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import logging
import time
import os
from typing import Optional
from dataclasses import dataclass
from datetime import datetime

@dataclass
class ConversionMetrics:
    """Stores metrics about the conversion process."""
    total_duration: float = 0
    chunk_times: list = None
    compression_time: float = 0
    input_size: int = 0
    output_size: int = 0
    
    def __post_init__(self):
        self.chunk_times = []

def convert_jsonl_to_parquet(
    input_path: str,
    output_path: str,
    compression: str = 'snappy',
    chunk_size: Optional[int] = 100000
) -> tuple[bool, ConversionMetrics]:
    """
    Convert JSONL file to Parquet format with performance metrics tracking.
    
    Args:
        input_path (str): Path to the input JSONL file
        output_path (str): Path where the output Parquet file will be saved
        compression (str, optional): Compression algorithm to use. Defaults to 'snappy'.
        chunk_size (int, optional): Number of rows to process at once. Defaults to 100000.
    
    Returns:
        tuple[bool, ConversionMetrics]: Success flag and metrics object
    """
    metrics = ConversionMetrics()
    start_time = time.time()
    
    try:
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)
        
        metrics.input_size = os.path.getsize(input_path)
        reader = pd.read_json(input_path, lines=True, chunksize=chunk_size)
        
        first_chunk = True
        for i, chunk in enumerate(reader):
            chunk_start = time.time()
            compression_start = time.time()
            
            # Handle nested JSON structures
            for column in chunk.select_dtypes(include=['object']):
                if isinstance(chunk[column].iloc[0], dict):
                    nested_df = pd.json_normalize(chunk[column].tolist())
                    nested_df.columns = f"{column}." + nested_df.columns
                    chunk = chunk.drop(columns=[column]).join(nested_df)
                elif isinstance(chunk[column].iloc[0], list):
                    chunk[column] = chunk[column].apply(lambda x: str(x) if x else None)
            
            table = pa.Table.from_pandas(chunk)
            
            if first_chunk:
                pq.write_table(table, output_path, compression=compression)
                first_chunk = False
            else:
                pq.write_table(table, output_path, compression=compression,
                             append=True)
            
            compression_time = time.time() - compression_start
            metrics.compression_time += compression_time
            
            chunk_duration = time.time() - chunk_start
            metrics.chunk_times.append(chunk_duration)
            
            logger.info(f"Chunk {i+1}: Processing time={chunk_duration:.2f}s, "
                       f"Compression time={compression_time:.2f}s")
        
        metrics.output_size = os.path.getsize(output_path)
        metrics.total_duration = time.time() - start_time
        
        logger.info(f"Conversion completed in {metrics.total_duration:.2f}s")
        logger.info(f"Average chunk processing time: "
                   f"{sum(metrics.chunk_times)/len(metrics.chunk_times):.2f}s")
        logger.info(f"Total compression time: {metrics.compression_time:.2f}s")
        logger.info(f"Compression ratio: "
                   f"{metrics.output_size/metrics.input_size:.2%}")
        
        return True, metrics
        
    except Exception as e:
        logger.error(f"Error during conversion: {str(e)}")
        return False, metrics

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Convert JSONL files to Parquet format',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='Example:\n  python converter.py --input input.jsonl --output output.parquet --compression gzip'
    )
    parser.add_argument('--input', required=True, help='Input JSONL file path')
    parser.add_argument('--output', required=True, help='Output Parquet file path')
    parser.add_argument('--compression', default='snappy',
                      choices=['snappy', 'gzip', 'brotli', 'none'])
    parser.add_argument('--chunk-size', type=int, default=100000,
                      help='Number of rows to process at once (default: 100000)')
    
    args = parser.parse_args()
    
    success, metrics = convert_jsonl_to_parquet(
        args.input,
        args.output,
        compression=args.compression,
        chunk_size=args.chunk_size
    )
    
    if success:
        print("\nConversion Metrics Summary:")
        print("-" * 50)
        print(f"Total Duration: {metrics.total_duration:.2f} seconds")
        print(f"Average Chunk Processing Time: {sum(metrics.chunk_times)/len(metrics.chunk_times):.2f} seconds")
        print(f"Total Compression Time: {metrics.compression_time:.2f} seconds")
        print(f"Input File Size: {metrics.input_size / (1024*1024):.2f} MB")
        print(f"Output File Size: {metrics.output_size / (1024*1024):.2f} MB")
        print(f"Compression Ratio: {metrics.output_size/metrics.input_size:.2%}")
        print(f"Number of Chunks Processed: {len(metrics.chunk_times)}")
    else:
        print("error in conversion")

    