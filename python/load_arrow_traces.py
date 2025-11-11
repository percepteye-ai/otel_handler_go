#!/usr/bin/env python3
"""
Utility script to load and work with Arrow trace files for LLM training.

This script demonstrates how to:
1. Load Arrow trace batch files with FULL OTLP format
2. Extract OTLP spans (complete nested structure)
3. Convert to HuggingFace datasets
4. Stream data for PyTorch training
5. Process traces efficiently
"""

import glob
import json
from typing import List, Dict, Optional, Iterator
import pyarrow as pa
import pyarrow.feather as feather
from pathlib import Path
from collections import defaultdict


def load_arrow_batch(file_path: str) -> pa.Table:
    """Load a single Arrow batch file.

    Args:
        file_path: Path to .arrow file

    Returns:
        PyArrow Table with trace data
    """
    return feather.read_table(file_path)


def load_all_arrow_batches(pattern: str) -> pa.Table:
    """Load and concatenate all Arrow batch files matching pattern.

    Args:
        pattern: Glob pattern for batch files (e.g., "output*.batch_*.arrow")

    Returns:
        Concatenated PyArrow Table with all trace data
    """
    batch_files = sorted(glob.glob(pattern))

    if not batch_files:
        raise FileNotFoundError(f"No files found matching pattern: {pattern}")

    print(f"Found {len(batch_files)} batch files")

    tables = []
    for i, file_path in enumerate(batch_files, 1):
        print(f"Loading batch {i}/{len(batch_files)}: {file_path}")
        table = feather.read_table(file_path)
        tables.append(table)

    print(f"Concatenating {len(tables)} tables...")
    combined_table = pa.concat_tables(tables)

    print(f"✓ Loaded {len(combined_table)} total spans")
    return combined_table


def load_otlp_spans_from_arrow(arrow_file: str) -> List[Dict]:
    """Load OTLP spans from Arrow file (preserves full OTLP structure).

    Args:
        arrow_file: Path to Arrow file

    Returns:
        List of OTLP span dictionaries with complete structure

    Example:
        >>> spans = load_otlp_spans_from_arrow('traces.batch_0000.arrow')
        >>> print(spans[0]['traceId'])
        >>> print(spans[0]['attributes'])  # Full nested attributes
        >>> print(spans[0]['events'])      # Full nested events
    """
    table = feather.read_table(arrow_file)
    df = table.to_pandas()

    otlp_spans = []
    for _, row in df.iterrows():
        # Parse the full OTLP span from JSON
        otlp_span = json.loads(row['otlp_span'])
        otlp_spans.append(otlp_span)

    return otlp_spans


def load_otlp_traces_from_arrow(arrow_file: str) -> Dict[str, List[Dict]]:
    """Load OTLP spans grouped by trace_id from Arrow file.

    Args:
        arrow_file: Path to Arrow file

    Returns:
        Dictionary mapping trace_id -> list of OTLP spans

    Example:
        >>> traces = load_otlp_traces_from_arrow('traces.batch_0000.arrow')
        >>> for trace_id, spans in traces.items():
        ...     print(f"Trace {trace_id}: {len(spans)} spans")
    """
    table = feather.read_table(arrow_file)
    df = table.to_pandas()

    traces = defaultdict(list)
    for _, row in df.iterrows():
        # Parse the full OTLP span from JSON
        otlp_span = json.loads(row['otlp_span'])
        trace_id = row['trace_id']
        traces[trace_id].append(otlp_span)

    return dict(traces)


def arrow_to_otlp_json(arrow_files: List[str], output_file: str):
    """Convert Arrow files back to full OTLP JSON format.

    Creates a complete OTLP JSON file with ResourceSpans structure.

    Args:
        arrow_files: List of Arrow file paths
        output_file: Output JSON file path

    Example:
        >>> arrow_to_otlp_json(['traces.batch_0000.arrow'], 'traces_otlp.json')
    """
    all_traces = defaultdict(list)

    # Load all spans from all Arrow files
    for arrow_file in arrow_files:
        print(f"Loading {arrow_file}...")
        traces = load_otlp_traces_from_arrow(arrow_file)

        for trace_id, spans in traces.items():
            all_traces[trace_id].extend(spans)

    # Build OTLP ResourceSpans structure
    resource_spans_list = []

    for trace_id, spans in all_traces.items():
        # Group by service (resource)
        service_groups = defaultdict(list)
        for span in spans:
            # Get service name from index or use default
            service_name = 'unknown'  # This was stored separately in Arrow
            service_groups[service_name].append(span)

        for service_name, service_spans in service_groups.items():
            resource_span = {
                "resource": {
                    "attributes": [
                        {
                            "key": "service.name",
                            "value": {"stringValue": service_name}
                        }
                    ]
                },
                "scopeSpans": [
                    {
                        "spans": service_spans
                    }
                ]
            }
            resource_spans_list.append(resource_span)

    # Write OTLP JSON
    otlp_data = {"resourceSpans": resource_spans_list}

    with open(output_file, 'w') as f:
        json.dump(otlp_data, f, indent=2)

    print(f"✓ Written {len(resource_spans_list)} resource spans to {output_file}")
    print(f"  Total traces: {len(all_traces)}")
    print(f"  Total spans: {sum(len(spans) for spans in all_traces.values())}")


def stream_otlp_spans(pattern: str, batch_size: int = 32) -> Iterator[List[Dict]]:
    """Stream OTLP spans from Arrow files (memory efficient).

    Args:
        pattern: Glob pattern for batch files
        batch_size: Number of spans per batch

    Yields:
        Batches of complete OTLP spans

    Example:
        >>> for batch in stream_otlp_spans('traces*.arrow', batch_size=100):
        ...     for span in batch:
        ...         print(span['traceId'], span['name'])
        ...         print(span['attributes'])  # Full nested structure
    """
    batch_files = sorted(glob.glob(pattern))

    for file_path in batch_files:
        print(f"Streaming OTLP spans from: {file_path}")
        table = feather.read_table(file_path)
        df = table.to_pandas()

        current_batch = []
        for _, row in df.iterrows():
            # Parse full OTLP span from JSON
            otlp_span = json.loads(row['otlp_span'])
            current_batch.append(otlp_span)

            if len(current_batch) >= batch_size:
                yield current_batch
                current_batch = []

        # Yield remaining spans
        if current_batch:
            yield current_batch


def arrow_to_huggingface_dataset(arrow_files: List[str]):
    """Load Arrow files directly into HuggingFace Dataset (recommended for training).

    Args:
        arrow_files: List of Arrow file paths

    Returns:
        HuggingFace Dataset ready for training

    Example:
        >>> dataset = arrow_to_huggingface_dataset(['traces.batch_0000.arrow'])
        >>> dataloader = dataset.with_format("torch")
        >>> for batch in dataloader:
        ...     # Train your model
    """
    try:
        from datasets import Dataset
    except ImportError:
        print("ERROR: HuggingFace datasets not installed")
        print("Install with: pip install datasets")
        return None

    # Load all Arrow files
    if len(arrow_files) == 1:
        dataset = Dataset.from_file(arrow_files[0])
    else:
        # Load first file
        dataset = Dataset.from_file(arrow_files[0])
        # Concatenate remaining files
        for file_path in arrow_files[1:]:
            dataset = dataset.concatenate(Dataset.from_file(file_path))

    print(f"✓ Created HuggingFace Dataset with {len(dataset)} spans")
    print(f"  Columns: {dataset.column_names}")
    return dataset


def stream_spans_for_training(pattern: str, batch_size: int = 32):
    """Stream spans from Arrow files for training (memory efficient).

    Args:
        pattern: Glob pattern for batch files
        batch_size: Number of spans per batch

    Yields:
        Batches of spans as dictionaries

    Example:
        >>> for batch in stream_spans_for_training('traces*.arrow', batch_size=32):
        ...     # Process batch for training
        ...     traces = batch['trace_id']
        ...     spans = batch['span_id']
        ...     names = batch['name']
    """
    batch_files = sorted(glob.glob(pattern))

    for file_path in batch_files:
        print(f"Streaming from: {file_path}")
        table = feather.read_table(file_path)

        # Convert to batches
        num_rows = len(table)
        for i in range(0, num_rows, batch_size):
            batch = table.slice(i, min(batch_size, num_rows - i))
            yield batch.to_pydict()


def get_trace_statistics(arrow_file: str) -> Dict:
    """Get statistics about traces in an Arrow file.

    Args:
        arrow_file: Path to Arrow file

    Returns:
        Dictionary with statistics
    """
    table = feather.read_table(arrow_file)
    df = table.to_pandas()

    stats = {
        'total_spans': len(df),
        'unique_traces': df['trace_id'].nunique(),
        'unique_services': df['service_name'].nunique(),
        'span_kinds': df['kind'].value_counts().to_dict(),
        'avg_duration_ms': (df['duration_ns'] / 1_000_000).mean(),
        'total_duration_seconds': (df['duration_ns'].sum() / 1_000_000_000),
    }

    return stats


def extract_span_names(arrow_file: str, limit: int = 100) -> List[str]:
    """Extract unique span names for LLM training.

    Args:
        arrow_file: Path to Arrow file
        limit: Maximum number of unique names to return

    Returns:
        List of unique span names
    """
    table = feather.read_table(arrow_file)
    df = table.to_pandas()

    unique_names = df['name'].unique()[:limit]
    return unique_names.tolist()


def parse_attributes_json(attributes_json: str) -> List[Dict]:
    """Parse attributes from JSON string column.

    Args:
        attributes_json: JSON string from attributes_json column

    Returns:
        List of attribute dictionaries
    """
    return json.loads(attributes_json)


def create_training_examples(arrow_file: str, output_jsonl: str):
    """Convert Arrow traces to JSONL format for LLM fine-tuning.

    Creates training examples in format:
    {
      "input": "Trace analysis for service X",
      "output": "Summary of trace behavior..."
    }

    Args:
        arrow_file: Path to Arrow file
        output_jsonl: Path to output JSONL file
    """
    table = feather.read_table(arrow_file)
    df = table.to_pandas()

    # Group by trace_id
    traces = df.groupby('trace_id')

    with open(output_jsonl, 'w') as f:
        for trace_id, group in traces:
            # Create training example from trace
            service_names = group['service_name'].unique()
            span_names = group['name'].tolist()
            total_duration_ms = group['duration_ns'].sum() / 1_000_000

            # Example format - customize for your LLM training needs
            example = {
                "trace_id": trace_id,
                "service_names": service_names.tolist(),
                "span_count": len(group),
                "span_names": span_names,
                "total_duration_ms": total_duration_ms,
                "root_span": group[group['parent_span_id'] == ''].iloc[0]['name'] if any(group['parent_span_id'] == '') else None,
            }

            f.write(json.dumps(example) + '\n')

    print(f"✓ Created {len(traces)} training examples in {output_jsonl}")


# Example usage functions
def example_pytorch_training():
    """Example: Using Arrow traces with PyTorch DataLoader"""
    try:
        from datasets import Dataset
        from torch.utils.data import DataLoader
    except ImportError:
        print("Install: pip install datasets torch")
        return

    # Load Arrow files into HuggingFace Dataset
    arrow_files = sorted(glob.glob("traces*.batch_*.arrow"))
    if not arrow_files:
        print("No Arrow files found!")
        return

    print(f"Loading {len(arrow_files)} Arrow files...")
    dataset = arrow_to_huggingface_dataset(arrow_files)

    # Convert to PyTorch format
    dataset = dataset.with_format("torch")

    # Create DataLoader
    dataloader = DataLoader(dataset, batch_size=32, shuffle=True)

    # Training loop example
    print("\nExample training loop:")
    for batch_idx, batch in enumerate(dataloader):
        # Your model training code here
        print(f"Batch {batch_idx}: {len(batch['trace_id'])} spans")

        # Access fields
        trace_ids = batch['trace_id']
        span_names = batch['name']
        durations = batch['duration_ns']

        if batch_idx >= 2:  # Just show first few batches
            break

    print("\n✓ PyTorch training example complete!")


def example_streaming():
    """Example: Stream spans without loading all into memory"""
    print("Streaming example:")

    total_processed = 0
    for batch in stream_spans_for_training("traces*.batch_*.arrow", batch_size=100):
        # Process batch
        batch_size = len(batch['trace_id'])
        total_processed += batch_size

        print(f"Processed batch: {batch_size} spans (total: {total_processed})")

        if total_processed >= 1000:  # Just show first 1000
            break

    print(f"\n✓ Streaming example complete! Processed {total_processed} spans")


def main():
    """Main function with usage examples"""
    import sys

    if len(sys.argv) < 2:
        print("Usage: python load_arrow_traces.py <command> [args]")
        print("\nCommands for OTLP format (NEW - preserves full structure):")
        print("  load-otlp <file.arrow>          - Load and display OTLP spans")
        print("  to-otlp-json <pattern> <out.json> - Convert Arrow to OTLP JSON")
        print("  show-otlp <file.arrow>          - Show first OTLP span (full structure)")
        print("\nLegacy commands:")
        print("  stats <file.arrow>              - Show statistics about traces")
        print("  load <pattern>                  - Load and display info about Arrow files")
        print("  to-jsonl <file.arrow> <out.jsonl> - Convert to JSONL for training")
        print("  example-pytorch                 - Run PyTorch training example")
        print("  example-streaming               - Run streaming example")
        print("\nExamples:")
        print("  python load_arrow_traces.py show-otlp traces.batch_0000.arrow")
        print("  python load_arrow_traces.py load-otlp traces.batch_0000.arrow")
        print("  python load_arrow_traces.py to-otlp-json 'traces*.arrow' output.json")
        print("  python load_arrow_traces.py stats traces.batch_0000.arrow")
        return

    command = sys.argv[1]

    if command == "show-otlp":
        if len(sys.argv) < 3:
            print("Usage: python load_arrow_traces.py show-otlp <file.arrow>")
            return

        file_path = sys.argv[2]
        print(f"Loading OTLP spans from {file_path}...")
        spans = load_otlp_spans_from_arrow(file_path)

        if not spans:
            print("No spans found!")
            return

        print(f"\nLoaded {len(spans)} OTLP spans")
        print("\nFirst OTLP span (full structure):")
        print("=" * 80)
        print(json.dumps(spans[0], indent=2))
        print("=" * 80)

        print("\nOTLP Span Structure:")
        print(f"  - traceId: {spans[0].get('traceId')}")
        print(f"  - spanId: {spans[0].get('spanId')}")
        print(f"  - name: {spans[0].get('name')}")
        print(f"  - kind: {spans[0].get('kind')}")
        print(f"  - attributes: {len(spans[0].get('attributes', []))} items")
        print(f"  - events: {len(spans[0].get('events', []))} items")

    elif command == "load-otlp":
        if len(sys.argv) < 3:
            print("Usage: python load_arrow_traces.py load-otlp <file.arrow>")
            return

        file_path = sys.argv[2]
        print(f"Loading OTLP traces from {file_path}...")
        traces = load_otlp_traces_from_arrow(file_path)

        print(f"\nLoaded {len(traces)} traces")
        print("\nTrace Summary:")
        print("=" * 80)
        for i, (trace_id, spans) in enumerate(list(traces.items())[:5]):
            print(f"\nTrace {i+1}: {trace_id}")
            print(f"  Spans: {len(spans)}")
            print(f"  Span names: {[s['name'] for s in spans[:3]]}")
            if len(spans) > 3:
                print(f"  ... and {len(spans) - 3} more")

        if len(traces) > 5:
            print(f"\n... and {len(traces) - 5} more traces")
        print("=" * 80)

    elif command == "to-otlp-json":
        if len(sys.argv) < 4:
            print("Usage: python load_arrow_traces.py to-otlp-json <pattern> <output.json>")
            print("Example: python load_arrow_traces.py to-otlp-json 'traces*.arrow' output.json")
            return

        pattern = sys.argv[2]
        output_file = sys.argv[3]

        arrow_files = sorted(glob.glob(pattern))
        if not arrow_files:
            print(f"No files found matching pattern: {pattern}")
            return

        print(f"Converting {len(arrow_files)} Arrow files to OTLP JSON...")
        arrow_to_otlp_json(arrow_files, output_file)

    elif command == "stats":
        if len(sys.argv) < 3:
            print("Usage: python load_arrow_traces.py stats <file.arrow>")
            return

        file_path = sys.argv[2]
        print(f"Analyzing {file_path}...")
        stats = get_trace_statistics(file_path)

        print("\nTrace Statistics:")
        print("=" * 60)
        for key, value in stats.items():
            print(f"  {key}: {value}")
        print("=" * 60)

    elif command == "load":
        if len(sys.argv) < 3:
            print("Usage: python load_arrow_traces.py load <pattern>")
            return

        pattern = sys.argv[2]
        table = load_all_arrow_batches(pattern)

        print(f"\nTable schema:")
        print(table.schema)

        print(f"\nFirst 5 rows:")
        print(table.slice(0, 5).to_pandas())

    elif command == "to-jsonl":
        if len(sys.argv) < 4:
            print("Usage: python load_arrow_traces.py to-jsonl <file.arrow> <output.jsonl>")
            return

        input_file = sys.argv[2]
        output_file = sys.argv[3]
        create_training_examples(input_file, output_file)

    elif command == "example-pytorch":
        example_pytorch_training()

    elif command == "example-streaming":
        example_streaming()

    else:
        print(f"Unknown command: {command}")
        print("Run without arguments to see usage")


if __name__ == "__main__":
    main()
