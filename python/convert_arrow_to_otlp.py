#!/usr/bin/env python3
"""
Simple script to convert Arrow files back to OTLP format.

Usage:
    python convert_arrow_to_otlp.py <arrow_file_path>
    
Output:
    Creates OTLP JSON file in otlp_outputs/ folder
"""

import sys
import os
from pathlib import Path
import json
from collections import defaultdict

# Add current directory to path to import load_arrow_traces
sys.path.insert(0, str(Path(__file__).parent))

from load_arrow_traces import load_otlp_traces_from_arrow


def convert_arrow_to_otlp(arrow_file_path: str, output_dir: str = "otlp_outputs"):
    """
    Convert Arrow file to OTLP JSON format.
    
    Args:
        arrow_file_path: Path to input Arrow file
        output_dir: Output directory (default: otlp_outputs)
    """
    # Validate input file
    if not os.path.exists(arrow_file_path):
        print(f"ERROR: Arrow file not found: {arrow_file_path}")
        return False
    
    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    # Generate output filename
    arrow_filename = Path(arrow_file_path).stem
    output_file = output_path / f"{arrow_filename}.otlp.json"
    
    print(f"Converting: {arrow_file_path}")
    print(f"Output: {output_file}")
    
    try:
        # Load OTLP traces from Arrow file
        traces = load_otlp_traces_from_arrow(arrow_file_path)
        
        if not traces:
            print("ERROR: No traces found in Arrow file")
            return False
        
        # Build OTLP ResourceSpans structure
        resource_spans_list = []
        
        # Group spans by service name (from the Arrow file)
        service_groups = defaultdict(lambda: defaultdict(list))
        
        # Load the arrow file to get service names
        import pyarrow.feather as feather
        table = feather.read_table(arrow_file_path)
        df = table.to_pandas()
        
        # Map trace_id to service_name
        trace_to_service = {}
        for _, row in df.iterrows():
            trace_id = row['trace_id']
            service_name = row.get('service_name', 'unknown')
            trace_to_service[trace_id] = service_name
        
        # Group spans by service
        for trace_id, spans in traces.items():
            service_name = trace_to_service.get(trace_id, 'unknown')
            service_groups[service_name][trace_id].extend(spans)
        
        # Build ResourceSpans for each service
        for service_name, trace_dict in service_groups.items():
            all_spans = []
            for trace_id, spans in trace_dict.items():
                all_spans.extend(spans)
            
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
                        "spans": all_spans
                    }
                ]
            }
            resource_spans_list.append(resource_span)
        
        # Write OTLP JSON
        otlp_data = {"resourceSpans": resource_spans_list}
        
        with open(output_file, 'w') as f:
            json.dump(otlp_data, f, indent=2)
        
        total_spans = sum(len(spans) for spans in traces.values())
        print(f"✓ Successfully converted!")
        print(f"  - Output file: {output_file}")
        print(f"  - Total traces: {len(traces)}")
        print(f"  - Total spans: {total_spans}")
        print(f"  - Resource spans: {len(resource_spans_list)}")
        
        return True
        
    except Exception as e:
        print(f"ERROR: Conversion failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main entry point"""
    if len(sys.argv) < 2:
        print("Usage: python convert_arrow_to_otlp.py <arrow_file_path> [output_dir]")
        print("\nExample:")
        print("  python convert_arrow_to_otlp.py ../outputs/traces_otlp.batch_0000.arrow")
        print("  python convert_arrow_to_otlp.py ../outputs/traces_otlp.batch_0000.arrow otlp_outputs")
        sys.exit(1)
    
    arrow_file = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "otlp_outputs"
    
    # Handle relative paths - if arrow file is relative, make it relative to repo root
    if not os.path.isabs(arrow_file):
        repo_root = Path(__file__).parent.parent
        arrow_file = str(repo_root / arrow_file)
    
    success = convert_arrow_to_otlp(arrow_file, output_dir)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

