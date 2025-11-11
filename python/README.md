# Arrow to OTLP Converter (Python)

This Python module converts Arrow files back to OTLP JSON format.

## Setup

1. Create virtual environment (if not already created):
```bash
cd python
python3 -m venv venv
```

2. Activate virtual environment:
```bash
source venv/bin/activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

Convert an Arrow file to OTLP format:

```bash
# Activate venv first
source venv/bin/activate

# Convert single arrow file
python convert_arrow_to_otlp.py ../outputs/traces_otlp.batch_0000.arrow

# Output will be saved to: otlp_outputs/traces_otlp.batch_0000.otlp.json
```

Or use the wrapper script from repo root:

```bash
./python/convert.sh outputs/traces_otlp.batch_0000.arrow
```

## Output

- Input: Arrow file (e.g., `outputs/traces_otlp.batch_0000.arrow`)
- Output: OTLP JSON file in `otlp_outputs/` folder (gitignored)

The output file contains full OTLP ResourceSpans structure with all nested attributes and events preserved.

