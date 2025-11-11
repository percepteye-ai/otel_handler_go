# Start Here - One Command to Run

## Run the Converter

```bash
cd otlp-converter-go
./run.sh
```

That's it! This will:
1. Check if Go is installed
2. Install dependencies
3. Build the binary
4. Run the conversion
5. Create output files: `traces_otlp.batch_*.arrow`

## What it does

Converts `badger_export.json` → Arrow files with full OTLP format

**Speed:** 50-100x faster than Python

## After Conversion

Read the output with Python:

```bash
cd ../converter_fast
python load_arrow_traces.py show-otlp ../otlp-converter-go/traces_otlp.batch_0000.arrow
```

## Options

To customize, run directly:

```bash
./otlp-converter -input badger_export.json -output traces_otlp -max 10000
```

See `README.md` for all options.
