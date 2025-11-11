# Missing Fields Analysis and Fixes

## Summary

This document identifies missing fields in the OTLP conversion process and documents the fixes applied.

## Issues Found

### 1. ✅ FIXED: `service.name` Missing
**Problem:** The `service.name` attribute was not being extracted from Jaeger `Process.ServiceName` and was defaulting to "unknown" in ResourceSpans.

**Root Cause:** The Go converter was only converting process tags to attributes but wasn't extracting `Process.ServiceName` itself.

**Fix Applied:**
- Added extraction of `Process.ServiceName` as `service.name` attribute in `converter.go` lines 131-124
- Updated Python converter to extract `service.name` from span attributes first, then fallback to Arrow file column

**Status:** ✅ Fixed (requires re-conversion of Arrow files)

---

### 2. ✅ FIXED: Error Status Not Set
**Problem:** Spans with error tags (`error=true`, `error.message`, `error.type`) were not having their status code set to `STATUS_CODE_ERROR`.

**Root Cause:** Error tags were being converted to attributes but status wasn't being updated.

**Fix Applied:**
- Added error detection logic in `converter.go` lines 115-126
- Sets `STATUS_CODE_ERROR` when error tags are present
- Extracts `error.message` as status message

**Status:** ✅ Fixed

---

### 3. ✅ FIXED: Trace Flags Missing
**Problem:** Jaeger span `Flags` field was not being converted to OTLP `traceFlags`.

**Root Cause:** Flags field was not being extracted from Jaeger spans.

**Fix Applied:**
- Added `TraceFlags` field to `OTLPSpan` struct in `otlp.go` line 15
- Extract flags from `jaegerSpan.Flags` in `converter.go` line 83

**Status:** ✅ Fixed

---

### 4. ✅ FIXED: Links Missing
**Problem:** Jaeger span references that aren't parent-child relationships (e.g., `FOLLOWS_FROM`) were not being converted to OTLP links.

**Root Cause:** Only `CHILD_OF` references were being processed as parent span IDs.

**Fix Applied:**
- Added `Links` field to `OTLPSpan` struct in `otlp.go` lines 16, 19-24
- Process non-parent references as links in `converter.go` lines 87-108

**Status:** ✅ Fixed

---

## Fields That May Be Missing (But Not Critical)

These fields are not present in the current data but may be available in other Jaeger exports:

### Standard OTLP Resource Attributes (May be in Process Tags)
- `service.instance.id` - Instance identifier
- `service.namespace` - Service namespace
- `deployment.environment` - Deployment environment
- `host.id` - Host identifier
- `host.type` - Host type
- `cloud.provider` - Cloud provider
- `cloud.region` - Cloud region
- `cloud.availability_zone` - Availability zone

**Note:** These are typically set via instrumentation or environment variables. If they exist in Jaeger process tags, they will be automatically converted to attributes.

### Other Fields
- `Warnings` - Jaeger spans have a `Warnings` field, but it's rarely used
- `ProcessID` - Jaeger has `ProcessID` but it's typically redundant with process tags

---

## Testing Recommendations

1. **Re-run conversion** with fixed Go code to generate new Arrow files
2. **Verify service.name** is present in ResourceSpans
3. **Check error spans** have `STATUS_CODE_ERROR` status
4. **Verify traceFlags** are present in spans
5. **Check links** are present for non-parent references

---

## Files Modified

1. `converter.go` - Added error status detection, trace flags, links extraction, service.name extraction
2. `otlp.go` - Added TraceFlags and Links fields to OTLPSpan struct
3. `python/convert_arrow_to_otlp.py` - Improved service name extraction from attributes

---

## Next Steps

1. Rebuild Go converter: `go build -o otlp-converter`
2. Re-run conversion on source data to generate new Arrow files with fixes
3. Re-run Python converter to generate OTLP JSON with proper service names

