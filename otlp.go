package main

// OTLPSpan represents a complete OTLP span structure
type OTLPSpan struct {
	TraceID           string      `json:"traceId"`
	SpanID            string      `json:"spanId"`
	ParentSpanID      string      `json:"parentSpanId,omitempty"`
	Name              string      `json:"name"`
	Kind              string      `json:"kind"`
	StartTimeUnixNano string      `json:"startTimeUnixNano"`
	EndTimeUnixNano   string      `json:"endTimeUnixNano"`
	Attributes        []Attribute `json:"attributes"`
	Events            []Event     `json:"events"`
	Status            Status      `json:"status"`
	TraceFlags        string      `json:"traceFlags,omitempty"`
	Links             []Link      `json:"links,omitempty"`
}

// Link represents an OTLP link (for distributed tracing)
type Link struct {
	TraceID    string      `json:"traceId"`
	SpanID     string      `json:"spanId"`
	Attributes []Attribute `json:"attributes,omitempty"`
}

// Attribute represents an OTLP attribute (key-value pair)
type Attribute struct {
	Key   string         `json:"key"`
	Value AttributeValue `json:"value"`
}

// AttributeValue represents the value of an attribute
type AttributeValue struct {
	StringValue string   `json:"stringValue,omitempty"`
	BoolValue   *bool    `json:"boolValue,omitempty"`
	IntValue    *int64   `json:"intValue,omitempty"`
	DoubleValue *float64 `json:"doubleValue,omitempty"`
	BytesValue  string   `json:"bytesValue,omitempty"`
}

// Event represents an OTLP event (log)
type Event struct {
	TimeUnixNano string      `json:"timeUnixNano"`
	Name         string      `json:"name"`
	Attributes   []Attribute `json:"attributes"`
}

// Status represents the status of a span
type Status struct {
	Code    string `json:"code"`
	Message string `json:"message,omitempty"`
}

// ResourceSpans represents OTLP ResourceSpans structure
type ResourceSpans struct {
	Resource   Resource    `json:"resource"`
	ScopeSpans []ScopeSpans `json:"scopeSpans"`
}

// Resource represents OTLP Resource
type Resource struct {
	Attributes []Attribute `json:"attributes"`
}

// ScopeSpans represents OTLP ScopeSpans
type ScopeSpans struct {
	Spans []*OTLPSpan `json:"spans"`
}

// OTLPExport represents the top-level OTLP export structure
type OTLPExport struct {
	ResourceSpans []ResourceSpans `json:"resourceSpans"`
}
