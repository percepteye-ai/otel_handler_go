package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	jaeger "github.com/jaegertracing/jaeger/model"
)

type Converter struct {
	config       *Config
	traces       map[string][]*OTLPSpan
	tracesLock   sync.Mutex
	writeChan    chan map[string][]*OTLPSpan
	totalSpans   int
	batchCount   int
	statsLock    sync.Mutex
}

func NewConverter(config *Config) *Converter {
	return &Converter{
		config:     config,
		traces:     make(map[string][]*OTLPSpan),
		writeChan:  make(chan map[string][]*OTLPSpan, 3),
		totalSpans: 0,
		batchCount: 0,
	}
}

func (c *Converter) Worker(entryChan <-chan BadgerEntry, resultChan chan<- *OTLPSpan, wg *sync.WaitGroup) {
	defer wg.Done()

	for entry := range entryChan {
		span := c.parseEntry(entry)
		if span != nil {
			resultChan <- span
		}
	}
}

func (c *Converter) parseEntry(entry BadgerEntry) *OTLPSpan {
	// Decode hex value
	valueBytes, err := hex.DecodeString(entry.Value)
	if err != nil {
		return nil
	}

	// Parse Jaeger protobuf span
	var jaegerSpan jaeger.Span
	if err := proto.Unmarshal(valueBytes, &jaegerSpan); err != nil {
		return nil
	}

	// Convert to OTLP
	otlpSpan := c.convertJaegerToOTLP(&jaegerSpan)
	return otlpSpan
}

func (c *Converter) convertJaegerToOTLP(jaegerSpan *jaeger.Span) *OTLPSpan {
	// Convert trace ID and span ID to hex strings
	traceIDBytes := make([]byte, 16)
	spanIDBytes := make([]byte, 8)

	jaegerSpan.TraceID.MarshalTo(traceIDBytes)
	jaegerSpan.SpanID.MarshalTo(spanIDBytes)

	otlp := &OTLPSpan{
		TraceID:           hex.EncodeToString(traceIDBytes),
		SpanID:            hex.EncodeToString(spanIDBytes),
		Name:              jaegerSpan.OperationName,
		Kind:              "SPAN_KIND_INTERNAL",
		StartTimeUnixNano: fmt.Sprintf("%d", jaegerSpan.StartTime.UnixNano()),
		EndTimeUnixNano:   fmt.Sprintf("%d", jaegerSpan.StartTime.Add(jaegerSpan.Duration).UnixNano()),
		Attributes:        make([]Attribute, 0),
		Events:            make([]Event, 0),
		Status: Status{
			Code: "STATUS_CODE_UNSET",
		},
	}

	// Set parent span ID if exists
	if len(jaegerSpan.References) > 0 {
		for _, ref := range jaegerSpan.References {
			if ref.RefType == jaeger.SpanRefType_CHILD_OF {
				parentSpanIDBytes := make([]byte, 8)
				ref.SpanID.MarshalTo(parentSpanIDBytes)
				otlp.ParentSpanID = hex.EncodeToString(parentSpanIDBytes)
				break
			}
		}
	}

	// Convert tags to attributes
	for _, tag := range jaegerSpan.Tags {
		attr := c.convertTag(tag)
		otlp.Attributes = append(otlp.Attributes, attr)

		// Check for span.kind
		if tag.Key == "span.kind" {
			if tag.VStr == "server" {
				otlp.Kind = "SPAN_KIND_SERVER"
			} else if tag.VStr == "client" {
				otlp.Kind = "SPAN_KIND_CLIENT"
			} else if tag.VStr == "producer" {
				otlp.Kind = "SPAN_KIND_PRODUCER"
			} else if tag.VStr == "consumer" {
				otlp.Kind = "SPAN_KIND_CONSUMER"
			}
		}
	}

	// Convert process tags to attributes
	if jaegerSpan.Process != nil {
		for _, tag := range jaegerSpan.Process.Tags {
			attr := c.convertTag(tag)
			otlp.Attributes = append(otlp.Attributes, attr)
		}
	}

	// Convert logs to events
	for _, log := range jaegerSpan.Logs {
		event := Event{
			TimeUnixNano: fmt.Sprintf("%d", log.Timestamp.UnixNano()),
			Name:         "log",
			Attributes:   make([]Attribute, 0),
		}

		for _, field := range log.Fields {
			attr := c.convertTag(field)
			event.Attributes = append(event.Attributes, attr)

			// Use "event" field as event name if present
			if field.Key == "event" {
				event.Name = field.VStr
			}
		}

		otlp.Events = append(otlp.Events, event)
	}

	return otlp
}

func (c *Converter) convertTag(tag jaeger.KeyValue) Attribute {
	attr := Attribute{
		Key: tag.Key,
	}

	switch tag.VType {
	case jaeger.ValueType_STRING:
		attr.Value = AttributeValue{StringValue: tag.VStr}
	case jaeger.ValueType_BOOL:
		attr.Value = AttributeValue{BoolValue: &tag.VBool}
	case jaeger.ValueType_INT64:
		attr.Value = AttributeValue{IntValue: &tag.VInt64}
	case jaeger.ValueType_FLOAT64:
		attr.Value = AttributeValue{DoubleValue: &tag.VFloat64}
	case jaeger.ValueType_BINARY:
		hexStr := hex.EncodeToString(tag.VBinary)
		attr.Value = AttributeValue{BytesValue: hexStr}
	default:
		attr.Value = AttributeValue{StringValue: tag.VStr}
	}

	return attr
}

func (c *Converter) ResultCollector(resultChan <-chan *OTLPSpan, done chan<- struct{}) {
	defer close(done)

	lastWrite := time.Now()
	processedCount := 0

	for span := range resultChan {
		c.tracesLock.Lock()
		c.traces[span.TraceID] = append(c.traces[span.TraceID], span)
		c.tracesLock.Unlock()

		processedCount++

		// Check if we should write
		if processedCount%c.config.WriteInterval == 0 || time.Since(lastWrite) > 30*time.Second {
			c.flushTraces()
			lastWrite = time.Now()
			fmt.Printf("Processed %d spans, queued for writing...\n", processedCount)
		}
	}

	// Final flush
	if len(c.traces) > 0 {
		c.flushTraces()
	}
}

func (c *Converter) flushTraces() {
	c.tracesLock.Lock()
	tracesCopy := c.traces
	c.traces = make(map[string][]*OTLPSpan)
	c.tracesLock.Unlock()

	if len(tracesCopy) == 0 {
		return
	}

	// Send to writer (non-blocking)
	select {
	case c.writeChan <- tracesCopy:
	default:
		// If channel full, write synchronously
		c.writeToArrow(tracesCopy)
	}
}

func (c *Converter) BackgroundWriter(done chan<- struct{}) {
	defer close(done)

	for traces := range c.writeChan {
		c.writeToArrow(traces)
	}
}

func (c *Converter) writeToArrow(traces map[string][]*OTLPSpan) {
	c.statsLock.Lock()
	batchNum := c.batchCount
	c.batchCount++
	c.statsLock.Unlock()

	filename := fmt.Sprintf("%s.batch_%04d.arrow", c.config.OutputFile, batchNum)

	// Convert traces to rows for Arrow
	rows := make([]ArrowRow, 0)
	spanCount := 0

	for _, spans := range traces {
		for _, span := range spans {
			// Serialize full OTLP span to JSON
			spanJSON, err := json.Marshal(span)
			if err != nil {
				continue
			}

			// Extract service name from attributes
			serviceName := "unknown"
			for _, attr := range span.Attributes {
				if attr.Key == "service.name" {
					serviceName = attr.Value.StringValue
					break
				}
			}

			row := ArrowRow{
				OTLPSpan:    string(spanJSON),
				TraceID:     span.TraceID,
				SpanID:      span.SpanID,
				ServiceName: serviceName,
				Name:        span.Name,
			}

			rows = append(rows, row)
			spanCount++
		}
	}

	// Write to Arrow file
	if err := WriteArrowFile(filename, rows); err != nil {
		fmt.Printf("Error writing Arrow file: %v\n", err)
		return
	}

	c.statsLock.Lock()
	c.totalSpans += spanCount
	c.statsLock.Unlock()

	fmt.Printf("Wrote %d spans to %s\n", spanCount, filename)
}

func (c *Converter) Shutdown() {
	close(c.writeChan)
}

func (c *Converter) TotalSpans() int {
	c.statsLock.Lock()
	defer c.statsLock.Unlock()
	return c.totalSpans
}

func (c *Converter) BatchCount() int {
	c.statsLock.Lock()
	defer c.statsLock.Unlock()
	return c.batchCount
}
