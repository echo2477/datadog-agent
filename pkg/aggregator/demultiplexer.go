// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

package aggregator

import (
	"time"

	"github.com/DataDog/datadog-agent/pkg/epforwarder"
	"github.com/DataDog/datadog-agent/pkg/forwarder"
	"github.com/DataDog/datadog-agent/pkg/metrics"
	orch "github.com/DataDog/datadog-agent/pkg/orchestrator/config"
	"github.com/DataDog/datadog-agent/pkg/serializer"
	"github.com/DataDog/datadog-agent/pkg/util/log"
	"github.com/DataDog/datadog-agent/pkg/version"
)

// Demultiplexer is composed of the samplers and their multiple pipelines,
// the event platform forwarder, orchestrator data buffers and other data
// that need to then be sent to the forwarder.
type Demultiplexer interface {
	Run()
	Stop()
	FlushAggregator(start time.Time, waitForSerializer bool)
	AddTimeSamples(sample metrics.MetricSample)
	AddCheckSample(sample metrics.MetricSample)
	Serializer() *serializer.Serializer // XXX(remy): remove me
	Aggregator() *BufferedAggregator    // XXX(remy): remove me
}

// AgentDemultiplexer is the demultiplexer implementation for the main Agent
type AgentDemultiplexer struct {
	// Input

	// Processing
	aggregator *BufferedAggregator

	// Output
	output
}

// DemultiplexerOptions are the options used to initialize a Demultiplexer.
type DemultiplexerOptions struct {
	Forwarder                  *forwarder.Options
	NoopEventPlatformForwarder bool
	NoOrchestratorForwarder    bool
	FlushInterval              time.Duration
	StartupTelemetry           string
}

type outputForwarders struct {
	shared        *forwarder.DefaultForwarder
	orchestrator  *forwarder.DefaultForwarder
	eventPlatform epforwarder.EventPlatformForwarder
}

type output struct {
	forwarders outputForwarders

	sharedSerializer *serializer.Serializer
}

// DefaultDemultiplexerOptions returns the default options to initialize a Demultiplexer.
func DefaultDemultiplexerOptions(keysPerDomain map[string][]string) DemultiplexerOptions {
	return DemultiplexerOptions{
		Forwarder:        forwarder.NewOptions(keysPerDomain),
		FlushInterval:    DefaultFlushInterval,
		StartupTelemetry: version.AgentVersion,
	}
}

// NewAgentDemultiplexer creates a new Demultiplexer
// TODO(remy): finish this comment
func NewAgentDemultiplexer(options DemultiplexerOptions, hostname string) Demultiplexer {
	// prepare the multiple forwarders
	// -------------------------------

	log.Debugf("Starting forwarders")
	// orchestrator forwarder
	orchestratorForwarder := orch.NewOrchestratorForwarder()

	// event platform forwarder
	var eventPlatformForwarder epforwarder.EventPlatformForwarder
	if options.NoopEventPlatformForwarder {
		eventPlatformForwarder = epforwarder.NewNoopEventPlatformForwarder()
	} else {
		eventPlatformForwarder = epforwarder.NewEventPlatformForwarder()
	}

	sharedForwarder := forwarder.NewDefaultForwarder(options.Forwarder)

	// prepare the serializer
	// ----------------------

	sharedSerializer := serializer.NewSerializer(sharedForwarder, orchestratorForwarder)

	// prepare processing parts XXX(remy): processor?
	// --

	agg := InitAggregatorWithFlushInterval(sharedSerializer, eventPlatformForwarder, hostname, options.FlushInterval)
	agg.AddAgentStartupTelemetry(options.StartupTelemetry)

	// --

	return &AgentDemultiplexer{
		// Input
		aggregator: agg,

		// Processing

		// Output
		output: output{
			forwarders: outputForwarders{
				shared:        sharedForwarder,
				orchestrator:  orchestratorForwarder,
				eventPlatform: eventPlatformForwarder,
			},

			sharedSerializer: sharedSerializer,
		},
	}
}

// Run runs all demultiplexer parts
func (d *AgentDemultiplexer) Run() {
	if d.forwarders.orchestrator != nil {
		d.forwarders.orchestrator.Start() //nolint:errcheck
	} else {
		log.Debug("not starting the orchestrator forwarder")
	}
	if d.forwarders.eventPlatform != nil {
		d.forwarders.eventPlatform.Start()
	} else {
		log.Debug("not starting the event platform forwarder")
	}
	if d.forwarders.shared != nil {
		d.forwarders.shared.Start() //nolint:errcheck
	} else {
		log.Debug("not starting the shared forwarder")
	}
	log.Debug("Forwarders started")

	d.aggregator.run()
	log.Debug("Aggregator started")
}

// Stop stops the demultiplexer
func (d *AgentDemultiplexer) Stop() {
	d.aggregator.Stop()

	if d.output.forwarders.orchestrator != nil {
		d.output.forwarders.orchestrator.Stop()
	}
	if d.output.forwarders.shared != nil {
		d.output.forwarders.shared.Stop()
	}
	if d.output.forwarders.eventPlatform != nil {
		d.output.forwarders.eventPlatform.Stop()
	}
}

// FlushAggregator flushes all data from the aggregator to the serializer
func (d *AgentDemultiplexer) FlushAggregator(start time.Time, waitForSerializer bool) {
	d.aggregator.Flush(start, waitForSerializer)
}

// AddTimeSamples adds a sample into the time samplers (DogStatsD) pipelines
// XXX(remy): implement me
func (d *AgentDemultiplexer) AddTimeSamples(sample metrics.MetricSample) {
	// XXX(remy): we should probably embed the `batcher` directly in the Demultiplexer

	// XXX(remy): this is the entry point to the time sampler pipelines, we will probably want to do something
	// XXX(remy): like `pipeline[sample.Key%d.pipelinesCount].samples <- sample`
	// XXX(remy): where all readers of these channels are running in a different routine
}

// AddCheckSample adds a check sample into the check samplers pipeline.
// XXX(remy): implement me
func (d *AgentDemultiplexer) AddCheckSample(sample metrics.MetricSample) {
	// XXX(remy): for now, send it to the aggregator
}

// Serializer returns the shared serializer
// XXX(remy): we will probably want to remove that getter to pass the demultiplexer everywhere instead
func (d *AgentDemultiplexer) Serializer() *serializer.Serializer {
	return d.output.sharedSerializer
}

// Aggregator returns the main buffered aggregator
// XXX(remy): we will probably want to remove that getter to pass the demultiplexer everywhere instead
func (d *AgentDemultiplexer) Aggregator() *BufferedAggregator {
	return d.aggregator
}

// ------------------------------

// ServerlessDemultiplexer is a simple demultiplexer used by the serverless flavor of the Agent
type ServerlessDemultiplexer struct {
	aggregator *BufferedAggregator
	serializer *serializer.Serializer
	forwarder  *forwarder.SyncForwarder
}

// NewServerlessDemultiplexer creates a new Demultiplexer for the serverless agent
// TODO(remy): finish this comment
func NewServerlessDemultiplexer(keysPerDomain map[string][]string, hostname string, forwarderTimeout time.Duration) Demultiplexer {
	forwarder := forwarder.NewSyncForwarder(keysPerDomain, forwarderTimeout)
	serializer := serializer.NewSerializer(forwarder, nil)
	aggregator := InitAggregator(serializer, nil, hostname)

	return &ServerlessDemultiplexer{
		aggregator: aggregator,
		serializer: serializer,
		forwarder:  forwarder,
	}
}

// Run runs all demultiplexer parts
func (d *ServerlessDemultiplexer) Run() {
	if d.forwarder != nil {
		d.forwarder.Start() //nolint:errcheck
	} else {
		log.Debug("not starting the forwarder")
	}
	log.Debug("Forwarder started")

	d.aggregator.run()
	log.Debug("Aggregator started")
}

// Stop stops the wrapped aggregator and the forwarder.
func (d *ServerlessDemultiplexer) Stop() {
	d.aggregator.Stop()

	if d.forwarder != nil {
		d.forwarder.Stop()
	}
}

// FlushAggregator flushes all data from the aggregator to the serializer
func (d *ServerlessDemultiplexer) FlushAggregator(start time.Time, waitForSerializer bool) {
	d.aggregator.Flush(start, waitForSerializer)
}

// AddTimeSamples adds a sample into the time samplers (DogStatsD) pipelines
// XXX(remy): implement me
func (d *ServerlessDemultiplexer) AddTimeSamples(sample metrics.MetricSample) {
	// XXX(remy): we should probably embed the `batcher` directly in the Demultiplexer

	// XXX(remy): this is the entry point to the time sampler pipelines, we will probably want to do something
	// XXX(remy): like `pipeline[sample.Key%d.pipelinesCount].samples <- sample`
	// XXX(remy): where all readers of these channels are running in a different routine
}

// AddCheckSample adds a check sample into the check samplers pipeline.
// XXX(remy): implement me
func (d *ServerlessDemultiplexer) AddCheckSample(sample metrics.MetricSample) {
	// XXX(remy): for now, send it to the aggregator but in this implementation we should not do anything
}

// Serializer returns the shared serializer
// XXX(remy): we will probably want to remove that getter to pass the demultiplexer everywhere instead
func (d *ServerlessDemultiplexer) Serializer() *serializer.Serializer {
	return d.serializer
}

// Aggregator returns the main buffered aggregator
// XXX(remy): we will probably want to remove that getter to pass the demultiplexer everywhere instead
func (d *ServerlessDemultiplexer) Aggregator() *BufferedAggregator {
	return d.aggregator
}
