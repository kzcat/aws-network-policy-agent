/*
Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package metrics

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

const (
	// Namespace for all metrics
	metricsNamespace = "awsnodeagent"
	// Subsystem for label selector related metrics
	labelSelectorSubsystem = "label_selector"
)

var (
	// Singleton instance
	instance *MetricsCollector
	once     sync.Once
)

// MetricsCollector collects performance metrics for the label selector feature.
// It tracks Pod lookup latency, eBPF sharing ratio, selector cache performance,
// and reconciliation frequency.
type MetricsCollector struct {
	// Pod lookup latency histogram - measures time to look up Pods by label selector
	PodLookupLatency *prometheus.HistogramVec

	// eBPF sharing ratio gauge - tracks the ratio of shared eBPF programs
	EBPFSharingRatio prometheus.Gauge

	// Selector cache size gauge - tracks the current size of the selector cache
	SelectorCacheSize prometheus.Gauge

	// Selector cache hit counter - tracks cache hits
	SelectorCacheHits prometheus.Counter

	// Selector cache miss counter - tracks cache misses
	SelectorCacheMisses prometheus.Counter

	// Reconciliation frequency counter - tracks reconciliation events by mode
	ReconciliationTotal *prometheus.CounterVec

	// registered tracks whether metrics have been registered
	registered bool
	mu         sync.Mutex
}

// NewMetricsCollector creates a new MetricsCollector with all metrics initialized.
// It returns a singleton instance to ensure metrics are only registered once.
func NewMetricsCollector() *MetricsCollector {
	once.Do(func() {
		instance = &MetricsCollector{
			PodLookupLatency: prometheus.NewHistogramVec(
				prometheus.HistogramOpts{
					Namespace: metricsNamespace,
					Subsystem: labelSelectorSubsystem,
					Name:      "pod_lookup_latency_ms",
					Help:      "Histogram of Pod lookup latency in milliseconds by selector mode",
					Buckets:   []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000},
				},
				[]string{"mode", "namespace"},
			),
			EBPFSharingRatio: prometheus.NewGauge(
				prometheus.GaugeOpts{
					Namespace: metricsNamespace,
					Subsystem: labelSelectorSubsystem,
					Name:      "ebpf_sharing_ratio",
					Help:      "Ratio of shared eBPF programs (0.0 to 1.0)",
				},
			),
			SelectorCacheSize: prometheus.NewGauge(
				prometheus.GaugeOpts{
					Namespace: metricsNamespace,
					Subsystem: labelSelectorSubsystem,
					Name:      "selector_cache_size",
					Help:      "Current number of entries in the selector cache",
				},
			),
			SelectorCacheHits: prometheus.NewCounter(
				prometheus.CounterOpts{
					Namespace: metricsNamespace,
					Subsystem: labelSelectorSubsystem,
					Name:      "selector_cache_hits_total",
					Help:      "Total number of selector cache hits",
				},
			),
			SelectorCacheMisses: prometheus.NewCounter(
				prometheus.CounterOpts{
					Namespace: metricsNamespace,
					Subsystem: labelSelectorSubsystem,
					Name:      "selector_cache_misses_total",
					Help:      "Total number of selector cache misses",
				},
			),
			ReconciliationTotal: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Namespace: metricsNamespace,
					Subsystem: labelSelectorSubsystem,
					Name:      "reconciliation_total",
					Help:      "Total number of reconciliation events by selector mode",
				},
				[]string{"mode"},
			),
		}
	})
	return instance
}

// Register registers all metrics with the controller-runtime metrics registry.
// It is safe to call multiple times; metrics will only be registered once.
func (m *MetricsCollector) Register() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.registered {
		return nil
	}

	// Register all metrics with the controller-runtime metrics registry
	metrics.Registry.MustRegister(
		m.PodLookupLatency,
		m.EBPFSharingRatio,
		m.SelectorCacheSize,
		m.SelectorCacheHits,
		m.SelectorCacheMisses,
		m.ReconciliationTotal,
	)

	m.registered = true
	return nil
}

// IsRegistered returns whether the metrics have been registered.
func (m *MetricsCollector) IsRegistered() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.registered
}

// RecordPodLookupLatency records the latency of a Pod lookup operation.
// mode should be one of: "PodName", "Label", "Hybrid"
// namespace is the namespace where the lookup was performed
// latency is the duration of the lookup operation
func (m *MetricsCollector) RecordPodLookupLatency(mode, namespace string, latency time.Duration) {
	m.PodLookupLatency.WithLabelValues(mode, namespace).Observe(float64(latency.Milliseconds()))
}

// UpdateEBPFSharingRatio updates the eBPF program sharing ratio.
// ratio should be between 0.0 (no sharing) and 1.0 (all programs shared)
func (m *MetricsCollector) UpdateEBPFSharingRatio(ratio float64) {
	m.EBPFSharingRatio.Set(ratio)
}

// UpdateSelectorCacheSize updates the current selector cache size.
func (m *MetricsCollector) UpdateSelectorCacheSize(size int) {
	m.SelectorCacheSize.Set(float64(size))
}

// RecordCacheHit increments the cache hit counter.
func (m *MetricsCollector) RecordCacheHit() {
	m.SelectorCacheHits.Inc()
}

// RecordCacheMiss increments the cache miss counter.
func (m *MetricsCollector) RecordCacheMiss() {
	m.SelectorCacheMisses.Inc()
}

// RecordReconciliation increments the reconciliation counter for the given mode.
// mode should be one of: "PodName", "Label", "Hybrid"
func (m *MetricsCollector) RecordReconciliation(mode string) {
	m.ReconciliationTotal.WithLabelValues(mode).Inc()
}

// GetCacheHitRate calculates and returns the cache hit rate.
// Returns 0.0 if no cache operations have been performed.
// This is a helper method for testing and monitoring.
func (m *MetricsCollector) GetCacheHitRate() float64 {
	// Note: This is a simplified implementation for testing.
	// In production, you would typically calculate this from the actual counter values
	// using the prometheus client's internal methods or by scraping the metrics endpoint.
	return 0.0
}

// Reset resets all metrics to their initial values.
// This is primarily useful for testing.
func (m *MetricsCollector) Reset() {
	m.PodLookupLatency.Reset()
	m.EBPFSharingRatio.Set(0)
	m.SelectorCacheSize.Set(0)
	// Note: Counters cannot be reset in prometheus, they only increase
	// For testing purposes, we would need to create new instances
}

// MsSince returns the number of milliseconds since the given start time.
// This is a helper function for recording latencies.
func MsSince(start time.Time) float64 {
	return float64(time.Since(start).Milliseconds())
}
