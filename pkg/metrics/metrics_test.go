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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewMetricsCollector(t *testing.T) {
	// Test that NewMetricsCollector returns a valid collector
	collector := NewMetricsCollector()
	assert.NotNil(t, collector)
	assert.NotNil(t, collector.PodLookupLatency)
	assert.NotNil(t, collector.EBPFSharingRatio)
	assert.NotNil(t, collector.SelectorCacheSize)
	assert.NotNil(t, collector.SelectorCacheHits)
	assert.NotNil(t, collector.SelectorCacheMisses)
	assert.NotNil(t, collector.ReconciliationTotal)
}

func TestMetricsCollector_Singleton(t *testing.T) {
	// Test that NewMetricsCollector returns the same instance
	collector1 := NewMetricsCollector()
	collector2 := NewMetricsCollector()
	assert.Same(t, collector1, collector2)
}

func TestMetricsCollector_RecordPodLookupLatency(t *testing.T) {
	collector := NewMetricsCollector()

	// Should not panic when recording latency
	assert.NotPanics(t, func() {
		collector.RecordPodLookupLatency("PodName", "default", 100*time.Millisecond)
		collector.RecordPodLookupLatency("Label", "kube-system", 50*time.Millisecond)
		collector.RecordPodLookupLatency("Hybrid", "test-ns", 200*time.Millisecond)
	})
}

func TestMetricsCollector_UpdateEBPFSharingRatio(t *testing.T) {
	collector := NewMetricsCollector()

	// Should not panic when updating ratio
	assert.NotPanics(t, func() {
		collector.UpdateEBPFSharingRatio(0.0)
		collector.UpdateEBPFSharingRatio(0.5)
		collector.UpdateEBPFSharingRatio(1.0)
	})
}

func TestMetricsCollector_UpdateSelectorCacheSize(t *testing.T) {
	collector := NewMetricsCollector()

	// Should not panic when updating cache size
	assert.NotPanics(t, func() {
		collector.UpdateSelectorCacheSize(0)
		collector.UpdateSelectorCacheSize(100)
		collector.UpdateSelectorCacheSize(1000)
	})
}

func TestMetricsCollector_RecordCacheHitMiss(t *testing.T) {
	collector := NewMetricsCollector()

	// Should not panic when recording cache hits/misses
	assert.NotPanics(t, func() {
		collector.RecordCacheHit()
		collector.RecordCacheHit()
		collector.RecordCacheMiss()
	})
}

func TestMetricsCollector_RecordReconciliation(t *testing.T) {
	collector := NewMetricsCollector()

	// Should not panic when recording reconciliation
	assert.NotPanics(t, func() {
		collector.RecordReconciliation("PodName")
		collector.RecordReconciliation("Label")
		collector.RecordReconciliation("Hybrid")
	})
}

func TestMetricsCollector_Reset(t *testing.T) {
	collector := NewMetricsCollector()

	// Record some metrics
	collector.RecordPodLookupLatency("PodName", "default", 100*time.Millisecond)
	collector.UpdateEBPFSharingRatio(0.5)
	collector.UpdateSelectorCacheSize(50)

	// Should not panic when resetting
	assert.NotPanics(t, func() {
		collector.Reset()
	})
}

func TestMsSince(t *testing.T) {
	start := time.Now()
	time.Sleep(10 * time.Millisecond)
	ms := MsSince(start)

	// Should return a positive value
	assert.Greater(t, ms, float64(0))
}
