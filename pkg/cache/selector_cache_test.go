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

package cache

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/aws/aws-network-policy-agent/api/v1alpha1"
	npatypes "github.com/aws/aws-network-policy-agent/pkg/types"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/types"
)

func TestNewSelectorCache(t *testing.T) {
	tests := []struct {
		name            string
		maxSize         int
		ttl             time.Duration
		expectedMaxSize int
		expectedTTL     time.Duration
	}{
		{
			name:            "valid parameters",
			maxSize:         50,
			ttl:             10 * time.Minute,
			expectedMaxSize: 50,
			expectedTTL:     10 * time.Minute,
		},
		{
			name:            "zero maxSize uses default",
			maxSize:         0,
			ttl:             10 * time.Minute,
			expectedMaxSize: 100,
			expectedTTL:     10 * time.Minute,
		},
		{
			name:            "negative maxSize uses default",
			maxSize:         -1,
			ttl:             10 * time.Minute,
			expectedMaxSize: 100,
			expectedTTL:     10 * time.Minute,
		},
		{
			name:            "zero TTL uses default",
			maxSize:         50,
			ttl:             0,
			expectedMaxSize: 50,
			expectedTTL:     5 * time.Minute,
		},
		{
			name:            "negative TTL uses default",
			maxSize:         50,
			ttl:             -1 * time.Minute,
			expectedMaxSize: 50,
			expectedTTL:     5 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := NewSelectorCache(tt.maxSize, tt.ttl)
			assert.NotNil(t, cache)
			assert.Equal(t, tt.expectedMaxSize, cache.maxSize)
			assert.Equal(t, tt.expectedTTL, cache.ttl)
			assert.NotNil(t, cache.cache)
			assert.NotNil(t, cache.lruList)
		})
	}
}

func TestSelectorCache_SetAndGet(t *testing.T) {
	cache := NewSelectorCache(10, 5*time.Minute)

	pods := []npatypes.Pod{
		{
			NamespacedName: types.NamespacedName{Name: "pod1", Namespace: "default"},
			PodIP:          v1alpha1.NetworkAddress("10.0.0.1"),
		},
		{
			NamespacedName: types.NamespacedName{Name: "pod2", Namespace: "default"},
			PodIP:          v1alpha1.NetworkAddress("10.0.0.2"),
		},
	}

	// Set entry
	cache.Set("hash123", pods)

	// Get entry
	result, found := cache.Get("hash123")
	assert.True(t, found)
	assert.Equal(t, len(pods), len(result))
	assert.Equal(t, pods[0].Name, result[0].Name)
	assert.Equal(t, pods[1].Name, result[1].Name)
}

func TestSelectorCache_GetNonExistent(t *testing.T) {
	cache := NewSelectorCache(10, 5*time.Minute)

	result, found := cache.Get("nonexistent")
	assert.False(t, found)
	assert.Nil(t, result)
}

func TestSelectorCache_TTLExpiration(t *testing.T) {
	cache := NewSelectorCache(10, 1*time.Second)

	pods := []npatypes.Pod{
		{
			NamespacedName: types.NamespacedName{Name: "pod1", Namespace: "default"},
			PodIP:          v1alpha1.NetworkAddress("10.0.0.1"),
		},
	}

	// Use a mock time function
	currentTime := time.Now()
	cache.SetTimeFunc(func() time.Time { return currentTime })

	// Set entry
	cache.Set("hash123", pods)

	// Entry should be found immediately
	result, found := cache.Get("hash123")
	assert.True(t, found)
	assert.NotNil(t, result)

	// Advance time past TTL
	currentTime = currentTime.Add(2 * time.Second)

	// Entry should be expired
	result, found = cache.Get("hash123")
	assert.False(t, found)
	assert.Nil(t, result)

	// Cache should be empty after expired entry is removed
	assert.Equal(t, 0, cache.Size())
}

func TestSelectorCache_LRUEviction(t *testing.T) {
	cache := NewSelectorCache(3, 5*time.Minute)

	// Add 3 entries
	cache.Set("hash1", []npatypes.Pod{{NamespacedName: types.NamespacedName{Name: "pod1"}}})
	cache.Set("hash2", []npatypes.Pod{{NamespacedName: types.NamespacedName{Name: "pod2"}}})
	cache.Set("hash3", []npatypes.Pod{{NamespacedName: types.NamespacedName{Name: "pod3"}}})

	assert.Equal(t, 3, cache.Size())

	// Access hash1 to make it recently used
	_, found := cache.Get("hash1")
	assert.True(t, found)

	// Add a 4th entry, should evict hash2 (least recently used)
	cache.Set("hash4", []npatypes.Pod{{NamespacedName: types.NamespacedName{Name: "pod4"}}})

	assert.Equal(t, 3, cache.Size())

	// hash2 should be evicted
	_, found = cache.Get("hash2")
	assert.False(t, found)

	// hash1, hash3, hash4 should still exist
	_, found = cache.Get("hash1")
	assert.True(t, found)
	_, found = cache.Get("hash3")
	assert.True(t, found)
	_, found = cache.Get("hash4")
	assert.True(t, found)
}

func TestSelectorCache_UpdateExisting(t *testing.T) {
	cache := NewSelectorCache(10, 5*time.Minute)

	pods1 := []npatypes.Pod{
		{NamespacedName: types.NamespacedName{Name: "pod1"}},
	}
	pods2 := []npatypes.Pod{
		{NamespacedName: types.NamespacedName{Name: "pod2"}},
		{NamespacedName: types.NamespacedName{Name: "pod3"}},
	}

	// Set initial entry
	cache.Set("hash123", pods1)
	result, _ := cache.Get("hash123")
	assert.Equal(t, 1, len(result))

	// Update entry
	cache.Set("hash123", pods2)
	result, _ = cache.Get("hash123")
	assert.Equal(t, 2, len(result))
	assert.Equal(t, "pod2", result[0].Name)

	// Size should still be 1
	assert.Equal(t, 1, cache.Size())
}

func TestSelectorCache_Invalidate(t *testing.T) {
	cache := NewSelectorCache(10, 5*time.Minute)

	cache.Set("hash1", []npatypes.Pod{{NamespacedName: types.NamespacedName{Name: "pod1"}}})
	cache.Set("hash2", []npatypes.Pod{{NamespacedName: types.NamespacedName{Name: "pod2"}}})

	assert.Equal(t, 2, cache.Size())

	// Invalidate hash1
	cache.Invalidate("hash1")

	assert.Equal(t, 1, cache.Size())

	_, found := cache.Get("hash1")
	assert.False(t, found)

	_, found = cache.Get("hash2")
	assert.True(t, found)
}

func TestSelectorCache_InvalidateNonExistent(t *testing.T) {
	cache := NewSelectorCache(10, 5*time.Minute)

	cache.Set("hash1", []npatypes.Pod{{NamespacedName: types.NamespacedName{Name: "pod1"}}})

	// Invalidating non-existent key should not panic
	cache.Invalidate("nonexistent")

	assert.Equal(t, 1, cache.Size())
}

func TestSelectorCache_InvalidateAll(t *testing.T) {
	cache := NewSelectorCache(10, 5*time.Minute)

	cache.Set("hash1", []npatypes.Pod{{NamespacedName: types.NamespacedName{Name: "pod1"}}})
	cache.Set("hash2", []npatypes.Pod{{NamespacedName: types.NamespacedName{Name: "pod2"}}})
	cache.Set("hash3", []npatypes.Pod{{NamespacedName: types.NamespacedName{Name: "pod3"}}})

	assert.Equal(t, 3, cache.Size())

	cache.InvalidateAll()

	assert.Equal(t, 0, cache.Size())

	_, found := cache.Get("hash1")
	assert.False(t, found)
}

func TestSelectorCache_DataIsolation(t *testing.T) {
	cache := NewSelectorCache(10, 5*time.Minute)

	pods := []npatypes.Pod{
		{NamespacedName: types.NamespacedName{Name: "pod1"}},
	}

	cache.Set("hash123", pods)

	// Modify original slice
	pods[0].Name = "modified"

	// Cached data should not be affected
	result, _ := cache.Get("hash123")
	assert.Equal(t, "pod1", result[0].Name)

	// Modify returned slice
	result[0].Name = "also-modified"

	// Cached data should not be affected
	result2, _ := cache.Get("hash123")
	assert.Equal(t, "pod1", result2[0].Name)
}


func TestSelectorCache_InvalidateForPod(t *testing.T) {
	cache := NewSelectorCache(10, 5*time.Minute)

	pod1 := npatypes.Pod{NamespacedName: types.NamespacedName{Name: "pod1", Namespace: "default"}}
	pod2 := npatypes.Pod{NamespacedName: types.NamespacedName{Name: "pod2", Namespace: "default"}}
	pod3 := npatypes.Pod{NamespacedName: types.NamespacedName{Name: "pod3", Namespace: "default"}}

	// Set up cache entries with overlapping Pods
	cache.Set("selector1", []npatypes.Pod{pod1, pod2})
	cache.Set("selector2", []npatypes.Pod{pod2, pod3})
	cache.Set("selector3", []npatypes.Pod{pod3})

	assert.Equal(t, 3, cache.Size())

	// Invalidate entries containing pod2
	invalidated := cache.InvalidateForPod(pod2.NamespacedName)

	// Should have invalidated selector1 and selector2
	assert.Equal(t, 2, len(invalidated))
	assert.Contains(t, invalidated, "selector1")
	assert.Contains(t, invalidated, "selector2")

	// Only selector3 should remain
	assert.Equal(t, 1, cache.Size())

	_, found := cache.Get("selector1")
	assert.False(t, found)

	_, found = cache.Get("selector2")
	assert.False(t, found)

	_, found = cache.Get("selector3")
	assert.True(t, found)
}

func TestSelectorCache_InvalidateForPodNonExistent(t *testing.T) {
	cache := NewSelectorCache(10, 5*time.Minute)

	pod1 := npatypes.Pod{NamespacedName: types.NamespacedName{Name: "pod1", Namespace: "default"}}
	cache.Set("selector1", []npatypes.Pod{pod1})

	// Invalidate for a Pod that doesn't exist in cache
	nonExistentPod := types.NamespacedName{Name: "nonexistent", Namespace: "default"}
	invalidated := cache.InvalidateForPod(nonExistentPod)

	assert.Nil(t, invalidated)
	assert.Equal(t, 1, cache.Size())
}

func TestSelectorCache_InvalidateForPodDelete(t *testing.T) {
	cache := NewSelectorCache(10, 5*time.Minute)

	pod1 := npatypes.Pod{NamespacedName: types.NamespacedName{Name: "pod1", Namespace: "default"}}
	pod2 := npatypes.Pod{NamespacedName: types.NamespacedName{Name: "pod2", Namespace: "default"}}

	cache.Set("selector1", []npatypes.Pod{pod1, pod2})
	cache.Set("selector2", []npatypes.Pod{pod1})

	assert.Equal(t, 2, cache.Size())

	// Simulate Pod deletion
	invalidated := cache.InvalidateForPodDelete(pod1.NamespacedName)

	// Both selectors should be invalidated since pod1 was in both
	assert.Equal(t, 2, len(invalidated))
	assert.Equal(t, 0, cache.Size())
}

func TestSelectorCache_InvalidateForPodCreate(t *testing.T) {
	cache := NewSelectorCache(10, 5*time.Minute)

	pod1 := npatypes.Pod{NamespacedName: types.NamespacedName{Name: "pod1", Namespace: "default"}}
	cache.Set("selector1", []npatypes.Pod{pod1})
	cache.Set("selector2", []npatypes.Pod{pod1})

	assert.Equal(t, 2, cache.Size())

	// Simulate new Pod creation - should invalidate all entries
	cache.InvalidateForPodCreate()

	assert.Equal(t, 0, cache.Size())
}

func TestSelectorCache_InvalidateForPodLabelChange(t *testing.T) {
	cache := NewSelectorCache(10, 5*time.Minute)

	pod1 := npatypes.Pod{NamespacedName: types.NamespacedName{Name: "pod1", Namespace: "default"}}
	pod2 := npatypes.Pod{NamespacedName: types.NamespacedName{Name: "pod2", Namespace: "default"}}

	cache.Set("selector1", []npatypes.Pod{pod1})
	cache.Set("selector2", []npatypes.Pod{pod1, pod2})
	cache.Set("selector3", []npatypes.Pod{pod2})

	assert.Equal(t, 3, cache.Size())

	// Simulate label change on pod1
	invalidated := cache.InvalidateForPodLabelChange(pod1.NamespacedName)

	// Should return the selectors that contained pod1
	assert.Equal(t, 2, len(invalidated))
	assert.Contains(t, invalidated, "selector1")
	assert.Contains(t, invalidated, "selector2")

	// All entries should be invalidated since label change might affect any selector
	assert.Equal(t, 0, cache.Size())
}

func TestSelectorCache_InvalidateForPodLabelChangeNonExistent(t *testing.T) {
	cache := NewSelectorCache(10, 5*time.Minute)

	pod1 := npatypes.Pod{NamespacedName: types.NamespacedName{Name: "pod1", Namespace: "default"}}
	cache.Set("selector1", []npatypes.Pod{pod1})

	assert.Equal(t, 1, cache.Size())

	// Simulate label change on a Pod not in cache
	nonExistentPod := types.NamespacedName{Name: "nonexistent", Namespace: "default"}
	invalidated := cache.InvalidateForPodLabelChange(nonExistentPod)

	// Should return nil since Pod wasn't in cache
	assert.Nil(t, invalidated)

	// But all entries should still be invalidated since the Pod might now match selectors
	assert.Equal(t, 0, cache.Size())
}

func TestSelectorCache_GetSelectorsForPod(t *testing.T) {
	cache := NewSelectorCache(10, 5*time.Minute)

	pod1 := npatypes.Pod{NamespacedName: types.NamespacedName{Name: "pod1", Namespace: "default"}}
	pod2 := npatypes.Pod{NamespacedName: types.NamespacedName{Name: "pod2", Namespace: "default"}}

	cache.Set("selector1", []npatypes.Pod{pod1, pod2})
	cache.Set("selector2", []npatypes.Pod{pod1})
	cache.Set("selector3", []npatypes.Pod{pod2})

	// pod1 should be in selector1 and selector2
	selectors := cache.GetSelectorsForPod(pod1.NamespacedName)
	assert.Equal(t, 2, len(selectors))
	assert.Contains(t, selectors, "selector1")
	assert.Contains(t, selectors, "selector2")

	// pod2 should be in selector1 and selector3
	selectors = cache.GetSelectorsForPod(pod2.NamespacedName)
	assert.Equal(t, 2, len(selectors))
	assert.Contains(t, selectors, "selector1")
	assert.Contains(t, selectors, "selector3")

	// Non-existent Pod should return nil
	nonExistentPod := types.NamespacedName{Name: "nonexistent", Namespace: "default"}
	selectors = cache.GetSelectorsForPod(nonExistentPod)
	assert.Nil(t, selectors)
}

func TestSelectorCache_PodMappingsCleanupOnEviction(t *testing.T) {
	cache := NewSelectorCache(2, 5*time.Minute)

	pod1 := npatypes.Pod{NamespacedName: types.NamespacedName{Name: "pod1", Namespace: "default"}}
	pod2 := npatypes.Pod{NamespacedName: types.NamespacedName{Name: "pod2", Namespace: "default"}}
	pod3 := npatypes.Pod{NamespacedName: types.NamespacedName{Name: "pod3", Namespace: "default"}}

	// Fill cache to capacity
	cache.Set("selector1", []npatypes.Pod{pod1})
	cache.Set("selector2", []npatypes.Pod{pod2})

	// Verify pod1 is tracked
	selectors := cache.GetSelectorsForPod(pod1.NamespacedName)
	assert.Equal(t, 1, len(selectors))

	// Add third entry, should evict selector1 (LRU)
	cache.Set("selector3", []npatypes.Pod{pod3})

	// pod1 should no longer be tracked since selector1 was evicted
	selectors = cache.GetSelectorsForPod(pod1.NamespacedName)
	assert.Nil(t, selectors)

	// pod2 and pod3 should still be tracked
	selectors = cache.GetSelectorsForPod(pod2.NamespacedName)
	assert.Equal(t, 1, len(selectors))

	selectors = cache.GetSelectorsForPod(pod3.NamespacedName)
	assert.Equal(t, 1, len(selectors))
}

func TestSelectorCache_PodMappingsCleanupOnUpdate(t *testing.T) {
	cache := NewSelectorCache(10, 5*time.Minute)

	pod1 := npatypes.Pod{NamespacedName: types.NamespacedName{Name: "pod1", Namespace: "default"}}
	pod2 := npatypes.Pod{NamespacedName: types.NamespacedName{Name: "pod2", Namespace: "default"}}
	pod3 := npatypes.Pod{NamespacedName: types.NamespacedName{Name: "pod3", Namespace: "default"}}

	// Set initial entry with pod1 and pod2
	cache.Set("selector1", []npatypes.Pod{pod1, pod2})

	// Verify both pods are tracked
	selectors := cache.GetSelectorsForPod(pod1.NamespacedName)
	assert.Equal(t, 1, len(selectors))
	selectors = cache.GetSelectorsForPod(pod2.NamespacedName)
	assert.Equal(t, 1, len(selectors))

	// Update entry to only contain pod2 and pod3
	cache.Set("selector1", []npatypes.Pod{pod2, pod3})

	// pod1 should no longer be tracked
	selectors = cache.GetSelectorsForPod(pod1.NamespacedName)
	assert.Nil(t, selectors)

	// pod2 and pod3 should be tracked
	selectors = cache.GetSelectorsForPod(pod2.NamespacedName)
	assert.Equal(t, 1, len(selectors))
	selectors = cache.GetSelectorsForPod(pod3.NamespacedName)
	assert.Equal(t, 1, len(selectors))
}

// =============================================================================
// Memory Consumption Benchmarks (Task 13.2)
// Requirements: 9.3 - Measure memory consumption for selector caches under sustained load
// =============================================================================

// generateBenchmarkPods creates a slice of Pods for benchmarking
func generateBenchmarkPods(count int, namespace string) []npatypes.Pod {
	pods := make([]npatypes.Pod, count)
	for i := 0; i < count; i++ {
		pods[i] = npatypes.Pod{
			NamespacedName: types.NamespacedName{
				Name:      fmt.Sprintf("pod-%d", i),
				Namespace: namespace,
			},
			PodIP: v1alpha1.NetworkAddress(fmt.Sprintf("10.0.%d.%d", i/256, i%256)),
		}
	}
	return pods
}

// BenchmarkSelectorCacheMemoryUnderLoad measures selector cache memory under sustained load
// This benchmark tests memory consumption at different cache sizes and entry counts
func BenchmarkSelectorCacheMemoryUnderLoad(b *testing.B) {
	cacheSizes := []int{100, 500, 1000}
	podsPerEntry := []int{10, 50, 100}

	for _, cacheSize := range cacheSizes {
		for _, podCount := range podsPerEntry {
			b.Run(fmt.Sprintf("CacheSize_%d_PodsPerEntry_%d", cacheSize, podCount), func(b *testing.B) {
				benchmarkCacheMemoryUnderLoad(b, cacheSize, podCount)
			})
		}
	}
}

func benchmarkCacheMemoryUnderLoad(b *testing.B, cacheSize int, podsPerEntry int) {
	var memStatsBefore, memStatsAfter runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&memStatsBefore)

	cache := NewSelectorCache(cacheSize, 5*time.Minute)

	// Fill the cache to capacity
	for i := 0; i < cacheSize; i++ {
		pods := generateBenchmarkPods(podsPerEntry, fmt.Sprintf("ns-%d", i))
		cache.Set(fmt.Sprintf("selector-hash-%d", i), pods)
	}

	runtime.GC()
	runtime.ReadMemStats(&memStatsAfter)

	totalMemory := memStatsAfter.Alloc - memStatsBefore.Alloc
	b.ReportMetric(float64(totalMemory), "bytes/cache")
	b.ReportMetric(float64(totalMemory)/float64(cacheSize), "bytes/entry")
	b.ReportMetric(float64(totalMemory)/float64(cacheSize*podsPerEntry), "bytes/pod")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Benchmark cache operations under sustained load
		cache.Get(fmt.Sprintf("selector-hash-%d", i%cacheSize))
	}
}

// BenchmarkSelectorCacheMemoryWithEviction measures memory behavior during eviction
func BenchmarkSelectorCacheMemoryWithEviction(b *testing.B) {
	cacheSizes := []int{100, 500}

	for _, cacheSize := range cacheSizes {
		b.Run(fmt.Sprintf("CacheSize_%d", cacheSize), func(b *testing.B) {
			benchmarkCacheMemoryWithEviction(b, cacheSize)
		})
	}
}

func benchmarkCacheMemoryWithEviction(b *testing.B, cacheSize int) {
	var memStatsBefore, memStatsAfter runtime.MemStats

	cache := NewSelectorCache(cacheSize, 5*time.Minute)

	// Fill the cache to capacity
	for i := 0; i < cacheSize; i++ {
		pods := generateBenchmarkPods(50, fmt.Sprintf("ns-%d", i))
		cache.Set(fmt.Sprintf("selector-hash-%d", i), pods)
	}

	runtime.GC()
	runtime.ReadMemStats(&memStatsBefore)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Add new entries causing eviction
		pods := generateBenchmarkPods(50, fmt.Sprintf("ns-new-%d", i))
		cache.Set(fmt.Sprintf("selector-hash-new-%d", i), pods)
	}
	b.StopTimer()

	runtime.GC()
	runtime.ReadMemStats(&memStatsAfter)

	// Memory should remain stable due to LRU eviction
	b.ReportMetric(float64(memStatsAfter.TotalAlloc-memStatsBefore.TotalAlloc)/float64(b.N), "bytes/op")
}

// BenchmarkSelectorCacheMemoryComparison compares memory usage between different cache configurations
func BenchmarkSelectorCacheMemoryComparison(b *testing.B) {
	b.Run("SmallCache_100entries", func(b *testing.B) {
		benchmarkCacheMemoryConfig(b, 100, 50)
	})
	b.Run("MediumCache_500entries", func(b *testing.B) {
		benchmarkCacheMemoryConfig(b, 500, 50)
	})
	b.Run("LargeCache_1000entries", func(b *testing.B) {
		benchmarkCacheMemoryConfig(b, 1000, 50)
	})
}

func benchmarkCacheMemoryConfig(b *testing.B, cacheSize int, podsPerEntry int) {
	var memStatsBefore, memStatsAfter runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&memStatsBefore)

	cache := NewSelectorCache(cacheSize, 5*time.Minute)

	// Fill the cache
	for i := 0; i < cacheSize; i++ {
		pods := generateBenchmarkPods(podsPerEntry, fmt.Sprintf("ns-%d", i))
		cache.Set(fmt.Sprintf("selector-hash-%d", i), pods)
	}

	runtime.GC()
	runtime.ReadMemStats(&memStatsAfter)

	memoryUsed := memStatsAfter.Alloc - memStatsBefore.Alloc
	b.ReportMetric(float64(memoryUsed), "bytes/total")
	b.ReportMetric(float64(memoryUsed)/float64(cacheSize), "bytes/entry")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Simulate sustained load with mixed operations
		idx := i % cacheSize
		cache.Get(fmt.Sprintf("selector-hash-%d", idx))
		if i%10 == 0 {
			// Occasional invalidation
			cache.Invalidate(fmt.Sprintf("selector-hash-%d", idx))
			pods := generateBenchmarkPods(podsPerEntry, fmt.Sprintf("ns-%d", idx))
			cache.Set(fmt.Sprintf("selector-hash-%d", idx), pods)
		}
	}
}

// BenchmarkSelectorCachePodTrackingMemory measures memory overhead of Pod-to-selector tracking
func BenchmarkSelectorCachePodTrackingMemory(b *testing.B) {
	scenarios := []struct {
		name           string
		cacheSize      int
		podsPerEntry   int
		overlappingPct int // Percentage of Pods that appear in multiple selectors
	}{
		{"NoOverlap", 100, 50, 0},
		{"LowOverlap_10pct", 100, 50, 10},
		{"HighOverlap_50pct", 100, 50, 50},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			benchmarkPodTrackingMemory(b, scenario.cacheSize, scenario.podsPerEntry, scenario.overlappingPct)
		})
	}
}

func benchmarkPodTrackingMemory(b *testing.B, cacheSize int, podsPerEntry int, overlapPct int) {
	var memStatsBefore, memStatsAfter runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&memStatsBefore)

	cache := NewSelectorCache(cacheSize, 5*time.Minute)

	// Create a pool of shared Pods for overlap
	sharedPodCount := (podsPerEntry * overlapPct) / 100
	sharedPods := generateBenchmarkPods(sharedPodCount, "shared-ns")

	// Fill the cache with entries that have some overlapping Pods
	for i := 0; i < cacheSize; i++ {
		uniquePods := generateBenchmarkPods(podsPerEntry-sharedPodCount, fmt.Sprintf("ns-%d", i))
		allPods := append(uniquePods, sharedPods...)
		cache.Set(fmt.Sprintf("selector-hash-%d", i), allPods)
	}

	runtime.GC()
	runtime.ReadMemStats(&memStatsAfter)

	memoryUsed := memStatsAfter.Alloc - memStatsBefore.Alloc
	b.ReportMetric(float64(memoryUsed), "bytes/total")
	b.ReportMetric(float64(memoryUsed)/float64(cacheSize), "bytes/entry")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Test Pod lookup performance with tracking overhead
		if sharedPodCount > 0 {
			cache.GetSelectorsForPod(sharedPods[i%sharedPodCount].NamespacedName)
		}
	}
}

// BenchmarkSelectorCacheInvalidationMemory measures memory behavior during invalidation operations
func BenchmarkSelectorCacheInvalidationMemory(b *testing.B) {
	b.Run("InvalidateForPod", func(b *testing.B) {
		benchmarkInvalidationMemory(b, "pod")
	})
	b.Run("InvalidateForPodLabelChange", func(b *testing.B) {
		benchmarkInvalidationMemory(b, "labelChange")
	})
	b.Run("InvalidateAll", func(b *testing.B) {
		benchmarkInvalidationMemory(b, "all")
	})
}

func benchmarkInvalidationMemory(b *testing.B, invalidationType string) {
	cacheSize := 100
	podsPerEntry := 50

	cache := NewSelectorCache(cacheSize, 5*time.Minute)

	// Create shared Pods that appear in multiple selectors
	sharedPods := generateBenchmarkPods(10, "shared-ns")

	// Fill the cache
	for i := 0; i < cacheSize; i++ {
		uniquePods := generateBenchmarkPods(podsPerEntry-10, fmt.Sprintf("ns-%d", i))
		allPods := append(uniquePods, sharedPods...)
		cache.Set(fmt.Sprintf("selector-hash-%d", i), allPods)
	}

	var memStatsBefore, memStatsAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memStatsBefore)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		switch invalidationType {
		case "pod":
			cache.InvalidateForPod(sharedPods[i%len(sharedPods)].NamespacedName)
		case "labelChange":
			cache.InvalidateForPodLabelChange(sharedPods[i%len(sharedPods)].NamespacedName)
		case "all":
			cache.InvalidateAll()
		}

		// Refill cache after invalidation
		for j := 0; j < cacheSize; j++ {
			uniquePods := generateBenchmarkPods(podsPerEntry-10, fmt.Sprintf("ns-%d", j))
			allPods := append(uniquePods, sharedPods...)
			cache.Set(fmt.Sprintf("selector-hash-%d", j), allPods)
		}
	}
	b.StopTimer()

	runtime.GC()
	runtime.ReadMemStats(&memStatsAfter)

	b.ReportMetric(float64(memStatsAfter.TotalAlloc-memStatsBefore.TotalAlloc)/float64(b.N), "bytes/op")
}

// TestSelectorCacheMemoryReport generates a memory usage report for documentation
func TestSelectorCacheMemoryReport(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory report in short mode")
	}

	t.Log("\n========================================")
	t.Log("SELECTOR CACHE MEMORY USAGE REPORT")
	t.Log("========================================\n")

	configurations := []struct {
		name         string
		cacheSize    int
		podsPerEntry int
	}{
		{"Small (100 entries, 10 pods/entry)", 100, 10},
		{"Medium (500 entries, 50 pods/entry)", 500, 50},
		{"Large (1000 entries, 100 pods/entry)", 1000, 100},
	}

	for _, config := range configurations {
		var memStatsBefore, memStatsAfter runtime.MemStats

		runtime.GC()
		runtime.ReadMemStats(&memStatsBefore)

		cache := NewSelectorCache(config.cacheSize, 5*time.Minute)

		for i := 0; i < config.cacheSize; i++ {
			pods := generateBenchmarkPods(config.podsPerEntry, fmt.Sprintf("ns-%d", i))
			cache.Set(fmt.Sprintf("selector-hash-%d", i), pods)
		}

		runtime.GC()
		runtime.ReadMemStats(&memStatsAfter)

		memoryUsed := memStatsAfter.Alloc - memStatsBefore.Alloc
		totalPods := config.cacheSize * config.podsPerEntry

		t.Logf("Configuration: %s", config.name)
		t.Logf("  Total Memory: %d bytes (%.2f MB)", memoryUsed, float64(memoryUsed)/(1024*1024))
		t.Logf("  Memory per Entry: %d bytes", memoryUsed/uint64(config.cacheSize))
		t.Logf("  Memory per Pod: %d bytes", memoryUsed/uint64(totalPods))
		t.Logf("  Cache Size: %d entries", cache.Size())
		t.Log("")
	}

	t.Log("========================================")
	t.Log("Run benchmarks with:")
	t.Log("  go test -bench=BenchmarkSelectorCacheMemory -benchmem ./pkg/cache/...")
	t.Log("========================================")
}
