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

	policyk8sawsv1 "github.com/aws/aws-network-policy-agent/api/v1alpha1"
	npatypes "github.com/aws/aws-network-policy-agent/pkg/types"
	"k8s.io/apimachinery/pkg/types"
)

// =============================================================================
// Memory Consumption Benchmarks
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
			PodIP: policyk8sawsv1.NetworkAddress(fmt.Sprintf("10.0.%d.%d", i/256, i%256)),
		}
	}
	return pods
}

// BenchmarkSelectorCacheMemoryUnderLoad measures selector cache memory under sustained load
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
		cache.Set(fmt.Sprintf("ns-%d", i), fmt.Sprintf("selector-hash-%d", i), nil, pods)
	}

	runtime.GC()
	runtime.ReadMemStats(&memStatsAfter)

	totalMemory := memStatsAfter.Alloc - memStatsBefore.Alloc
	b.ReportMetric(float64(totalMemory), "bytes/cache")
	b.ReportMetric(float64(totalMemory)/float64(cacheSize), "bytes/entry")
	b.ReportMetric(float64(totalMemory)/float64(cacheSize*podsPerEntry), "bytes/pod")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get(fmt.Sprintf("ns-%d", i%cacheSize), fmt.Sprintf("selector-hash-%d", i%cacheSize))
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

	for i := 0; i < cacheSize; i++ {
		pods := generateBenchmarkPods(50, fmt.Sprintf("ns-%d", i))
		cache.Set(fmt.Sprintf("ns-%d", i), fmt.Sprintf("selector-hash-%d", i), nil, pods)
	}

	runtime.GC()
	runtime.ReadMemStats(&memStatsBefore)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pods := generateBenchmarkPods(50, fmt.Sprintf("ns-new-%d", i))
		cache.Set(fmt.Sprintf("ns-new-%d", i), fmt.Sprintf("selector-hash-new-%d", i), nil, pods)
	}
	b.StopTimer()

	runtime.GC()
	runtime.ReadMemStats(&memStatsAfter)

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

	for i := 0; i < cacheSize; i++ {
		pods := generateBenchmarkPods(podsPerEntry, fmt.Sprintf("ns-%d", i))
		cache.Set(fmt.Sprintf("ns-%d", i), fmt.Sprintf("selector-hash-%d", i), nil, pods)
	}

	runtime.GC()
	runtime.ReadMemStats(&memStatsAfter)

	memoryUsed := memStatsAfter.Alloc - memStatsBefore.Alloc
	b.ReportMetric(float64(memoryUsed), "bytes/total")
	b.ReportMetric(float64(memoryUsed)/float64(cacheSize), "bytes/entry")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % cacheSize
		cache.Get(fmt.Sprintf("ns-%d", idx), fmt.Sprintf("selector-hash-%d", idx))
		if i%10 == 0 {
			cache.Invalidate(fmt.Sprintf("ns-%d", idx), fmt.Sprintf("selector-hash-%d", idx))
			pods := generateBenchmarkPods(podsPerEntry, fmt.Sprintf("ns-%d", idx))
			cache.Set(fmt.Sprintf("ns-%d", idx), fmt.Sprintf("selector-hash-%d", idx), nil, pods)
		}
	}
}

// BenchmarkSelectorCachePodTrackingMemory measures memory overhead of Pod-to-selector tracking
func BenchmarkSelectorCachePodTrackingMemory(b *testing.B) {
	scenarios := []struct {
		name           string
		cacheSize      int
		podsPerEntry   int
		overlappingPct int
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

	sharedPodCount := (podsPerEntry * overlapPct) / 100
	sharedPods := generateBenchmarkPods(sharedPodCount, "shared-ns")

	for i := 0; i < cacheSize; i++ {
	
uniquePods := generateBenchmarkPods(podsPerEntry-sharedPodCount, fmt.Sprintf("ns-%d", i))
		allPods := append(uniquePods, sharedPods...)
		cache.Set(fmt.Sprintf("ns-%d", i), fmt.Sprintf("selector-hash-%d", i), nil, allPods)
	}

	runtime.GC()
	runtime.ReadMemStats(&memStatsAfter)

	memoryUsed := memStatsAfter.Alloc - memStatsBefore.Alloc
	b.ReportMetric(float64(memoryUsed), "bytes/total")
	b.ReportMetric(float64(memoryUsed)/float64(cacheSize), "bytes/entry")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if sharedPodCount > 0 {
			cache.GetSelectorsForPod(sharedPods[i%sharedPodCount].NamespacedName)
		}
	}
}

// BenchmarkSelectorCacheInvalidationMemory measures memory behavior during invalidation operations
func BenchmarkSelectorCacheInvalidationMemory(b *testing.B) {
	b.Run("InvalidateForPodDelete", func(b *testing.B) {
		benchmarkInvalidationMemory(b, "delete")
	})
	b.Run("InvalidateAll", func(b *testing.B) {
		benchmarkInvalidationMemory(b, "all")
	})
}

func benchmarkInvalidationMemory(b *testing.B, invalidationType string) {
	cacheSize := 100
	podsPerEntry := 50

	cache := NewSelectorCache(cacheSize, 5*time.Minute)
	sharedPods := generateBenchmarkPods(10, "shared-ns")

	for i := 0; i < cacheSize; i++ {
	
uniquePods := generateBenchmarkPods(podsPerEntry-10, fmt.Sprintf("ns-%d", i))
		allPods := append(uniquePods, sharedPods...)
		cache.Set(fmt.Sprintf("ns-%d", i), fmt.Sprintf("selector-hash-%d", i), nil, allPods)
	}

	var memStatsBefore, memStatsAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memStatsBefore)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		switch invalidationType {
		case "delete":
			cache.InvalidateForPodDelete(sharedPods[i%len(sharedPods)].NamespacedName)
		case "all":
			cache.InvalidateAll()
		}

		for j := 0; j < cacheSize; j++ {
		
uniquePods := generateBenchmarkPods(podsPerEntry-10, fmt.Sprintf("ns-%d", j))
			allPods := append(uniquePods, sharedPods...)
			cache.Set(fmt.Sprintf("ns-%d", j), fmt.Sprintf("selector-hash-%d", j), nil, allPods)
		}
	}
	b.StopTimer()

	runtime.GC()
	runtime.ReadMemStats(&memStatsAfter)

	b.ReportMetric(float64(memStatsAfter.TotalAlloc-memStatsBefore.TotalAlloc)/float64(b.N), "bytes/op")
}

// TestSelectorCacheMemoryReport generates a memory usage report
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
			cache.Set(fmt.Sprintf("ns-%d", i), fmt.Sprintf("selector-hash-%d", i), nil, pods)
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
}
