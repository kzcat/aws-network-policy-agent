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
	"testing"
	"time"

	policyk8sawsv1 "github.com/aws/aws-network-policy-agent/api/v1alpha1"
	npatypes "github.com/aws/aws-network-policy-agent/pkg/types"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
			PodIP:          policyk8sawsv1.NetworkAddress("10.0.0.1"),
		},
		{
			NamespacedName: types.NamespacedName{Name: "pod2", Namespace: "default"},
			PodIP:          policyk8sawsv1.NetworkAddress("10.0.0.2"),
		},
	}

	selector := &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}}

	// Set entry
	cache.Set("default", "hash123", selector, pods)

	// Get entry
	result, found := cache.Get("default", "hash123")
	assert.True(t, found)
	assert.Equal(t, len(pods), len(result))
	assert.Equal(t, pods[0].Name, result[0].Name)
	assert.Equal(t, pods[1].Name, result[1].Name)

	// Get with different namespace should fail
	_, found = cache.Get("other", "hash123")
	assert.False(t, found)
}

func TestSelectorCache_GetNonExistent(t *testing.T) {
	cache := NewSelectorCache(10, 5*time.Minute)

	result, found := cache.Get("default", "nonexistent")
	assert.False(t, found)
	assert.Nil(t, result)
}

func TestSelectorCache_TTLExpiration(t *testing.T) {
	cache := NewSelectorCache(10, 1*time.Second)

	pods := []npatypes.Pod{
		{
			NamespacedName: types.NamespacedName{Name: "pod1", Namespace: "default"},
			PodIP:          policyk8sawsv1.NetworkAddress("10.0.0.1"),
		},
	}

	// Use a mock time function
	currentTime := time.Now()
	cache.SetTimeFunc(func() time.Time { return currentTime })

	// Set entry
	cache.Set("default", "hash123", nil, pods)

	// Entry should be found immediately
	result, found := cache.Get("default", "hash123")
	assert.True(t, found)
	assert.NotNil(t, result)

	// Advance time past TTL
	currentTime = currentTime.Add(2 * time.Second)

	// Entry should be expired
	result, found = cache.Get("default", "hash123")
	assert.False(t, found)
	assert.Nil(t, result)

	// Cache should be empty after expired entry is removed
	assert.Equal(t, 0, cache.Size())
}

func TestSelectorCache_LRUEviction(t *testing.T) {
	cache := NewSelectorCache(3, 5*time.Minute)

	// Add 3 entries
	cache.Set("default", "hash1", nil, []npatypes.Pod{{NamespacedName: types.NamespacedName{Name: "pod1", Namespace: "default"}}})
	cache.Set("default", "hash2", nil, []npatypes.Pod{{NamespacedName: types.NamespacedName{Name: "pod2", Namespace: "default"}}})
	cache.Set("default", "hash3", nil, []npatypes.Pod{{NamespacedName: types.NamespacedName{Name: "pod3", Namespace: "default"}}})

	assert.Equal(t, 3, cache.Size())

	// Access hash1 to make it recently used
	_, found := cache.Get("default", "hash1")
	assert.True(t, found)

	// Add a 4th entry, should evict hash2 (least recently used)
	cache.Set("default", "hash4", nil, []npatypes.Pod{{NamespacedName: types.NamespacedName{Name: "pod4", Namespace: "default"}}})

	assert.Equal(t, 3, cache.Size())

	// hash2 should be evicted
	_, found = cache.Get("default", "hash2")
	assert.False(t, found)

	// hash1, hash3, hash4 should still exist
	_, found = cache.Get("default", "hash1")
	assert.True(t, found)
	_, found = cache.Get("default", "hash3")
	assert.True(t, found)
	_, found = cache.Get("default", "hash4")
	assert.True(t, found)
}

func TestSelectorCache_UpdateExisting(t *testing.T) {
	cache := NewSelectorCache(10, 5*time.Minute)

	pods1 := []npatypes.Pod{
		{NamespacedName: types.NamespacedName{Name: "pod1", Namespace: "default"}},
	}
	pods2 := []npatypes.Pod{
		{NamespacedName: types.NamespacedName{Name: "pod2", Namespace: "default"}},
		{NamespacedName: types.NamespacedName{Name: "pod3", Namespace: "default"}},
	}

	// Set initial entry
	cache.Set("default", "hash123", nil, pods1)
	result, _ := cache.Get("default", "hash123")
	assert.Equal(t, 1, len(result))

	// Update entry
	cache.Set("default", "hash123", nil, pods2)
	result, _ = cache.Get("default", "hash123")
	assert.Equal(t, 2, len(result))
	assert.Equal(t, "pod2", result[0].Name)

	// Size should still be 1
	assert.Equal(t, 1, cache.Size())
}

func TestSelectorCache_Invalidate(t *testing.T) {
	cache := NewSelectorCache(10, 5*time.Minute)

	cache.Set("default", "hash1", nil, []npatypes.Pod{{NamespacedName: types.NamespacedName{Name: "pod1", Namespace: "default"}}})
	cache.Set("default", "hash2", nil, []npatypes.Pod{{NamespacedName: types.NamespacedName{Name: "pod2", Namespace: "default"}}})

	assert.Equal(t, 2, cache.Size())

	// Invalidate hash1
	cache.Invalidate("default", "hash1")

	assert.Equal(t, 1, cache.Size())

	_, found := cache.Get("default", "hash1")
	assert.False(t, found)

	_, found = cache.Get("default", "hash2")
	assert.True(t, found)
}

func TestSelectorCache_InvalidateAll(t *testing.T) {
	cache := NewSelectorCache(10, 5*time.Minute)

	cache.Set("default", "hash1", nil, []npatypes.Pod{{NamespacedName: types.NamespacedName{Name: "pod1", Namespace: "default"}}})
	cache.Set("default", "hash2", nil, []npatypes.Pod{{NamespacedName: types.NamespacedName{Name: "pod2", Namespace: "default"}}})

	assert.Equal(t, 2, cache.Size())

	cache.InvalidateAll()

	assert.Equal(t, 0, cache.Size())
}

func TestSelectorCache_InvalidateForPodUpdate(t *testing.T) {
	cache := NewSelectorCache(10, 5*time.Minute)

	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod1",
			Namespace: "default",
			Labels:    map[string]string{"app": "test"},
		},
	}

	selectorMatch := &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}}
	selectorNoMatch := &metav1.LabelSelector{MatchLabels: map[string]string{"app": "other"}}

	// 1. Entry containing the pod
	cache.Set("default", "hash1", selectorMatch, []npatypes.Pod{{NamespacedName: types.NamespacedName{Name: "pod1", Namespace: "default"}}})
	// 2. Entry in same namespace matching pod labels
	cache.Set("default", "hash2", selectorMatch, []npatypes.Pod{{NamespacedName: types.NamespacedName{Name: "pod2", Namespace: "default"}}})
	// 3. Entry in same namespace NOT matching pod labels
	cache.Set("default", "hash3", selectorNoMatch, []npatypes.Pod{{NamespacedName: types.NamespacedName{Name: "pod3", Namespace: "default"}}})
	// 4. Entry in different namespace matching pod labels
	cache.Set("other", "hash4", selectorMatch, []npatypes.Pod{{NamespacedName: types.NamespacedName{Name: "pod4", Namespace: "other"}}})

	assert.Equal(t, 4, cache.Size())

	// Invalidate for pod1 change
	invalidated := cache.InvalidateForPodUpdate(pod1)

	// Should have invalidated hash1 (presence) and hash2 (labels match)
	assert.Equal(t, 2, len(invalidated))
	assert.Contains(t, invalidated, "default:hash1")
	assert.Contains(t, invalidated, "default:hash2")

	// Remaining should be hash3 and hash4
	assert.Equal(t, 2, cache.Size())
	_, found := cache.Get("default", "hash3")
	assert.True(t, found)
	_, found = cache.Get("other", "hash4")
	assert.True(t, found)
}

func TestSelectorCache_InvalidateForPodDelete(t *testing.T) {
	cache := NewSelectorCache(10, 5*time.Minute)

	pod1 := npatypes.Pod{NamespacedName: types.NamespacedName{Name: "pod1", Namespace: "default"}}
	cache.Set("default", "hash1", nil, []npatypes.Pod{pod1})
	cache.Set("default", "hash2", nil, []npatypes.Pod{{NamespacedName: types.NamespacedName{Name: "pod2", Namespace: "default"}}})

	assert.Equal(t, 2, cache.Size())

	// Delete pod1
	cache.InvalidateForPodDelete(pod1.NamespacedName)

	assert.Equal(t, 1, cache.Size())
	_, found := cache.Get("default", "hash1")
	assert.False(t, found)
}

func TestSelectorCache_InvalidateByNamespace(t *testing.T) {
	cache := NewSelectorCache(10, 5*time.Minute)

	cache.Set("ns1", "hash1", nil, nil)
	cache.Set("ns1", "hash2", nil, nil)
	cache.Set("ns2", "hash3", nil, nil)

	assert.Equal(t, 3, cache.Size())

	cache.InvalidateByNamespace("ns1")

	assert.Equal(t, 1, cache.Size())
	_, found := cache.Get("ns2", "hash3")
	assert.True(t, found)
}