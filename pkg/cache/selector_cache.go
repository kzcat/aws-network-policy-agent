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
	"container/list"
	"sync"
	"time"

	npatypes "github.com/aws/aws-network-policy-agent/pkg/types"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
)

// CacheEntry represents a cached selector-to-Pod mapping with TTL support
type CacheEntry struct {
	Pods         []npatypes.Pod
	Timestamp    time.Time
	SelectorHash string
	Namespace    string                // Namespace of the selector
	Selector     *metav1.LabelSelector // The original selector
}

// SelectorCache provides LRU caching for selector-to-Pod mappings with TTL support.
// It is thread-safe and supports configurable size and TTL.
type SelectorCache struct {
	maxSize int
	ttl     time.Duration

	mu       sync.RWMutex
	cache    map[string]*list.Element // Key is namespace:hash
	lruList  *list.List
	timeFunc func() time.Time // For testing

	// podToSelectors tracks which cache keys contain each Pod
	// This enables efficient invalidation when a Pod is deleted
	podToSelectors map[types.NamespacedName]map[string]struct{}
}

// lruEntry wraps a cache entry with its key for LRU list management
type lruEntry struct {
	key   string
	entry *CacheEntry
}

// NewSelectorCache creates a new SelectorCache with the specified maximum size and TTL.
func NewSelectorCache(maxSize int, ttl time.Duration) *SelectorCache {
	if maxSize <= 0 {
		maxSize = 100 // Default size
	}
	if ttl <= 0 {
		ttl = 5 * time.Minute // Default TTL
	}

	return &SelectorCache{
		maxSize:        maxSize,
		ttl:            ttl,
		cache:          make(map[string]*list.Element),
		lruList:        list.New(),
		timeFunc:       time.Now,
		podToSelectors: make(map[types.NamespacedName]map[string]struct{}),
	}
}

func getCacheKey(namespace, selectorHash string) string {
	return namespace + ":" + selectorHash
}

// Get retrieves a cache entry by namespace and selector hash.
func (c *SelectorCache) Get(namespace, selectorHash string) ([]npatypes.Pod, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := getCacheKey(namespace, selectorHash)
	elem, exists := c.cache[key]
	if !exists {
		return nil, false
	}

	entry := elem.Value.(*lruEntry).entry

	// Check if entry has expired
	if c.timeFunc().Sub(entry.Timestamp) > c.ttl {
		c.removeElement(elem)
		return nil, false
	}

	c.lruList.MoveToFront(elem)

	pods := make([]npatypes.Pod, len(entry.Pods))
	copy(pods, entry.Pods)

	return pods, true
}

// Set adds or updates a cache entry.
func (c *SelectorCache) Set(namespace, selectorHash string, selector *metav1.LabelSelector, pods []npatypes.Pod) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := getCacheKey(namespace, selectorHash)
	podsCopy := make([]npatypes.Pod, len(pods))
	copy(podsCopy, pods)

	entry := &CacheEntry{
		Pods:         podsCopy,
		Timestamp:    c.timeFunc(),
		SelectorHash: selectorHash,
		Namespace:    namespace,
		Selector:     selector,
	}

	if elem, exists := c.cache[key]; exists {
		oldEntry := elem.Value.(*lruEntry).entry
		c.removePodMappings(key, oldEntry.Pods)

		elem.Value.(*lruEntry).entry = entry
		c.lruList.MoveToFront(elem)

		c.addPodMappings(key, podsCopy)
		return
	}

	if c.lruList.Len() >= c.maxSize {
		c.evictOldest()
	}

	lruEnt := &lruEntry{
		key:   key,
		entry: entry,
	}
	elem := c.lruList.PushFront(lruEnt)
	c.cache[key] = elem

	c.addPodMappings(key, podsCopy)
}

// Invalidate removes a specific entry from the cache.
func (c *SelectorCache) Invalidate(namespace, selectorHash string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := getCacheKey(namespace, selectorHash)
	if elem, exists := c.cache[key]; exists {
		c.removeElement(elem)
	}
}

// InvalidateAll clears all entries from the cache.
func (c *SelectorCache) InvalidateAll() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache = make(map[string]*list.Element)
	c.lruList.Init()
	c.podToSelectors = make(map[types.NamespacedName]map[string]struct{})
}

// Size returns the current number of entries in the cache.
func (c *SelectorCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.lruList.Len()
}

func (c *SelectorCache) evictOldest() {
	elem := c.lruList.Back()
	if elem != nil {
		c.removeElement(elem)
	}
}

func (c *SelectorCache) removeElement(elem *list.Element) {
	c.lruList.Remove(elem)
	lruEnt := elem.Value.(*lruEntry)
	c.removePodMappings(lruEnt.key, lruEnt.entry.Pods)
	delete(c.cache, lruEnt.key)
}

func (c *SelectorCache) SetTimeFunc(f func() time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.timeFunc = f
}

func (c *SelectorCache) addPodMappings(key string, pods []npatypes.Pod) {
	for _, pod := range pods {
		if c.podToSelectors[pod.NamespacedName] == nil {
			c.podToSelectors[pod.NamespacedName] = make(map[string]struct{})
		}
		c.podToSelectors[pod.NamespacedName][key] = struct{}{}
	}
}

func (c *SelectorCache) removePodMappings(key string, pods []npatypes.Pod) {
	for _, pod := range pods {
		if selectors, exists := c.podToSelectors[pod.NamespacedName]; exists {
			delete(selectors, key)
			if len(selectors) == 0 {
				delete(c.podToSelectors, pod.NamespacedName)
			}
		}
	}
}

// InvalidateForPodUpdate invalidates cache entries affected by a Pod change (create or label update).
func (c *SelectorCache) InvalidateForPodUpdate(pod *corev1.Pod) []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	podName := types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}
	invalidatedKeys := make(map[string]struct{})

	// 1. Invalidate entries that already contain this Pod (handles label updates where Pod was present)
	if selectors, exists := c.podToSelectors[podName]; exists {
		for key := range selectors {
			invalidatedKeys[key] = struct{}{}
		}
	}

	// 2. Invalidate entries in the same namespace where the Pod NOW matches (handles new Pods or label updates)
	for key, elem := range c.cache {
		entry := elem.Value.(*lruEntry).entry
		if entry.Namespace == pod.Namespace {
			if MatchesLabelSelector(pod.Labels, entry.Selector) {
				invalidatedKeys[key] = struct{}{}
			}
		}
	}

	result := make([]string, 0, len(invalidatedKeys))
	for key := range invalidatedKeys {
		result = append(result, key)
		if elem, exists := c.cache[key]; exists {
			c.removeElement(elem)
		}
	}

	return result
}

// InvalidateForPodDelete invalidates cache entries when a Pod is deleted.
func (c *SelectorCache) InvalidateForPodDelete(podName types.NamespacedName) []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	selectors, exists := c.podToSelectors[podName]
	if !exists {
		return nil
	}

	invalidatedKeys := make([]string, 0, len(selectors))
	for key := range selectors {
		invalidatedKeys = append(invalidatedKeys, key)
	}

	for _, key := range invalidatedKeys {
		if elem, exists := c.cache[key]; exists {
			c.removeElement(elem)
		}
	}

	return invalidatedKeys
}

// MatchesLabelSelector checks if a pod's labels match the given LabelSelector.
func MatchesLabelSelector(podLabels map[string]string, selector *metav1.LabelSelector) bool {
	if selector == nil {
		return true // nil selector matches all pods
	}

	labelSelector, err := metav1.LabelSelectorAsSelector(selector)
	if err != nil {
		return false
	}

	return labelSelector.Matches(labels.Set(podLabels))
}

// GetSelectorsForPod returns the cache keys that contain the specified Pod.
func (c *SelectorCache) GetSelectorsForPod(podName types.NamespacedName) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	selectors, exists := c.podToSelectors[podName]
	if !exists {
		return nil
	}

	result := make([]string, 0, len(selectors))
	for key := range selectors {
		result = append(result, key)
	}
	return result
}

// InvalidateByNamespace is kept for backward compatibility but is now more efficient.
func (c *SelectorCache) InvalidateByNamespace(namespace string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	keysToInvalidate := make([]string, 0)
	for key, elem := range c.cache {
		entry := elem.Value.(*lruEntry).entry
		if entry.Namespace == namespace {
			keysToInvalidate = append(keysToInvalidate, key)
		}
	}

	for _, key := range keysToInvalidate {
		if elem, exists := c.cache[key]; exists {
			c.removeElement(elem)
		}
	}
}