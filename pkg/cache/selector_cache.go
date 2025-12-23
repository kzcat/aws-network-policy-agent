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
	"k8s.io/apimachinery/pkg/types"
)

// CacheEntry represents a cached selector-to-Pod mapping with TTL support
type CacheEntry struct {
	Pods         []npatypes.Pod
	Timestamp    time.Time
	SelectorHash string
}

// SelectorCache provides LRU caching for selector-to-Pod mappings with TTL support.
// It is thread-safe and supports configurable size and TTL.
type SelectorCache struct {
	maxSize int
	ttl     time.Duration

	mu       sync.RWMutex
	cache    map[string]*list.Element
	lruList  *list.List
	timeFunc func() time.Time // For testing

	// podToSelectors tracks which selector hashes contain each Pod
	// This enables efficient invalidation when a Pod changes
	podToSelectors map[types.NamespacedName]map[string]struct{}
}

// lruEntry wraps a cache entry with its key for LRU list management
type lruEntry struct {
	key   string
	entry *CacheEntry
}

// NewSelectorCache creates a new SelectorCache with the specified maximum size and TTL.
// maxSize determines the maximum number of entries in the cache.
// ttl determines how long entries remain valid before expiring.
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

// Get retrieves a cache entry by selector hash.
// Returns the cached Pods and true if found and not expired, nil and false otherwise.
func (c *SelectorCache) Get(selectorHash string) ([]npatypes.Pod, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, exists := c.cache[selectorHash]
	if !exists {
		return nil, false
	}

	entry := elem.Value.(*lruEntry).entry

	// Check if entry has expired
	if c.timeFunc().Sub(entry.Timestamp) > c.ttl {
		// Remove expired entry
		c.removeElement(elem)
		return nil, false
	}

	// Move to front (most recently used)
	c.lruList.MoveToFront(elem)

	// Return a copy of the Pods slice to prevent external modification
	pods := make([]npatypes.Pod, len(entry.Pods))
	copy(pods, entry.Pods)

	return pods, true
}

// Set adds or updates a cache entry for the given selector hash.
// If the cache is at capacity, the least recently used entry is evicted.
func (c *SelectorCache) Set(selectorHash string, pods []npatypes.Pod) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Make a copy of the pods slice to prevent external modification
	podsCopy := make([]npatypes.Pod, len(pods))
	copy(podsCopy, pods)

	entry := &CacheEntry{
		Pods:         podsCopy,
		Timestamp:    c.timeFunc(),
		SelectorHash: selectorHash,
	}

	// Check if entry already exists
	if elem, exists := c.cache[selectorHash]; exists {
		// Remove old Pod-to-selector mappings
		oldEntry := elem.Value.(*lruEntry).entry
		c.removePodMappings(selectorHash, oldEntry.Pods)

		// Update existing entry
		elem.Value.(*lruEntry).entry = entry
		c.lruList.MoveToFront(elem)

		// Add new Pod-to-selector mappings
		c.addPodMappings(selectorHash, podsCopy)
		return
	}

	// Evict LRU entry if at capacity
	if c.lruList.Len() >= c.maxSize {
		c.evictOldest()
	}

	// Add new entry
	lruEnt := &lruEntry{
		key:   selectorHash,
		entry: entry,
	}
	elem := c.lruList.PushFront(lruEnt)
	c.cache[selectorHash] = elem

	// Add Pod-to-selector mappings
	c.addPodMappings(selectorHash, podsCopy)
}

// Invalidate removes a specific entry from the cache by selector hash.
func (c *SelectorCache) Invalidate(selectorHash string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, exists := c.cache[selectorHash]; exists {
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

// evictOldest removes the least recently used entry from the cache.
// Must be called with the lock held.
func (c *SelectorCache) evictOldest() {
	elem := c.lruList.Back()
	if elem != nil {
		c.removeElement(elem)
	}
}

// removeElement removes an element from both the cache map and LRU list.
// Must be called with the lock held.
func (c *SelectorCache) removeElement(elem *list.Element) {
	c.lruList.Remove(elem)
	lruEnt := elem.Value.(*lruEntry)

	// Remove Pod-to-selector mappings
	c.removePodMappings(lruEnt.key, lruEnt.entry.Pods)

	delete(c.cache, lruEnt.key)
}

// SetTimeFunc sets a custom time function for testing purposes.
// This allows tests to control the passage of time for TTL testing.
func (c *SelectorCache) SetTimeFunc(f func() time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.timeFunc = f
}

// addPodMappings adds Pod-to-selector mappings for tracking.
// Must be called with the lock held.
func (c *SelectorCache) addPodMappings(selectorHash string, pods []npatypes.Pod) {
	for _, pod := range pods {
		if c.podToSelectors[pod.NamespacedName] == nil {
			c.podToSelectors[pod.NamespacedName] = make(map[string]struct{})
		}
		c.podToSelectors[pod.NamespacedName][selectorHash] = struct{}{}
	}
}

// removePodMappings removes Pod-to-selector mappings.
// Must be called with the lock held.
func (c *SelectorCache) removePodMappings(selectorHash string, pods []npatypes.Pod) {
	for _, pod := range pods {
		if selectors, exists := c.podToSelectors[pod.NamespacedName]; exists {
			delete(selectors, selectorHash)
			// Clean up empty maps
			if len(selectors) == 0 {
				delete(c.podToSelectors, pod.NamespacedName)
			}
		}
	}
}

// InvalidateForPod invalidates all cache entries that contain the specified Pod.
// This should be called when a Pod's labels change or when a Pod is deleted.
// Returns the list of selector hashes that were invalidated.
func (c *SelectorCache) InvalidateForPod(podName types.NamespacedName) []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	selectors, exists := c.podToSelectors[podName]
	if !exists {
		return nil
	}

	// Collect selector hashes to invalidate
	invalidated := make([]string, 0, len(selectors))
	for selectorHash := range selectors {
		invalidated = append(invalidated, selectorHash)
	}

	// Invalidate each selector's cache entry
	for _, selectorHash := range invalidated {
		if elem, exists := c.cache[selectorHash]; exists {
			c.removeElement(elem)
		}
	}

	return invalidated
}

// InvalidateForPodCreate invalidates all cache entries when a new Pod is created.
// Since a new Pod might match any existing selector, we need to invalidate all entries.
// This is a conservative approach that ensures correctness.
// For more targeted invalidation, the caller should evaluate which selectors
// the new Pod matches and invalidate only those.
func (c *SelectorCache) InvalidateForPodCreate() {
	c.InvalidateAll()
}

// InvalidateForPodLabelChange invalidates cache entries affected by a Pod label change.
// This should be called when a Pod's labels are modified.
// It invalidates entries that contained the Pod (old labels) since the Pod
// may no longer match those selectors, and all entries since the Pod may now
// match new selectors.
func (c *SelectorCache) InvalidateForPodLabelChange(podName types.NamespacedName) []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Get selectors that currently contain this Pod
	selectors, exists := c.podToSelectors[podName]
	if !exists {
		// Pod wasn't in any cached entries, but label change means it might
		// now match some selectors - invalidate all to be safe
		c.cache = make(map[string]*list.Element)
		c.lruList.Init()
		c.podToSelectors = make(map[types.NamespacedName]map[string]struct{})
		return nil
	}

	// Collect selector hashes that were invalidated
	invalidated := make([]string, 0, len(selectors))
	for selectorHash := range selectors {
		invalidated = append(invalidated, selectorHash)
	}

	// Since labels changed, the Pod might now match different selectors
	// Invalidate all entries to ensure correctness
	c.cache = make(map[string]*list.Element)
	c.lruList.Init()
	c.podToSelectors = make(map[types.NamespacedName]map[string]struct{})

	return invalidated
}

// InvalidateForPodDelete invalidates cache entries when a Pod is deleted.
// Returns the list of selector hashes that were invalidated.
func (c *SelectorCache) InvalidateForPodDelete(podName types.NamespacedName) []string {
	return c.InvalidateForPod(podName)
}

// GetSelectorsForPod returns the selector hashes that contain the specified Pod.
// This is useful for debugging and testing.
func (c *SelectorCache) GetSelectorsForPod(podName types.NamespacedName) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	selectors, exists := c.podToSelectors[podName]
	if !exists {
		return nil
	}

	result := make([]string, 0, len(selectors))
	for selectorHash := range selectors {
		result = append(result, selectorHash)
	}
	return result
}

// InvalidateByNamespace invalidates all cache entries that contain Pods in the specified namespace.
// This is called when a Pod in a namespace is created, updated, or deleted to ensure
// that any cached selector results for that namespace are refreshed.
func (c *SelectorCache) InvalidateByNamespace(namespace string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Find all Pods in the specified namespace and collect their selector hashes
	selectorsToInvalidate := make(map[string]struct{})

	for podName, selectors := range c.podToSelectors {
		if podName.Namespace == namespace {
			for selectorHash := range selectors {
				selectorsToInvalidate[selectorHash] = struct{}{}
			}
		}
	}

	// Invalidate all affected cache entries
	for selectorHash := range selectorsToInvalidate {
		if elem, exists := c.cache[selectorHash]; exists {
			c.lruList.Remove(elem)
			lruEnt := elem.Value.(*lruEntry)
			// Remove Pod-to-selector mappings for all Pods in this entry
			c.removePodMappings(lruEnt.key, lruEnt.entry.Pods)
			delete(c.cache, lruEnt.key)
		}
	}
}
