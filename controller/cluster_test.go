/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package controller

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/kvrocks-controller/consts"
	"github.com/apache/kvrocks-controller/probe"
	"github.com/apache/kvrocks-controller/store"
	"github.com/apache/kvrocks-controller/store/engine"
)

type MockClusterStore struct {
	*store.ClusterStore

	mu       sync.Mutex
	clusters map[string]*store.Cluster
}

func NewMockClusterStore() *MockClusterStore {
	return &MockClusterStore{
		ClusterStore: store.NewClusterStore(engine.NewMock()),
		clusters:     make(map[string]*store.Cluster),
	}
}

func (s *MockClusterStore) CreateCluster(ctx context.Context, ns string, cluster *store.Cluster) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clusters[cluster.Name] = cluster
	return nil
}

func (s *MockClusterStore) GetCluster(ctx context.Context, ns, cluster string) (*store.Cluster, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.clusters[cluster]
	if !ok {
		return nil, consts.ErrNotFound
	}
	return c, nil
}

func (s *MockClusterStore) UpdateCluster(ctx context.Context, ns string, cluster *store.Cluster) error {
	cluster.Version.Inc()
	return s.SetCluster(ctx, ns, cluster)
}

func (s *MockClusterStore) SetCluster(ctx context.Context, ns string, cluster *store.Cluster) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clusters[cluster.Name] = cluster
	return nil
}

func (s *MockClusterStore) RemoveCluster(ctx context.Context, ns, cluster string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.clusters, cluster)
	return nil
}

func TestCluster_LoadAndProbe(t *testing.T) {
	ctx := context.Background()
	ns := "test-ns"
	clusterName := "test-clusterProbe"
	cluster, err := store.NewCluster(clusterName, []string{"127.0.0.1:7770", "127.0.0.1:7771"}, 2)
	require.NoError(t, err)

	nodes := make([]*store.ClusterNode, 0)
	for _, shard := range cluster.Shards {
		for _, node := range shard.Nodes {
			clusterNode, _ := node.(*store.ClusterNode)
			nodes = append(nodes, clusterNode)
		}
	}
	require.NoError(t, cluster.Reset(ctx))
	defer func() {
		require.NoError(t, cluster.Reset(ctx))
	}()

	s := NewMockClusterStore()
	require.NoError(t, s.CreateCluster(ctx, ns, cluster))
	probePool := probe.NewProbePool(ctx)
	defer probePool.Stop()

	clusterProbe := NewClusterChecker(probePool, s, ns, clusterName, func() bool {
		return true
	})
	clusterProbe.WithPingInterval(100 * time.Millisecond)
	clusterProbe.Start()
	defer clusterProbe.Close()

	ticker := time.NewTicker(400 * time.Millisecond)
	defer ticker.Stop()
	<-ticker.C

	for _, node := range nodes {
		info, err := node.GetClusterInfo(ctx)
		require.NoError(t, err)
		require.EqualValues(t, 1, info.CurrentEpoch)
	}
	require.NoError(t, s.UpdateCluster(ctx, ns, cluster))

	<-ticker.C
	// should sync the clusterName info
	for _, node := range nodes {
		info, err := node.GetClusterInfo(ctx)
		require.NoError(t, err)
		require.EqualValues(t, 2, info.CurrentEpoch)
	}
	require.NoError(t, s.UpdateCluster(ctx, ns, cluster))
	clusterProbe.sendSyncEvent()
	<-ticker.C
	for _, node := range nodes {
		info, err := node.GetClusterInfo(ctx)
		require.NoError(t, err)
		require.EqualValues(t, 3, info.CurrentEpoch)
	}
}

func TestCluster_MigrateSlot(t *testing.T) {
	ctx := context.Background()
	ns := "test-ns"
	clusterName := "test-clusterProbe"
	cluster, err := store.NewCluster(clusterName, []string{"127.0.0.1:7770", "127.0.0.1:7771"}, 1)
	require.NoError(t, err)

	require.NoError(t, cluster.Reset(ctx))
	require.NoError(t, cluster.SyncToNodes(ctx))
	defer func() {
		require.NoError(t, cluster.Reset(ctx))
	}()
	require.NoError(t, cluster.MigrateSlot(ctx, 0, 1, false))

	s := NewMockClusterStore()
	require.NoError(t, s.CreateCluster(ctx, ns, cluster))

	probePool := probe.NewProbePool(ctx)
	defer probePool.Stop()

	clusterProbe := NewClusterChecker(probePool, s, ns, clusterName, func() bool {
		return true
	})
	clusterProbe.WithPingInterval(100 * time.Millisecond)
	clusterProbe.Start()
	defer clusterProbe.Close()

	ticker := time.NewTicker(400 * time.Millisecond)
	defer ticker.Stop()
	<-ticker.C
}
