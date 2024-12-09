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

/*
Controller Topo:
Namespace
	-> Cluster
		-> Shard (contains several slots)
			-> Nodes (master and replica)

Probe is maintained at the Controller level.
Any Node has its unique id, and the ProbePool maintains
  a map of Probe with the id as the key.

And in zk or etcd, the topology is stored in a json format directly,
the key is Cluster, and the value is the all the information of the cluster.
*/

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/apache/kvrocks-controller/consts"
	"github.com/apache/kvrocks-controller/logger"
	"github.com/apache/kvrocks-controller/probe"
	"github.com/apache/kvrocks-controller/server/helper"
	"github.com/apache/kvrocks-controller/store"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	ErrClusterNotInitialized = errors.New("CLUSTERDOWN The cluster is not initialized")
	ErrRestoringBackUp       = errors.New("LOADING kvrocks is restoring the db from backup")
)

type ClusterCheckOptions struct {
	checkInterval   time.Duration
	maxFailureCount int64
	quorum          int
}

type ClusterChecker struct {
	options      ClusterCheckOptions
	clusterStore store.Store

	clusterMu sync.RWMutex
	// protected by clusterMu
	cluster *store.Cluster
	// protected by clusterMu
	// node id -> shard index
	nodeShardMap map[string]int

	namespace   string
	clusterName string

	probePool *probe.ProbePool

	ctx    context.Context
	cancel context.CancelFunc
	eg     errgroup.Group

	isControllerLeader func() bool
}

func NewClusterChecker(probePool *probe.ProbePool, s store.Store, namespace, cluster string, isLeader func() bool) *ClusterChecker {
	ctx, cancel := context.WithCancel(context.Background())
	c := &ClusterChecker{
		options: ClusterCheckOptions{
			checkInterval:   time.Second * 3,
			maxFailureCount: 5,
			quorum:          2,
		},
		clusterStore: s,

		clusterMu: sync.RWMutex{},
		cluster:   nil,

		namespace:          namespace,
		clusterName:        cluster,
		probePool:          probePool,
		ctx:                ctx,
		cancel:             cancel,
		eg:                 errgroup.Group{},
		isControllerLeader: isLeader,
	}

	return c
}

func (c *ClusterChecker) Start() {
	c.eg.Go(func() error {
		c.probeLoop()
		return nil
	})

	c.eg.Go(func() error {
		c.migrationLoop()
		return nil
	})
}

func (c *ClusterChecker) GetNodeStatus(nodeID string) (*probe.ProbeStatus, bool) {
	return c.probePool.GetProbeStatus(nodeID)
}

func (c *ClusterChecker) updateProbe(oldInfo, newInfo *store.Cluster) {
	if newInfo == nil {
		logger.Get().Fatal("Cluster info is nil")
		return
	}

	oldNodes := []store.Node{}
	newNodes := []store.Node{}

	if oldInfo != nil {
		for _, shard := range oldInfo.Shards {
			oldNodes = append(oldNodes, shard.Nodes...)
		}
	}

	for _, shard := range newInfo.Shards {
		newNodes = append(newNodes, shard.Nodes...)
	}

	sort.Slice(oldNodes, func(i, j int) bool {
		return oldNodes[i].ID() < oldNodes[j].ID()
	})
	sort.Slice(newNodes, func(i, j int) bool {
		return newNodes[i].ID() < newNodes[j].ID()
	})

	deleteIds := []string{}
	insertNodes := []store.Node{}

	i, j := 0, 0
	for i < len(oldNodes) && j < len(newNodes) {
		oldID := oldNodes[i].ID()
		newID := newNodes[j].ID()

		if oldID == newID {
			i++
			j++
			continue
		}

		if oldID < newID {
			deleteIds = append(deleteIds, oldNodes[i].ID())
			i++
			continue
		}

		if oldID > newID {
			insertNodes = append(insertNodes, newNodes[j])
			j++
			continue
		}
	}

	for i < len(oldNodes) {
		deleteIds = append(deleteIds, oldNodes[i].ID())
		i++
	}

	for j < len(newNodes) {
		insertNodes = append(insertNodes, newNodes[j])
		j++
	}

	c.probePool.RemoveProbes(deleteIds)
	probes := []*probe.Probe{}

	for _, node := range insertNodes {
		probe := probe.NewProbe(node.ID(), c.makeProber(node), c.options.checkInterval)
		probes = append(probes, probe)
	}

	c.probePool.AddProbes(probes)
}

func (c *ClusterChecker) makeProber(node store.Node) probe.ProbeFunc {
	return func(ctx context.Context) error {
		clusterInfo, err := node.GetClusterInfo(ctx)
		if err != nil {
			errMsg := err.Error()

			if strings.Contains(errMsg, ErrRestoringBackUp.Error()) {
				return nil
			}

			if strings.Contains(errMsg, ErrClusterNotInitialized.Error()) {
				return ErrClusterNotInitialized
			}

			return err
		}

		// update the cluster info if the version is newer
		c.clusterMu.RLock()
		defer c.clusterMu.RUnlock()

		if c.cluster == nil {
			return nil
		}

		curVersion := c.cluster.Version.Load()
		if clusterInfo.CurrentEpoch < curVersion {
			go func() {
				logger.Get().Info("Update %s to the latest version", zap.String("node", node.ID()))
				err := node.SyncClusterInfo(ctx, c.cluster)
				if err != nil {
					logger.Get().Error("Failed to sync the cluster info", zap.Error(err))
				}
			}()
		}

		if clusterInfo.CurrentEpoch > curVersion {
			logger.Get().Warn("The node is in a higher version",
				zap.String("node", node.ID()),
				zap.Int64("node.version", clusterInfo.CurrentEpoch),
				zap.Int64("cluster.version", curVersion),
			)
		}

		return nil
	}
}

func (c *ClusterChecker) TryFailOver() {
	// for each leader of the shard, check if it's unhealthy
	// if it's unhealthy, try to failover

	// todo: Here is an area that can be optimized. Perhaps we can send the check tasks for each shard to be handled by the followers
	eg := errgroup.Group{}
	for index, shard := range c.cluster.Shards {
		shard := shard
		eg.Go(func() error {
			c.tryFailOverShard(shard, index)
			return nil
		})
	}
	eg.Wait()
}

func (c *ClusterChecker) tryFailOverShard(shard *store.Shard, shardIndex int) {
	logger := logger.Get().With(
		zap.String("namespace", c.namespace),
		zap.String("cluster", c.clusterName),
		zap.Int("shardIndex", shardIndex),
	)
	// get the leader node
	leader := shard.GetMasterNode()
	if leader == nil {
		err := consts.ErrNoMaster
		logger.Error("Failed to get the leader node", zap.Error(err))
		return
	}

	nodeId := leader.ID()
	// check if the leader is unhealthy
	subjectiveOffline := false
	if status, ok := c.probePool.GetProbeStatus(nodeId); ok {
		logger.Error("Failed to get the probe status, mark the node as healthy by default")
		subjectiveOffline = false
	} else {
		subjectiveOffline = !status.IsHealthy()
	}

	healthy, err := c.judgeNode(nodeId, subjectiveOffline)
	if err != nil {
		logger.Error("Failed to judge the node", zap.Error(err))
		return
	}

	if healthy {
		return
	}

	newLeader, err := shard.PromoteNewMaster(c.ctx, nodeId, "")
	if err != nil {
		logger.Error("Failed to promote the new master", zap.Error(err))
		return
	}
	logger.Info("Promoted the new master", zap.String("new_master", newLeader))
}

func (c *ClusterChecker) judgeNode(nodeID string, subjectiveOffline bool) (bool, error) {
	controllers := c.clusterStore.ListController()
	if len(controllers) < c.options.quorum {
		logger.Get().Error("The controllers are not enough, can't failover")
		return false, consts.ErrControllerNotEnough
	}

	// query other controller to get the node status
	queryChan := make(chan struct {
		status *probe.ProbeStatus
		err    error
	}, len(controllers))
	defer close(queryChan)

	eg := errgroup.Group{}
	for _, controller := range controllers {
		controller := controller
		eg.Go(func() error {
			ctx, cancel := context.WithTimeout(c.ctx, time.Second*3)
			defer cancel()
			status, err := helper.QueryHealthStatus(ctx, controller.Addr, c.namespace, c.clusterName, nodeID)
			queryChan <- struct {
				status *probe.ProbeStatus
				err    error
			}{
				status: status,
				err:    err,
			}

			return nil
		})
	}

	_ = eg.Wait()

	outDatedCount := 0
	healthyCount := 0
	unhealthyCount := 0

	if subjectiveOffline {
		unhealthyCount += 1
	} else {
		healthyCount += 1
	}

	for i := 0; i < len(controllers); i++ {
		result := <-queryChan
		if result.err != nil {
			logger.Get().Warn("Failed to query the health status", zap.Error(result.err))
			continue
		}

		probes := result.status.RecentProbes
		last := probes[len(probes)-1]

		if time.Since(last.Time) > c.options.checkInterval*2 {
			outDatedCount += 1
			continue
		}

		if last.Err == nil {
			healthyCount++
		} else {
			unhealthyCount++
		}
	}

	logger.Get().With(
		zap.String("node", nodeID),
	).Info("The node health status", zap.Int("healthy", healthyCount), zap.Int("unhealthy", unhealthyCount))

	return unhealthyCount >= c.options.quorum, nil
}

func buildShardMap(cluster *store.Cluster) map[string]int {
	nodeShardMap := make(map[string]int)
	for i, shard := range cluster.Shards {
		for _, node := range shard.Nodes {
			nodeShardMap[node.ID()] = i
		}
	}
	return nodeShardMap
}

func (c *ClusterChecker) probeLoop() {
	log := logger.Get().With(
		zap.String("namespace", c.namespace),
		zap.String("clusterName", c.clusterName),
	)
	checkTicker := time.NewTicker(c.options.checkInterval)
	defer checkTicker.Stop()

	probeTick := func() error {
		newClusterInfo, err := c.clusterStore.GetCluster(c.ctx, c.namespace, c.clusterName)
		if err != nil {
			log.Error("Failed to get the cluster info", zap.Error(err))
			return err
		}

		needUpdate := false
		var oldClusterInfo *store.Cluster = nil
		{
			c.clusterMu.RLock()
			if c.cluster == nil {
				needUpdate = true
			} else if c.cluster.Version.Load() < newClusterInfo.Version.Load() {
				needUpdate = true
				oldClusterInfo = c.cluster
			}
			c.clusterMu.RUnlock()
		}

		if needUpdate {
			log.Info("Update the cluster info, new version", zap.Int64("version", newClusterInfo.Version.Load()))
			newMap := buildShardMap(newClusterInfo)
			c.clusterMu.Lock()
			c.cluster = newClusterInfo
			c.nodeShardMap = newMap
			c.clusterMu.Unlock()
			c.updateProbe(oldClusterInfo, newClusterInfo)
		}

		if c.isControllerLeader() {
			c.TryFailOver()
		}

		return nil
	}

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-checkTicker.C:
			_ = probeTick()
		}
	}
}

func (c *ClusterChecker) WithPingInterval(interval time.Duration) *ClusterChecker {
	c.options.checkInterval = interval
	if c.options.checkInterval < 200*time.Millisecond {
		c.options.checkInterval = 200 * time.Millisecond
	}
	return c
}

func (c *ClusterChecker) WithQuorum(quorum int) *ClusterChecker {
	c.options.quorum = quorum
	if c.options.quorum < 1 {
		c.options.quorum = 2
	}
	return c
}

func (c *ClusterChecker) WithMaxFailureCount(count int64) *ClusterChecker {
	c.options.maxFailureCount = count
	if c.options.maxFailureCount < 1 {
		c.options.maxFailureCount = 5
	}
	return c
}

func (c *ClusterChecker) syncClusterToNodes(ctx context.Context) error {
	clusterInfo, err := c.clusterStore.GetCluster(ctx, c.namespace, c.clusterName)
	if err != nil {
		return err
	}
	for _, shard := range clusterInfo.Shards {
		for _, node := range shard.Nodes {
			// sync the clusterName to the latest version
			if err := node.SyncClusterInfo(ctx, clusterInfo); err != nil {
				return err
			}
		}
	}

	// try update
	if c.cluster == nil || c.cluster.Version.Load() < clusterInfo.Version.Load() {
		c.clusterMu.Lock()
		c.cluster = clusterInfo
		c.clusterMu.Unlock()
	}
	return nil
}

func (c *ClusterChecker) sendSyncEvent() {
	if !c.isControllerLeader() {
		logger.Get().Warn("Not the leader, skip the sync event")
		return
	}

	go func() {
		if err := c.syncClusterToNodes(c.ctx); err != nil {
			logger.Get().Error("Failed to sync the cluster info", zap.Error(err))
		}
	}()
}

func (c *ClusterChecker) tryUpdateMigrationStatus(ctx context.Context, cluster *store.Cluster) {
	log := logger.Get().With(
		zap.String("namespace", c.namespace),
		zap.String("cluster", c.clusterName))

	for i, shard := range cluster.Shards {
		if !shard.IsMigrating() {
			continue
		}

		sourceNodeClusterInfo, err := shard.GetMasterNode().GetClusterInfo(ctx)
		if err != nil {
			log.Error("Failed to get the cluster info from the source node", zap.Error(err))
			return
		}
		if sourceNodeClusterInfo.MigratingSlot != shard.MigratingSlot {
			log.Error("Mismatch migrate slot", zap.Int("slot", shard.MigratingSlot))
		}
		if shard.TargetShardIndex < 0 || shard.TargetShardIndex >= len(cluster.Shards) {
			log.Error("Invalid target shard index", zap.Int("index", shard.TargetShardIndex))
		}
		targetMasterNode := cluster.Shards[shard.TargetShardIndex].GetMasterNode()

		switch sourceNodeClusterInfo.MigratingState {
		case "none", "start":
			continue
		case "fail":
			c.clusterMu.Lock()
			cluster.Shards[i].ClearMigrateState()
			c.clusterMu.Unlock()
			if err := c.clusterStore.SetCluster(ctx, c.namespace, cluster); err != nil {
				log.Error("Failed to clear the migrate state", zap.Error(err))
			}
			log.Warn("Failed to migrate the slot", zap.Int("slot", shard.MigratingSlot))
		case "success":
			err := cluster.SetSlot(ctx, shard.MigratingSlot, targetMasterNode.ID())
			if err != nil {
				log.Error("Failed to set the slot", zap.Error(err))
				return
			}
			cluster.Shards[i].SlotRanges = store.RemoveSlotFromSlotRanges(cluster.Shards[i].SlotRanges, shard.MigratingSlot)
			cluster.Shards[shard.TargetShardIndex].SlotRanges = store.AddSlotToSlotRanges(
				cluster.Shards[shard.TargetShardIndex].SlotRanges, shard.MigratingSlot)
			cluster.Shards[i].ClearMigrateState()
			if err := c.clusterStore.SetCluster(ctx, c.namespace, cluster); err != nil {
				log.Error("Failed to update the cluster", zap.Error(err))
			} else {
				log.Info("Migrate the slot successfully", zap.Int("slot", shard.MigratingSlot))
			}
		default:
			log.Error("Unknown migrating state", zap.String("state", sourceNodeClusterInfo.MigratingState))
		}
	}
}

func (c *ClusterChecker) migrationLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if !c.isControllerLeader() {
				continue
			}

			c.clusterMu.Lock()
			cluster := c.cluster
			c.clusterMu.Unlock()
			if cluster == nil {
				continue
			}
			c.tryUpdateMigrationStatus(c.ctx, cluster)
		}
	}
}

func (c *ClusterChecker) Close() {
	c.cancel()
	c.eg.Wait()
}
