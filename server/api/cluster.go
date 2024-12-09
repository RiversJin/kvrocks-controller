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

package api

import (
	"errors"
	"fmt"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/apache/kvrocks-controller/consts"
	"github.com/apache/kvrocks-controller/controller"
	"github.com/apache/kvrocks-controller/probe"
	"github.com/apache/kvrocks-controller/server/helper"
	"github.com/apache/kvrocks-controller/store"
)

type MigrateSlotRequest struct {
	Target   int  `json:"target" validate:"required"`
	Slot     int  `json:"slot" validate:"required"`
	SlotOnly bool `json:"slot_only"`
}

type CreateClusterRequest struct {
	Name     string   `json:"name" validate:"required"`
	Nodes    []string `json:"nodes" validate:"required"`
	Password string   `json:"password"`
	Replicas int      `json:"replicas"`
}

type ImportClusterRequest struct {
	Nodes    []string `json:"nodes" validate:"required"`
	Password string   `json:"password"`
}

type ClusterHandler struct {
	s store.Store
	c *controller.Controller
}

type ListClusterResponse struct {
	Clusters []string `json:"clusters"`
}

type CreateClusterResponse struct {
	Cluster store.Cluster `json:"cluster"`
}

// @Summary		List clusters
// @Description	List all clusters
// @Tags			cluster
// @Param			namespace	path		string										true	"Namespace"
// @Success		200			{object}	helper.Response{data=ListClusterResponse}	"集群列表"
// @Router			/namespaces/{namespace}/clusters [get]
func (handler *ClusterHandler) List(c *gin.Context) {
	namespace := c.Param("namespace")
	clusters, err := handler.s.ListCluster(c, namespace)
	if err != nil && !errors.Is(err, consts.ErrNotFound) {
		helper.ResponseError(c, err)
		return
	}
	helper.ResponseOK(c, gin.H{"clusters": clusters})
}

// @Summary		Get a cluster
// @Description	Get a cluster by providing cluster name
// @Tags			cluster
// @Produce		json
// @Param			namespace	path		string	true	"Namespace"
// @Param			cluster		path		string	true	"Cluster"
// @Success		200			{object}	helper.Response{data=CreateClusterResponse}
// @Router			/namespaces/{namespace}/clusters/{cluster} [get]
func (handler *ClusterHandler) Get(c *gin.Context) {
	cluster, _ := c.MustGet(consts.ContextKeyCluster).(*store.Cluster)
	helper.ResponseOK(c, gin.H{"cluster": cluster})
}

// @Summary		Create a cluster
// @Description	Create a cluster
// @Tags			cluster
// @Produce		json
// @Param			namespace	path		string					true	"Namespace"
// @Param			cluster		body		CreateClusterRequest	true	"Cluster"
// @Success		201			{object}	helper.Response{data=CreateClusterResponse}
// @Router			/namespaces/{namespace}/clusters [post]
func (handler *ClusterHandler) Create(c *gin.Context) {
	namespace := c.Param("namespace")
	var req CreateClusterRequest
	if err := c.BindJSON(&req); err != nil {
		helper.ResponseBadRequest(c, err)
		return
	}

	clusterStore := handler.s
	if err := clusterStore.CheckNewNodes(c, req.Nodes); err != nil {
		helper.ResponseError(c, err)
		return
	}

	cluster, err := store.NewCluster(req.Name, req.Nodes, req.Replicas)
	if err != nil {
		helper.ResponseBadRequest(c, err)
		return
	}
	cluster.SetPassword(req.Password)
	checkClusterMode := strings.ToLower(c.GetHeader(consts.HeaderDontCheckClusterMode)) == "yes"
	for _, node := range cluster.GetNodes() {
		if !checkClusterMode {
			break
		}
		version, err := node.CheckClusterMode(c)
		if err != nil {
			helper.ResponseError(c, err)
			return
		}
		if version != -1 {
			helper.ResponseBadRequest(c, errors.New("node is already in cluster mode"))
			return
		}
	}

	if err := clusterStore.CreateCluster(c, namespace, cluster); err != nil {
		helper.ResponseError(c, err)
		return
	}
	helper.ResponseCreated(c, gin.H{"cluster": cluster})
}

// @Summary		Remove a cluster
// @Description	Remove a cluster by providing cluster name
// @Tags			cluster
// @Param			namespace	path	string	true	"Namespace"
// @Param			cluster		path	string	true	"Cluster"
// @Router			/namespaces/{namespace}/clusters/{cluster} [delete]
// @Success		204
func (handler *ClusterHandler) Remove(c *gin.Context) {
	namespace := c.Param("namespace")
	cluster := c.Param("cluster")
	err := handler.s.RemoveCluster(c, namespace, cluster)
	if err != nil {
		helper.ResponseError(c, err)
		return
	}
	helper.ResponseNoContent(c)
}

// @Summary		Migrate slot
// @Description	Migrate slot to another node
// @Tags			cluster
// @Param			namespace	path	string				true	"Namespace"
// @Param			migrate		body	MigrateSlotRequest	true	"Migrate"
// @Router			/namespaces/{namespace}/clusters/{cluster}/migrate [post]
func (handler *ClusterHandler) MigrateSlot(c *gin.Context) {
	namespace := c.Param("namespace")
	cluster, _ := c.MustGet(consts.ContextKeyCluster).(*store.Cluster)

	var req MigrateSlotRequest
	if err := c.BindJSON(&req); err != nil {
		helper.ResponseBadRequest(c, err)
		return
	}

	err := cluster.MigrateSlot(c, req.Slot, req.Target, req.SlotOnly)
	if err != nil {
		helper.ResponseError(c, err)
		return
	}

	if req.SlotOnly {
		err = handler.s.UpdateCluster(c, namespace, cluster)
	} else {
		// The version should be increased after the slot migration is done
		err = handler.s.SetCluster(c, namespace, cluster)
	}
	if err != nil {
		helper.ResponseError(c, err)
		return
	}
	helper.ResponseOK(c, gin.H{"cluster": cluster})
}

// @Summary		Import nodes to a cluster
// @Description	Import nodes to a specified, existing cluster
// @Tags			cluster
// @Produce		json
// @Param			namespace	path		string					true	"Namespace"
// @Param			cluster		path		string					true	"Cluster"
// @Param			import		body		ImportClusterRequest	true	"Import Info"
// @Success		200			{object}	helper.Response{data=CreateClusterResponse}
// @Router			/namespaces/{namespace}/clusters/{cluster}/import [post]
func (handler *ClusterHandler) Import(c *gin.Context) {
	namespace := c.Param("namespace")
	clusterName := c.Param("cluster")
	var req ImportClusterRequest
	if err := c.BindJSON(&req); err != nil {
		helper.ResponseBadRequest(c, err)
		return
	}
	if len(req.Nodes) == 0 {
		helper.ResponseBadRequest(c, errors.New("nodes should NOT be empty"))
		return
	}

	firstNode := store.NewClusterNode(req.Nodes[0], req.Password)
	clusterNodesStr, err := firstNode.GetClusterNodesString(c)
	if err != nil {
		helper.ResponseError(c, err)
		return
	}
	cluster, err := store.ParseCluster(clusterNodesStr)
	if err != nil {
		helper.ResponseError(c, err)
		return
	}
	cluster.SetPassword(req.Password)

	newNodes := make([]string, 0)
	for _, node := range cluster.GetNodes() {
		newNodes = append(newNodes, node.Addr())
	}
	if err := handler.s.CheckNewNodes(c, newNodes); err != nil {
		helper.ResponseError(c, err)
		return
	}

	cluster.Name = clusterName
	if err := handler.s.CreateCluster(c, namespace, cluster); err != nil {
		helper.ResponseError(c, err)
		return
	}
	helper.ResponseOK(c, gin.H{"cluster": cluster})
}

// for swagger doc
var _ probe.ProbeStatus

// @Summary Query the health status of a specified node in a cluster
// @Tags cluster health
// @Produce json
// @Router /namespaces/{namespace}/clusters/{cluster}/nodes/{id}/health [get]
// @Param namespace path string true "Namespace"
// @Param cluster path string true "Cluster"
// @Param id path string true "Node ID"
// @Success 200 {object} helper.Response{data=probe.ProbeStatus}
func (handler *ClusterHandler) HealthCheck(c *gin.Context) {
	namespace := c.Param("namespace")
	cluster := c.Param("cluster")
	nodeID := c.Param("id")

	clusterChecker, err := handler.c.GetCluster(namespace, cluster)

	if err != nil {
		helper.ResponseError(c, fmt.Errorf("failed to get cluster: %w", err))
		return
	}

	status, ok := clusterChecker.GetNodeStatus(nodeID)

	if !ok {
		helper.ResponseError(c, fmt.Errorf("node %s not found", nodeID))
		return
	}

	helper.ResponseOK(c, status)
}
