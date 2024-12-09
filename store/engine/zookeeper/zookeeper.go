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
package zookeeper

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/apache/kvrocks-controller/common"
	"github.com/apache/kvrocks-controller/consts"
	"github.com/apache/kvrocks-controller/logger"
	"github.com/apache/kvrocks-controller/store/engine"
	"github.com/go-zookeeper/zk"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	sessionTTL = 6 * time.Second
)

type Config struct {
	Addrs     []string `yaml:"addrs"`
	Scheme    string   `yaml:"scheme"`
	Auth      string   `yaml:"auth"`
	ElectPath string   `yaml:"elect_path"`
}

type Zookeeper struct {
	conn      *zk.Conn
	acl       []zk.ACL // We will set this ACL for the node we have created
	leaderMu  sync.RWMutex
	leaderID  string
	myID      common.ControllerID
	electPath string
	isReady   atomic.Bool

	controllerListMu sync.RWMutex
	// protected by controllerListMu
	// set<controller>
	controllerList map[common.ControllerID]struct{}

	ctx    context.Context
	cancel context.CancelFunc
	eg     *errgroup.Group

	leaderChangeCh chan bool
}

func (e *Zookeeper) ListController() []common.ControllerID {
	e.controllerListMu.RLock()
	defer e.controllerListMu.RUnlock()

	controllers := make([]common.ControllerID, len(e.controllerList))
	for controller := range e.controllerList {
		if controller.Addr == e.myID.Addr {
			continue
		}
		controllers = append(controllers, controller)
	}
	return controllers
}

func New(id common.ControllerID, cfg *Config) (*Zookeeper, error) {
	conn, _, err := zk.Connect(cfg.Addrs, sessionTTL)
	if err != nil {
		return nil, err
	}
	electPath := consts.ElectPath
	if cfg.ElectPath != "" {
		electPath = cfg.ElectPath
	}
	acl := zk.WorldACL(zk.PermAll)
	if cfg.Scheme != "" && cfg.Auth != "" {
		err := conn.AddAuth(cfg.Scheme, []byte(cfg.Auth))
		if err == nil {
			acl = []zk.ACL{{Perms: zk.PermAll, Scheme: cfg.Scheme, ID: cfg.Auth}}
		} else {
			logger.Get().Warn("Zookeeper addAuth fail: " + err.Error())
		}
	}
	e := &Zookeeper{
		myID:           id,
		acl:            acl,
		electPath:      electPath,
		conn:           conn,
		leaderChangeCh: make(chan bool),
	}

	e.isReady.Store(false)

	var ctx context.Context
	ctx, e.cancel = context.WithCancel(context.Background())
	e.eg, e.ctx = errgroup.WithContext(ctx)

	e.eg.Go(func() error {
		e.electLoop()
		return nil
	})

	e.eg.Go(func() error {
		e.observeControllers()
		return nil
	})

	e.eg.Go(func() error {
		e.heartbeatLoop()
		return nil
	})

	return e, nil
}

func (e *Zookeeper) ID() string {
	return e.myID.String()
}

func (e *Zookeeper) Leader() string {
	e.leaderMu.RLock()
	defer e.leaderMu.RUnlock()
	return e.leaderID
}

func (e *Zookeeper) LeaderChange() <-chan bool {
	return e.leaderChangeCh
}

func (e *Zookeeper) IsReady(ctx context.Context) bool {
	for {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(100 * time.Millisecond):
			if e.isReady.Load() {
				return true
			}
		case <-ctx.Done():
			return e.isReady.Load()
		}
	}
}

func (e *Zookeeper) Get(ctx context.Context, key string) ([]byte, error) {
	data, _, err := e.conn.Get(key)
	if err != nil {
		if errors.Is(err, zk.ErrNoNode) {
			return nil, nil // Key does not exist
		}
		return nil, err
	}

	return data, nil
}

func (e *Zookeeper) Exists(ctx context.Context, key string) (bool, error) {
	exists, _, err := e.conn.Exists(key)
	if err != nil {
		return false, err
	}
	return exists, nil
}

// If the key exists, it will be set; if not, it will be created.
func (e *Zookeeper) Set(ctx context.Context, key string, value []byte) error {
	exist, _ := e.Exists(ctx, key)
	if exist {
		_, err := e.conn.Set(key, value, -1)
		return err
	}

	return e.Create(ctx, key, value, 0)
}

func (e *Zookeeper) Create(ctx context.Context, key string, value []byte, flags int32) error {
	lastSlashIndex := strings.LastIndex(key, "/")
	if lastSlashIndex > 0 {
		substring := key[:lastSlashIndex]
		// If the parent node does not exist, create the parent node recursively
		exist, _ := e.Exists(ctx, substring)
		if !exist {
			err := e.Create(ctx, substring, []byte{}, 0)
			if err != nil {
				return err
			}
		}
	}
	_, err := e.conn.Create(key, value, flags, e.acl)
	return err
}

func (e *Zookeeper) Delete(ctx context.Context, key string) error {
	err := e.conn.Delete(key, -1)
	if errors.Is(err, zk.ErrNoNode) {
		return nil // Key does not exist
	}
	return err
}

func (e *Zookeeper) List(ctx context.Context, prefix string) ([]engine.Entry, error) {
	children, _, err := e.conn.Children(prefix)
	if errors.Is(err, zk.ErrNoNode) {
		return []engine.Entry{}, nil
	} else if err != nil {
		return nil, err
	}

	entries := make([]engine.Entry, 0)
	for _, child := range children {
		key := prefix + "/" + child
		data, _, err := e.conn.Get(key)
		if err != nil {
			return nil, err
		}

		entry := engine.Entry{
			Key:   child,
			Value: data,
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

func (e *Zookeeper) SetleaderID(newLeaderID string) {
	if newLeaderID != "" && newLeaderID != e.leaderID {
		if !e.isReady.Load() {
			// we set ready flag when leaderID first changed
			e.isReady.Store(true)
		}
		e.leaderMu.Lock()
		e.leaderID = newLeaderID
		e.leaderMu.Unlock()
		e.leaderChangeCh <- true
	}
}

func (e *Zookeeper) heartbeatLoop() error {
	key := consts.ControllerPath + e.myID.String()
	_, err := e.conn.Create(key, []byte{}, zk.FlagEphemeral, e.acl)
	if err != nil {
		logger.Get().Fatal("Create controller node fail: " + err.Error())
		return err
	}

	go func() {
		for {
			<-e.ctx.Done()
			logger.Get().Info("Exit the heartbeat loop")
			return
		}
	}()

	<-e.ctx.Done()
	logger.Get().Info("Exit the heartbeat loop, ephemeral node %s will be deleted", zap.String("key", key))
	return nil
}

func (e *Zookeeper) observeControllers() error {
	controllersZK, _, events, err := e.conn.ChildrenW(consts.ControllerPath)
	if err != nil {
		logger.Get().Fatal("Observe controllers fail: " + err.Error())
		return err
	}

	controllers := map[common.ControllerID]struct{}{}
	for _, idraw := range controllersZK {
		id, err := common.ParseControllerID(idraw)
		if err != nil {
			logger.Get().With(zap.Error(err)).Error("Parse controller ID fail")
			continue
		}

		controllers[id] = struct{}{}
	}

	e.controllerListMu.Lock()
	e.controllerList = controllers
	e.controllerListMu.Unlock()

	for {
		select {
		case <-e.ctx.Done():
			logger.Get().Info("Exit the observe controllers loop")
			return nil
		case event := <-events:
			if event.Err != nil {
				logger.Get().With(
					zap.Error(event.Err),
				).Error("Watch error")
				return event.Err
			}

			id, err := common.ParseControllerID(event.Path)
			if err != nil {
				logger.Get().With(zap.Error(err)).Error("Parse controller ID fail")
				continue
			}

			switch event.Type {
			case zk.EventNodeCreated:
				e.controllerListMu.Lock()
				e.controllerList[id] = struct{}{}
				e.controllerListMu.Unlock()

			case zk.EventNodeDeleted:
				if _, ok := controllers[id]; ok {
					e.controllerListMu.Lock()
					delete(e.controllerList, id)
					e.controllerListMu.Unlock()
				}
			default:
				logger.Get().Info("Received Event Type = %v", zap.Any("event", event))
			}

		}
	}

}

func (e *Zookeeper) electLoop() error {
	createNode := func() (cur []byte, ch <-chan zk.Event, err error) {
		err = e.Create(e.ctx, e.electPath, []byte(e.myID.String()), zk.FlagEphemeral)
		if err != nil && !errors.Is(err, zk.ErrNodeExists) {
			return
		}
		cur, _, ch, err = e.conn.GetW(e.electPath)
		return
	}

	for {
		cur, ch, err := createNode()
		if err != nil {
			logger.Get().Error("Create node fail: " + err.Error())
			time.Sleep(sessionTTL / 3)
			continue
		}
		e.SetleaderID(string(cur))

	inner:
		for {
			select {
			case resp := <-ch:
				if resp.Type == zk.EventNodeDeleted {
					break inner
				}
			case <-e.ctx.Done():
				logger.Get().Info("Exit the leader election loop")
				return nil
			}
		}
	}
}

func (e *Zookeeper) Close() error {
	e.cancel()
	e.eg.Wait()
	e.conn.Close()
	return nil
}
