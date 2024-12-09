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
package etcd

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/apache/kvrocks-controller/common"
	"github.com/apache/kvrocks-controller/consts"
	"golang.org/x/sync/errgroup"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.etcd.io/etcd/pkg/transport"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/apache/kvrocks-controller/logger"
	"github.com/apache/kvrocks-controller/store/engine"
)

const (
	sessionTTL         = 6
	defaultDailTimeout = 5 * time.Second
)

type Config struct {
	Addrs    []string `yaml:"addrs"`
	Username string   `yaml:"username"`
	Password string   `yaml:"password"`
	TLS      struct {
		Enable        bool   `yaml:"enable"`
		CertFile      string `yaml:"cert_file"`
		KeyFile       string `yaml:"key_file"`
		TrustedCAFile string `yaml:"ca_file"`
	} `yaml:"tls"`
	ElectPath string `yaml:"elect_path"`
}

type Etcd struct {
	client *clientv3.Client
	kv     clientv3.KV

	leaderMu  sync.RWMutex
	leaderID  string
	myID      common.ControllerID
	electPath string
	isReady   atomic.Bool

	controllerListMu sync.RWMutex
	// protected by controllerListMu
	// set<controller>
	controllerList map[common.ControllerID]struct{}

	ctx            context.Context
	cancel         context.CancelFunc
	eg             *errgroup.Group
	electionCh     chan *concurrency.Election
	leaderChangeCh chan bool
}

func New(id common.ControllerID, cfg *Config) (*Etcd, error) {
	clientConfig := clientv3.Config{
		Endpoints:   cfg.Addrs,
		DialTimeout: defaultDailTimeout,
		Logger:      logger.Get(),
	}

	if cfg.TLS.Enable {
		tlsInfo := transport.TLSInfo{
			CertFile:      cfg.TLS.CertFile,
			KeyFile:       cfg.TLS.KeyFile,
			TrustedCAFile: cfg.TLS.TrustedCAFile,
		}
		tlsConfig, err := tlsInfo.ClientConfig()
		if err != nil {
			return nil, err
		}

		clientConfig.TLS = tlsConfig
	}
	if cfg.Username != "" && cfg.Password != "" {
		clientConfig.Username = cfg.Username
		clientConfig.Password = cfg.Password
	}

	client, err := clientv3.New(clientConfig)
	if err != nil {
		return nil, err
	}

	electPath := consts.ElectPath
	if cfg.ElectPath != "" {
		electPath = cfg.ElectPath
	}
	e := &Etcd{
		myID:           id,
		electPath:      electPath,
		client:         client,
		kv:             clientv3.NewKV(client),
		electionCh:     make(chan *concurrency.Election),
		leaderChangeCh: make(chan bool),
	}
	e.isReady.Store(false)

	var ctx context.Context
	ctx, e.cancel = context.WithCancel(context.Background())
	e.eg, e.ctx = errgroup.WithContext(ctx)

	e.eg.Go(func() error {
		return e.observeController()
	})
	e.eg.Go(func() error {
		return e.electLoop()
	})
	e.eg.Go(func() error {
		e.observeLeaderEvent()
		return nil
	})
	e.eg.Go(func() error {
		return e.heartbeatLoop()
	})
	return e, nil
}

func (e *Etcd) ListController() []common.ControllerID {
	list := []common.ControllerID{}

	e.controllerListMu.RLock()
	defer e.controllerListMu.RUnlock()

	for controller := range e.controllerList {
		if e.myID.Addr == controller.Addr {
			continue
		}
		list = append(list, controller)
	}

	return list
}

func (e *Etcd) ID() string {
	return e.myID.String()
}

func (e *Etcd) Leader() string {
	e.leaderMu.RLock()
	defer e.leaderMu.RUnlock()
	return e.leaderID
}

func (e *Etcd) LeaderChange() <-chan bool {
	return e.leaderChangeCh
}

func (e *Etcd) IsReady(ctx context.Context) bool {
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

func (e *Etcd) Get(ctx context.Context, key string) ([]byte, error) {
	rsp, err := e.kv.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(rsp.Kvs) == 0 {
		return nil, consts.ErrNotFound
	}
	return rsp.Kvs[0].Value, nil
}

func (e *Etcd) Exists(ctx context.Context, key string) (bool, error) {
	_, err := e.Get(ctx, key)
	if err != nil {
		if errors.Is(err, consts.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (e *Etcd) Set(ctx context.Context, key string, value []byte) error {
	_, err := e.kv.Put(ctx, key, string(value))
	return err
}

func (e *Etcd) Delete(ctx context.Context, key string) error {
	_, err := e.kv.Delete(ctx, key)
	return err
}

func (e *Etcd) List(ctx context.Context, prefix string) ([]engine.Entry, error) {
	rsp, err := e.kv.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	prefixLen := len(prefix)
	entries := make([]engine.Entry, 0)
	for _, kv := range rsp.Kvs {
		if string(kv.Key) == prefix {
			continue
		}
		key := strings.TrimLeft(string(kv.Key[prefixLen+1:]), "/")
		if strings.ContainsRune(key, '/') {
			continue
		}
		entries = append(entries, engine.Entry{
			Key:   key,
			Value: kv.Value,
		})
	}
	return entries, nil
}

func (e *Etcd) heartbeatLoop() error {
	key := consts.ControllerPath + e.myID.String()

	leaseResp, err := e.client.Grant(e.ctx, sessionTTL)
	if err != nil {
		logger.Get().With(
			zap.Error(err),
		).Error("Failed to gran lease")
		return err
	}

	leaseID := leaseResp.ID

	_, err = e.client.Put(e.ctx, key, "", clientv3.WithLease(leaseID))
	if err != nil {
		logger.Get().With(
			zap.Error(err),
		).Error("Failed to put key")
		return err
	}

	keepAliveCh, err := e.client.KeepAlive(e.ctx, leaseID)
	if err != nil {
		logger.Get().With(
			zap.Error(err),
		).Error("Failed to keep alive")
		return err
	}

	go func() {
		for {
			select {
			case <-e.ctx.Done():
				return
			case kaResp, ok := <-keepAliveCh:
				if !ok {
					logger.Get().Warn("Keep alive channel is closed")
					return
				}

				logger.Get().Sugar().Infof("Lease %v kept alive with TTL %v", leaseID, kaResp.TTL)
			}
		}
	}()

	<-e.ctx.Done()

	// ctx is canceled, use a new context to revoke the lease
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = e.client.Revoke(timeoutCtx, leaseID)

	if err != nil {
		logger.Get().With(
			zap.Error(err),
		).Info("Failed to revoke lease")
	}

	return nil
}

func (e *Etcd) observeController() error {
	handleChange := func(inserts, deletes map[common.ControllerID]struct{}) {
		e.controllerListMu.Lock()
		defer e.controllerListMu.Unlock()

		for id := range inserts {
			if _, ok := e.controllerList[id]; !ok {
				e.controllerList[id] = struct{}{}
			}
		}

		for id := range deletes {
			if _, ok := e.controllerList[id]; ok {
				delete(e.controllerList, id)
			}
		}
	}

	rch := e.client.Watch(e.ctx, consts.ControllerPath, clientv3.WithPrefix())

	// after watch is created, get the current controller list
	resp, err := e.client.Get(e.ctx, consts.ControllerPath, clientv3.WithPrefix())
	if err != nil {
		logger.Get().With(
			zap.Error(err),
		).Error("Failed to get controller list")
		return err
	}

	controllerList := make(map[common.ControllerID]struct{})
	for _, kv := range resp.Kvs {
		rawid := string(kv.Key[len(consts.ControllerPath):])

		id, err := common.ParseControllerID(rawid)

		if err != nil {
			logger.Get().With(
				zap.Error(err),
			).Error("Failed to parse controller ID")
			continue
		}

		controllerList[id] = struct{}{}
	}

	e.controllerListMu.Lock()
	e.controllerList = controllerList
	e.controllerListMu.Unlock()

	for {
		select {
		case <-e.ctx.Done():
			return nil
		case wresp := <-rch:
			if wresp.Err() != nil {
				logger.Get().With(
					zap.Error(wresp.Err()),
				).Error("Watch error")
				return wresp.Err()
			}

			inserts := map[common.ControllerID]struct{}{}
			deletes := map[common.ControllerID]struct{}{}

			for _, ev := range wresp.Events {
				rawid := string(ev.Kv.Key[len(consts.ControllerPath):])
				id, err := common.ParseControllerID(rawid)

				if err != nil {
					logger.Get().With(
						zap.Error(err),
					).Error("Failed to parse controller ID")
					continue
				}

				switch ev.Type {
				case clientv3.EventTypePut:
					inserts[id] = struct{}{}
				case clientv3.EventTypeDelete:
					deletes[id] = struct{}{}
				}
			}
			handleChange(inserts, deletes)
		}
	}

}

func (e *Etcd) electLoop() error {
	for {
		select {
		case <-e.ctx.Done():
			return nil
		default:
		}

		session, err := concurrency.NewSession(e.client, concurrency.WithTTL(sessionTTL))
		if err != nil {
			logger.Get().With(
				zap.Error(err),
			).Error("Failed to create session")
			time.Sleep(sessionTTL / 3)
			continue
		}

		election := concurrency.NewElection(session, e.electPath)
		e.electionCh <- election

		for {
			select {
			case <-e.ctx.Done():
				return nil
			case <-time.After(time.Second):
				if err := election.Campaign(e.ctx, e.myID.String()); err != nil {
					logger.Get().With(
						zap.Error(err),
					).Error("Failed to campaign")
					break
				}
			}
		}
	}
}

func (e *Etcd) observeLeaderEvent() {
	var election *concurrency.Election
	select {
	case elect := <-e.electionCh:
		election = elect
	case <-e.ctx.Done():
		return
	}

	ch := election.Observe(e.ctx)
	for {
		select {
		case resp := <-ch:
			e.isReady.Store(true)
			if len(resp.Kvs) > 0 {
				newLeaderID := string(resp.Kvs[0].Value)
				e.leaderMu.Lock()
				e.leaderID = newLeaderID
				e.leaderMu.Unlock()
				e.leaderChangeCh <- true
				if newLeaderID != "" && newLeaderID == e.leaderID {
					continue
				}
			} else {
				ch = election.Observe(e.ctx)
				e.leaderChangeCh <- false
			}
		case elect := <-e.electionCh:
			election = elect
			ch = election.Observe(e.ctx)
		case <-e.ctx.Done():
			logger.Get().Info("Exit the leader change observe loop")
			return
		}
	}
}

func (e *Etcd) Close() error {
	e.cancel()
	return e.client.Close()
}
