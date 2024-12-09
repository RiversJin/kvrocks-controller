package probe

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/apache/kvrocks-controller/logger"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type UnhealthyProbe struct {
	ProbeID string
	Result  ProbeResult
}

type ProbePool struct {
	probes   map[string]*Probe
	probesMu sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
	eg     errgroup.Group

	unhealthyProbes       []UnhealthyProbe
	unhealthyProbesMu     sync.RWMutex
	unhealthyProbeVersion atomic.Int32
}

func NewProbePool(ctx context.Context) *ProbePool {
	probeCtx, cancel := context.WithCancel(ctx)

	return &ProbePool{
		probes:   map[string]*Probe{},
		probesMu: sync.RWMutex{},

		ctx:    probeCtx,
		cancel: cancel,
		eg:     errgroup.Group{},

		unhealthyProbes:       []UnhealthyProbe{},
		unhealthyProbesMu:     sync.RWMutex{},
		unhealthyProbeVersion: atomic.Int32{},
	}
}

func (pp *ProbePool) AddProbes(probes []*Probe) {
	pp.probesMu.Lock()
	defer pp.probesMu.Unlock()

	curVersion := pp.unhealthyProbeVersion.Load()
	notifyCB := func(id string, probeResult ProbeResult) {
		if pp.unhealthyProbeVersion.Load() != curVersion {
			logger.Get().Info("unhealthy probe version changed, ignore notify", zap.String("probe_id", id))
			return
		}

		pp.probesMu.RLock()
		defer pp.probesMu.RUnlock()

		if _, ok := pp.probes[id]; !ok {
			logger.Get().Info("probe not found", zap.String("probe_id", id))
			return
		}

		pp.unhealthyProbesMu.Lock()
		defer pp.unhealthyProbesMu.Unlock()

		pp.unhealthyProbes = append(pp.unhealthyProbes, UnhealthyProbe{
			ProbeID: id,
			Result:  probeResult,
		})
	}

	for _, p := range probes {
		if old, ok := pp.probes[p.ID]; ok {
			old.stop()
		}

		pp.probes[p.ID] = p
		pp.eg.Go(func() error {
			p.run(pp.ctx, notifyCB)
			return nil
		})
	}
}

func (pp *ProbePool) RemoveProbes(ids []string) {
	pp.probesMu.Lock()
	defer pp.probesMu.Unlock()

	for _, id := range ids {
		if p, ok := pp.probes[id]; ok {
			p.stop()
			delete(pp.probes, id)
		}
	}
}

func (pp *ProbePool) RemoveProbe(id string) bool {
	pp.probesMu.Lock()
	defer pp.probesMu.Unlock()

	p, ok := pp.probes[id]
	if ok {
		p.stop()
		delete(pp.probes, id)
	}
	return ok
}

func (pp *ProbePool) GetRecentFails(reset bool) []UnhealthyProbe {
	pp.unhealthyProbesMu.Lock()
	defer pp.unhealthyProbesMu.Unlock()

	fails := pp.unhealthyProbes
	if reset {
		pp.unhealthyProbes = []UnhealthyProbe{}
		pp.unhealthyProbeVersion.Add(1)
	}
	return fails
}

func (pp *ProbePool) GetProbeStatus(id string) (*ProbeStatus, bool) {
	pp.probesMu.RLock()
	defer pp.probesMu.RUnlock()

	p, ok := pp.probes[id]
	if !ok {
		return nil, false
	}

	p.statusMu.RLock()
	defer p.statusMu.RUnlock()

	copyStatus := p.status
	return &copyStatus, true
}

func (pp *ProbePool) Stop() {
	pp.probesMu.Lock()
	defer pp.probesMu.Unlock()

	pp.cancel()
	pp.eg.Wait()
}
