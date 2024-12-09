package probe

import (
	"context"
	"sync"
	"time"

	"github.com/apache/kvrocks-controller/logger"
	"go.uber.org/zap"
)

type ProbeFunc func(ctx context.Context) error

type ProbeResult struct {
	Time time.Time `json:"time"`
	Err  error     `json:"err"`
}

type ProbeStatus struct {
	ProbeID             string        `json:"probe_id"`
	ProbeIntervalSecond int           `json:"probe_interval_second"`
	RecentProbes        []ProbeResult `json:"recent_probes"`
}

type notifyFunc func(id string, probeResult ProbeResult)

type Probe struct {
	ID       string
	probeCB  ProbeFunc
	interval time.Duration
	notifyCB notifyFunc

	ctx    context.Context
	cancel context.CancelFunc

	statusMu sync.RWMutex
	status   ProbeStatus
}

func NewProbe(id string, probeCB ProbeFunc, interval time.Duration) *Probe {
	return &Probe{
		ID:       id,
		probeCB:  probeCB,
		interval: interval,
		notifyCB: func(_id string, _probeResult ProbeResult) {},

		ctx:      nil,
		cancel:   nil,
		statusMu: sync.RWMutex{},
		status:   ProbeStatus{},
	}
}

func (status *ProbeStatus) IsHealthy() bool {
	// todo: may be we can do better here
	last := status.RecentProbes[len(status.RecentProbes)-1]
	return last.Err == nil
}

func (p *Probe) run(ctx context.Context, notifyCB notifyFunc) {
	p.ctx, p.cancel = context.WithCancel(ctx)
	p.notifyCB = notifyCB

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	handleProbeResult := func(err error, ts time.Time) {
		p.statusMu.Lock()
		defer p.statusMu.Unlock()

		probeResult := ProbeResult{
			Time: ts,
			Err:  err,
		}

		if len(p.status.RecentProbes) >= 5 {
			p.status.RecentProbes = p.status.RecentProbes[1:]
		}

		p.status.RecentProbes = append(p.status.RecentProbes, probeResult)

		if err != nil {
			logger.Get().Error("Probe failed", zap.String("probe_id", p.ID), zap.Error(err))
			go p.notifyCB(p.ID, probeResult)
		}
	}

	for {
		select {
		case <-p.ctx.Done():
			logger.Get().Info("Probe stopped", zap.String("probe_id", p.ID))
			return
		case <-ticker.C:
			err := p.probeCB(p.ctx)
			now := time.Now()
			handleProbeResult(err, now)
		}
	}
}

func (p *Probe) stop() {
	p.cancel()
}
