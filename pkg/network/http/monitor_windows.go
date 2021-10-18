// +build windows,npm

package http

import (
	"sync"
	"time"

	"github.com/DataDog/datadog-agent/pkg/network/config"
	"github.com/DataDog/datadog-agent/pkg/network/driver"
	"github.com/DataDog/datadog-agent/pkg/util/log"
)

// Monitor is responsible for aggregating and emitting metrics based on
// batches of HTTP transactions received from the driver interface
type Monitor struct {
	di *httpDriverInterface

	eventLoopWG sync.WaitGroup
}

// NewMonitor returns a new Monitor instance
func NewMonitor(c *config.Config) (*Monitor, error) {
	di, err := newDriverInterface()
	if err != nil {
		return nil, err
	}

	return &Monitor{
		di: di,
	}, nil
}

// Start consuming HTTP events
func (m *Monitor) Start() {
	if m == nil {
		return
	}
	m.di.startReadingBuffers()

	m.eventLoopWG.Add(1)
	go func() {
		defer m.eventLoopWG.Done()
		report := time.NewTicker(30 * time.Second)
		defer report.Stop()
		for {
			select {
			case transactionBatch, ok := <-m.di.dataChannel:
				if !ok {
					return
				}
				m.process(transactionBatch)
			case <-report.C:
				m.di.flushPendingTransactions()
			}
		}
	}()

	return
}

func (m *Monitor) process(transactionBatch []driver.HttpTransactionType) {
	if m == nil {
		return
	}

	// TODO
	log.Infof("batch of %d transactions processed", len(transactionBatch))
}

// GetHTTPStats returns a map of HTTP stats stored in the following format:
// [source, dest tuple, request path] -> RequestStats object
func (m *Monitor) GetHTTPStats() map[Key]RequestStats {
	if m == nil {
		return nil
	}

	// TODO
	log.Infof("http stats fetched")
	return map[Key]RequestStats{}
}

// Stop HTTP monitoring
func (m *Monitor) Stop() error {
	if m == nil {
		return nil
	}

	err := m.di.close()
	m.eventLoopWG.Wait()
	return err
}
