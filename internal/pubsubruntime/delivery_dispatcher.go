package pubsubruntime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

const (
	deliveryQueueSize   = 256
	deliveryWorkerCount = 4
)

type deliveryJob struct {
	ctx          context.Context
	subscriberID string
	url          string
	payload      string
}

// deliveryDispatcher restores the old Logma bounded callback-dispatch shape:
// Redis receive loops enqueue work without waiting on slow HTTP callbacks, a
// fixed worker set bounds concurrency, and a full queue drops rather than
// growing memory without bound or blocking the Pub/Sub receive path.
type deliveryDispatcher struct {
	send webhookSender
	jobs chan deliveryJob
	wg   sync.WaitGroup

	mu      sync.RWMutex
	closed  bool
	dropped atomic.Uint64
}

func newDeliveryDispatcher(send webhookSender) *deliveryDispatcher {
	d := &deliveryDispatcher{send: send, jobs: make(chan deliveryJob, deliveryQueueSize)}
	d.wg.Add(deliveryWorkerCount)
	for i := 0; i < deliveryWorkerCount; i++ {
		go d.worker()
	}
	return d
}

func (d *deliveryDispatcher) worker() {
	defer d.wg.Done()
	for job := range d.jobs {
		if err := d.send(job.ctx, job.url, job.payload); err != nil && !errors.Is(err, context.Canceled) {
			// Callback URLs may contain signed/query-bearing material. Keep failure
			// diagnostics tied to the durable Subscription identity instead of
			// copying endpoint credentials into logs.
			fmt.Printf("Subscriber %s webhook delivery failed: %v\n", job.subscriberID, err)
		}
	}
}

func (d *deliveryDispatcher) dispatch(job deliveryJob) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed || job.ctx.Err() != nil {
		return false
	}
	select {
	case d.jobs <- job:
		return true
	default:
		// This executes on the Redis receive path. A synchronous log write here
		// could turn overload reporting into transport backpressure, so overflow
		// is counted and can be observed out-of-band instead.
		d.dropped.Add(1)
		return false
	}
}

func (d *deliveryDispatcher) droppedCount() uint64 {
	return d.dropped.Load()
}

func (d *deliveryDispatcher) close() {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	d.closed = true
	close(d.jobs)
	d.mu.Unlock()
	d.wg.Wait()
}
