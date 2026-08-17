package dbos

import (
	"context"
	"sync"
)

type admissionQueueKey struct {
	queueName    string
	partitionKey string
}

type workflowAdmission struct {
	mu          sync.Mutex
	limit       int
	current     int
	queueCounts map[admissionQueueKey]int
	queueLimits map[string]int
	waiters     []*workflowAdmissionWaiter
	wake        chan struct{}
}

type workflowAdmissionWaiter struct {
	key        admissionQueueKey
	queueLimit int
	process    bool
	queue      bool
}

func newWorkflowAdmission(limit int) *workflowAdmission {
	return &workflowAdmission{
		limit:       limit,
		queueCounts: make(map[admissionQueueKey]int),
		queueLimits: make(map[string]int),
		wake:        make(chan struct{}),
	}
}

func (a *workflowAdmission) state() (current int, limit int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.current, a.limit
}

func (a *workflowAdmission) enabled() bool {
	return a.limit > 0
}

func (a *workflowAdmission) queueCount(key admissionQueueKey) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.queueCounts[key]
}

func (a *workflowAdmission) currentQueueLimitLocked(key admissionQueueKey, fallback int) int {
	if key.queueName == "" {
		return fallback
	}
	if limit, ok := a.queueLimits[key.queueName]; ok {
		return limit
	}
	return fallback
}

func (a *workflowAdmission) canAcquireLocked(key admissionQueueKey, queueLimit int, process, queue bool) bool {
	if process && a.limit > 0 && a.current >= a.limit {
		return false
	}
	if queue {
		queueLimit = a.currentQueueLimitLocked(key, queueLimit)
		return key.queueName != "" && (queueLimit < 0 || a.queueCounts[key] < queueLimit)
	}
	return true
}

func (a *workflowAdmission) firstEligibleWaiterLocked(key admissionQueueKey, process, queue bool) int {
	for i, waiter := range a.waiters {
		if waiter.process != process || waiter.queue != queue {
			continue
		}
		if queue && waiter.key != key {
			continue
		}
		if a.canAcquireLocked(waiter.key, waiter.queueLimit, waiter.process, waiter.queue) {
			return i
		}
	}
	return -1
}

func (a *workflowAdmission) hasEligibleProcessWaiterLocked() bool {
	for _, waiter := range a.waiters {
		if waiter.process && a.canAcquireLocked(waiter.key, waiter.queueLimit, waiter.process, waiter.queue) {
			return true
		}
	}
	return false
}

func (a *workflowAdmission) updateQueueLimit(queueName string, queueLimit int) {
	if queueName == "" {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if current, ok := a.queueLimits[queueName]; ok && current == queueLimit {
		return
	}
	a.queueLimits[queueName] = queueLimit
	a.signalLocked()
}

func (a *workflowAdmission) reserveLocked(key admissionQueueKey, process, queue bool) {
	if process {
		a.current++
	}
	if queue {
		a.queueCounts[key]++
	}
}

func (a *workflowAdmission) permitKinds(key admissionQueueKey) (process, queue bool) {
	return a.limit > 0, key.queueName != ""
}

func (a *workflowAdmission) tryAcquire(key admissionQueueKey, queueLimit int) (*workflowAdmissionToken, bool) {
	process, queue := a.permitKinds(key)
	a.mu.Lock()
	defer a.mu.Unlock()
	if queue {
		if _, ok := a.queueLimits[key.queueName]; !ok {
			a.queueLimits[key.queueName] = queueLimit
		}
	}
	if process && a.hasEligibleProcessWaiterLocked() {
		return nil, false
	}
	if !process && a.firstEligibleWaiterLocked(key, process, queue) >= 0 {
		return nil, false
	}
	if !a.canAcquireLocked(key, queueLimit, process, queue) {
		return nil, false
	}
	a.reserveLocked(key, process, queue)
	return newWorkflowAdmissionToken(a, key, process, queue), true
}

func (a *workflowAdmission) acquire(ctx context.Context, key admissionQueueKey, queueLimit int) (*workflowAdmissionToken, error) {
	process, queue := a.permitKinds(key)
	if err := a.acquirePermit(ctx, key, queueLimit, process, queue); err != nil {
		return nil, err
	}
	return newWorkflowAdmissionToken(a, key, process, queue), nil
}

func (a *workflowAdmission) acquirePermit(ctx context.Context, key admissionQueueKey, queueLimit int, process, queue bool) error {
	if !process && !queue {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var waiter *workflowAdmissionWaiter
	for {
		a.mu.Lock()
		if err := ctx.Err(); err != nil {
			a.removeWaiterLocked(waiter)
			a.mu.Unlock()
			return context.Cause(ctx)
		}
		if queue {
			if _, ok := a.queueLimits[key.queueName]; !ok {
				a.queueLimits[key.queueName] = queueLimit
			}
		}
		canAcquire := a.canAcquireLocked(key, queueLimit, process, queue)
		if waiter == nil && a.firstEligibleWaiterLocked(key, process, queue) < 0 && canAcquire {
			a.reserveLocked(key, process, queue)
			a.mu.Unlock()
			return nil
		}
		if waiter == nil {
			waiter = &workflowAdmissionWaiter{key: key, queueLimit: queueLimit, process: process, queue: queue}
			a.waiters = append(a.waiters, waiter)
		}
		if canAcquire {
			waiterIndex := a.firstEligibleWaiterLocked(key, process, queue)
			if waiterIndex >= 0 && a.waiters[waiterIndex] == waiter {
				a.removeWaiterAtLocked(waiterIndex)
				a.reserveLocked(key, process, queue)
				a.mu.Unlock()
				return nil
			}
		}
		wake := a.wake
		a.mu.Unlock()

		select {
		case <-wake:
		case <-ctx.Done():
			a.mu.Lock()
			a.removeWaiterLocked(waiter)
			a.mu.Unlock()
			return context.Cause(ctx)
		}
	}
}

func (a *workflowAdmission) removeWaiterLocked(waiter *workflowAdmissionWaiter) {
	if waiter == nil {
		return
	}
	for i, candidate := range a.waiters {
		if candidate != waiter {
			continue
		}
		a.removeWaiterAtLocked(i)
		return
	}
}

func (a *workflowAdmission) removeWaiterAtLocked(index int) {
	copy(a.waiters[index:], a.waiters[index+1:])
	a.waiters = a.waiters[:len(a.waiters)-1]
	a.signalLocked()
}

func (a *workflowAdmission) signalLocked() {
	oldWake := a.wake
	a.wake = make(chan struct{})
	close(oldWake)
}

func (a *workflowAdmission) releasePermit(key admissionQueueKey, process, queue bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if process && a.current <= 0 {
		panic("workflow admission permit released without a held permit")
	}
	if queue && a.queueCounts[key] <= 0 {
		panic("workflow queue admission permit released without a held permit")
	}
	if process {
		a.current--
	}
	if queue {
		if count := a.queueCounts[key] - 1; count > 0 {
			a.queueCounts[key] = count
		} else {
			delete(a.queueCounts, key)
		}
	}
	a.signalLocked()
}

type workflowAdmissionToken struct {
	admission   *workflowAdmission
	key         admissionQueueKey
	process     bool
	queue       bool
	releaseOnce sync.Once
}

func newWorkflowAdmissionToken(admission *workflowAdmission, key admissionQueueKey, process, queue bool) *workflowAdmissionToken {
	return &workflowAdmissionToken{admission: admission, key: key, process: process, queue: queue}
}

func (t *workflowAdmissionToken) release() {
	if t == nil || t.admission == nil {
		return
	}
	t.releaseOnce.Do(func() {
		t.admission.releasePermit(t.key, t.process, t.queue)
	})
}
