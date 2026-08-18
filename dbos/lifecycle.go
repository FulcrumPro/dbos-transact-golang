package dbos

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
)

var (
	errDBOSDraining = errors.New("DBOS is draining")
	errDBOSShutDown = errors.New("DBOS is shutting down")
)

// runtimeLifecycle coordinates the producers that can create local workflow
// goroutines. It deliberately uses counters and a wake channel instead of a
// lock held across database work.
type runtimeLifecycle struct {
	mu sync.Mutex

	draining      bool
	shuttingDown  bool
	starts        int
	queueClaims   int
	schedules     int
	asyncEffects  int
	wake          chan struct{}
	drainSignal   chan struct{}
	drainDone     chan struct{}
	drainResult   error
	drainFinished bool
}

func newRuntimeLifecycle() *runtimeLifecycle {
	return &runtimeLifecycle{wake: make(chan struct{}), drainSignal: make(chan struct{})}
}

func (l *runtimeLifecycle) requestDrain(shutdown bool) (done chan struct{}, first bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if shutdown {
		l.shuttingDown = true
	}
	if !l.draining {
		l.draining = true
		l.drainDone = make(chan struct{})
		close(l.drainSignal)
		if shutdown {
			l.drainResult = errDBOSShutDown
		}
		return l.drainDone, true
	}
	if shutdown && !l.drainFinished {
		l.drainResult = errDBOSShutDown
	}
	return l.drainDone, false
}

func (l *runtimeLifecycle) markDrainFinished() {
	l.mu.Lock()
	l.drainFinished = true
	l.mu.Unlock()
}

func (l *runtimeLifecycle) drainError() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.drainResult
}

func (l *runtimeLifecycle) isDraining() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.draining
}

func (l *runtimeLifecycle) drainStarted() <-chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.drainSignal
}

func (c *dbosContext) queueClaimsAllowed() bool {
	return c.lifecycle == nil || !c.lifecycle.isDraining()
}

func (c *dbosContext) scheduleProductionAllowed() bool {
	return c.lifecycle == nil || !c.lifecycle.isDraining()
}

func (c *dbosContext) drainStarted() <-chan struct{} {
	if c.lifecycle == nil {
		return nil
	}
	return c.lifecycle.drainStarted()
}

func (c *dbosContext) beginQueueClaim() *lifecycleLease {
	if c.lifecycle == nil {
		return &lifecycleLease{}
	}
	return c.lifecycle.beginQueueClaim()
}

func (c *dbosContext) beginSchedule() *lifecycleLease {
	if c.lifecycle == nil {
		return &lifecycleLease{}
	}
	return c.lifecycle.beginSchedule()
}

func (c *dbosContext) beginAsyncEffect() *lifecycleLease {
	if c.lifecycle == nil {
		return &lifecycleLease{}
	}
	return c.lifecycle.beginAsyncEffect()
}

func (c *dbosContext) beginWorkflow(immediate, child, dequeue bool) (*workflowStartLease, error) {
	if c.lifecycle == nil {
		return nil, nil
	}
	return c.lifecycle.beginWorkflow(immediate, child, dequeue, c.workflowsWg)
}

func (c *dbosContext) checkLaunchContext() error {
	if c.Err() != nil {
		return models.NewInitializationError("DBOS launch canceled")
	}
	if c.lifecycle != nil && c.lifecycle.isDraining() {
		return models.NewInitializationError("DBOS is draining")
	}
	return nil
}

func (l *runtimeLifecycle) beginWorkflow(immediate, child, dequeue bool, wg *sync.WaitGroup) (*workflowStartLease, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.shuttingDown && !dequeue {
		return nil, errDBOSShutDown
	}
	if l.draining && !dequeue {
		if immediate && !child {
			return nil, errDBOSDraining
		}
		if !immediate && child {
			return nil, errDBOSDraining
		}
	}
	l.starts++
	return &workflowStartLease{lifecycle: l, wg: wg}, nil
}

func (l *runtimeLifecycle) beginQueueClaim() *lifecycleLease {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.draining || l.shuttingDown {
		return nil
	}
	l.queueClaims++
	return &lifecycleLease{lifecycle: l, kind: lifecycleQueueClaim}
}

func (l *runtimeLifecycle) beginSchedule() *lifecycleLease {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.draining || l.shuttingDown {
		return nil
	}
	l.schedules++
	return &lifecycleLease{lifecycle: l, kind: lifecycleSchedule}
}

func (l *runtimeLifecycle) beginAsyncEffect() *lifecycleLease {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.asyncEffects++
	return &lifecycleLease{lifecycle: l, kind: lifecycleAsyncEffect}
}

func (l *runtimeLifecycle) release(kind lifecycleLeaseKind) {
	l.mu.Lock()
	switch kind {
	case lifecycleStart:
		l.starts--
	case lifecycleQueueClaim:
		l.queueClaims--
	case lifecycleSchedule:
		l.schedules--
	case lifecycleAsyncEffect:
		l.asyncEffects--
	}
	oldWake := l.wake
	l.wake = make(chan struct{})
	close(oldWake)
	l.mu.Unlock()
}

func (l *runtimeLifecycle) waitForProducers() {
	for {
		l.mu.Lock()
		if l.starts == 0 && l.queueClaims == 0 && l.schedules == 0 {
			l.mu.Unlock()
			return
		}
		wake := l.wake
		l.mu.Unlock()
		<-wake
	}
}

func (l *runtimeLifecycle) waitForAsyncEffects() {
	for {
		l.mu.Lock()
		if l.asyncEffects == 0 {
			l.mu.Unlock()
			return
		}
		wake := l.wake
		l.mu.Unlock()
		<-wake
	}
}

type lifecycleLeaseKind uint8

const (
	lifecycleStart lifecycleLeaseKind = iota
	lifecycleQueueClaim
	lifecycleSchedule
	lifecycleAsyncEffect
)

type lifecycleLease struct {
	lifecycle *runtimeLifecycle
	kind      lifecycleLeaseKind
	once      sync.Once
}

func (l *lifecycleLease) done() {
	if l == nil || l.lifecycle == nil {
		return
	}
	l.once.Do(func() { l.lifecycle.release(l.kind) })
}

// workflowStartLease makes the WaitGroup Add and the lifecycle counter
// decrement one indivisible hand-off from the drain waiter's perspective.
type workflowStartLease struct {
	lifecycle *runtimeLifecycle
	wg        *sync.WaitGroup
	once      sync.Once
}

func (l *workflowStartLease) register() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		l.wg.Add(1)
		l.lifecycle.release(lifecycleStart)
	})
}

func (l *workflowStartLease) abort() {
	if l == nil {
		return
	}
	l.once.Do(func() { l.lifecycle.release(lifecycleStart) })
}

type queueClaimBatch struct {
	lease     *lifecycleLease
	remaining atomic.Int64
}

func newQueueClaimBatch(lease *lifecycleLease, count int) *queueClaimBatch {
	if lease == nil {
		return nil
	}
	if count == 0 {
		lease.done()
		return nil
	}
	batch := &queueClaimBatch{lease: lease}
	batch.remaining.Store(int64(count))
	return batch
}

func (b *queueClaimBatch) done() {
	if b != nil && b.remaining.Add(-1) == 0 {
		b.lease.done()
	}
}

func (c *dbosContext) completeDrain(done chan struct{}) {
	defer close(done)
	c.lifecycle.waitForProducers()
	c.stopScheduleProduction()
	c.scheduleReconcilerWg.Wait()
	c.workflowsWg.Wait()
	c.lifecycle.waitForAsyncEffects()
	if c.queueRunner != nil && c.queueRunnerStarted.Load() {
		<-c.queueRunner.completionChan
		c.queueRunnerStarted.Store(false)
	}
	c.lifecycle.markDrainFinished()
}

func (c *dbosContext) startDrain(shutdown bool) <-chan struct{} {
	done, first := c.lifecycle.requestDrain(shutdown)
	if first {
		go c.completeDrain(done)
	}
	return done
}

func (c *dbosContext) stopScheduleProduction() {
	c.scheduleMu.Lock()
	if c.workflowScheduler == nil || !c.workflowSchedulerStarted.Swap(false) {
		c.scheduleMu.Unlock()
		return
	}
	done := c.workflowScheduler.Stop()
	c.scheduleMu.Unlock()
	<-done.Done()
}

// Drain stops this process from taking new local work while allowing already
// admitted workflows and their immediately executed child workflows to finish.
// Queued child workflows are rejected so their parents can unwind before the
// drain deadline.
func Drain(ctx Context, timeout time.Duration) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	c, ok := ctx.(*dbosContext)
	if !ok || c.ctxCancelFunc == nil {
		return errors.New("Drain requires the root DBOS context returned by NewContext")
	}
	done := c.startDrain(false)
	select {
	case <-done:
		return c.lifecycle.drainError()
	default:
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	select {
	case <-done:
		return c.lifecycle.drainError()
	case <-waitCtx.Done():
		return waitCtx.Err()
	}
}
