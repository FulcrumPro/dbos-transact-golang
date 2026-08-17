package dbos

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"
	"github.com/stretchr/testify/require"
)

func TestMaxConcurrentWorkflowsConfig(t *testing.T) {
	_, err := processConfig(&Config{
		AppName:                "test",
		DatabaseURL:            "sqlite::memory:",
		MaxConcurrentWorkflows: -1,
	})
	require.EqualError(t, err, "maxConcurrentWorkflows cannot be negative")

	config, err := processConfig(&Config{
		AppName:     "test",
		DatabaseURL: "sqlite::memory:",
	})
	require.NoError(t, err)
	require.Zero(t, config.MaxConcurrentWorkflows)
}

func TestWorkflowAdmissionAcquireAndRelease(t *testing.T) {
	admission := newWorkflowAdmission(1)
	first, ok := admission.tryAcquire(admissionQueueKey{}, -1)
	require.True(t, ok)
	current, limit := admission.state()
	require.Equal(t, 1, current)
	require.Equal(t, 1, limit)

	type acquireResult struct {
		token *workflowAdmissionToken
		err   error
	}
	acquired := make(chan acquireResult, 1)
	go func() {
		token, err := admission.acquire(context.Background(), admissionQueueKey{}, -1)
		acquired <- acquireResult{token: token, err: err}
	}()

	require.Eventually(t, func() bool {
		admission.mu.Lock()
		defer admission.mu.Unlock()
		return len(admission.waiters) == 1
	}, time.Second, time.Millisecond)

	first.release()
	first.release()
	select {
	case result := <-acquired:
		require.NoError(t, result.err)
		result.token.release()
	case <-time.After(time.Second):
		t.Fatal("admission waiter was not woken after permit release")
	}
	current, _ = admission.state()
	require.Zero(t, current)
}

func TestWorkflowAdmissionCancelledWaiterDoesNotLeak(t *testing.T) {
	admission := newWorkflowAdmission(1)
	first, ok := admission.tryAcquire(admissionQueueKey{}, -1)
	require.True(t, ok)

	waitCtx, cancel := context.WithCancel(context.Background())
	waitResult := make(chan error, 1)
	go func() {
		_, err := admission.acquire(waitCtx, admissionQueueKey{}, -1)
		waitResult <- err
	}()
	require.Eventually(t, func() bool {
		admission.mu.Lock()
		defer admission.mu.Unlock()
		return len(admission.waiters) == 1
	}, time.Second, time.Millisecond)

	cancel()
	require.ErrorIs(t, <-waitResult, context.Canceled)
	require.Eventually(t, func() bool {
		admission.mu.Lock()
		defer admission.mu.Unlock()
		return len(admission.waiters) == 0
	}, time.Second, time.Millisecond)
	current, _ := admission.state()
	require.Equal(t, 1, current)
	first.release()
}

func TestWorkflowAdmissionQueueReservationCombinesLimits(t *testing.T) {
	admission := newWorkflowAdmission(2)
	queueA := admissionQueueKey{queueName: "queue-a"}
	queueB := admissionQueueKey{queueName: "queue-b"}

	first, ok := admission.tryAcquire(queueA, 1)
	require.True(t, ok)
	_, ok = admission.tryAcquire(queueA, 1)
	require.False(t, ok, "queue concurrency must constrain a reservation before the process cap")

	second, ok := admission.tryAcquire(queueB, 1)
	require.True(t, ok)
	_, ok = admission.tryAcquire(admissionQueueKey{}, -1)
	require.False(t, ok, "queued roots must consume process capacity")

	current, _ := admission.state()
	require.Equal(t, 2, current)
	require.Equal(t, 1, admission.queueCount(queueA))
	require.Equal(t, 1, admission.queueCount(queueB))
	first.release()
	second.release()
	current, _ = admission.state()
	require.Zero(t, current)
	require.Zero(t, admission.queueCount(queueA))
	require.Zero(t, admission.queueCount(queueB))
}

func TestWorkflowAdmissionUsesLatestQueueLimit(t *testing.T) {
	admission := newWorkflowAdmission(0)
	key := admissionQueueKey{queueName: "queue"}
	first, ok := admission.tryAcquire(key, 2)
	require.True(t, ok)
	second, ok := admission.tryAcquire(key, 2)
	require.True(t, ok)

	admission.updateQueueLimit(key.queueName, 1)
	_, ok = admission.tryAcquire(key, 2)
	require.False(t, ok)
	first.release()
	_, ok = admission.tryAcquire(key, 2)
	require.False(t, ok, "lowered limit must apply until existing work drains below it")
	second.release()

	third, ok := admission.tryAcquire(key, 2)
	require.True(t, ok)
	third.release()
}

func TestWorkflowAdmissionQueuedReservationDoesNotBargeWaitingRoot(t *testing.T) {
	admission := newWorkflowAdmission(1)
	first, ok := admission.tryAcquire(admissionQueueKey{}, -1)
	require.True(t, ok)

	waiting := make(chan *workflowAdmissionToken, 1)
	go func() {
		token, err := admission.acquire(context.Background(), admissionQueueKey{}, -1)
		if err == nil {
			waiting <- token
		}
	}()
	require.Eventually(t, func() bool {
		admission.mu.Lock()
		defer admission.mu.Unlock()
		return len(admission.waiters) == 1
	}, time.Second, time.Millisecond)

	first.release()
	queueToken, ok := admission.tryAcquire(admissionQueueKey{queueName: "queue"}, -1)
	require.False(t, ok, "a queue poll barged ahead of a waiting root")
	require.Nil(t, queueToken)

	select {
	case token := <-waiting:
		token.release()
	case <-time.After(time.Second):
		t.Fatal("waiting root did not acquire the released process slot")
	}
}

func TestWorkflowAdmissionTracksUnlimitedQueueRoots(t *testing.T) {
	key := admissionQueueKey{queueName: "queue"}
	admission := newWorkflowAdmission(0)
	first, ok := admission.tryAcquire(key, -1)
	require.True(t, ok)
	require.Equal(t, 1, admission.queueCount(key))

	admission.updateQueueLimit(key.queueName, 1)
	_, ok = admission.tryAcquire(key, -1)
	require.False(t, ok, "a finite limit must include roots started while the queue was unlimited")

	first.release()
	second, ok := admission.tryAcquire(key, -1)
	require.True(t, ok)
	second.release()
	require.Zero(t, admission.queueCount(key))
}

func TestWorkflowAdmissionUnlimitedQueueDoesNotConsumeProcessCapacity(t *testing.T) {
	admission := newWorkflowAdmission(0)
	token, ok := admission.tryAcquire(admissionQueueKey{queueName: "queue"}, -1)
	require.True(t, ok)
	current, limit := admission.state()
	require.Zero(t, current)
	require.Zero(t, limit)
	token.release()
}

func TestMaxConcurrentWorkflowsReturnsHandleBeforeCapacity(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{
		dropDB:                 true,
		checkLeaks:             true,
		maxConcurrentWorkflows: 1,
	})

	started := make(chan string, 2)
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseFirst) }) })
	workflow := func(_ Context, input string) (string, error) {
		started <- input
		if input == "first" {
			<-releaseFirst
		}
		return input, nil
	}
	RegisterWorkflow(dbosCtx, workflow)
	require.NoError(t, Launch(dbosCtx))

	first, err := RunWorkflow(dbosCtx, workflow, "first")
	require.NoError(t, err)
	require.Equal(t, "first", <-started)

	type handleResult struct {
		handle WorkflowHandle[string]
		err    error
	}
	secondResult := make(chan handleResult, 1)
	go func() {
		handle, runErr := RunWorkflow(dbosCtx, workflow, "second")
		secondResult <- handleResult{handle: handle, err: runErr}
	}()
	var second WorkflowHandle[string]
	select {
	case result := <-secondResult:
		require.NoError(t, result.err)
		second = result.handle
	case <-time.After(5 * time.Second):
		t.Fatal("RunWorkflow waited for execution capacity instead of returning a handle")
	}
	select {
	case input := <-started:
		t.Fatalf("waiting root started before capacity was released: %s", input)
	case <-time.After(100 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(releaseFirst) })
	require.Equal(t, "first", mustWorkflowResult(t, first))
	require.Equal(t, "second", <-started)
	require.Equal(t, "second", mustWorkflowResult(t, second))
}

type blockingOutcomeDB struct {
	sysdb.SystemDatabase
	entered chan struct{}
	release chan struct{}
	blocked atomic.Bool
}

func (d *blockingOutcomeDB) UpdateWorkflowOutcome(ctx context.Context, input sysdb.UpdateWorkflowOutcomeDBInput) (bool, error) {
	if d.blocked.CompareAndSwap(false, true) {
		close(d.entered)
		select {
		case <-d.release:
		case <-ctx.Done():
			return false, context.Cause(ctx)
		}
	}
	return d.SystemDatabase.UpdateWorkflowOutcome(ctx, input)
}

func TestMaxConcurrentWorkflowsHeldThroughOutcomeRecording(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{
		dropDB:                 true,
		checkLeaks:             true,
		maxConcurrentWorkflows: 1,
	})
	c := dbosCtx.(*dbosContext)
	firstBodyReturned := make(chan struct{})
	secondStarted := make(chan struct{})
	firstWorkflow := func(_ Context, _ string) (string, error) {
		close(firstBodyReturned)
		return "first", nil
	}
	secondWorkflow := func(_ Context, _ string) (string, error) {
		close(secondStarted)
		return "second", nil
	}
	RegisterWorkflow(c, firstWorkflow)
	RegisterWorkflow(c, secondWorkflow)

	blocker := &blockingOutcomeDB{
		SystemDatabase: c.systemDB,
		entered:        make(chan struct{}),
		release:        make(chan struct{}),
	}
	c.systemDB = blocker
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(blocker.release) }) })
	require.NoError(t, Launch(c))

	first, err := RunWorkflow(c, firstWorkflow, "")
	require.NoError(t, err)
	<-firstBodyReturned
	select {
	case <-blocker.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first workflow did not reach outcome recording")
	}

	second, err := RunWorkflow(c, secondWorkflow, "")
	require.NoError(t, err)
	select {
	case <-secondStarted:
		t.Fatal("second root started while the first outcome write held the worker slot")
	case <-time.After(100 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(blocker.release) })
	require.Equal(t, "first", mustWorkflowResult(t, first))
	select {
	case <-secondStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("second root did not start after the first outcome became durable")
	}
	require.Equal(t, "second", mustWorkflowResult(t, second))
}

func TestMaxConcurrentWorkflowsDirectChildBypassesRootLimit(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{
		dropDB:                 true,
		checkLeaks:             true,
		maxConcurrentWorkflows: 1,
	})

	childStarted := make(chan struct{})
	secondRootStarted := make(chan struct{})
	releaseChild := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseChild) }) })
	childWorkflow := func(_ Context, _ string) (string, error) {
		close(childStarted)
		<-releaseChild
		return "child", nil
	}
	parentWorkflow := func(ctx Context, _ string) (string, error) {
		handle, err := RunWorkflow(ctx, childWorkflow, "")
		if err != nil {
			return "", err
		}
		return handle.GetResult()
	}
	secondRoot := func(_ Context, _ string) (string, error) {
		close(secondRootStarted)
		return "second", nil
	}
	RegisterWorkflow(dbosCtx, childWorkflow)
	RegisterWorkflow(dbosCtx, parentWorkflow)
	RegisterWorkflow(dbosCtx, secondRoot)
	require.NoError(t, Launch(dbosCtx))

	parent, err := RunWorkflow(dbosCtx, parentWorkflow, "")
	require.NoError(t, err)
	select {
	case <-childStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("direct child did not start while its parent held the only root slot")
	}
	second, err := RunWorkflow(dbosCtx, secondRoot, "")
	require.NoError(t, err)
	select {
	case <-secondRootStarted:
		t.Fatal("awaiting a direct child yielded the parent's root slot")
	case <-time.After(100 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(releaseChild) })
	require.Equal(t, "child", mustWorkflowResult(t, parent))
	select {
	case <-secondRootStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("second root did not start after the parent completed")
	}
	require.Equal(t, "second", mustWorkflowResult(t, second))
}

func TestMaxConcurrentWorkflowsGoBypassesRootLimit(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{
		dropDB:                 true,
		checkLeaks:             true,
		maxConcurrentWorkflows: 1,
	})

	callbackStarted := make(chan struct{})
	secondRootStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseCallback) }) })
	parentWorkflow := func(ctx Context, _ string) (string, error) {
		result, err := Go(ctx, func(context.Context) (string, error) {
			close(callbackStarted)
			<-releaseCallback
			return "callback", nil
		})
		if err != nil {
			return "", err
		}
		outcome := <-result
		return outcome.Result, outcome.Err
	}
	secondRoot := func(_ Context, _ string) (string, error) {
		close(secondRootStarted)
		return "second", nil
	}
	RegisterWorkflow(dbosCtx, parentWorkflow)
	RegisterWorkflow(dbosCtx, secondRoot)
	require.NoError(t, Launch(dbosCtx))

	parent, err := RunWorkflow(dbosCtx, parentWorkflow, "")
	require.NoError(t, err)
	select {
	case <-callbackStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("Go callback did not start while its root held the only slot")
	}
	second, err := RunWorkflow(dbosCtx, secondRoot, "")
	require.NoError(t, err)
	select {
	case <-secondRootStarted:
		t.Fatal("awaiting Go yielded the parent's root slot")
	case <-time.After(100 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(releaseCallback) })
	require.Equal(t, "callback", mustWorkflowResult(t, parent))
	select {
	case <-secondRootStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("second root did not start after the parent completed")
	}
	require.Equal(t, "second", mustWorkflowResult(t, second))
}

func TestMaxConcurrentWorkflowsDoesNotReacquireForSteps(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{
		dropDB:                 true,
		checkLeaks:             true,
		maxConcurrentWorkflows: 1,
	})
	workflow := func(ctx Context, _ string) (string, error) {
		stepResult, err := RunAsStep(ctx, func(context.Context) (string, error) {
			return "step", nil
		})
		if err != nil {
			return "", err
		}
		txnResult, err := runAsTxn(ctx, func(context.Context, Tx) (string, error) {
			return "transaction", nil
		})
		if err != nil {
			return "", err
		}
		return stepResult + "+" + txnResult, nil
	}
	RegisterWorkflow(dbosCtx, workflow)
	require.NoError(t, Launch(dbosCtx))

	handle, err := RunWorkflow(dbosCtx, workflow, "")
	require.NoError(t, err)
	result, err := handle.GetResult(WithHandleTimeout(5 * time.Second))
	require.NoError(t, err)
	require.Equal(t, "step+transaction", result)
}

func TestMaxConcurrentWorkflowsZeroLeavesRootsUnlimited(t *testing.T) {
	const workflowCount = 4
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	started := make(chan struct{}, workflowCount)
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	workflow := func(_ Context, input int) (int, error) {
		started <- struct{}{}
		<-release
		return input, nil
	}
	RegisterWorkflow(dbosCtx, workflow)
	require.NoError(t, Launch(dbosCtx))

	handles := make([]WorkflowHandle[int], 0, workflowCount)
	for i := range workflowCount {
		handle, err := RunWorkflow(dbosCtx, workflow, i)
		require.NoError(t, err)
		handles = append(handles, handle)
	}
	for range workflowCount {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("zero MaxConcurrentWorkflows did not preserve unlimited roots")
		}
	}
	releaseOnce.Do(func() { close(release) })
	for i, handle := range handles {
		require.Equal(t, i, mustWorkflowResult(t, handle))
	}
}

func TestMaxConcurrentWorkflowsAppliesAcrossQueues(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{
		dropDB:                 true,
		checkLeaks:             true,
		maxConcurrentWorkflows: 1,
	})

	started := make(chan string, 2)
	release := make(chan struct{}, 2)
	workflow := func(_ Context, input string) (string, error) {
		started <- input
		<-release
		return input, nil
	}
	RegisterWorkflow(dbosCtx, workflow)
	queueA, err := registerWFQ(dbosCtx, "admission-process-a", WithQueueBasePollingInterval(10*time.Millisecond))
	require.NoError(t, err)
	queueB, err := registerWFQ(dbosCtx, "admission-process-b", WithQueueBasePollingInterval(10*time.Millisecond))
	require.NoError(t, err)
	require.NoError(t, Launch(dbosCtx))

	first, err := RunWorkflow(dbosCtx, workflow, "first", WithQueue(queueA))
	require.NoError(t, err)
	select {
	case input := <-started:
		require.Equal(t, "first", input)
	case <-time.After(5 * time.Second):
		t.Fatal("first queued root did not start")
	}
	second, err := RunWorkflow(dbosCtx, workflow, "second", WithQueue(queueB))
	require.NoError(t, err)
	select {
	case input := <-started:
		t.Fatalf("second queue bypassed the runtime root limit: %s", input)
	case <-time.After(100 * time.Millisecond):
	}

	release <- struct{}{}
	select {
	case input := <-started:
		require.Equal(t, "second", input)
	case <-time.After(5 * time.Second):
		t.Fatal("second queued root did not start after runtime capacity was released")
	}
	release <- struct{}{}
	require.Equal(t, "first", mustWorkflowResult(t, first))
	require.Equal(t, "second", mustWorkflowResult(t, second))
}

func TestWorkerConcurrencyRemainsAdditionalConstraint(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{
		dropDB:                 true,
		checkLeaks:             true,
		maxConcurrentWorkflows: 3,
	})

	started := make(chan struct{}, 3)
	release := make(chan struct{}, 3)
	workflow := func(_ Context, _ string) (string, error) {
		started <- struct{}{}
		<-release
		return "done", nil
	}
	RegisterWorkflow(dbosCtx, workflow)
	queue, err := registerWFQ(dbosCtx, "admission-worker-concurrency",
		WithWorkerConcurrency(1),
		WithQueueBasePollingInterval(10*time.Millisecond))
	require.NoError(t, err)
	require.NoError(t, Launch(dbosCtx))

	handles := make([]WorkflowHandle[string], 0, 3)
	for range 3 {
		handle, err := RunWorkflow(dbosCtx, workflow, "", WithQueue(queue))
		require.NoError(t, err)
		handles = append(handles, handle)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("worker-constrained queue did not start a root")
	}
	select {
	case <-started:
		t.Fatal("WorkerConcurrency allowed two roots concurrently")
	case <-time.After(100 * time.Millisecond):
	}

	for range 2 {
		release <- struct{}{}
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("next queued root did not start after worker capacity was released")
		}
	}
	release <- struct{}{}
	for _, handle := range handles {
		require.Equal(t, "done", mustWorkflowResult(t, handle))
	}
}

func TestWorkerConcurrencyUpdateIncludesAlreadyRunningRoots(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	started := make(chan string, 2)
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseFirst) }) })
	workflow := func(_ Context, input string) (string, error) {
		started <- input
		if input == "first" {
			<-releaseFirst
		}
		return input, nil
	}
	RegisterWorkflow(dbosCtx, workflow)
	queue, err := registerWFQ(dbosCtx, "admission-dynamic-worker-concurrency",
		WithQueueBasePollingInterval(10*time.Millisecond))
	require.NoError(t, err)
	require.NoError(t, Launch(dbosCtx))

	first, err := RunWorkflow(dbosCtx, workflow, "first", WithQueue(queue))
	require.NoError(t, err)
	select {
	case input := <-started:
		require.Equal(t, "first", input)
	case <-time.After(5 * time.Second):
		t.Fatal("first queued root did not start")
	}

	require.NoError(t, queue.SetWorkerConcurrency(dbosCtx, intPtr(1)))
	c := dbosCtx.(*dbosContext)
	require.Eventually(t, func() bool {
		current, ok := c.queueRunner.currentQueueConfig(queue.Name)
		return ok && current.WorkerConcurrency != nil && *current.WorkerConcurrency == 1
	}, 5*time.Second, 10*time.Millisecond, "queue worker did not reconcile the updated WorkerConcurrency")
	second, err := RunWorkflow(dbosCtx, workflow, "second", WithQueue(queue))
	require.NoError(t, err)
	select {
	case input := <-started:
		t.Fatalf("updated WorkerConcurrency forgot an already-running root: %s", input)
	case <-time.After(100 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(releaseFirst) })
	require.Equal(t, "first", mustWorkflowResult(t, first))
	select {
	case input := <-started:
		require.Equal(t, "second", input)
	case <-time.After(5 * time.Second):
		t.Fatal("second queued root did not start after capacity was released")
	}
	require.Equal(t, "second", mustWorkflowResult(t, second))
}

func TestDirectChildKeepsParentWorkerConcurrencyUntilParentReturns(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{
		dropDB:                 true,
		checkLeaks:             true,
		maxConcurrentWorkflows: 2,
	})

	childStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	releaseChild := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseChild) }) })
	child := func(_ Context, input string) (string, error) {
		close(childStarted)
		<-releaseChild
		return input, nil
	}
	parent := func(ctx Context, input string) (string, error) {
		if input == "second" {
			close(secondStarted)
			return input, nil
		}
		handle, err := RunWorkflow(ctx, child, input)
		if err != nil {
			return "", err
		}
		return handle.GetResult()
	}
	RegisterWorkflow(dbosCtx, child)
	RegisterWorkflow(dbosCtx, parent)
	queue, err := registerWFQ(dbosCtx, "admission-child-worker-concurrency",
		WithWorkerConcurrency(1),
		WithQueueBasePollingInterval(10*time.Millisecond))
	require.NoError(t, err)
	require.NoError(t, Launch(dbosCtx))

	first, err := RunWorkflow(dbosCtx, parent, "first", WithQueue(queue))
	require.NoError(t, err)
	select {
	case <-childStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("direct child did not start")
	}
	second, err := RunWorkflow(dbosCtx, parent, "second", WithQueue(queue))
	require.NoError(t, err)
	select {
	case <-secondStarted:
		t.Fatal("awaiting a direct child released the parent's WorkerConcurrency slot")
	case <-time.After(100 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(releaseChild) })
	require.Equal(t, "first", mustWorkflowResult(t, first))
	select {
	case <-secondStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("second queued root did not start after its predecessor returned")
	}
	require.Equal(t, "second", mustWorkflowResult(t, second))
}

func TestUnawaitedDirectChildDoesNotRetainParentWorkerConcurrency(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	childStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	releaseChild := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseChild) }) })
	child := func(_ Context, input string) (string, error) {
		close(childStarted)
		<-releaseChild
		return input, nil
	}
	parent := func(ctx Context, input string) (string, error) {
		if input == "second" {
			close(secondStarted)
			return input, nil
		}
		_, err := RunWorkflow(ctx, child, input)
		return input, err
	}
	RegisterWorkflow(dbosCtx, child)
	RegisterWorkflow(dbosCtx, parent)
	queue, err := registerWFQ(dbosCtx, "admission-unawaited-child",
		WithWorkerConcurrency(1),
		WithQueueBasePollingInterval(10*time.Millisecond))
	require.NoError(t, err)
	require.NoError(t, Launch(dbosCtx))

	first, err := RunWorkflow(dbosCtx, parent, "first", WithQueue(queue))
	require.NoError(t, err)
	select {
	case <-childStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("unawaited direct child did not start")
	}
	require.Equal(t, "first", mustWorkflowResult(t, first))
	second, err := RunWorkflow(dbosCtx, parent, "second", WithQueue(queue))
	require.NoError(t, err)
	select {
	case <-secondStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("unawaited direct child retained its completed parent's queue slot")
	}
	require.Equal(t, "second", mustWorkflowResult(t, second))

	releaseOnce.Do(func() { close(releaseChild) })
}

func mustWorkflowResult[R any](t *testing.T, handle WorkflowHandle[R]) R {
	t.Helper()
	result, err := handle.GetResult(WithHandleTimeout(10 * time.Second))
	require.NoError(t, err)
	return result
}
