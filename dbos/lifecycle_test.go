package dbos

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"

	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"
)

func newLifecycleTestContext() *dbosContext {
	base, cancel := context.WithCancelCause(context.Background())
	return &dbosContext{
		ctx:               base,
		ctxCancelFunc:     cancel,
		lifecycle:         newRuntimeLifecycle(),
		workflowsWg:       &sync.WaitGroup{},
		workflowScheduler: cron.New(cron.WithSeconds()),
		launchDone:        make(chan struct{}),
		shutdownDone:      make(chan struct{}),
		logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestDrainTimeoutSharesCompletionWaiter(t *testing.T) {
	c := newLifecycleTestContext()
	start, err := c.beginWorkflow(true, false, false)
	require.NoError(t, err)

	done, first := c.lifecycle.requestDrain(false)
	require.True(t, first)
	secondDone, second := c.lifecycle.requestDrain(false)
	require.False(t, second)
	require.Equal(t, done, secondDone)
	go c.completeDrain(done)

	require.ErrorIs(t, Drain(c, 10*time.Millisecond), context.DeadlineExceeded)
	start.register()
	c.workflowsWg.Done()
	require.NoError(t, Drain(c, time.Second))
	select {
	case <-done:
	default:
		t.Fatal("drain completion waiter did not finish")
	}
}

func TestDrainWaitsForQueueClaimBeforeWorkflowCompletion(t *testing.T) {
	c := newLifecycleTestContext()
	claim := c.beginQueueClaim()
	require.NotNil(t, claim)

	done := c.startDrain(false)
	require.ErrorIs(t, Drain(c, 10*time.Millisecond), context.DeadlineExceeded)
	claim.done()
	require.NoError(t, Drain(c, time.Second))
	select {
	case <-done:
	default:
		t.Fatal("drain completion waiter did not finish")
	}
}

func TestDrainAllowsAdmittedChildButRejectsNewTopLevel(t *testing.T) {
	c := newLifecycleTestContext()
	require.NotNil(t, c.startDrain(false))

	_, err := c.beginWorkflow(true, false, false)
	require.ErrorIs(t, err, errDBOSDraining)
	child, err := c.beginWorkflow(true, true, false)
	require.NoError(t, err)
	child.register()
	c.workflowsWg.Done()
	require.NoError(t, Drain(c, time.Second))
}

func TestDrainReportsShutdownWhenShutdownWins(t *testing.T) {
	c := newLifecycleTestContext()
	start, err := c.beginWorkflow(true, false, false)
	require.NoError(t, err)

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- c.shutdownWithLaunchWait(10*time.Millisecond, true) }()
	select {
	case err := <-shutdownDone:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("shutdown did not return")
	}
	start.abort()
	require.ErrorIs(t, Drain(c, time.Second), errDBOSShutDown)
	require.False(t, errors.Is(c.lifecycle.drainError(), errDBOSDraining))
}

func TestDrainAllowsRunningWorkflowToStartDirectChild(t *testing.T) {
	cancelCtx, err := NewContext(context.Background(), Config{
		AppName:     "drain-child",
		DatabaseURL: "sqlite:" + t.TempDir() + "/dbos.db",
	})
	require.NoError(t, err)
	c := cancelCtx.(*dbosContext)

	parentStarted := make(chan struct{})
	releaseParent := make(chan struct{})
	child := func(_ Context, input string) (string, error) { return input, nil }
	parent := func(ctx Context, input string) (string, error) {
		close(parentStarted)
		<-releaseParent
		h, err := RunWorkflow(ctx, child, input)
		if err != nil {
			return "", err
		}
		return h.GetResult()
	}
	RegisterWorkflow(c, child)
	RegisterWorkflow(c, parent)
	require.NoError(t, Launch(c))

	drainResult := make(chan error, 1)
	h, err := RunWorkflow(c, parent, "child-result")
	require.NoError(t, err)
	<-parentStarted
	go func() { drainResult <- Drain(c, 5*time.Second) }()
	require.Eventually(t, c.lifecycle.isDraining, time.Second, time.Millisecond)

	_, err = RunWorkflow(c, child, "new-top-level")
	require.ErrorContains(t, err, errDBOSDraining.Error())
	close(releaseParent)
	result, err := h.GetResult()
	require.NoError(t, err)
	require.Equal(t, "child-result", result)
	require.NoError(t, <-drainResult)
	require.NoError(t, Shutdown(c, time.Second))
}

func TestDrainRejectsQueuedChildFromRunningWorkflow(t *testing.T) {
	c := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	queue, err := RegisterQueue(c, "drain-child-queue", WithQueueBasePollingInterval(10*time.Millisecond))
	require.NoError(t, err)

	parentStarted := make(chan struct{})
	startChild := make(chan struct{})
	child := func(Context, string) (string, error) { return "child", nil }
	parent := func(ctx Context, input string) (string, error) {
		close(parentStarted)
		<-startChild
		handle, err := RunWorkflow(ctx, child, input, WithQueue(queue))
		if err != nil {
			return "", err
		}
		return handle.GetResult()
	}
	RegisterWorkflow(c, child)
	RegisterWorkflow(c, parent)
	require.NoError(t, Launch(c))
	defer Shutdown(c, 10*time.Second)

	parentHandle, err := RunWorkflow(c, parent, "input")
	require.NoError(t, err)
	<-parentStarted
	drainResult := make(chan error, 1)
	go func() { drainResult <- Drain(c, 5*time.Second) }()
	require.Eventually(t, func() bool {
		return c.(*dbosContext).lifecycle.isDraining()
	}, time.Second, time.Millisecond)
	close(startChild)

	require.NoError(t, <-drainResult)
	_, err = parentHandle.GetResult()
	require.ErrorContains(t, err, errDBOSDraining.Error())
	status, err := parentHandle.GetStatus()
	require.NoError(t, err)
	require.Equal(t, WorkflowStatusPending, status.Status)
}

func TestDrainRejectsEnqueueFromRunningWorkflow(t *testing.T) {
	c := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	queue, err := RegisterQueue(c, "drain-enqueue-child-queue")
	require.NoError(t, err)

	parentStarted := make(chan struct{})
	startEnqueue := make(chan struct{})
	child := func(Context, string) (string, error) { return "child", nil }
	childName := resolveWorkflowFunctionName(child)
	parent := func(ctx Context, input string) (string, error) {
		close(parentStarted)
		<-startEnqueue
		handle, err := Enqueue[string](ctx, queue.GetName(), childName, input)
		if err != nil {
			return "", err
		}
		return handle.GetResult()
	}
	RegisterWorkflow(c, child)
	RegisterWorkflow(c, parent)
	require.NoError(t, Launch(c))
	defer Shutdown(c, 10*time.Second)

	parentHandle, err := RunWorkflow(c, parent, "input")
	require.NoError(t, err)
	<-parentStarted
	drainResult := make(chan error, 1)
	go func() { drainResult <- Drain(c, 5*time.Second) }()
	require.Eventually(t, func() bool {
		return c.(*dbosContext).lifecycle.isDraining()
	}, time.Second, time.Millisecond)
	close(startEnqueue)

	require.NoError(t, <-drainResult)
	_, err = parentHandle.GetResult()
	require.ErrorContains(t, err, errDBOSDraining.Error())
	status, err := parentHandle.GetStatus()
	require.NoError(t, err)
	require.Equal(t, WorkflowStatusPending, status.Status)
}

func TestShutdownInterruptsWorkflowAwaitingQueuedChild(t *testing.T) {
	ctx, err := NewContext(context.Background(), Config{
		AppName:     "shutdown-awaiting-child",
		DatabaseURL: "sqlite:" + t.TempDir() + "/dbos.db",
	})
	require.NoError(t, err)
	c := ctx.(*dbosContext)
	t.Cleanup(func() { _ = Shutdown(c, 2*time.Second) })

	queue, err := RegisterQueue(c, "shutdown-awaiting-child-queue")
	require.NoError(t, err)
	ListenQueues(c, "unrelated-queue")

	type enqueueResult struct {
		workflowID string
		err        error
	}
	childEnqueued := make(chan enqueueResult, 1)
	child := func(_ Context, input string) (string, error) { return input, nil }
	childName := resolveWorkflowFunctionName(child)
	parent := func(ctx Context, input string) (string, error) {
		handle, err := Enqueue[string](ctx, queue.GetName(), childName, input)
		if err != nil {
			childEnqueued <- enqueueResult{err: err}
			return "", err
		}
		childEnqueued <- enqueueResult{workflowID: handle.GetWorkflowID()}
		return handle.GetResult()
	}
	RegisterWorkflow(c, child)
	RegisterWorkflow(c, parent)
	require.NoError(t, Launch(c))

	parentHandle, err := RunWorkflow(c, parent, "child-result")
	require.NoError(t, err)
	enqueued := <-childEnqueued
	require.NoError(t, enqueued.err)
	childHandle, err := RetrieveWorkflow[string](c, enqueued.workflowID)
	require.NoError(t, err)
	status, err := childHandle.GetStatus()
	require.NoError(t, err)
	require.Equal(t, WorkflowStatusEnqueued, status.Status)

	require.ErrorIs(t, Drain(c, 25*time.Millisecond), context.DeadlineExceeded)
	require.NoError(t, Shutdown(c, 2*time.Second))
	_, err = parentHandle.GetResult()
	require.ErrorIs(t, err, context.Canceled)
}

func TestDrainRejectsDerivedContext(t *testing.T) {
	c := newLifecycleTestContext()
	err := Drain(WithValue(c, "key", "value"), time.Second)
	require.EqualError(t, err, "Drain requires the root DBOS context returned by NewContext")
}

func TestDrainFinishesClaimedQueueWorkAndStopsDequeue(t *testing.T) {
	c := setupDBOS(t, setupDBOSOptions{dropDB: true})
	started := make(chan struct{})
	release := make(chan struct{})
	var executions atomic.Int32
	workflow := func(_ Context, input string) (string, error) {
		executions.Add(1)
		if input == "running" {
			close(started)
			<-release
		}
		return input, nil
	}
	RegisterWorkflow(c, workflow)
	require.NoError(t, Launch(c))
	queue, err := RegisterQueue(c, "drain-queue", WithQueueBasePollingInterval(time.Millisecond))
	require.NoError(t, err)

	running, err := RunWorkflow(c, workflow, "running", WithQueue(queue))
	require.NoError(t, err)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("queued workflow did not start")
	}

	drainResult := make(chan error, 1)
	go func() { drainResult <- Drain(c, 5*time.Second) }()
	select {
	case err := <-drainResult:
		t.Fatalf("Drain returned before claimed workflow finished: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	result, err := running.GetResult()
	require.NoError(t, err)
	require.Equal(t, "running", result)
	require.NoError(t, <-drainResult)

	waiting, err := RunWorkflow(c, workflow, "waiting", WithQueue(queue))
	require.NoError(t, err)
	time.Sleep(25 * time.Millisecond)
	status, err := waiting.GetStatus()
	require.NoError(t, err)
	require.Equal(t, WorkflowStatusEnqueued, status.Status)
	require.EqualValues(t, 1, executions.Load())
}

func TestDrainWaitsForUnawaitedGoEffect(t *testing.T) {
	cancelCtx, err := NewContext(context.Background(), Config{
		AppName:                "drain-go-effect",
		DatabaseURL:            "sqlite:" + t.TempDir() + "/dbos.db",
		MaxConcurrentWorkflows: 1,
	})
	require.NoError(t, err)
	c := cancelCtx.(*dbosContext)

	started := make(chan struct{})
	release := make(chan struct{})
	completed := make(chan struct{})
	workflow := func(ctx Context, _ string) (string, error) {
		_, err := Go(ctx, func(context.Context) (string, error) {
			close(started)
			<-release
			close(completed)
			return "effect", nil
		})
		return "coordinator", err
	}
	RegisterWorkflow(c, workflow)
	require.NoError(t, Launch(c))

	handle, err := RunWorkflow(c, workflow, "input")
	require.NoError(t, err)
	result, err := handle.GetResult()
	require.NoError(t, err)
	require.Equal(t, "coordinator", result)
	<-started

	require.ErrorIs(t, Drain(c, 25*time.Millisecond), context.DeadlineExceeded)
	select {
	case <-completed:
		t.Fatal("effect completed before it was released")
	default:
	}

	close(release)
	require.NoError(t, Drain(c, time.Second))
	<-completed
	require.NoError(t, Shutdown(c, time.Second))
}

type cancellationPollingDB struct {
	sysdb.SystemDatabase

	queuePolls    atomic.Int32
	schedulePolls atomic.Int32
	queueWorker   chan struct{}
	schedule      chan struct{}
	queueOnce     sync.Once
	scheduleOnce  sync.Once
}

func (d *cancellationPollingDB) observeQueue() {
	d.queuePolls.Add(1)
}

func (d *cancellationPollingDB) observeSchedule() {
	d.schedulePolls.Add(1)
}

func (d *cancellationPollingDB) ListQueues(context.Context, []string) ([]models.QueueConfig, error) {
	d.observeQueue()
	return nil, nil
}

func (d *cancellationPollingDB) TransitionDelayedWorkflows(context.Context) error {
	d.observeQueue()
	return nil
}

func (d *cancellationPollingDB) DequeueWorkflows(context.Context, sysdb.DequeueWorkflowsInput) ([]sysdb.DequeuedWorkflow, error) {
	d.observeQueue()
	d.queueOnce.Do(func() { close(d.queueWorker) })
	return nil, nil
}

func (d *cancellationPollingDB) ListSchedules(context.Context, sysdb.ListSchedulesDBInput) ([]models.WorkflowSchedule, error) {
	d.observeSchedule()
	d.scheduleOnce.Do(func() { close(d.schedule) })
	return nil, nil
}

func TestParentCancellationStopsQueueAndSchedulePollers(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	c := newLifecycleTestContext()
	c.ctx, c.ctxCancelFunc = context.WithCancelCause(parent)
	c.config = &Config{SchedulerPollingInterval: time.Hour}
	c.workflowRegistry = &sync.Map{}
	c.workflowCustomNametoFQN = &sync.Map{}
	c.queueRunner = newQueueRunner(c.logger)
	c.queueRunner.internalQueue.basePollingInterval = time.Millisecond
	pollDB := &cancellationPollingDB{
		queueWorker: make(chan struct{}),
		schedule:    make(chan struct{}),
	}
	c.systemDB = pollDB

	queueDone := make(chan struct{})
	go func() {
		c.queueRunner.run(c)
		close(queueDone)
	}()
	scheduleDone := make(chan struct{})
	c.scheduleReconcilerWg.Add(1)
	go func() {
		defer c.scheduleReconcilerWg.Done()
		c.runScheduleReconciler()
		close(scheduleDone)
	}()

	select {
	case <-pollDB.queueWorker:
	case <-time.After(time.Second):
		t.Fatal("queue worker did not poll")
	}
	select {
	case <-pollDB.schedule:
	case <-time.After(time.Second):
		t.Fatal("schedule reconciler did not poll")
	}

	cancelParent()
	select {
	case <-queueDone:
	case <-time.After(time.Second):
		t.Fatal("queue runner did not stop after parent cancellation")
	}
	select {
	case <-scheduleDone:
	case <-time.After(time.Second):
		t.Fatal("schedule reconciler did not stop after parent cancellation")
	}

	queuePolls := pollDB.queuePolls.Load()
	schedulePolls := pollDB.schedulePolls.Load()
	time.Sleep(25 * time.Millisecond)
	require.Equal(t, queuePolls, pollDB.queuePolls.Load())
	require.Equal(t, schedulePolls, pollDB.schedulePolls.Load())
}

type blockingVersionRegistrationDB struct {
	sysdb.SystemDatabase
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingVersionRegistrationDB) CreateApplicationVersion(ctx context.Context, _ string, _ *string) error {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return context.Cause(ctx)
}

func TestShutdownRacingLaunchCompletes(t *testing.T) {
	ctx, err := NewContext(context.Background(), Config{
		AppName:     "shutdown-launch-race",
		DatabaseURL: "sqlite:" + t.TempDir() + "/dbos.db",
	})
	require.NoError(t, err)
	c := ctx.(*dbosContext)
	blockingDB := &blockingVersionRegistrationDB{
		SystemDatabase: c.systemDB,
		entered:        make(chan struct{}),
		release:        make(chan struct{}),
	}
	c.systemDB = blockingDB

	launchResult := make(chan error, 1)
	go func() { launchResult <- Launch(c) }()
	<-blockingDB.entered

	shutdownResult := make(chan error, 1)
	go func() { shutdownResult <- Shutdown(c, 2*time.Second) }()
	require.Eventually(t, func() bool { return c.shutdownStarted.Load() }, time.Second, time.Millisecond)
	close(blockingDB.release)

	require.Error(t, <-launchResult)
	require.NoError(t, <-shutdownResult)
}

func TestLaunchClosesWorkflowRegistrationBeforeStartup(t *testing.T) {
	ctx, err := NewContext(context.Background(), Config{
		AppName:     "launch-registration-race",
		DatabaseURL: "sqlite:" + t.TempDir() + "/dbos.db",
	})
	require.NoError(t, err)
	c := ctx.(*dbosContext)

	blockingDB := &blockingVersionRegistrationDB{
		SystemDatabase: c.systemDB,
		entered:        make(chan struct{}),
		release:        make(chan struct{}),
	}
	c.systemDB = blockingDB

	RegisterWorkflow(c, simpleWorkflow)
	launchResult := make(chan error, 1)
	go func() { launchResult <- Launch(c) }()
	<-blockingDB.entered

	registrationResult := make(chan any, 1)
	go func() {
		defer func() { registrationResult <- recover() }()
		RegisterWorkflow(WithValue(c, "derived", true), simpleWorkflowError)
	}()

	recovered := <-registrationResult
	require.Equal(t, "Cannot register workflow after DBOS has launched", recovered)

	close(blockingDB.release)
	require.NoError(t, <-launchResult)
	require.NoError(t, Shutdown(c, time.Second))
}
