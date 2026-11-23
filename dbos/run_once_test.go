package dbos

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	runOnceWorkflowName  = "run-once-harness"
	ordinaryWorkflowName = "ordinary-harness"
	runOnceTopic         = "run-once-go"
	runOnceAppName       = "run-once-test"
	runOnceQueueName     = "run-once-queue"
)

// runOnceProbe is the body shared by the run-once harness workflow and its
// ordinary control. Each start of the body ("pop") increments a counter row
// with plain SQL outside any DBOS step, then blocks in Recv so the workflow
// stays PENDING mid-body.
type runOnceProbe struct {
	mu      sync.Mutex
	recvErr map[string]error
}

func newRunOnceProbe() *runOnceProbe {
	return &runOnceProbe{recvErr: map[string]error{}}
}

func (p *runOnceProbe) runOnceBody(ctx Context, in string) (string, error) {
	return p.body(ctx, in)
}

func (p *runOnceProbe) ordinaryBody(ctx Context, in string) (string, error) {
	return p.body(ctx, in)
}

func (p *runOnceProbe) body(ctx Context, _ string) (string, error) {
	id, err := GetWorkflowID(ctx)
	if err != nil {
		return "", err
	}
	if err := popRunOnceCounter(ctx, id); err != nil {
		return "", err
	}
	msg, err := Recv[string](ctx, runOnceTopic, time.Hour)
	p.mu.Lock()
	p.recvErr[id] = err
	p.mu.Unlock()
	if err != nil {
		return "", err
	}
	return msg, nil
}

func (p *runOnceProbe) recvError(id string) (error, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	err, ok := p.recvErr[id]
	return err, ok
}

func runOnceSysDB(ctx Client) *sysdb.SysDB {
	return ctx.(*dbosContext).systemDB.(*sysdb.SysDB)
}

func ensureRunOnceCounterTable(t *testing.T, ctx Client) {
	t.Helper()
	_, err := runOnceSysDB(ctx).Pool().Exec(context.Background(),
		`CREATE TABLE IF NOT EXISTS run_once_pops (workflow_id TEXT PRIMARY KEY, pops INTEGER NOT NULL)`)
	require.NoError(t, err)
}

func popRunOnceCounter(ctx Context, workflowID string) error {
	s := runOnceSysDB(ctx)
	q := s.Dialect().RewriteQuery(`INSERT INTO run_once_pops (workflow_id, pops) VALUES ($1, 1)
		ON CONFLICT (workflow_id) DO UPDATE SET pops = run_once_pops.pops + 1`)
	_, err := s.Pool().Exec(context.Background(), q, workflowID)
	return err
}

func runOncePops(t *testing.T, ctx Client, workflowID string) int {
	t.Helper()
	s := runOnceSysDB(ctx)
	var pops int
	err := s.Pool().QueryRow(context.Background(),
		s.Dialect().RewriteQuery(`SELECT pops FROM run_once_pops WHERE workflow_id = $1`), workflowID).Scan(&pops)
	if errors.Is(err, sysdb.ErrNoRows) {
		return 0
	}
	require.NoError(t, err)
	return pops
}

type runOnceRow struct {
	Status      WorkflowStatusType
	Attempts    int
	RunOnce     bool // read from the stored attributes, the marker's only home
	Attributes  map[string]any
	QueueName   *string
	CompletedAt *int64
	UpdatedAt   int64
	Error       *string
}

func readRunOnceRow(t *testing.T, ctx Client, workflowID string) runOnceRow {
	t.Helper()
	s := runOnceSysDB(ctx)
	var r runOnceRow
	var attributes *string
	err := s.Pool().QueryRow(context.Background(), s.RenderSQL(
		`SELECT status, recovery_attempts, attributes, queue_name, completed_at, updated_at, error FROM %sworkflow_status WHERE workflow_uuid = $1`,
		s.Dialect().SchemaPrefix(s.Schema())), workflowID).
		Scan(&r.Status, &r.Attempts, &attributes, &r.QueueName, &r.CompletedAt, &r.UpdatedAt, &r.Error)
	require.NoError(t, err, "reading workflow %s", workflowID)
	if attributes != nil {
		require.NoError(t, json.Unmarshal([]byte(*attributes), &r.Attributes))
		r.RunOnce = r.Attributes["dbos.run_once"] == true
	}
	return r
}

func countWorkflowRows(t *testing.T, ctx Client) int {
	t.Helper()
	s := runOnceSysDB(ctx)
	var n int
	require.NoError(t, s.Pool().QueryRow(context.Background(),
		s.RenderSQL(`SELECT COUNT(*) FROM %sworkflow_status`, s.Dialect().SchemaPrefix(s.Schema()))).Scan(&n))
	return n
}

func waitForPops(t *testing.T, ctx Client, workflowID string, want int) {
	t.Helper()
	require.Eventually(t, func() bool { return runOncePops(t, ctx, workflowID) >= want },
		20*time.Second, 20*time.Millisecond, "workflow %s never reached %d body starts", workflowID, want)
}

func waitForRow(t *testing.T, ctx Client, workflowID string, cond func(runOnceRow) bool, what string) runOnceRow {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		last := readRunOnceRow(t, ctx, workflowID)
		if cond(last) {
			return last
		}
		if time.Now().After(deadline) {
			require.FailNowf(t, "condition never satisfied", "workflow %s never reached %s; last row %+v", workflowID, what, last)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func isCancelledRow(r runOnceRow) bool { return r.Status == WorkflowStatusCancelled }

type runOnceRuntimeConfig struct {
	dbURL      string
	appVersion string
	executorID string
	adminPort  int
	maxRoots   int
	logger     *slog.Logger
}

// newRunOnceRuntime launches a runtime with the run-once harness and its
// ordinary control registered. Runtimes built from the same config share the
// system database, application, and version.
func newRunOnceRuntime(t *testing.T, cfg runOnceRuntimeConfig, probe *runOnceProbe) Context {
	t.Helper()
	c, err := NewContext(context.Background(), Config{
		DatabaseURL:            cfg.dbURL,
		AppName:                runOnceAppName,
		ApplicationVersion:     cfg.appVersion,
		ExecutorID:             cfg.executorID,
		AdminServer:            cfg.adminPort > 0,
		AdminServerHost:        "127.0.0.1",
		AdminServerPort:        cfg.adminPort,
		MaxConcurrentWorkflows: cfg.maxRoots,
		Logger:                 cfg.logger,
	})
	require.NoError(t, err)
	RegisterWorkflow(c, probe.runOnceBody, WithWorkflowName(runOnceWorkflowName), WithRunOnce())
	RegisterWorkflow(c, probe.ordinaryBody, WithWorkflowName(ordinaryWorkflowName))
	require.NoError(t, Launch(c))
	t.Cleanup(func() { Shutdown(c, 30*time.Second) })
	ensureRunOnceCounterTable(t, c)
	return c
}

// runOnceObserver opens a client on the shared system database, to read rows
// while no runtime is up.
func runOnceObserver(t *testing.T, dbURL string) Client {
	t.Helper()
	c, err := NewClient(context.Background(), ClientConfig{DatabaseURL: dbURL, AppName: runOnceAppName})
	require.NoError(t, err)
	t.Cleanup(func() { Shutdown(c, 10*time.Second) })
	return c
}

func registerRunOnceQueue(t *testing.T, ctx Context, opts ...QueueOption) Queue {
	t.Helper()
	q, err := RegisterQueue(ctx, runOnceQueueName, append([]QueueOption{WithQueueBasePollingInterval(50 * time.Millisecond)}, opts...)...)
	require.NoError(t, err)
	return q
}

func freeTCPPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return port
}

// startRunOncePair enqueues one run-once harness workflow (with the marker) and
// one ordinary control, and waits until both bodies are running.
func startRunOncePair(t *testing.T, ctx Context, q Queue, probe *runOnceProbe) (runOnceID, ordinaryID string) {
	t.Helper()
	runOnceID = "run-once-" + uuid.NewString()
	ordinaryID = "ordinary-" + uuid.NewString()
	_, err := RunWorkflow(ctx, probe.runOnceBody, "", WithQueue(q), WithWorkflowID(runOnceID))
	require.NoError(t, err)
	_, err = RunWorkflow(ctx, probe.ordinaryBody, "", WithQueue(q), WithWorkflowID(ordinaryID))
	require.NoError(t, err)
	waitForPops(t, ctx, runOnceID, 1)
	waitForPops(t, ctx, ordinaryID, 1)
	require.True(t, readRunOnceRow(t, ctx, runOnceID).RunOnce, "the marker must be stored on the run-once row")
	require.False(t, readRunOnceRow(t, ctx, ordinaryID).RunOnce, "the ordinary control must not carry the marker")
	return runOnceID, ordinaryID
}

func freshRunOnceDB(t *testing.T) string {
	t.Helper()
	dbURL := backendDatabaseURL(t)
	if !useSqliteBackend() {
		resetTestDatabase(t, dbURL)
	}
	return dbURL
}

// assertDeadRecovery checks the outcome of recovering a dead executor's two
// PENDING workflows: the run-once body is not started again and the row is
// CANCELLED; the ordinary control is re-run and completes.
func assertDeadRecovery(t *testing.T, survivor Context, runOnceID, ordinaryID string) {
	t.Helper()
	row := waitForRow(t, survivor, runOnceID, isCancelledRow, "CANCELLED")
	assert.Equal(t, 1, row.Attempts, "recovery cancels the row without a second start")
	assert.Nil(t, row.QueueName, "a refused run-once row leaves its queue")
	assert.NotNil(t, row.CompletedAt, "a refused run-once row is completed like a cancellation")

	waitForPops(t, survivor, ordinaryID, 2)
	require.NoError(t, Send(survivor, ordinaryID, "done", runOnceTopic))
	waitForRow(t, survivor, ordinaryID, func(r runOnceRow) bool { return r.Status == WorkflowStatusSuccess }, "SUCCESS")

	assert.Equal(t, 1, runOncePops(t, survivor, runOnceID), "the run-once body must start exactly once")
	// The first body may have checkpointed its Recv timeout before it stopped;
	// the cancellation adds nothing.
	steps, err := GetWorkflowSteps(survivor, runOnceID)
	require.NoError(t, err)
	assert.LessOrEqual(t, len(steps), 1)
	for _, st := range steps {
		assert.Equal(t, "DBOS.sleep", st.StepName)
	}
}

// assertLiveRecovery checks the outcome of recovering the executor that is still
// running both bodies: the run-once row is CANCELLED and the live body's next
// DBOS call fails with WorkflowCancelled; the ordinary control keeps running
// and completes.
func assertLiveRecovery(t *testing.T, live Context, probe *runOnceProbe, runOnceID, ordinaryID string) {
	t.Helper()
	waitForRow(t, live, runOnceID, isCancelledRow, "CANCELLED")
	// Wait for the control's re-dequeue before waking it, so its outcome write
	// finds the row PENDING again.
	waitForRow(t, live, ordinaryID, func(r runOnceRow) bool {
		return r.Status == WorkflowStatusPending && r.Attempts == 2
	}, "PENDING after its second dequeue")

	require.NoError(t, Send(live, runOnceID, "wake", runOnceTopic))
	require.NoError(t, Send(live, ordinaryID, "done", runOnceTopic))

	require.Eventually(t, func() bool { _, ok := probe.recvError(runOnceID); return ok },
		20*time.Second, 20*time.Millisecond, "the live run-once body never returned from Recv")
	recvErr, _ := probe.recvError(runOnceID)
	assert.ErrorIs(t, recvErr, ErrWorkflowCancelled, "the live body's next DBOS call must see the cancellation")

	waitForRow(t, live, ordinaryID, func(r runOnceRow) bool { return r.Status == WorkflowStatusSuccess }, "SUCCESS")
	assert.Equal(t, WorkflowStatusCancelled, readRunOnceRow(t, live, runOnceID).Status, "the live body must not overwrite the cancellation")
	assert.Equal(t, 1, runOncePops(t, live, runOnceID))
	assert.Equal(t, 1, runOncePops(t, live, ordinaryID), "the live control is not run a second time on its own executor")
}

func postAdminRecovery(t *testing.T, port int, executorIDs ...string) []string {
	t.Helper()
	body, err := json.Marshal(executorIDs)
	require.NoError(t, err)
	resp, err := http.Post(fmt.Sprintf("http://127.0.0.1:%d/dbos-workflow-recovery", port), "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var ids []string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&ids))
	return ids
}

func conductorRecovery(t *testing.T, ctx Context, executorIDs ...string) {
	t.Helper()
	mockServer := newMockWebSocketServer()
	t.Cleanup(mockServer.shutdown)
	cond, err := newConductor(ctx.(*dbosContext), conductorConfig{
		url:     mockServer.getURL(),
		apiKey:  "test-key",
		appName: runOnceAppName,
	})
	require.NoError(t, err)
	cond.pingInterval = 100 * time.Millisecond
	cond.pingTimeout = 200 * time.Millisecond
	cond.reconnectWait = 100 * time.Millisecond
	cond.launch()
	t.Cleanup(func() { cond.shutdown(2 * time.Second) })
	require.True(t, mockServer.waitForConnection(5*time.Second))

	req, err := json.Marshal(map[string]any{"type": "recovery", "request_id": "run-once-recovery", "executor_ids": executorIDs})
	require.NoError(t, err)
	require.NoError(t, mockServer.sendTextMessage(req))
	deadline := time.After(10 * time.Second)
	for {
		select {
		case raw := <-mockServer.messages:
			var resp recoveryConductorResponse
			if json.Unmarshal(raw, &resp) == nil && resp.Type == recoveryMessage {
				require.Nil(t, resp.ErrorMessage)
				require.True(t, resp.Success)
				return
			}
		case <-deadline:
			t.Fatal("timed out waiting for the conductor recovery response")
		}
	}
}

// TestRunOnceRecovery drives every recovery path that re-enqueues a PENDING
// workflow (launch, the admin endpoint, Conductor) against a run-once
// workflow and an ordinary control, for a dead executor and a live one.
func TestRunOnceRecovery(t *testing.T) {
	t.Run("LaunchRecoveryOfDeadExecutor", func(t *testing.T) {
		probe := newRunOnceProbe()
		cfg := runOnceRuntimeConfig{dbURL: freshRunOnceDB(t), appVersion: "run-once-" + uuid.NewString(), executorID: "exec-a"}
		a := newRunOnceRuntime(t, cfg, probe)
		q := registerRunOnceQueue(t, a)
		runOnceID, ordinaryID := startRunOncePair(t, a, q, probe)

		require.NoError(t, Shutdown(a, 30*time.Second))
		obs := runOnceObserver(t, cfg.dbURL)
		require.Equal(t, WorkflowStatusPending, readRunOnceRow(t, obs, runOnceID).Status, "an executor stopping mid-body leaves the row PENDING")
		require.Equal(t, WorkflowStatusPending, readRunOnceRow(t, obs, ordinaryID).Status)

		b := newRunOnceRuntime(t, cfg, probe) // same executor ID: launch recovery re-enqueues A's rows
		assertDeadRecovery(t, b, runOnceID, ordinaryID)
	})

	t.Run("AdminRecovery", func(t *testing.T) {
		t.Run("DeadExecutor", func(t *testing.T) {
			probe := newRunOnceProbe()
			cfg := runOnceRuntimeConfig{dbURL: freshRunOnceDB(t), appVersion: "run-once-" + uuid.NewString(), executorID: "exec-a"}
			a := newRunOnceRuntime(t, cfg, probe)
			q := registerRunOnceQueue(t, a)
			runOnceID, ordinaryID := startRunOncePair(t, a, q, probe)
			require.NoError(t, Shutdown(a, 30*time.Second))

			cfg.executorID = "exec-b"
			cfg.adminPort = freeTCPPort(t)
			b := newRunOnceRuntime(t, cfg, probe)
			recovered := postAdminRecovery(t, cfg.adminPort, "exec-a")
			assert.ElementsMatch(t, []string{runOnceID, ordinaryID}, recovered)
			assertDeadRecovery(t, b, runOnceID, ordinaryID)
		})
		t.Run("LiveExecutor", func(t *testing.T) {
			probe := newRunOnceProbe()
			cfg := runOnceRuntimeConfig{dbURL: freshRunOnceDB(t), appVersion: "run-once-" + uuid.NewString(), executorID: "exec-a", adminPort: freeTCPPort(t)}
			a := newRunOnceRuntime(t, cfg, probe)
			q := registerRunOnceQueue(t, a)
			runOnceID, ordinaryID := startRunOncePair(t, a, q, probe)

			recovered := postAdminRecovery(t, cfg.adminPort, "exec-a")
			assert.ElementsMatch(t, []string{runOnceID, ordinaryID}, recovered)
			assertLiveRecovery(t, a, probe, runOnceID, ordinaryID)
		})
	})

	t.Run("ConductorRecovery", func(t *testing.T) {
		t.Run("DeadExecutor", func(t *testing.T) {
			probe := newRunOnceProbe()
			cfg := runOnceRuntimeConfig{dbURL: freshRunOnceDB(t), appVersion: "run-once-" + uuid.NewString(), executorID: "exec-a"}
			a := newRunOnceRuntime(t, cfg, probe)
			q := registerRunOnceQueue(t, a)
			runOnceID, ordinaryID := startRunOncePair(t, a, q, probe)
			require.NoError(t, Shutdown(a, 30*time.Second))

			cfg.executorID = "exec-b"
			b := newRunOnceRuntime(t, cfg, probe)
			conductorRecovery(t, b, "exec-a")
			assertDeadRecovery(t, b, runOnceID, ordinaryID)
		})
		t.Run("LiveExecutor", func(t *testing.T) {
			probe := newRunOnceProbe()
			cfg := runOnceRuntimeConfig{dbURL: freshRunOnceDB(t), appVersion: "run-once-" + uuid.NewString(), executorID: "exec-a"}
			a := newRunOnceRuntime(t, cfg, probe)
			q := registerRunOnceQueue(t, a)
			runOnceID, ordinaryID := startRunOncePair(t, a, q, probe)

			conductorRecovery(t, a, "exec-a")
			assertLiveRecovery(t, a, probe, runOnceID, ordinaryID)
		})
	})

	// A second runtime with the same executor ID launches while the first is
	// still running both bodies: its launch recovery re-enqueues them.
	t.Run("OverlappingRuntimeSameExecutorID", func(t *testing.T) {
		probe := newRunOnceProbe()
		cfg := runOnceRuntimeConfig{dbURL: freshRunOnceDB(t), appVersion: "run-once-" + uuid.NewString(), executorID: "exec-a"}
		a := newRunOnceRuntime(t, cfg, probe)
		q := registerRunOnceQueue(t, a)
		runOnceID, ordinaryID := startRunOncePair(t, a, q, probe)

		b := newRunOnceRuntime(t, cfg, probe)
		row := waitForRow(t, b, runOnceID, isCancelledRow, "CANCELLED")
		assert.Equal(t, 1, row.Attempts)

		require.NoError(t, Send(b, runOnceID, "wake", runOnceTopic))
		require.Eventually(t, func() bool { _, ok := probe.recvError(runOnceID); return ok },
			20*time.Second, 20*time.Millisecond, "A's run-once body never returned from Recv")
		recvErr, _ := probe.recvError(runOnceID)
		assert.ErrorIs(t, recvErr, ErrWorkflowCancelled, "A's next DBOS call must see the cancellation")
		assert.Equal(t, 1, runOncePops(t, b, runOnceID), "neither runtime may start the run-once body again")

		// The control is recovered, not cancelled, and completes on whichever
		// execution consumes the message.
		waitForRow(t, b, ordinaryID, func(r runOnceRow) bool {
			return r.Status == WorkflowStatusPending && r.Attempts == 2
		}, "PENDING after its second dequeue")
		require.NoError(t, Send(b, ordinaryID, "done", runOnceTopic))
		waitForRow(t, b, ordinaryID, func(r runOnceRow) bool { return r.Status == WorkflowStatusSuccess }, "SUCCESS")
	})
}

// ackLossPool makes the first commit of a matching transaction land durably
// and then report a retryable error, as a lost commit acknowledgement does.
// Optional hooks run just before the real commit and after it.
type ackLossPool struct {
	sysdb.Pool
	mu       sync.Mutex
	match    func(query string, args []any) bool
	before   func()
	after    func()
	loseAck  bool
	fired    bool
	firedSig chan struct{}
}

func (p *ackLossPool) arm(match func(string, []any) bool, before, after func(), loseAck bool) <-chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.match, p.before, p.after, p.loseAck, p.fired = match, before, after, loseAck, false
	p.firedSig = make(chan struct{})
	return p.firedSig
}

func (p *ackLossPool) BeginTx(ctx context.Context, opts TxOptions) (Tx, error) {
	tx, err := p.Pool.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &ackLossTx{Tx: tx, p: p}, nil
}

func (p *ackLossPool) matches(query string, args []any) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return !p.fired && p.match != nil && p.match(query, args)
}

type ackLossTx struct {
	sysdb.Tx
	p       *ackLossPool
	matched bool
}

func (t *ackLossTx) Exec(ctx context.Context, q string, args ...any) (sysdb.Result, error) {
	t.matched = t.matched || t.p.matches(q, args)
	return t.Tx.Exec(ctx, q, args...)
}

func (t *ackLossTx) Query(ctx context.Context, q string, args ...any) (sysdb.Rows, error) {
	t.matched = t.matched || t.p.matches(q, args)
	return t.Tx.Query(ctx, q, args...)
}

func (t *ackLossTx) QueryRow(ctx context.Context, q string, args ...any) sysdb.Row {
	t.matched = t.matched || t.p.matches(q, args)
	return t.Tx.QueryRow(ctx, q, args...)
}

func (t *ackLossTx) Commit(ctx context.Context) error {
	if !t.matched {
		return t.Tx.Commit(ctx)
	}
	t.p.mu.Lock()
	fire := !t.p.fired
	t.p.fired = true
	before, after, loseAck, sig := t.p.before, t.p.after, t.p.loseAck, t.p.firedSig
	t.p.mu.Unlock()
	if !fire {
		return t.Tx.Commit(ctx)
	}
	defer close(sig)
	if before != nil {
		before()
	}
	if err := t.Tx.Commit(ctx); err != nil {
		return err
	}
	if after != nil {
		after()
	}
	if loseAck {
		return fmt.Errorf("injected lost commit acknowledgement: %w", pgx.ErrTxClosed)
	}
	return nil
}

// isDequeueStatusInsertFor matches the status insert of a dequeued start, whose
// last argument is the recovery_attempts increment (1); the enqueue's own
// insert passes 0.
func isDequeueStatusInsertFor(workflowID string) func(string, []any) bool {
	return func(q string, args []any) bool {
		return strings.Contains(q, "INSERT INTO") && strings.Contains(q, "workflow_status") &&
			strings.Contains(q, "ON CONFLICT (workflow_uuid)") && len(args) > 0 && args[0] == workflowID &&
			args[len(args)-1] == 1
	}
}

// TestRunOnceLostStartAck pins that a run-once workflow fails safe on its first
// run: when the dequeue's status commit lands but its acknowledgement is lost,
// the retried insert is a second start and cancels the workflow before the body
// runs. The ordinary control under the same fault starts its body once.
func TestRunOnceLostStartAck(t *testing.T) {
	ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
	probe := newRunOnceProbe()
	RegisterWorkflow(ctx, probe.runOnceBody, WithWorkflowName(runOnceWorkflowName), WithRunOnce())
	RegisterWorkflow(ctx, probe.ordinaryBody, WithWorkflowName(ordinaryWorkflowName))
	s := runOnceSysDB(ctx)
	pool := &ackLossPool{Pool: s.Pool()}
	s.SetPool(pool) // before Launch: nothing reads the pool concurrently with the swap
	require.NoError(t, Launch(ctx))
	ensureRunOnceCounterTable(t, ctx)
	q := registerRunOnceQueue(t, ctx)

	t.Run("RunOnce", func(t *testing.T) {
		id := "run-once-" + uuid.NewString()
		fired := pool.arm(isDequeueStatusInsertFor(id), nil, nil, true)
		_, err := RunWorkflow(ctx, probe.runOnceBody, "", WithQueue(q), WithWorkflowID(id))
		require.NoError(t, err)
		<-fired
		row := waitForRow(t, ctx, id, isCancelledRow, "CANCELLED")
		assert.Equal(t, 2, row.Attempts, "the retried status insert is the second start")
		assert.Equal(t, 0, runOncePops(t, ctx, id), "the body must never start")
		steps, err := GetWorkflowSteps(ctx, id)
		require.NoError(t, err)
		assert.Empty(t, steps)
	})

	t.Run("OrdinaryControl", func(t *testing.T) {
		id := "ordinary-" + uuid.NewString()
		fired := pool.arm(isDequeueStatusInsertFor(id), nil, nil, true)
		handle, err := RunWorkflow(ctx, probe.ordinaryBody, "", WithQueue(q), WithWorkflowID(id))
		require.NoError(t, err)
		<-fired
		waitForPops(t, ctx, id, 1)
		require.NoError(t, Send(ctx, id, "done", runOnceTopic))
		res, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "done", res)
		assert.Equal(t, 1, runOncePops(t, ctx, id))
		assert.Equal(t, 2, readRunOnceRow(t, ctx, id).Attempts)
	})
}

// TestRunOnceRepeatInsertCancels drives InsertWorkflowStatus directly: the
// second dequeue insert of a marked row refuses it, an unmarked row dequeued
// for a run-once registration fails, and an ordinary row is never refused.
func TestRunOnceRepeatInsertCancels(t *testing.T) {
	ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
	require.NoError(t, Launch(ctx))
	s := runOnceSysDB(ctx)
	bg := context.Background()

	enqueue := func(id string, runOnce bool) {
		tx, err := s.Pool().BeginTx(bg, TxOptions{})
		require.NoError(t, err)
		defer tx.Rollback(bg)
		_, err = s.InsertWorkflowStatus(bg, sysdb.InsertWorkflowStatusDBInput{Tx: tx, Status: WorkflowStatus{
			ID: id, Name: "w", Status: WorkflowStatusEnqueued, QueueName: "q", CreatedAt: time.Now(), RunOnce: runOnce,
			ExecutorID: "exec-recovery-test", ApplicationVersion: "v-recovery-test",
		}})
		require.NoError(t, err)
		require.NoError(t, tx.Commit(bg))
		// What DequeueWorkflows does before the dispatched start.
		_, err = s.Pool().Exec(bg, s.RenderSQL(`UPDATE %sworkflow_status SET status = $1 WHERE workflow_uuid = $2`,
			s.Dialect().SchemaPrefix(s.Schema())), WorkflowStatusPending, id)
		require.NoError(t, err)
	}
	dequeueInsert := func(id string, registrationRunOnce bool) *sysdb.InsertWorkflowResult {
		tx, err := s.Pool().BeginTx(bg, TxOptions{})
		require.NoError(t, err)
		defer tx.Rollback(bg)
		res, err := s.InsertWorkflowStatus(bg, sysdb.InsertWorkflowStatusDBInput{
			Tx: tx, IncrementAttempts: true, MaxRetries: 100,
			RegistrationRunOnce: registrationRunOnce, RunOnceMarkerMissingError: "marker missing",
			Status: WorkflowStatus{ID: id, Name: "w", Status: WorkflowStatusPending, CreatedAt: time.Now(), ExecutorID: "exec-recovery-test"},
		})
		require.NoError(t, err)
		require.NoError(t, tx.Commit(bg))
		return res
	}

	t.Run("SecondDequeueOfMarkedRow", func(t *testing.T) {
		id := uuid.NewString()
		enqueue(id, true)
		first := dequeueInsert(id, true)
		assert.False(t, first.RunOnceRefused, "the first dequeue starts the body")
		assert.Equal(t, WorkflowStatusPending, first.Status)
		second := dequeueInsert(id, true)
		assert.True(t, second.RunOnceRefused)
		assert.Equal(t, WorkflowStatusCancelled, second.Status)
		row := readRunOnceRow(t, ctx, id)
		assert.Equal(t, WorkflowStatusCancelled, row.Status)
		assert.Equal(t, 2, row.Attempts)
		assert.Nil(t, row.QueueName)
		assert.NotNil(t, row.CompletedAt)
	})
	t.Run("MarkedRowRefusedWhateverTheRegistration", func(t *testing.T) {
		id := uuid.NewString()
		enqueue(id, true)
		dequeueInsert(id, false)
		assert.True(t, dequeueInsert(id, false).RunOnceRefused)
		assert.Equal(t, WorkflowStatusCancelled, readRunOnceRow(t, ctx, id).Status)
	})
	t.Run("UnmarkedRowForRunOnceRegistration", func(t *testing.T) {
		id := uuid.NewString()
		enqueue(id, false)
		res := dequeueInsert(id, true)
		assert.True(t, res.RunOnceRefused)
		assert.Equal(t, WorkflowStatusError, res.Status)
		row := readRunOnceRow(t, ctx, id)
		assert.Equal(t, WorkflowStatusError, row.Status)
		require.NotNil(t, row.Error)
		assert.Equal(t, "marker missing", *row.Error)
	})
	t.Run("RecoveryCancelsOnlyStartedMarkedRows", func(t *testing.T) {
		started, unstarted, ordinary := uuid.NewString(), uuid.NewString(), uuid.NewString()
		enqueue(started, true)
		dequeueInsert(started, true)
		enqueue(unstarted, true) // PENDING with recovery_attempts 0: dequeued, never started
		enqueue(ordinary, false)
		dequeueInsert(ordinary, false)
		ids, err := s.ReenqueueForRecovery(bg, []string{"exec-recovery-test"}, "v-recovery-test", models.InternalQueueName)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{started, unstarted, ordinary}, ids)
		assert.Equal(t, WorkflowStatusCancelled, readRunOnceRow(t, ctx, started).Status)
		assert.Equal(t, WorkflowStatusEnqueued, readRunOnceRow(t, ctx, unstarted).Status)
		assert.Equal(t, WorkflowStatusEnqueued, readRunOnceRow(t, ctx, ordinary).Status)
	})
	t.Run("OrdinaryRowNeverRefused", func(t *testing.T) {
		id := uuid.NewString()
		enqueue(id, false)
		for range 3 {
			res := dequeueInsert(id, false)
			assert.False(t, res.RunOnceRefused)
			assert.Equal(t, WorkflowStatusPending, res.Status)
		}
	})
}

// TestRunOnceStartRules covers how a run-once workflow may be started: only by
// dequeueing a row that carries the marker.
func TestRunOnceStartRules(t *testing.T) {
	ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
	probe := newRunOnceProbe()
	RegisterWorkflow(ctx, probe.runOnceBody, WithWorkflowName(runOnceWorkflowName), WithRunOnce())
	RegisterWorkflow(ctx, probe.ordinaryBody, WithWorkflowName(ordinaryWorkflowName))
	require.NoError(t, Launch(ctx))
	ensureRunOnceCounterTable(t, ctx)
	q := registerRunOnceQueue(t, ctx)

	t.Run("DirectStartRefused", func(t *testing.T) {
		before := countWorkflowRows(t, ctx)
		_, err := RunWorkflow(ctx, probe.runOnceBody, "", WithWorkflowID("direct-"+uuid.NewString()))
		require.ErrorIs(t, err, ErrRunOnceWorkflow)
		assert.Equal(t, before, countWorkflowRows(t, ctx), "a refused start writes no row")
	})

	t.Run("DirectStartOfRunningWorkflowRefused", func(t *testing.T) {
		id := "run-once-" + uuid.NewString()
		_, err := RunWorkflow(ctx, probe.runOnceBody, "", WithQueue(q), WithWorkflowID(id))
		require.NoError(t, err)
		waitForPops(t, ctx, id, 1)
		_, err = RunWorkflow(ctx, probe.runOnceBody, "", WithWorkflowID(id))
		require.ErrorIs(t, err, ErrRunOnceWorkflow)
		require.NoError(t, Send(ctx, id, "done", runOnceTopic))
		waitForRow(t, ctx, id, func(r runOnceRow) bool { return r.Status == WorkflowStatusSuccess }, "SUCCESS")
		assert.Equal(t, 1, runOncePops(t, ctx, id))
	})

	t.Run("QueuedRunWorkflowMarksRunOnceRegistration", func(t *testing.T) {
		id := "auto-marked-" + uuid.NewString()
		_, err := RunWorkflow(ctx, probe.runOnceBody, "", WithQueue(q), WithWorkflowID(id))
		require.NoError(t, err)
		waitForPops(t, ctx, id, 1)
		assert.True(t, readRunOnceRow(t, ctx, id).RunOnce)
		require.NoError(t, Send(ctx, id, "done", runOnceTopic))
		waitForRow(t, ctx, id, func(r runOnceRow) bool { return r.Status == WorkflowStatusSuccess }, "SUCCESS")
	})

	t.Run("EnqueuedByNameWithoutMarker", func(t *testing.T) {
		id := "unmarked-" + uuid.NewString()
		handle, err := Enqueue[string](ctx, runOnceQueueName, runOnceWorkflowName, "", WithEnqueueWorkflowID(id))
		require.NoError(t, err)
		_, err = handle.GetResult()
		require.ErrorIs(t, err, ErrRunOnceWorkflow)
		assert.Contains(t, err.Error(), "run-once workflow enqueued without the run-once marker")
		row := readRunOnceRow(t, ctx, id)
		assert.Equal(t, WorkflowStatusError, row.Status)
		assert.False(t, row.RunOnce)
		assert.Equal(t, 0, runOncePops(t, ctx, id), "the body must not run")
	})

	t.Run("ClientEnqueueWithMarker", func(t *testing.T) {
		id := "client-" + uuid.NewString()
		handle, err := Enqueue[string](ctx, runOnceQueueName, runOnceWorkflowName, "", WithEnqueueWorkflowID(id), WithEnqueueRunOnce())
		require.NoError(t, err)
		waitForPops(t, ctx, id, 1)
		assert.True(t, readRunOnceRow(t, ctx, id).RunOnce)
		require.NoError(t, Send(ctx, id, "done", runOnceTopic))
		res, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "done", res)
		assert.Equal(t, 1, runOncePops(t, ctx, id))
	})
}

// TestRunOnceResumeAndForkRefused: Resume and Fork of a run-once workflow fail
// and write nothing, for single ids, mixed batches, calls from inside a
// workflow (on its transaction), and a workflow re-imported from an export.
func TestRunOnceResumeAndForkRefused(t *testing.T) {
	ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
	probe := newRunOnceProbe()
	RegisterWorkflow(ctx, probe.runOnceBody, WithWorkflowName(runOnceWorkflowName), WithRunOnce())
	RegisterWorkflow(ctx, probe.ordinaryBody, WithWorkflowName(ordinaryWorkflowName))
	inWorkflow := func(ctx Context, target string) (string, error) {
		_, rerr := ResumeWorkflow[string](ctx, target)
		_, ferr := ForkWorkflow[string](ctx, ForkWorkflowInput{OriginalWorkflowID: target})
		return fmt.Sprintf("resume=%v fork=%v", errors.Is(rerr, ErrRunOnceWorkflow), errors.Is(ferr, ErrRunOnceWorkflow)), nil
	}
	RegisterWorkflow(ctx, inWorkflow, WithWorkflowName("run-once-in-workflow-resume"))
	require.NoError(t, Launch(ctx))
	ensureRunOnceCounterTable(t, ctx)
	q := registerRunOnceQueue(t, ctx)

	start := func(prefix string, runOnce bool) string {
		id := prefix + uuid.NewString()
		var err error
		if runOnce {
			_, err = RunWorkflow(ctx, probe.runOnceBody, "", WithQueue(q), WithWorkflowID(id))
		} else {
			_, err = RunWorkflow(ctx, probe.ordinaryBody, "", WithQueue(q), WithWorkflowID(id))
		}
		require.NoError(t, err)
		waitForPops(t, ctx, id, 1)
		return id
	}
	cancel := func(id string) {
		require.NoError(t, CancelWorkflow(ctx, id))
		waitForRow(t, ctx, id, isCancelledRow, "CANCELLED")
	}

	cancelledRunOnce := start("run-once-cancelled-", true)
	cancel(cancelledRunOnce)
	pendingRunOnce := start("run-once-pending-", true)
	cancelledOrdinary := start("ordinary-cancelled-", false)
	cancel(cancelledOrdinary)

	snapshot := func() map[string]runOnceRow {
		return map[string]runOnceRow{
			cancelledRunOnce:  readRunOnceRow(t, ctx, cancelledRunOnce),
			pendingRunOnce:    readRunOnceRow(t, ctx, pendingRunOnce),
			cancelledOrdinary: readRunOnceRow(t, ctx, cancelledOrdinary),
		}
	}
	before := snapshot()
	rowsBefore := countWorkflowRows(t, ctx)
	require.Equal(t, WorkflowStatusPending, before[pendingRunOnce].Status)

	t.Run("Resume", func(t *testing.T) {
		_, err := ResumeWorkflow[string](ctx, cancelledRunOnce)
		require.ErrorIs(t, err, ErrRunOnceWorkflow)
		_, err = ResumeWorkflow[string](ctx, pendingRunOnce)
		require.ErrorIs(t, err, ErrRunOnceWorkflow)
		_, err = ResumeWorkflows[string](ctx, []string{cancelledOrdinary, cancelledRunOnce})
		require.ErrorIs(t, err, ErrRunOnceWorkflow)
		assert.Contains(t, err.Error(), "the whole batch")
		_, err = ResumeWorkflows[string](ctx, []string{cancelledRunOnce, cancelledOrdinary, pendingRunOnce})
		require.ErrorIs(t, err, ErrRunOnceWorkflow)
		assert.Contains(t, err.Error(), cancelledRunOnce, "the refusal names every run-once workflow")
		assert.Contains(t, err.Error(), pendingRunOnce)
		assert.NotContains(t, err.Error(), cancelledOrdinary)
		assert.Equal(t, before, snapshot(), "a refused resume writes nothing")
	})

	t.Run("Fork", func(t *testing.T) {
		_, err := ForkWorkflow[string](ctx, ForkWorkflowInput{OriginalWorkflowID: cancelledRunOnce})
		require.ErrorIs(t, err, ErrRunOnceWorkflow)
		_, err = ForkWorkflows[string](ctx, ForkWorkflowsInput{Workflows: []ForkWorkflowSpec{
			{OriginalWorkflowID: cancelledOrdinary}, {OriginalWorkflowID: pendingRunOnce}, {OriginalWorkflowID: cancelledRunOnce},
		}})
		require.ErrorIs(t, err, ErrRunOnceWorkflow)
		assert.Contains(t, err.Error(), pendingRunOnce, "the refusal names every run-once workflow")
		assert.Contains(t, err.Error(), cancelledRunOnce)
		assert.Equal(t, rowsBefore, countWorkflowRows(t, ctx), "a refused fork inserts no row")
		assert.Equal(t, before, snapshot())
	})

	t.Run("InsideWorkflow", func(t *testing.T) {
		handle, err := RunWorkflow(ctx, inWorkflow, cancelledRunOnce)
		require.NoError(t, err)
		res, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "resume=true fork=true", res)
		assert.Equal(t, rowsBefore+1, countWorkflowRows(t, ctx), "only the calling workflow's own row is new")
		assert.Equal(t, before, snapshot())
	})

	t.Run("NeverAdmitted", func(t *testing.T) {
		id := "run-once-unpolled-" + uuid.NewString()
		_, err := Enqueue[string](ctx, "run-once-queue-nobody-polls", runOnceWorkflowName, "", WithEnqueueWorkflowID(id), WithEnqueueRunOnce())
		require.NoError(t, err)
		require.Equal(t, 0, readRunOnceRow(t, ctx, id).Attempts)
		require.NoError(t, CancelWorkflow(ctx, id))
		_, err = ResumeWorkflow[string](ctx, id)
		require.ErrorIs(t, err, ErrRunOnceWorkflow)
		assert.Equal(t, WorkflowStatusCancelled, readRunOnceRow(t, ctx, id).Status)
	})

	t.Run("ControlOrdinaryResumes", func(t *testing.T) {
		handle, err := ResumeWorkflow[string](ctx, cancelledOrdinary)
		require.NoError(t, err)
		// Its first body is still live on this executor, so the resumed row is
		// dequeued back to PENDING and that body finishes it.
		waitForRow(t, ctx, cancelledOrdinary, func(r runOnceRow) bool { return r.Status == WorkflowStatusPending }, "PENDING")
		require.NoError(t, Send(ctx, cancelledOrdinary, "done", runOnceTopic))
		res, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "done", res)
	})

	t.Run("ExportImportKeepsMarker", func(t *testing.T) {
		id := start("run-once-export-", true)
		require.NoError(t, Send(ctx, id, "done", runOnceTopic))
		waitForRow(t, ctx, id, func(r runOnceRow) bool { return r.Status == WorkflowStatusSuccess }, "SUCCESS")

		statuses, err := ListWorkflows(ctx, WithFilterWorkflowIDs(id))
		require.NoError(t, err)
		require.Len(t, statuses, 1)
		assert.True(t, statuses[0].RunOnce, "ListWorkflows reports the marker")
		assert.NotContains(t, statuses[0].Attributes, "dbos.run_once", "the reserved key is not a user attribute")

		s := runOnceSysDB(ctx)
		exported, err := s.ExportWorkflow(ctx, id, false)
		require.NoError(t, err)
		wire, err := json.Marshal(exported) // the Conductor export/import wire form
		require.NoError(t, err)
		require.NoError(t, DeleteWorkflows(ctx, []string{id}))
		var imported []ExportedWorkflow
		require.NoError(t, json.Unmarshal(wire, &imported))
		require.NoError(t, s.ImportWorkflow(ctx, imported))

		assert.True(t, readRunOnceRow(t, ctx, id).RunOnce, "the marker survives export and import")
		_, err = ResumeWorkflow[string](ctx, id)
		require.ErrorIs(t, err, ErrRunOnceWorkflow)
		_, err = ForkWorkflow[string](ctx, ForkWorkflowInput{OriginalWorkflowID: id})
		require.ErrorIs(t, err, ErrRunOnceWorkflow)
	})
}

// TestRunOnceReservedAttribute: the run-once marker is the reserved attribute
// "dbos.run_once". It is merged into user attributes at insert, callers cannot
// set or clear it through any attributes API, and it survives a replacement.
func TestRunOnceReservedAttribute(t *testing.T) {
	ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
	probe := newRunOnceProbe()
	RegisterWorkflow(ctx, probe.runOnceBody, WithWorkflowName(runOnceWorkflowName), WithRunOnce())
	RegisterWorkflow(ctx, probe.ordinaryBody, WithWorkflowName(ordinaryWorkflowName))
	require.NoError(t, Launch(ctx))
	ensureRunOnceCounterTable(t, ctx)
	q := registerRunOnceQueue(t, ctx)

	t.Run("ForgedOrClearedMarkerRefused", func(t *testing.T) {
		before := countWorkflowRows(t, ctx)
		for _, value := range []any{true, false} {
			forged := map[string]any{"dbos.run_once": value}
			_, err := RunWorkflow(ctx, probe.ordinaryBody, "", WithQueue(q), WithWorkflowAttributes(forged))
			require.ErrorIs(t, err, ErrInvalidOption)
			_, err = Enqueue[string](ctx, runOnceQueueName, ordinaryWorkflowName, "", WithEnqueueAttributes(forged))
			require.ErrorIs(t, err, ErrInvalidOption)
		}
		assert.Equal(t, before, countWorkflowRows(t, ctx), "a refused start writes no row")
	})

	t.Run("MarkerMergedAndKept", func(t *testing.T) {
		id := "run-once-attrs-" + uuid.NewString()
		_, err := RunWorkflow(ctx, probe.runOnceBody, "", WithQueue(q), WithWorkflowID(id),
			WithWorkflowAttributes(map[string]any{"customer": "acme"}))
		require.NoError(t, err)
		waitForPops(t, ctx, id, 1)
		row := readRunOnceRow(t, ctx, id)
		assert.Equal(t, map[string]any{"customer": "acme", "dbos.run_once": true}, row.Attributes)

		require.ErrorIs(t, SetWorkflowAttributes(ctx, id, map[string]any{"dbos.run_once": false}), ErrInvalidOption)

		// Read-modify-write of the listed attributes.
		listed, err := ListWorkflows(ctx, WithFilterWorkflowIDs(id))
		require.NoError(t, err)
		require.Len(t, listed, 1)
		assert.True(t, listed[0].RunOnce)
		assert.Equal(t, map[string]any{"customer": "acme"}, listed[0].Attributes)
		listed[0].Attributes["note"] = "seen"
		require.NoError(t, SetWorkflowAttributes(ctx, id, listed[0].Attributes))
		assert.Equal(t, map[string]any{"customer": "acme", "note": "seen", "dbos.run_once": true}, readRunOnceRow(t, ctx, id).Attributes)

		require.NoError(t, SetWorkflowAttributes(ctx, id, map[string]any{"customer": "globex"}))
		assert.Equal(t, map[string]any{"customer": "globex", "dbos.run_once": true}, readRunOnceRow(t, ctx, id).Attributes)
		require.NoError(t, SetWorkflowAttributes(ctx, id, nil))
		assert.Equal(t, map[string]any{"dbos.run_once": true}, readRunOnceRow(t, ctx, id).Attributes)
		_, err = ResumeWorkflow[string](ctx, id)
		require.ErrorIs(t, err, ErrRunOnceWorkflow, "clearing user attributes keeps the workflow run-once")

		require.NoError(t, Send(ctx, id, "done", runOnceTopic))
		waitForRow(t, ctx, id, func(r runOnceRow) bool { return r.Status == WorkflowStatusSuccess }, "SUCCESS")
	})

	t.Run("OrdinaryAttributesUntouched", func(t *testing.T) {
		id := "ordinary-attrs-" + uuid.NewString()
		_, err := RunWorkflow(ctx, probe.ordinaryBody, "", WithQueue(q), WithWorkflowID(id),
			WithWorkflowAttributes(map[string]any{"customer": "acme"}))
		require.NoError(t, err)
		waitForPops(t, ctx, id, 1)
		assert.Equal(t, map[string]any{"customer": "acme"}, readRunOnceRow(t, ctx, id).Attributes)
		require.NoError(t, SetWorkflowAttributes(ctx, id, nil))
		assert.Nil(t, readRunOnceRow(t, ctx, id).Attributes)
		require.NoError(t, Send(ctx, id, "done", runOnceTopic))
		waitForRow(t, ctx, id, func(r runOnceRow) bool { return r.Status == WorkflowStatusSuccess }, "SUCCESS")
	})
}

// TestRunOnceRecoveryWithSaturatedPermits: a live run-once body holds the
// only queue or process permit. Recovery must cancel it without needing a
// permit, and the live body's next DBOS call sees the cancellation. A marked
// workflow that never started keeps waiting for a permit and then runs.
func TestRunOnceRecoveryWithSaturatedPermits(t *testing.T) {
	cases := []struct {
		name      string
		maxRoots  int
		queueOpts []QueueOption
	}{
		{name: "QueueWorkerConcurrencyOne", queueOpts: []QueueOption{WithWorkerConcurrency(1)}},
		{name: "ProcessCapFull", maxRoots: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			probe := newRunOnceProbe()
			cfg := runOnceRuntimeConfig{dbURL: freshRunOnceDB(t), appVersion: "run-once-" + uuid.NewString(),
				executorID: "exec-a", adminPort: freeTCPPort(t), maxRoots: tc.maxRoots}
			a := newRunOnceRuntime(t, cfg, probe)
			q := registerRunOnceQueue(t, a, tc.queueOpts...)

			liveID := "run-once-live-" + uuid.NewString()
			_, err := RunWorkflow(a, probe.runOnceBody, "", WithQueue(q), WithWorkflowID(liveID))
			require.NoError(t, err)
			waitForPops(t, a, liveID, 1)
			waitingID := "run-once-waiting-" + uuid.NewString()
			_, err = RunWorkflow(a, probe.runOnceBody, "", WithQueue(q), WithWorkflowID(waitingID))
			require.NoError(t, err)

			recovered := postAdminRecovery(t, cfg.adminPort, "exec-a")
			assert.Equal(t, []string{liveID}, recovered)
			row := waitForRow(t, a, liveID, isCancelledRow, "CANCELLED")
			assert.Equal(t, 1, row.Attempts)
			waiting := readRunOnceRow(t, a, waitingID)
			assert.Equal(t, WorkflowStatusEnqueued, waiting.Status, "a marked workflow that never started is not cancelled")
			assert.Equal(t, 0, waiting.Attempts)

			require.NoError(t, Send(a, liveID, "wake", runOnceTopic))
			require.Eventually(t, func() bool { _, ok := probe.recvError(liveID); return ok },
				20*time.Second, 20*time.Millisecond, "the live body never returned from Recv")
			recvErr, _ := probe.recvError(liveID)
			assert.ErrorIs(t, recvErr, ErrWorkflowCancelled)
			assert.Equal(t, 1, runOncePops(t, a, liveID))

			// The cancelled body released its permit: the waiting one now runs once.
			waitForPops(t, a, waitingID, 1)
			require.NoError(t, Send(a, waitingID, "done", runOnceTopic))
			waitForRow(t, a, waitingID, func(r runOnceRow) bool { return r.Status == WorkflowStatusSuccess }, "SUCCESS")
		})
	}
}

// TestRunOnceListingSurvivesReplay: a workflow's checkpointed ListWorkflows
// step keeps RunOnce when the step is replayed from its checkpoint.
func TestRunOnceListingSurvivesReplay(t *testing.T) {
	ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
	probe := newRunOnceProbe()
	RegisterWorkflow(ctx, probe.runOnceBody, WithWorkflowName(runOnceWorkflowName), WithRunOnce())
	lister := func(ctx Context, target string) (string, error) {
		statuses, err := ListWorkflows(ctx, WithFilterWorkflowIDs(target))
		if err != nil {
			return "", err
		}
		if len(statuses) != 1 {
			return "", fmt.Errorf("listed %d workflows", len(statuses))
		}
		return fmt.Sprintf("run_once=%v attributes=%v", statuses[0].RunOnce, statuses[0].Attributes), nil
	}
	RegisterWorkflow(ctx, lister, WithWorkflowName("run-once-lister"))
	require.NoError(t, Launch(ctx))
	ensureRunOnceCounterTable(t, ctx)
	q := registerRunOnceQueue(t, ctx)

	target := "run-once-listed-" + uuid.NewString()
	_, err := RunWorkflow(ctx, probe.runOnceBody, "", WithQueue(q), WithWorkflowID(target))
	require.NoError(t, err)
	waitForPops(t, ctx, target, 1)

	listing, err := RunWorkflow(ctx, lister, target)
	require.NoError(t, err)
	first, err := listing.GetResult()
	require.NoError(t, err)
	assert.Equal(t, "run_once=true attributes=map[]", first)

	// The fork starts after the listing step, so it replays that step's checkpoint.
	replay, err := ForkWorkflow[string](ctx, ForkWorkflowInput{OriginalWorkflowID: listing.GetWorkflowID(), StartStep: 1})
	require.NoError(t, err)
	replayed, err := replay.GetResult()
	require.NoError(t, err)
	assert.Equal(t, first, replayed)

	require.NoError(t, Send(ctx, target, "done", runOnceTopic))
	waitForRow(t, ctx, target, func(r runOnceRow) bool { return r.Status == WorkflowStatusSuccess }, "SUCCESS")
}

// TestSendFailureLogsMessageSize: a failed Send logs the message size, never
// the message itself.
func TestSendFailureLogsMessageSize(t *testing.T) {
	var buf safeBuffer
	ctx, err := NewContext(context.Background(), Config{
		DatabaseURL: backendDatabaseURL(t),
		AppName:     "send-log-test",
		Logger:      slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	require.NoError(t, err)
	t.Cleanup(func() { Shutdown(ctx, 10*time.Second) })
	require.NoError(t, Launch(ctx))
	s := runOnceSysDB(ctx)

	secret := "qbwc-ticket-s3cr3t-payload"
	err = s.Send(context.Background(), sysdb.WorkflowSendInput{
		DestinationID: "missing-" + uuid.NewString(), Message: &secret, Topic: "t", Serialization: "DBOS_JSON",
	})
	require.ErrorIs(t, err, ErrNonExistentWorkflow)
	logged := buf.String()
	assert.NotContains(t, logged, secret)
	assert.Contains(t, logged, fmt.Sprintf(`"message_bytes":%d`, len(secret)))

	buf.Reset()
	var typedNil *string
	err = s.Send(context.Background(), sysdb.WorkflowSendInput{
		DestinationID: "missing-" + uuid.NewString(), Message: typedNil, Topic: "t", Serialization: "DBOS_JSON",
	})
	require.Error(t, err, "a NULL message violates the column constraint")
	assert.Contains(t, buf.String(), `"message_bytes":0`)
}

type safeBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *safeBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *safeBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func (s *safeBuffer) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.b.Reset()
}

// TestRecvCancellation records what a Recv whose context is cancelled leaves
// behind: whether the message was consumed, and how a body finds a message
// whose consume committed although Recv returned an error.
func TestRecvCancellation(t *testing.T) {
	ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})

	type gate struct {
		cancel  chan context.CancelFunc
		proceed chan struct{}
		first   chan error
	}
	var gates sync.Map // workflow ID -> *gate
	newGate := func(id string) *gate {
		g := &gate{cancel: make(chan context.CancelFunc, 1), proceed: make(chan struct{}), first: make(chan error, 1)}
		gates.Store(id, g)
		return g
	}

	// cancelThenRecv: the first Recv runs on a cancellable child context; the
	// second runs on the workflow context once the test lets it proceed.
	cancelThenRecv := func(ctx Context, _ string) (string, error) {
		id, err := GetWorkflowID(ctx)
		if err != nil {
			return "", err
		}
		gv, _ := gates.Load(id)
		g := gv.(*gate)
		child, cancel := WithCancel(ctx)
		defer cancel()
		g.cancel <- cancel
		<-g.proceed
		_, err = Recv[string](child, "cmd", 30*time.Second)
		g.first <- err
		<-g.proceed
		return Recv[string](ctx, "cmd", 30*time.Second)
	}
	RegisterWorkflow(ctx, cancelThenRecv, WithWorkflowName("recv-cancel-then-recv"))

	// recoverFromSteps: when Recv fails, the body reads its own steps
	// (paginated, outputs loaded) to find a consume that committed anyway.
	var applied sync.Map // command -> *atomic.Int64
	recoverFromSteps := func(ctx Context, _ string) (string, error) {
		id, err := GetWorkflowID(ctx)
		if err != nil {
			return "", err
		}
		gv, _ := gates.Load(id)
		g := gv.(*gate)
		child, cancel := WithCancel(ctx)
		defer cancel()
		g.cancel <- cancel
		<-g.proceed
		cmd, err := Recv[string](child, "cmd", 30*time.Second)
		g.first <- err
		if err != nil {
			cmd = ""
			const pageSize = 2
			for offset := 0; cmd == ""; offset += pageSize {
				steps, serr := GetWorkflowSteps(ctx, id, WithStepsLoadOutput(true), WithStepsLimit(pageSize), WithStepsOffset(offset))
				if serr != nil {
					return "", serr
				}
				for _, st := range steps {
					if st.StepName == "DBOS.recv" && st.Error == nil && st.Output != nil {
						text, ok := st.Output.(string) // DBOS_JSON step outputs list as their JSON text
						if !ok {
							return "", fmt.Errorf("recv output listed as %T", st.Output)
						}
						if uerr := json.Unmarshal([]byte(text), &cmd); uerr != nil {
							return "", fmt.Errorf("decoding recv output %q: %w", text, uerr)
						}
					}
				}
				if len(steps) < pageSize {
					break
				}
			}
			if cmd == "" {
				return "", fmt.Errorf("no committed recv found after %w", err)
			}
		}
		n, _ := applied.LoadOrStore(cmd, new(atomic.Int64))
		n.(*atomic.Int64).Add(1)
		return cmd, nil
	}
	RegisterWorkflow(ctx, recoverFromSteps, WithWorkflowName("recv-recover-from-steps"))

	s := runOnceSysDB(ctx)
	pool := &ackLossPool{Pool: s.Pool()}
	s.SetPool(pool) // before Launch
	require.NoError(t, Launch(ctx))

	unconsumed := func(id string) int {
		var n int
		require.NoError(t, s.Pool().QueryRow(context.Background(), s.RenderSQL(
			`SELECT COUNT(*) FROM %snotifications WHERE destination_uuid = $1 AND consumed = false`,
			s.Dialect().SchemaPrefix(s.Schema())), id).Scan(&n))
		return n
	}
	isConsumeFor := func(id string) func(string, []any) bool {
		return func(q string, args []any) bool {
			return strings.Contains(q, "SET consumed = true") && len(args) > 0 && args[0] == id
		}
	}

	t.Run("CancelWhileWaiting", func(t *testing.T) {
		id := "recv-wait-" + uuid.NewString()
		g := newGate(id)
		handle, err := RunWorkflow(ctx, cancelThenRecv, "", WithWorkflowID(id))
		require.NoError(t, err)
		cancel := <-g.cancel
		g.proceed <- struct{}{}
		// The timeout checkpoint is recorded just before Recv starts waiting.
		require.Eventually(t, func() bool {
			steps, err := GetWorkflowSteps(ctx, id)
			return err == nil && len(steps) == 1 && steps[0].StepName == "DBOS.sleep"
		}, 10*time.Second, 10*time.Millisecond, "the first Recv never reached its wait")
		cancel()
		firstErr := <-g.first
		require.Error(t, firstErr)
		assert.ErrorIs(t, firstErr, context.Canceled)

		require.NoError(t, Send(ctx, id, "cmd-1", "cmd"))
		assert.Equal(t, 1, unconsumed(id), "a Recv cancelled while waiting consumes nothing")
		began := time.Now()
		g.proceed <- struct{}{}
		res, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "cmd-1", res)
		assert.Less(t, time.Since(began), 10*time.Second, "the next Recv takes the pending message without waiting out its timeout")
		assert.Equal(t, 0, unconsumed(id))
	})

	t.Run("CancelWhileConsuming", func(t *testing.T) {
		skipIfSqlite(t, "uses a Postgres row lock to hold the consume statement")
		skipIfCockroach(t, "reads lock waits from pg_stat_activity")
		id := "recv-consume-" + uuid.NewString()
		g := newGate(id)
		handle, err := RunWorkflow(ctx, cancelThenRecv, "", WithWorkflowID(id))
		require.NoError(t, err)
		cancel := <-g.cancel
		require.NoError(t, Send(ctx, id, "cmd-2", "cmd"))

		lockTx, err := s.Pool().BeginTx(context.Background(), TxOptions{})
		require.NoError(t, err)
		defer lockTx.Rollback(context.Background())
		_, err = lockTx.Exec(context.Background(), s.RenderSQL(
			`SELECT 1 FROM %snotifications WHERE destination_uuid = $1 FOR UPDATE`, s.Dialect().SchemaPrefix(s.Schema())), id)
		require.NoError(t, err)

		g.proceed <- struct{}{}
		require.Eventually(t, func() bool {
			var waiting int
			err := s.Pool().QueryRow(context.Background(),
				`SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock' AND query LIKE '%SET consumed = true%'`).Scan(&waiting)
			return err == nil && waiting > 0
		}, 10*time.Second, 10*time.Millisecond, "the consume statement never blocked on the row lock")
		cancel()
		firstErr := <-g.first
		require.Error(t, firstErr, "a Recv cancelled mid-consume returns an error")
		require.NoError(t, lockTx.Rollback(context.Background()))
		assert.Equal(t, 1, unconsumed(id), "the cancelled consume rolled back")

		g.proceed <- struct{}{}
		res, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "cmd-2", res, "the next Recv delivers the message")
		assert.Equal(t, 0, unconsumed(id))
	})

	t.Run("ConsumeCommitsDespiteCancel", func(t *testing.T) {
		id := "recv-commit-" + uuid.NewString()
		g := newGate(id)
		handle, err := RunWorkflow(ctx, recoverFromSteps, "", WithWorkflowID(id))
		require.NoError(t, err)
		cancel := <-g.cancel
		require.NoError(t, Send(ctx, id, "cmd-3", "cmd"))
		fired := pool.arm(isConsumeFor(id), cancel, nil, false) // cancel between the consume and its commit
		g.proceed <- struct{}{}
		<-fired
		firstErr := <-g.first
		assert.NoError(t, firstErr, "a consume that commits is returned even though the context was cancelled first")
		res, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "cmd-3", res)
		assert.Equal(t, 0, unconsumed(id))
	})

	t.Run("CommittedConsumeWithLostAck", func(t *testing.T) {
		id := "recv-lost-ack-" + uuid.NewString()
		g := newGate(id)
		handle, err := RunWorkflow(ctx, recoverFromSteps, "", WithWorkflowID(id))
		require.NoError(t, err)
		cancel := <-g.cancel
		require.NoError(t, Send(ctx, id, "cmd-4", "cmd"))
		fired := pool.arm(isConsumeFor(id), nil, cancel, true) // commit lands, child cancelled, ack lost
		g.proceed <- struct{}{}
		<-fired
		firstErr := <-g.first
		require.Error(t, firstErr, "Recv reports an error although its consume committed")
		res, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "cmd-4", res, "the body recovers the exact command from its own steps")
		n, ok := applied.Load("cmd-4")
		require.True(t, ok)
		assert.EqualValues(t, 1, n.(*atomic.Int64).Load(), "the command is applied once")
		assert.Equal(t, 0, unconsumed(id))
	})
}

// The models package builds the error; this pins its public surface.
func TestRunOnceWorkflowErrorRoundTrip(t *testing.T) {
	orig := models.NewRunOnceWorkflowError("wf-1", "it cannot be resumed")
	assert.Equal(t, "RunOnceWorkflow", orig.Code.String())
	assert.Equal(t, ErrorCodeRunOnceWorkflow, orig.Code)
	assert.ErrorIs(t, orig, ErrRunOnceWorkflow)

	got := roundTripError(t, orig)
	assert.ErrorIs(t, got, ErrRunOnceWorkflow)
	assert.Contains(t, got.Error(), "workflow wf-1 is run-once: it cannot be resumed")

	b, err := json.Marshal(orig)
	require.NoError(t, err)
	assert.JSONEq(t, `{"message":"workflow wf-1 is run-once: it cannot be resumed","code":"RunOnceWorkflow"}`, string(b))
	var decoded Error
	require.NoError(t, json.Unmarshal(b, &decoded))
	assert.Equal(t, ErrorCodeRunOnceWorkflow, decoded.Code)
}
