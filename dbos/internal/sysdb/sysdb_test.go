package sysdb

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
)

func TestNotificationLoopCompletionDoesNotRequireShutdownWaiter(t *testing.T) {
	s := &SysDB{
		dialect: SqliteDialect{},
		logger:  slog.New(slog.DiscardHandler),
	}
	var previousDone chan struct{}
	for launch := 0; launch < 2; launch++ {
		ctx, cancel := context.WithCancel(context.Background())
		s.Launch(ctx)
		s.notificationLoopMu.Lock()
		done := s.notificationLoopDone
		s.notificationLoopMu.Unlock()
		if done == previousDone {
			t.Fatal("notification completion channel was reused across launches")
		}
		previousDone = done
		cancel()

		select {
		case _, ok := <-done:
			if ok {
				t.Fatal("notification loop completion channel was sent to instead of closed")
			}
		case <-time.After(time.Second):
			t.Fatal("notification loop did not exit")
		}
	}
}

func TestStreamWakeChannelCleanupPreservesConcurrentReaders(t *testing.T) {
	s := &SysDB{streamNotifier: newNotifyRegistry(_DBOS_STREAMS_CHANNEL, true)}
	const readers = 32

	type subscription struct {
		ch      chan struct{}
		cleanup func()
	}
	subs := make([]subscription, readers)
	for i := range subs {
		subs[i].ch, subs[i].cleanup = s.StreamWakeChannel("workflow", "key")
	}

	var cleanupWG sync.WaitGroup
	for i := 0; i < readers; i += 2 {
		cleanupWG.Add(1)
		go func(cleanup func()) {
			defer cleanupWG.Done()
			cleanup()
		}(subs[i].cleanup)
	}
	cleanupWG.Wait()

	s.streamNotifier.notify("workflow::key")
	for i := 1; i < readers; i += 2 {
		select {
		case <-subs[i].ch:
		case <-time.After(time.Second):
			t.Fatalf("reader %d was unregistered by another reader's cleanup", i)
		}
		subs[i].cleanup()
	}
}

// fakeRows simulates a result set that is truncated mid-stream: it yields its
// rows, then Next() returns false with the error parked on Err() — exactly how
// pgx/database/sql surface a connection dropped during iteration.
type fakeRows struct {
	rows [][]any
	idx  int
	err  error
}

func (r *fakeRows) Next() bool {
	if r.idx < len(r.rows) {
		r.idx++
		return true
	}
	return false
}

func (r *fakeRows) Scan(dest ...any) error {
	for i, v := range r.rows[r.idx-1] {
		if v == nil {
			continue // leave dest at its zero value (NULL column)
		}
		reflect.ValueOf(dest[i]).Elem().Set(reflect.ValueOf(v))
	}
	return nil
}

func (r *fakeRows) Err() error   { return r.err }
func (r *fakeRows) Close() error { return nil }

type fakeQueryPool struct {
	rows         Rows
	rowsSequence []Rows
	queries      []string
	queryArgs    [][]any
}

func (p *fakeQueryPool) Query(ctx context.Context, q string, args ...any) (Rows, error) {
	p.queries = append(p.queries, q)
	p.queryArgs = append(p.queryArgs, args)
	if len(p.rowsSequence) > 0 {
		rows := p.rowsSequence[0]
		p.rowsSequence = p.rowsSequence[1:]
		return rows, nil
	}
	return p.rows, nil
}

func (p *fakeQueryPool) Exec(ctx context.Context, q string, args ...any) (Result, error) {
	return nil, errors.New("not implemented")
}

func (p *fakeQueryPool) QueryRow(ctx context.Context, q string, args ...any) Row {
	panic("not implemented")
}

func (p *fakeQueryPool) BeginTx(ctx context.Context, opts TxOptions) (Tx, error) {
	return nil, errors.New("not implemented")
}

func (p *fakeQueryPool) Ping(ctx context.Context) error { return nil }
func (p *fakeQueryPool) Close()                         {}

func newFakeSysDB(rows Rows) *SysDB {
	return &SysDB{
		pool:    &fakeQueryPool{rows: rows},
		dialect: PostgresDialect{},
		schema:  "dbos",
		logger:  slog.New(slog.DiscardHandler),
	}
}

// A truncated schedule list returned as success makes the scheduler reconciler
// remove every schedule missing from it, so mid-iteration errors must surface.
func TestListSchedulesSurfacesRowsErr(t *testing.T) {
	connErr := errors.New("simulated connection loss")
	rows := &fakeRows{
		rows: [][]any{{
			"schedule-id-1",             // schedule_id
			"sched-1",                   // schedule_name
			"wf",                        // workflow_name
			nil,                         // workflow_class_name
			"* * * * *",                 // schedule
			models.ScheduleStatusActive, // status
			"null",                      // context
			nil,                         // last_fired_at
			false,                       // automatic_backfill
			"UTC",                       // cron_timezone
			nil,                         // queue_name
		}},
		err: connErr,
	}

	schedules, err := newFakeSysDB(rows).ListSchedules(context.Background(), ListSchedulesDBInput{})
	if err == nil {
		t.Fatalf("ListSchedules returned truncated list of %d schedule(s) as success; want error", len(schedules))
	}
	if !errors.Is(err, connErr) {
		t.Fatalf("ListSchedules error = %v; want wrapped %v", err, connErr)
	}
}

func TestGetQueuePartitionsSurfacesRowsErr(t *testing.T) {
	connErr := errors.New("simulated connection loss")
	rows := &fakeRows{
		rows: [][]any{{"partition-1"}},
		err:  connErr,
	}

	partitions, err := newFakeSysDB(rows).GetQueuePartitions(context.Background(), GetQueuePartitionsInput{
		QueueName: "test-queue",
		Limit:     10,
	})
	if err == nil {
		t.Fatalf("GetQueuePartitions returned truncated list of %d partition(s) as success; want error", len(partitions))
	}
	if !errors.Is(err, connErr) {
		t.Fatalf("GetQueuePartitions error = %v; want wrapped %v", err, connErr)
	}
}

func TestGetQueuePartitionsUsesBoundedCursorPage(t *testing.T) {
	pool := &fakeQueryPool{rowsSequence: []Rows{
		&fakeRows{rows: [][]any{{"partition-d"}}},
		&fakeRows{rows: [][]any{{"partition-a"}}},
	}}
	db := &SysDB{
		pool:    pool,
		dialect: PostgresDialect{},
		schema:  "dbos",
		logger:  slog.New(slog.DiscardHandler),
	}
	after := "partition-c"

	partitions, err := db.GetQueuePartitions(context.Background(), GetQueuePartitionsInput{
		QueueName:         "test-queue",
		AfterPartitionKey: &after,
		Limit:             2,
	})
	if err != nil {
		t.Fatalf("GetQueuePartitions returned error: %v", err)
	}
	if !reflect.DeepEqual(partitions, []string{"partition-d", "partition-a"}) {
		t.Fatalf("GetQueuePartitions returned %v; want cursor page with wrap", partitions)
	}
	if len(pool.queries) != 2 {
		t.Fatalf("GetQueuePartitions issued %d queries; want one forward and one wrap query", len(pool.queries))
	}
	if !strings.Contains(pool.queries[0], "queue_partition_key > $3") || !strings.Contains(pool.queries[0], "LIMIT $4") {
		t.Fatalf("forward query is not keyset-bounded: %s", pool.queries[0])
	}
	if !strings.Contains(pool.queries[1], "queue_partition_key <= $3") || !strings.Contains(pool.queries[1], "LIMIT $4") {
		t.Fatalf("wrap query is not keyset-bounded: %s", pool.queries[1])
	}
	if got := pool.queryArgs[0][3]; got != 2 {
		t.Fatalf("forward query limit = %v; want 2", got)
	}
	if got := pool.queryArgs[1][3]; got != 1 {
		t.Fatalf("wrap query limit = %v; want remaining capacity 1", got)
	}
}

func TestGetQueuePartitionsRejectsUnboundedRequest(t *testing.T) {
	_, err := newFakeSysDB(&fakeRows{}).GetQueuePartitions(context.Background(), GetQueuePartitionsInput{
		QueueName: "test-queue",
	})
	if err == nil {
		t.Fatal("GetQueuePartitions accepted a non-positive limit")
	}
}

// context.DeadlineExceeded satisfies net.Error, so IsRetryable's trailing
// net.Error check used to classify it -- and anything wrapping it -- as a
// transient driver failure. DBOS builds its own timeout errors on top of that
// cause, so a Recv/GetEvent timeout would be retried forever by the infinite
// system-database retrier while the workflow context was still live.
func TestIsRetryableRejectsContextErrors(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"deadline", context.DeadlineExceeded},
		{"canceled", context.Canceled},
		{"wrapped deadline", models.NewTimeoutError("wf", "DBOS.recv", "no message received", context.DeadlineExceeded)},
		{"wrapped canceled", models.NewTimeoutError("wf", "", "interrupted", context.Canceled)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if (PostgresDialect{}).IsRetryable(tc.err, nil) {
				t.Fatalf("IsRetryable(%v) = true; want false", tc.err)
			}
		})
	}
}

func TestConnStringSetsPoolMaxConns(t *testing.T) {
	cases := []struct {
		connString string
		want       bool
	}{
		{"postgres://user:pass@localhost:5432/dbos?sslmode=disable&pool_max_conns=7", true},
		{"postgres://user:pass@localhost:5432/dbos?pool_max_conns=7", true},
		{"postgres://user:pass@localhost:5432/dbos?sslmode=disable", false},
		{"host=localhost port=5432 dbname=dbos pool_max_conns=7", true},
		{"host=localhost port=5432 dbname=dbos", false},
	}
	for _, c := range cases {
		if got := connStringSetsPoolMaxConns(c.connString); got != c.want {
			t.Errorf("connStringSetsPoolMaxConns(%q) = %v, want %v", c.connString, got, c.want)
		}
	}
}
