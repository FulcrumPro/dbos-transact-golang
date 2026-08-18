package dbos

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"
	"github.com/stretchr/testify/require"
)

func TestGarbageCollectionRowsThresholdUsesUUIDTieBreak(t *testing.T) {
	ctx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	workflow := func(Context, string) (string, error) { return "result", nil }
	RegisterWorkflow(ctx, workflow)
	require.NoError(t, Launch(ctx))
	defer Shutdown(ctx, 10*time.Second)

	const workflowCount = 4
	handles := make([]WorkflowHandle[string], workflowCount)
	for i := range handles {
		handle, err := RunWorkflow(ctx, workflow, "")
		require.NoError(t, err)
		handles[i] = handle
		_, err = handle.GetResult()
		require.NoError(t, err)
	}

	systemDB := ctx.(*dbosContext).systemDB.(*sysdb.SysDB)
	createdAt := int64(1_700_000_000_000)
	updateQuery := systemDB.RenderSQL(
		`UPDATE %sworkflow_status SET created_at = $1 WHERE workflow_uuid = $2`,
		systemDB.Dialect().SchemaPrefix(systemDB.Schema()),
	)
	workflowIDs := make([]string, len(handles))
	for i, handle := range handles {
		workflowIDs[i] = handle.GetWorkflowID()
		_, err := systemDB.Pool().Exec(context.Background(), updateQuery, createdAt, workflowIDs[i])
		require.NoError(t, err)
	}

	// Use the same timestamp for the explicit cutoff so the UUID boundary is
	// the only difference between the two deletion policies.
	threshold := 2
	require.NoError(t, systemDB.GarbageCollectWorkflows(ctx, sysdb.GarbageCollectWorkflowsInput{
		CutoffEpochTimestampMs: &createdAt,
		RowsThreshold:          &threshold,
	}))

	remaining, err := ListWorkflows(ctx)
	require.NoError(t, err)
	require.Len(t, remaining, threshold)

	sort.Strings(workflowIDs)
	expectedIDs := workflowIDs[len(workflowIDs)-threshold:]
	actualIDs := make([]string, len(remaining))
	for i, workflow := range remaining {
		actualIDs[i] = workflow.ID
	}
	require.ElementsMatch(t, expectedIDs, actualIDs)
}

func TestGarbageCollectionPreservesCompletedChildOfRunningParent(t *testing.T) {
	ctx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	queue, err := RegisterQueue(ctx, "gc-child-queue", WithWorkerConcurrency(1))
	require.NoError(t, err)

	child := func(Context, string) (string, error) { return "child-result", nil }
	childID := make(chan string, 1)
	allowAwait := make(chan struct{})
	parent := func(workflowCtx Context, input string) (string, error) {
		handle, err := RunWorkflow(workflowCtx, child, input, WithQueue(queue))
		if err != nil {
			return "", err
		}
		childID <- handle.GetWorkflowID()
		<-allowAwait
		return handle.GetResult()
	}
	RegisterWorkflow(ctx, child)
	RegisterWorkflow(ctx, parent)
	require.NoError(t, Launch(ctx))
	defer Shutdown(ctx, 10*time.Second)

	parentHandle, err := RunWorkflow(ctx, parent, "input")
	require.NoError(t, err)
	queuedChildID := <-childID
	require.Eventually(t, func() bool {
		status, statusErr := RetrieveWorkflow[any](ctx, queuedChildID)
		if statusErr != nil {
			return false
		}
		workflowStatus, statusErr := status.GetStatus()
		return statusErr == nil && workflowStatus.Status == WorkflowStatusSuccess
	}, 5*time.Second, 10*time.Millisecond)

	systemDB := ctx.(*dbosContext).systemDB.(*sysdb.SysDB)
	query := systemDB.RenderSQL(
		`UPDATE %sworkflow_status SET created_at = $1 WHERE workflow_uuid = $2`,
		systemDB.Dialect().SchemaPrefix(systemDB.Schema()),
	)
	_, err = systemDB.Pool().Exec(context.Background(), query, int64(1), queuedChildID)
	require.NoError(t, err)

	cutoff := time.Now().UnixMilli()
	require.NoError(t, systemDB.GarbageCollectWorkflows(ctx, sysdb.GarbageCollectWorkflowsInput{
		CutoffEpochTimestampMs: &cutoff,
	}))
	_, err = RetrieveWorkflow[any](ctx, queuedChildID)
	require.NoError(t, err, "GC must preserve a completed child while its parent can still consume the result")

	close(allowAwait)
	result, err := parentHandle.GetResult()
	require.NoError(t, err)
	require.Equal(t, "child-result", result)
}

func TestPollingHandleFailsWhenWorkflowWasDeleted(t *testing.T) {
	ctx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	workflow := func(Context, string) (string, error) { return "result", nil }
	RegisterWorkflow(ctx, workflow)
	require.NoError(t, Launch(ctx))
	defer Shutdown(ctx, 10*time.Second)

	handle, err := RunWorkflow(ctx, workflow, "")
	require.NoError(t, err)
	_, err = handle.GetResult()
	require.NoError(t, err)
	pollingHandle, err := RetrieveWorkflow[string](ctx, handle.GetWorkflowID())
	require.NoError(t, err)
	require.NoError(t, DeleteWorkflows(ctx, []string{handle.GetWorkflowID()}))

	started := time.Now()
	_, err = pollingHandle.GetResult(WithHandleTimeout(time.Second))
	require.Error(t, err)
	require.Less(t, time.Since(started), 500*time.Millisecond)
}
